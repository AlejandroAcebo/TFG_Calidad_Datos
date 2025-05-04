import os

from pyspark.sql.functions import col, when, lit, concat, to_date, current_timestamp

os.environ["SPARK_VERSION"] = "3.5"
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult

from dash import dash_table, dash, dcc, html
import dash
import plotly.express as px
from dash import html
import pandas as pd
from pydeequ.analyzers import *
import datetime as dt
from pyspark.sql.functions import col, to_timestamp
from datetime import datetime

# Configurar la versión de Spark (opcional si está bien instalado)

class Analisis:
    def __init__(self):

        # Crear la sesión de Spark con el driver JDBC
        self.spark = (SparkSession.builder
                      .appName("Azure SQL Connection with PySpark")
                      .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.7-spark-3.5")
                      .config("spark.jars", "/home/x/drivers/mssql-jdbc-12.10.0.jre8.jar")  # Ruta al driver de conexion
                      .getOrCreate())

        # Reducir logs a solo errores
        self.spark.sparkContext.setLogLevel("WARN")

        # Configurar la URL de conexión a Azure SQL Database
        server = "localhost"  # Esto luego lo puedo cambiar si lanzo en otro servidor de momemto solo lanzo en local
        database = "AdventureWorksLT2022"
        url = f"jdbc:sqlserver://{server}:1433;databaseName={database};encrypt=false;trustServerCertificate=true;"

        # Propiedades de conexión
        properties = {
            "user": "sa",
            "password": "root",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }

        # Prueba la conexión cargando una tabla (asegúrate de tener una en la BD)
        self.df_address = self.spark.read.jdbc(url=url, table="SalesLT.Address", properties=properties)
        self.df_customer = self.spark.read.jdbc(url=url, table="SalesLT.Customer", properties=properties)
        self.df_customer_address = self.spark.read.jdbc(url=url, table="SalesLT.CustomerAddress", properties=properties)
        self.df_product = self.spark.read.jdbc(url=url, table="SalesLT.Product", properties=properties)
        self.df_salesOrderDetail = self.spark.read.jdbc(url=url, table="SalesLT.SalesOrderDetail", properties=properties)
        self.df_productCategory = self.spark.read.jdbc(url=url, table="SalesLT.ProductCategory", properties=properties)

    def realizarAnalisis(self):
        ##################################################### ACTUALIDAD ###############################################
        # Definir fecha límite como datetime
        global analisis_resultado_dp, check_resultado_dp
        fecha_limite = dt.datetime(2006, 1, 1)

        # Convertir la columna "ModifiedDate" a tipo Timestamp si es necesario
        self.df_address = self.df_address.withColumn(
            "ModifiedDate", to_timestamp(col("ModifiedDate"), "yyyy-MM-dd HH:mm:ss")
        )
        self.df_customer = self.df_customer.withColumn(
            "ModifiedDate", to_timestamp(col("ModifiedDate"), "yyyy-MM-dd HH:mm:ss")
        )

        # Registrar los DataFrames como vistas temporales para usarlas en SQL
        self.df_address.createOrReplaceTempView("df_address_table")
        self.df_customer.createOrReplaceTempView("df_customer_table")

        # Usar SQL para obtener el total de registros y el número de registros válidos
        total_address = self.df_address.count()
        valid_address = self.spark.sql(f"""
            SELECT COUNT(*) AS valid_count
            FROM df_address_table
            WHERE ModifiedDate >= '{fecha_limite.strftime('%Y-%m-%d %H:%M:%S')}'
        """).collect()[0]["valid_count"]

        total_customer = self.df_customer.count()
        valid_customer = self.spark.sql(f"""
            SELECT COUNT(*) AS valid_count
            FROM df_customer_table
            WHERE ModifiedDate >= '{fecha_limite.strftime('%Y-%m-%d %H:%M:%S')}'
        """).collect()[0]["valid_count"]

        # Calcular el porcentaje de registros válidos
        porcentaje_address = valid_address / total_address if total_address > 0 else 0
        porcentaje_customer = valid_customer / total_customer if total_customer > 0 else 0

        # Definir el umbral (porcentaje mínimo requerido)
        umbral = 0.80  # 80% de registros válidos

        # Crear un check personalizado con la comparación directa de los porcentajes

        check_resultado_address = (
            Check(self.spark, CheckLevel.Warning, "validacion_address_fecha_modificacion")
            .hasSize(lambda x: (valid_address / total_address) >= umbral,
                     # Verificamos si el porcentaje de registros válidos cumple el umbral
                     f"El porcentaje de registros en Address con fecha superior a 2006 es: {porcentaje_address:.2f}")
        )

        check_resultado_customer = (
            Check(self.spark, CheckLevel.Warning, "validacion_customer_fecha_modificacion")
            .hasSize(lambda x: (valid_customer / total_customer) >= umbral,
                     # Verificamos si el porcentaje de registros válidos cumple el umbral
                     f"El porcentaje de registros en Customer con fecha superior a 2006 es: {porcentaje_customer:.2f}")
        )


        ################################################# COMPLETITUD ##################################################
        analisis_customer = (AnalysisRunner(self.spark)
                               .onData(self.df_customer)
                               .addAnalyzer(Completeness("Customer_ID"))
                               .addAnalyzer(Completeness("Title"))
                               .addAnalyzer(Completeness("MiddleName")))

        analisis_address = (AnalysisRunner(self.spark)
                                        .onData(self.df_address)
                                        .addAnalyzer(Completeness("AddressLine2")))

        analisis_product = (AnalysisRunner(self.spark)
                                        .onData(self.df_product)
                                        .addAnalyzer(Completeness("Size"))
                                        .addAnalyzer(Completeness("Weight")))

        ############################################ CONSISTENCIA ######################################################
        self.df_validacion_productos = (self.df_salesOrderDetail.join(self.df_product, on="ProductID", how="left")
                                        .withColumn("es_producto_correcto", col("ProductID").isNotNull()))

        # Se cuentan la cantidad de ventas con productos validos
        existen_productos = self.df_validacion_productos.filter(col("es_producto_correcto") == True).count()

        porcentaje_validos = existen_productos / self.df_salesOrderDetail.count()

        # Definicion del porcentaje personalizado
        check_resultado_product_sales = (Check(self.spark, CheckLevel.Error, "Comprobación ventas tienen productos válidos")
                                 .satisfies(f"{porcentaje_validos} >= 0.0"
                                            , "Porcentaje de ventas con productos que existen"))

        self.df_validacion_categoria = (self.df_product.join(self.df_productCategory, on="ProductCategoryID", how="left")
                                        .withColumn("es_categoria_correcta", col("ProductCategoryID").isNotNull()))

        # Se cuentan la cantidad de productos con categorias validas
        existen_categorias = self.df_validacion_categoria.filter(col("es_categoria_correcta") == True).count()


        porcentaje_validos = existen_categorias / self.df_product.count()

        # Definicion del porcentaje personalizado
        check_resultado_category_product = (
            Check(self.spark, CheckLevel.Error, "Comprobación productos que tienen categorías válidas")
            .satisfies(f"{porcentaje_validos} >= 0.0"
                       , "Porcentaje de productos con categoría válida"))

        ############################################## CREDIBILIDAD ####################################################

        # El telefono debe contener al menos 9 digitos y no puede haber letras pero si signos tipo parentesis, guiones...
        phone_pattern = r"^(?=(?:\D*\d){9,})[^\p{L}]*$"
        analisis_customer.addAnalyzer(PatternMatch("Phone", phone_pattern))

        # Comprueba que el Title de los clientes no tome valores distintos a Mr. o Ms.
        analisis_customer.addAnalyzer(Compliance("Title_valido", "Title IN ('Mr.', 'Ms.')"))

        # Los codigos postales pueden contener entre 5 y 7 caracteres debido a que puede tener espacios el cp
        cp_pattern = r"^[A-Za-z0-9 ]{5,7}$"
        analisis_address.addAnalyzer(PatternMatch("PostalCode", cp_pattern))

        productNumber_pattern = r"^[A-Za-z]{2}-[A-Za-z0-9]{4}-.*$"
        analisis_product.addAnalyzer(PatternMatch("ProductNumber", productNumber_pattern))

        ################################################ EXACTITUD #####################################################

        # Comprobar si las filas de la columna email cumplen el formato texto@texto.texto
        email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        analisis_customer.addAnalyzer(PatternMatch("EmailAddress", email_pattern))

        tipo_address = ["Main Office","Shipping"]
        # ESta condicion comprueba que todos los AddressType tengan un valor recogido en la lista
        condicion = f"AddressType IN ({', '.join([f'\'{x}\'' for x in tipo_address])})"
        analisis_customer_address = (AnalysisRunner(self.spark)
                                .onData(self.df_customer_address)
                                .addAnalyzer(Compliance("AddressType", condicion)))

        ############################################### PRECISION ######################################################
        # Comprobacion si las columnas tienen datos con 2 decimales o mas
        decimal_pattern = r'^\d+\.\d{2,}$'
        (analisis_product.addAnalyzer(PatternMatch("StandardCost", decimal_pattern))
                         .addAnalyzer(PatternMatch("ListPrice", decimal_pattern))
                         .addAnalyzer(PatternMatch("Weight", decimal_pattern)))

        ############################################# LANZADO ANALIZADORES #############################################
        analisis_resultado_customer = analisis_customer.run()
        analisis_resultado_address = analisis_address.run()
        analisis_resultado_product= analisis_product.run()
        analisis_resultado_customer_address = analisis_customer_address.run()

        ################################### CONSTRUCCION ANALIZADORES A DATAFRAME ######################################
        analisis_resultado_customer_df = self.procesar_dataframe(
            AnalyzerContext.successMetricsAsDataFrame(self.spark, analisis_resultado_customer))
        analisis_resultado_address_df = self.procesar_dataframe(
            AnalyzerContext.successMetricsAsDataFrame(self.spark, analisis_resultado_address))
        analisis_resultado_product_df = self.procesar_dataframe(
            AnalyzerContext.successMetricsAsDataFrame(self.spark, analisis_resultado_product))
        analisis_resultado_customer_address_df = self.procesar_dataframe(
            AnalyzerContext.successMetricsAsDataFrame(self.spark,analisis_resultado_customer_address))

        ################################################ LANZADO CHECKS ################################################

        # Ejecucion de los checks
        check_resultado_address = self.ejecutar_verificacion(self.df_address, check_resultado_address)
        check_resultado_customer = self.ejecutar_verificacion(self.df_customer, check_resultado_customer)
        check_resultado_product_sales = self.ejecutar_verificacion(self.df_validacion_productos,
                                                                   check_resultado_product_sales)
        check_resultado_category_product = self.ejecutar_verificacion(self.df_validacion_categoria,
                                                                 check_resultado_category_product)

        ####################################### CONSTRUCCION CHECKS A DATAFRAME ########################################

        # Crear los DataFrames de resultados de verificación
        check_resultado_address_df = self.procesar_dataframe(
            VerificationResult.successMetricsAsDataFrame(self.spark, check_resultado_address))
        check_resultado_customer_df = self.procesar_dataframe(
            VerificationResult.successMetricsAsDataFrame(self.spark, check_resultado_customer))
        check_resultado_product_sales_df = self.procesar_dataframe(
            VerificationResult.successMetricsAsDataFrame(self.spark, check_resultado_product_sales))
        check_resultado_category_product_df = self.procesar_dataframe(
            VerificationResult.successMetricsAsDataFrame(self.spark, check_resultado_category_product))



        ############################# LLAMADA PARA ESCRITURA EN BD O GENERACION UI #####################################
        # Si esta en true entonces genera la UI en Dash sino escribe en base de datos
        generacion_ui = False
        if not generacion_ui:
            ######################################### ESCRITURA EN BD ##################################################
            self.registro_basedatos_analisis("analisis",analisis_resultado_customer_df)
            self.registro_basedatos_analisis("analisis", analisis_resultado_address_df)
            self.registro_basedatos_analisis("analisis", analisis_resultado_product_df)

            # Escritura de checks
            self.registro_basedatos_analisis("check",check_resultado_address_df)
            self.registro_basedatos_analisis("check", check_resultado_customer_df)
            self.registro_basedatos_analisis("check",check_resultado_product_sales_df)
            self.registro_basedatos_analisis("check", check_resultado_category_product_df)

        else:
            ############################################# UNION ANALIZADORES ###########################################
            analisis_resultado_dp = pd.concat(
                 [analisis_resultado_customer_df.toPandas(),
                  analisis_resultado_address_df.toPandas(),
                  analisis_resultado_product_df.toPandas(),
                  analisis_resultado_customer_address_df.toPandas()],
                 ignore_index=True
            )

            ############################################## UNION CHECKS ################################################
            check_resultado_dp = pd.concat(
                [check_resultado_address_df.toPandas(),
                  check_resultado_customer_df.toPandas(),
                  check_resultado_product_sales_df.toPandas(),
                  check_resultado_category_product_df.toPandas()],
                 ignore_index=True
            )
            ################################################# LANZADO DE UI ############################################
            self.generar_ui_dash(check_resultado_dp, analisis_resultado_dp)


    # Correr los run de los checks
    def ejecutar_verificacion(self,df,check):
        return VerificationSuite(self.spark).onData(df).addCheck(check).run()

    # Metodo que procesa los dataframe, añade la columna porcentaje y el tiempo que se hizo el analisis.
    def procesar_dataframe(self,df):
        df = df.withColumn(
            "Porcentaje",
            concat((col("value") * 100).cast("int").cast("String"), lit("%"))
        )
        df = df.withColumn("fechaHora", current_timestamp())
        return df

    def registro_basedatos_analisis(self,tipo,registrar_bd):

        # CONEXIÓN
        server_register = "localhost"
        database_register = "RegisterAnalysis"
        url_register = (f"jdbc:sqlserver://{server_register}:1433;databaseName={database_register};encrypt=false;"
                        f"trustServerCertificate=true;")
        properties = {
            "user": "sa",
            "password": "root",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }

        # Escritura en la base de datos correspondiente
        try:
            global table_name
            if tipo == "check":
                table_name = "dbo.check_resultado"
            elif tipo == "analisis":
                table_name = "dbo.analisis_resultado"

            registrar_bd.write.jdbc(
                url=url_register,
                table=table_name,
                mode="append",
                properties=properties
            )
        except Exception as e:
            print(f"Se ha producido una excepcion:  {e} ")
        finally:
            print("Se ha escrito correctamente sobre la tabla : " + table_name)


    # Este metodo se usara si se quiere lanzar de forma web
    def generar_ui_dash(self, check_resultado_dp, analisis_resultado_dp):
        app = dash.Dash(__name__)

        # Umbral de porcentaje con valor predeterminado (esto puede cambiarse con el input)
        umbral_porcentaje = 80  # Inicialmente 80%

        # Convertir porcentajes de 'check_resultado_dp' a valores numéricos para los gráficos
        check_resultado_dp['Porcentaje_Numerico'] = check_resultado_dp['Porcentaje'].str.replace('%', '').astype(float)

        # Crear gráfico de barras
        fig = px.bar(
            check_resultado_dp,
            x='instance',
            y='Porcentaje_Numerico',  # Usamos la columna sin '%'
            title='Resultados de Checks',
            color='Porcentaje_Numerico',
            color_continuous_scale='reds',  # Para hacer un gradiente de colores según el valor
            barmode='group'
        )

        # Ajustes de visualización del gráfico de barras
        fig.update_layout(
            xaxis_title='',
            yaxis_title='Porcentaje',
            showlegend=False,
            plot_bgcolor='white',
        )

        # Convertir porcentajes de 'analisis_resultado_dp' a valores numéricos para los gráficos
        analisis_resultado_dp['Porcentaje_Numerico'] = analisis_resultado_dp['Porcentaje'].str.replace('%', '').astype(
            float)

        # Crear gráfico de líneas
        fig_line = px.line(
            analisis_resultado_dp,
            x='instance',
            y='Porcentaje_Numerico',  # Usamos la columna sin '%'
            title='Análisis de Resultados',
            markers=True
        )

        # Ajustes de visualización del gráfico de líneas
        fig_line.update_layout(
            xaxis_title='',
            yaxis_title='Porcentaje',
            yaxis=dict(
                range=[0, 100],  # Rango de 0 a 100 para el eje Y
            ),
            showlegend=False,
            plot_bgcolor='white',
        )

        # Layout de la aplicación Dash
        app.layout = html.Div([
            html.H1("Resultados de Análisis PyDeequ", style={
                'textAlign': 'center',
                'fontSize': '32px',
                'fontWeight': 'bold',
                'color': '#4CAF50',
                'marginBottom': '40px'
            }),

            # Tabla de "Análisis Resultados"
            html.Div([
                html.H3("Tabla 1: Resultados de Análisis", style={'textAlign': 'center', 'marginBottom': '20px'}),
                dash_table.DataTable(
                    id='table-analisis',
                    columns=[
                        {"name": col, "id": col} if col != "Porcentaje_Numerico" else {"name": "Porcentaje",
                                                                                       "id": "Porcentaje"}
                        for col in analisis_resultado_dp.columns if col != "Porcentaje_Numerico"
                        # Aquí eliminamos la columna "Porcentaje_Numerico"
                    ],
                    data=analisis_resultado_dp.to_dict('records'),
                    style_table={
                        'overflowY': 'auto',  # Permite desplazamiento solo si es necesario
                        'borderRadius': '10px',
                        'boxShadow': '0 4px 8px rgba(0, 0, 0, 0.1)',
                        'marginBottom': '40px'
                    },
                    style_header={
                        'backgroundColor': '#4CAF50',
                        'fontWeight': 'bold',
                        'color': 'white',
                        'textAlign': 'center',
                        'fontSize': '16px'
                    },
                    style_cell={
                        'textAlign': 'center',
                        'fontSize': '14px',
                        'padding': '12px',
                        'borderBottom': '1px solid #ddd'
                    },
                    style_data={
                        'backgroundColor': '#f9f9f9',
                        'color': '#333'
                    },
                )
            ]),

            # Gráfico de Líneas: Resultados de Análisis
            html.Div([
                html.H3("Gráfico de Líneas: Resultados de Análisis",
                        style={'textAlign': 'center', 'marginBottom': '20px'}),
                dcc.Graph(
                    id='line-graph',
                    figure=fig_line
                ),
            ]),

            # Tabla de "Check Resultados"
            html.Div([
                html.H3("Tabla 2: Resultados de Check", style={'textAlign': 'center', 'marginBottom': '20px'}),
                dash_table.DataTable(
                    id='table-check',
                    columns=[
                        {"name": col, "id": col} if col != "Porcentaje_Numerico" else {"name": "Porcentaje",
                                                                                       "id": "Porcentaje"}
                        for col in check_resultado_dp.columns if col != "Porcentaje_Numerico"
                        # Aquí eliminamos la columna "Porcentaje_Numerico"
                    ],
                    data=check_resultado_dp.to_dict('records'),
                    style_table={
                        'overflowY': 'auto',  # Permite desplazamiento solo si es necesario
                        'borderRadius': '10px',
                        'boxShadow': '0 4px 8px rgba(0, 0, 0, 0.1)',
                    },
                    style_header={
                        'backgroundColor': '#4CAF50',
                        'fontWeight': 'bold',
                        'color': 'white',
                        'textAlign': 'center',
                        'fontSize': '16px'
                    },
                    style_cell={
                        'textAlign': 'center',
                        'fontSize': '14px',
                        'padding': '12px',
                        'borderBottom': '1px solid #ddd'
                    },
                    style_data={
                        'backgroundColor': '#f9f9f9',
                        'color': '#333'
                    },
                )
            ]),

            # Gráfico de los Resultados de Check
            html.Div([
                html.H3("Gráfico de Resultados", style={'textAlign': 'center', 'marginBottom': '20px'}),
                dcc.Graph(
                    id='graph-checks',
                    figure=fig
                )
            ])
        ])

        app.run_server(debug=True)


if __name__ == "__main__":
    an = Analisis()
    an.realizarAnalisis()

