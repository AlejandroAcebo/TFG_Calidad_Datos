import os
os.environ["SPARK_VERSION"] = "3.5"

from pyspark.sql.functions import col, when, lit, concat, to_date, current_timestamp
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
import base64
from dash import dash_table, dash, dcc, html, Output, Input, State
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
        self.database = "AdventureWorksLT2022"
        url = f"jdbc:sqlserver://{server}:1433;databaseName={self.database};encrypt=false;trustServerCertificate=true;"

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
        global analisis_resultado_dp, check_resultado_dp

        ##################################################### ACTUALIDAD ###############################################

        fecha_limite = "2006-01-01 00:00:00"

        # Crear el check para Address
        check_resultado_address = (
            Check(self.spark, CheckLevel.Warning, "Validación Address ModifiedDate")
            .satisfies(f"ModifiedDate >= TIMESTAMP('{fecha_limite}')", "Fecha válida address")
        )

        # Crear el check para Customer
        check_resultado_customer = (
            Check(self.spark, CheckLevel.Warning, "Validación Customer ModifiedDate")
            .satisfies(f"ModifiedDate >= TIMESTAMP('{fecha_limite}')", "Fecha válida customer")
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

        # Los codigos postales pueden contener entre 5 y 6 caracteres debido a que puede tener espacios el cp
        cp_pattern = r"^[A-Za-z0-9 ]{5,6}$"
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
        generacion_ui = True
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

    # Metodo que procesa los dataframe, añade la columna porcentaje, el tiempo y cambia formatos de columnas.
    def procesar_dataframe(self,df):
        df = df.withColumn(
            "Porcentaje",
            concat((col("value") * 100).cast("int").cast("String"), lit("%"))
        )
        df = df.withColumn("Fecha y hora de ejecución", current_timestamp())
        df = (
            df.withColumnRenamed("entity", "Tipo test")
            .withColumnRenamed("instance", "Nombre de indicador")
            .withColumnRenamed("name", "Tipo test PyDeequ")
            .withColumnRenamed("value", "Valor")
        )

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
        app = dash.Dash(__name__, suppress_callback_exceptions=True)

        # Layout de la UI en Dash
        app.layout = html.Div([
            html.H1("Resultados de Análisis PyDeequ", style={
                'textAlign': 'center', 'fontSize': '32px', 'fontWeight': 'bold',
                'color': '#4CAF50', 'marginBottom': '40px'
            }),

            html.Div([
                # Cargar JSON
                html.Div([
                    dcc.Upload(
                        id='upload-json',
                        children=html.Button('📂 Cargar JSON', style={
                            'backgroundColor': '#2196F3', 'color': 'white',
                            'border': 'none', 'padding': '10px 20px', 'fontSize': '16px',
                            'cursor': 'pointer', 'borderRadius': '8px'
                        }),
                        multiple=False
                    )
                ], style={'display': 'inline-block', 'width': '50%', 'textAlign': 'left'}),

                # Guardar JSON
                html.Div([
                    html.Button("💾 Guardar JSON", id="guardar-btn", n_clicks=0, style={
                        'backgroundColor': '#4CAF50', 'color': 'white',
                        'border': 'none', 'padding': '10px 20px', 'fontSize': '16px',
                        'cursor': 'pointer', 'borderRadius': '8px'
                    }),
                    dcc.Download(id="descarga-json")
                ], style={'display': 'inline-block', 'width': '50%', 'textAlign': 'right'})
            ], style={'marginBottom': '30px'}),

            # Tabla 1: Análisis Resultados
            html.Div([
                html.H3("Tabla 1: Resultados de Análisis", style={'textAlign': 'center'}),
                dash_table.DataTable(
                    id='table-analisis',
                    columns=[{"name": col, "id": col} for col in analisis_resultado_dp.columns if
                             col != 'Porcentaje_Numerico'],
                    data=analisis_resultado_dp.to_dict('records'),
                    style_table={'overflowY': 'auto', 'borderRadius': '10px',
                                 'boxShadow': '0 4px 8px rgba(0, 0, 0, 0.1)', 'marginBottom': '40px'},
                    style_header={'backgroundColor': '#4CAF50', 'fontWeight': 'bold', 'color': 'white',
                                  'textAlign': 'center'},
                    style_cell={'textAlign': 'center', 'padding': '10px', 'fontSize': '14px',
                                'borderBottom': '1px solid #ddd'},
                    style_data={'backgroundColor': '#f9f9f9'}
                )
            ]),

            # Tabla 2: Check Resultados
            html.Div([
                html.H3("Tabla 2: Resultados de Check", style={'textAlign': 'center'}),
                dash_table.DataTable(
                    id='table-check',
                    columns=[{"name": col, "id": col} for col in check_resultado_dp.columns if
                             col != 'Porcentaje_Numerico'],
                    data=check_resultado_dp.to_dict('records'),
                    style_table={'overflowY': 'auto', 'borderRadius': '10px',
                                 'boxShadow': '0 4px 8px rgba(0, 0, 0, 0.1)', 'marginBottom': '40px'},
                    style_header={'backgroundColor': '#4CAF50', 'fontWeight': 'bold', 'color': 'white',
                                  'textAlign': 'center'},
                    style_cell={'textAlign': 'center', 'padding': '10px', 'fontSize': '14px',
                                'borderBottom': '1px solid #ddd'},
                    style_data={'backgroundColor': '#f9f9f9'}
                )
            ])
        ], style={'padding': '20px', 'maxWidth': '1200px', 'margin': '0 auto'})

        registrar_callbacks(app, check_resultado_dp, analisis_resultado_dp, self.database)
        app.run_server(debug=True)

def registrar_callbacks(app, check_resultado_dp, analisis_resultado_dp, database):
    @app.callback(
        Output("descarga-json", "data"),
        Input("guardar-btn", "n_clicks"),
        prevent_initial_call=True
    )
    def generar_json_combinado(n_clicks):
        if n_clicks <= 0:
            return None

        fecha_actual = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        nombre_archivo = f"analisis_{database}_{fecha_actual}.json"

        analisis_dict = analisis_resultado_dp.drop(columns=["Porcentaje_Numerico"], errors='ignore').copy()
        check_dict = check_resultado_dp.drop(columns=["Porcentaje_Numerico"], errors='ignore').copy()

        for df in [analisis_dict, check_dict]:
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].astype(str)

        datos_combinados = {
            "analisis": analisis_dict.to_dict(orient="records"),
            "checks": check_dict.to_dict(orient="records")
        }

        return dict(content=json.dumps(datos_combinados, indent=4), filename=nombre_archivo)

    @app.callback(
        [Output('table-analisis', 'data'),
         Output('table-check', 'data')],
        Input('upload-json', 'contents'),
        prevent_initial_call=True
    )
    def cargar_json(contents):
        if contents is None:
            raise dash.exceptions.PreventUpdate

        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        json_data = json.loads(decoded.decode('utf-8'))

        df_analisis = pd.DataFrame(json_data.get("analisis", []))
        df_checks = pd.DataFrame(json_data.get("checks", []))

        return df_analisis.to_dict('records'), df_checks.to_dict('records')


if __name__ == "__main__":
    an = Analisis()
    an.realizarAnalisis()

