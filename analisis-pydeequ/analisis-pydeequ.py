import datetime
import os

from pyspark.sql.functions import col, when, lit, concat

os.environ["SPARK_VERSION"] = "3.5"
from pydeequ import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult

from dash import dash_table, dash
from dash import html
import pandas as pd
from pydeequ.analyzers import *

# Configurar la versión de Spark (opcional si está bien instalado)

class Analisis:
    def __init__(self):

        # Crear la sesión de Spark con el driver JDBC
        self.spark = (SparkSession.builder
                      .appName("PostgreSQL Connection with PySpark")
                      .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.7-spark-3.5")
                      .config("spark.jars", "/home/x/Downloads/postgresql-42.7.5.jar")  # Ruta correcta al driver
                      .getOrCreate())

        # Esto es para que solo muestre errores y no todos los warnings, en caso de algun fallo se podria cambiar para
        # revisar los warnings
        self.spark.sparkContext.setLogLevel("ERROR")
        # Definir la URL de la base de datos
        url = "jdbc:postgresql://127.0.0.1:5432/postgres"

        # Propiedades de conexión
        properties = {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver"
        }

        # Leer datos desde PostgreSQL
        self.df_clientes = self.spark.read.jdbc(url=url, table="clientes", properties=properties)
        self.df_pedidos = self.spark.read.jdbc(url=url, table="pedidos", properties=properties)
        self.df_facturas = self.spark.read.jdbc(url=url, table="facturas", properties=properties)

    # Metodo para mostrar datos (esto es para probar que la conexion funciona bien)
    def mostrar_datos(self):
        self.df_clientes.show(5)
        self.df_pedidos.show(5)
        self.df_facturas.show(5)

    # Metodo para comprobar la calidad de ciertas columnas
    def comprobarCalidad(self):

        ########################################### ACTUALIDAD ###############################################
        fecha_limite = datetime.datetime.now() - datetime.timedelta(days=14)
        fecha_limite = fecha_limite.strftime('%Y-%m-%d %H:%M:%S')

        check_resultado_pedidos = (Check(self.spark, CheckLevel.Warning, "validacion_pedidos")
                                   .isLessThanOrEqualTo("ultima_actualizacion", fecha_limite,
                                       "La fecha de ultima_actualizacion no debe ser mayor a 14 días")
                                        .where("estado == 'pendiente' OR estado == 'procesando'"))

        check_resultado_facturas = Check(self.spark, CheckLevel.Warning, "validacion_facturas")
        fecha_limite = datetime.datetime.now() - datetime.timedelta(days=7)
        fecha_limite = fecha_limite.strftime('%Y-%m-%d %H:%M:%S')
        (check_resultado_facturas.isLessThanOrEqualTo("fecha_emision", fecha_limite,
                                       "La fecha de emision")
                                        .where("estado_pago == 'Pendiente'"))

        ########################################## COMPLETITUD ##################################################
        analisis_resultados = (AnalysisRunner(self.spark)
                               .onData(self.df_clientes)
                               .addAnalyzer(Completeness("id_cliente"))
                               .addAnalyzer(Completeness("nombre"))
                               .addAnalyzer(Completeness("email"))
                               .addAnalyzer(Completeness("telefono")))

        ########################################## CONSISTENCIA ####################################################

        analisis_resultados.addAnalyzer(Minimum("id_cliente",col("id_cliente") > 1))
        analisis_resultados.addAnalyzer(Completeness("id_cliente"))

        # Comprobar si la columna DNI tiene valores repetidos
        analisis_resultados.addAnalyzer(Uniqueness(["dni"]))

        df_validacion = self.df_pedidos.join(self.df_clientes, on="id_cliente", how="left") \
            .withColumn("existe", col("id_cliente").isNotNull())

        # Se cuentan la cantidad de clientes que existen en pedidos y luego estos clientes no existen
        existen = df_validacion.filter(col("existe") == True).count()

        porcentaje_validos = existen / self.df_pedidos.count()

        # Definicion del porcentaje personalizado
        check_cliente_pedidos = (Check(self.spark, CheckLevel.Error, "Comprobación pedidos tienen cliente válidos")
                                 .satisfies(f"{porcentaje_validos} >= 0.0"
                                            , "Porcentaje de clientes válidos que realizaron pedidos"))

        ####################################### CREDIBILIDAD ########################################################

        telf_pattern = r"^\d{9,}$"
        analisis_resultados.addAnalyzer(PatternMatch("telf", telf_pattern))

        # Comprobar si las filas de la columna email
        email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"

        (analisis_resultados.addAnalyzer(PatternMatch("email", email_pattern)))

        ################################################ EXACTITUD ###############################################
        # Comprobacion si todas las filas contienen en el campo tipo_cliente normal o vip
        posibles_tipos = ["normal", "vip"]
        check_contenido_tipo = (Check(self.spark, CheckLevel.Warning,
                                      "Verifica si hay alguna fila que tiene este valor distinto al posible")
                                .isContainedIn("tipo_cliente", posibles_tipos))


        ############################################# LANZADO ANALIZADOR ###########################################
        resultados = analisis_resultados.run()

        ############################################# EXTRAS #######################################################

        # Con analyzer no hay ningun metodo para esta comprobacion pero con checks podemos ver si hay filas en las que las
        # siguientes columnas sean un 90% iguales teniendo en cuenta que si el email es 90% parecido es el mismo
        #columnas_comprobar = ["nombre", "email", "telefono","cp","poblacion"]
        #check_similaridad_filas = (Check(self.spark, CheckLevel.Warning, "Verifica si hay filas duplicadas")
        # .hasDistinctness(columnas_comprobar, lambda v: v > 0.9,"Las filas deberían ser menos de un 90% iguales"))


        ################################################ LANZADO CHECKS ###########################################

        check_resultado_pedidos = (VerificationSuite(self.spark)
                              .onData(self.df_pedidos)
                              .addCheck(check_resultado_pedidos)
                              .run())

        check_resultado_facturas = (VerificationSuite(self.spark)
                                    .onData(self.df_facturas)
                                    .addCheck(check_resultado_facturas)
                                    .run())


        check_resultado_clientes = (VerificationSuite(self.spark).
                           onData(self.df_clientes)
                           .addCheck(check_contenido_tipo)
                           .run())

        check_resultado_clientes_pedidos = (VerificationSuite(self.spark).
                                    onData(df_validacion)
                                    .addCheck(check_cliente_pedidos)
                                    .run())

        ####################################### CONSTRUCCION CHECKS A DATAFRAME #####################################
        check_resultado_pedidos_df = (VerificationResult.successMetricsAsDataFrame(self.spark, check_resultado_pedidos))
        check_resultado_pedidos_df = check_resultado_pedidos_df.withColumn(
            "Porcentaje",
            concat((col("value") * 100).cast("int").cast("String"), lit("%"))
        )
        check_resultado_pedidos_df.show()
        check_resultado_facturas_df = (VerificationResult.successMetricsAsDataFrame(self.spark, check_resultado_facturas))
        check_resultado_facturas_df = check_resultado_facturas_df.withColumn(
            "Porcentaje",
            concat((col("value") * 100).cast("int").cast("String"), lit("%"))
        )
        check_resultado_clientes_df = (VerificationResult.successMetricsAsDataFrame(self.spark, check_resultado_clientes))
        check_resultado_clientes_df = check_resultado_clientes_df.withColumn(
            "Porcentaje",
            concat((col("value") * 100).cast("int").cast("String"), lit("%"))
        )
        check_resultado_clientes_pedidos_df = (VerificationResult
                                               .successMetricsAsDataFrame(self.spark,check_resultado_clientes_pedidos))
        check_resultado_clientes_pedidos_df = check_resultado_clientes_pedidos_df.withColumn(
            "Porcentaje",
            concat((col("value") * 100).cast("int").cast("String"), lit("%"))
        )
        check_resultado_facturas_df.show()

        ###################################### UNION CHECKS ###################################################

        check_resultado_pedidos_df.show()
        check_resultado_facturas_df.show()
        check_resultado_clientes_df.show()
        check_resultado_clientes_pedidos_df.show()

        check_resultado_dp = pd.concat(
            [check_resultado_pedidos_df.toPandas(),
             check_resultado_facturas_df.toPandas(),
             check_resultado_clientes_df.toPandas(),
             check_resultado_clientes_pedidos_df.toPandas()],
            ignore_index=True
        )

        ######################################## UNION ANALIZADORES ############################################
        analisis_resultado_clientes_df = AnalyzerContext.successMetricsAsDataFrame(self.spark,resultados)
        analisis_resultado_clientes_df = analisis_resultado_clientes_df.withColumn(
                        "Porcentaje",
                        concat((col("value") * 100).cast("int").cast("String"), lit("%"))
        )
        analisis_resultado_clientes_df.show()
        analisis_resultado_dp = analisis_resultado_clientes_df.toPandas()


        ################################# GENERACION UI CON CHECKS Y ANALIZADORES ################################
        self.generar_ui_dash(check_resultado_dp, analisis_resultado_dp)

    def generar_ui_dash(self, check_resultado_dp, analisis_resultado_dp):
        # Crear la app de Dash
        app = dash.Dash(__name__)

        # Layout con el estilo
        app.layout = html.Div([
            # Encabezado
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
                    columns=[{"name": col, "id": col} for col in analisis_resultado_dp.columns],
                    data=analisis_resultado_dp.to_dict('records'),
                    style_table={
                        'height': '400px',
                        'overflowY': 'auto',
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

            # Tabla de "Check Resultados"
            html.Div([
                html.H3("Tabla 2: Resultados de Check", style={'textAlign': 'center', 'marginBottom': '20px'}),
                dash_table.DataTable(
                    id='table-check',
                    columns=[{"name": col, "id": col} for col in check_resultado_dp.columns],
                    data=check_resultado_dp.to_dict('records'),
                    style_table={
                        'height': '400px',
                        'overflowY': 'auto',
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

        ])

        # Ejecutar la aplicación Dash
        app.run_server(debug=True)


if __name__ == "__main__":

    an = Analisis()
    an.mostrar_datos()
    an.comprobarCompletitud()
