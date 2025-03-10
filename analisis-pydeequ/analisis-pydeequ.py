import os
os.environ["SPARK_VERSION"] = "3.5"

import dash
import dash_table
from dash import dcc, html
import pandas as pd
from pyspark.sql import SparkSession
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

        # Definir la URL de la base de datos
        url = "jdbc:postgresql://127.0.0.1:5432/postgres"

        # Propiedades de conexión
        properties = {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver"
        }

        # Tabla a leer
        table_name = "clientes"

        # Leer datos desde PostgreSQL
        self.df = self.spark.read.jdbc(url=url, table=table_name, properties=properties)

    # Metodo para mostrar datos (esto es para probar que la conexion funciona bien)
    def mostrar_datos(self):
        self.df.show(5)
        self.df.printSchema()

    # Metodo para comprobar la completitud de ciertas columnas
    def comprobarCompletitud(self):
        analisisResultado = (AnalysisRunner(self.spark)
                             .onData(self.df)
                             .addAnalyzer(Completeness("id_cliente"))
                             .addAnalyzer(Completeness("nombre"))
                             .addAnalyzer(Completeness("email"))
                             .addAnalyzer(Completeness("telefono")))

        # Comprobar si las filas de las columnas email y telefono cumplen con el formato que deben
        email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        telf_pattern = r"^\d{3,4}-\d{4}$"
        (analisisResultado.addAnalyzer(PatternMatch("email",email_pattern))
                            .addAnalyzer(PatternMatch("telefono",telf_pattern)))

        # Comprobar si el numero_secuencia de todas las filas es mayor que 1 porque por definicion menor que 1 es incoherente
        analisisResultado.addAnalyzer(Compliance("Todos >= 1","numero_secuencia >= 1"))

        # Comprobar si la columna DNI tiene valores repetidos
        analisisResultado.addAnalyzer(Uniqueness(["dni"]))

        resultados = analisisResultado.run()
        analisisResultado_df = AnalyzerContext.successMetricsAsDataFrame(self.spark,resultados)
        analisisResultado_pd = analisisResultado_df.toPandas()
        analisisResultado_df.show()

        self.generar_ui_dash(analisisResultado_pd)

    def generar_ui_dash(self, analisisResultado_pd):
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

            # Tabla con el estilo aplicado
            dash_table.DataTable(
                id='table',
                columns=[{"name": i, "id": i} for i in analisisResultado_pd.columns],
                data=analisisResultado_pd.to_dict('records'),
                style_table={
                    'height': '400px',
                    'overflowY': 'auto',
                    'borderRadius': '10px',
                    'boxShadow': '0 4px 8px rgba(0, 0, 0, 0.1)'
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
        ])
        app.run_server(debug=True)


if __name__ == "__main__":

    an = Analisis()
    an.mostrar_datos()
    an.comprobarCompletitud()
