import os
os.environ["SPARK_VERSION"] = "3.5"

from Analizador_Calidad_Datos.Analisis_Generalizados.consistencia import analizar_consistencia
from Analizador_Calidad_Datos.Analisis_Generalizados.credibilidad import analizar_credibilidad
from Analizador_Calidad_Datos.Analisis_Generalizados.exactitud import analizar_exactitud
from Analizador_Calidad_Datos.Analisis_Generalizados.precision import analizar_precision

from pydeequ.verification import VerificationResult
from pydeequ.analyzers import AnalyzerContext
import streamlit as st
from pyspark.sql import SparkSession
from Analizador_Calidad_Datos.Analisis_Generalizados.completitud import analizar_completitud

def conectar_bd(user, password, server, database):
    spark = (SparkSession.builder
             .appName("Azure SQL Connection with PySpark")
             .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.7-spark-3.5")
             .config("spark.jars", "/home/x/drivers/mssql-jdbc-12.10.0.jre8.jar")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    url = f"jdbc:sqlserver://{server}:1433;databaseName={database};encrypt=false;trustServerCertificate=true;"

    properties = {
        "user": user,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    return spark, url, properties


def listar_schemas(spark, url, props):
    schemas_df = spark.read.jdbc(url, "INFORMATION_SCHEMA.SCHEMATA", properties=props)
    return schemas_df.select("SCHEMA_NAME").rdd.flatMap(lambda x: x).collect()


def listar_tablas(spark, url, props, schema):
    query = f"(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}') AS tablas"
    tablas_df = spark.read.jdbc(url, table=query, properties=props)
    return tablas_df.rdd.flatMap(lambda x: x).collect()


def listar_columnas(spark, url, props, tabla):
    df_leido = spark.read.jdbc(url=url, table=tabla, properties=props)

    return df_leido.columns


def ui():
    # Definicion de variables globales
    global patron, tabla_seleccionada_2, columna_2, tipo_exactitud, tipo_credibilidad, num_decimales

    # 1.Datos de conexión
    st.title("Analizador calidad de datos")
    st.sidebar.header("🔌 Conexión a Base de Datos")
    host = st.sidebar.text_input("Host", value="localhost")
    user = st.sidebar.text_input("Usuario")
    password = st.sidebar.text_input("Contraseña", type="password")
    database = st.sidebar.text_input("Base de Datos")
    st.warning("Actualmente solamente permite conexión con SQL Server")

    if st.sidebar.button("Conectar"):
        conn = conectar_bd(user, password, host, database)
        st.session_state["conn"] = conn
        st.success("Conectado con éxito")


    # 2.Selección de tabla y columna
    if "conn" in st.session_state:
        spark, url, properties = st.session_state["conn"]

        schemas = listar_schemas(spark, url, properties)
        schema_seleccionado = st.selectbox("Selecciona un esquema", schemas)
        print(schema_seleccionado)
        tablas = listar_tablas(spark, url, properties, schema_seleccionado)
        tabla_seleccionada = st.selectbox("Selecciona una tabla", tablas)
        print(tabla_seleccionada)
        columnas = listar_columnas(spark, url, properties, f"{schema_seleccionado}.{tabla_seleccionada}")
        columna = st.selectbox("Selecciona una columna", columnas)
        print(columna)
        tipo_analisis = st.selectbox("Selecciona el tipo de análisis", ["Completitud","Credibilidad","Consistencia"
            ,"Exactitud","Precision","No funciona"])

        match tipo_analisis:
            case "Credibilidad":
                tipos_credibilidad_opciones = ["Patron", "Conjunto valores"]
                tipo_credibilidad = st.selectbox("Selecciona el tipo", tipos_credibilidad_opciones)
                patron = st.text_input("Escribe el patrón a filtrar o posibles valores separados por comas")
                st.caption("Ejemplo patron: ^(?=(?:\D*\d){9,})[^\p{L}]*$")
                st.caption("Ejemplo posibles valores: Main Office,Shipping")

            case "Exactitud":
                tipos_exactitud_opciones = ["Sintactica","Semantica"]
                tipo_exactitud = st.selectbox("Selecciona el tipo",tipos_exactitud_opciones)
                patron = st.text_input("Escribe el patrón a filtrar o posibles valores separados por comas")
                st.caption("Ejemplo patron: ^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")
                st.caption("Ejemplo posibles valores: Main Office,Shipping")

            case"Precision":
                num_decimales = st.text_input("Introduce la cantidad de decimales que debe tener la columna,"
                                                  "solo número entero")

            case "Consistencia":
                tablas_opciones = [t for t in tablas if t != tabla_seleccionada]
                tabla_seleccionada_2 = st.selectbox("Selecciona segunda tabla", tablas_opciones)

                columnas_opciones = listar_columnas(spark, url, properties, f"{schema_seleccionado}.{tabla_seleccionada_2}")
                columna_2 = st.selectbox("Selecciona la segunda columna", columnas_opciones)

        # 3.Ejecuion del analisis seleccionado
        if st.button("Ejecutar Análisis"):
            df = spark.read.jdbc(url=url, table=schema_seleccionado + "." +tabla_seleccionada, properties=properties)

            match tipo_analisis:
                case "Completitud":
                    print(spark.read,df.count(),columna)
                    resultado = analizar_completitud(spark, df, columna)
                    creacion_dataframe_analyzer(spark, resultado)

                case "Exactitud":
                    resultado = analizar_exactitud(spark, df, columna, patron, tipo_exactitud)
                    creacion_dataframe_analyzer(spark, resultado)

                case "Credibilidad":
                    print(spark.read, df.count(), columna)
                    resultado = analizar_credibilidad(spark, df, columna,patron,tipo_credibilidad)
                    creacion_dataframe_analyzer(spark, resultado)

                case "Precision":
                    resultado = analizar_precision(spark, df, columna,num_decimales)
                    creacion_dataframe_analyzer(spark, resultado,1)

                case "Consistencia":
                    df_2 = spark.read.jdbc(url=url, table=schema_seleccionado + "." +tabla_seleccionada_2, properties=properties)
                    resultado = analizar_consistencia(spark,df,df_2,columna, columna_2)
                    creacion_dataframe_check(spark, resultado)

def creacion_dataframe_analyzer(spark,resultado):
    resultado_df = AnalyzerContext.successMetricsAsDataFrame(spark, resultado)
    st.write("Resultado de la validación:")
    st.dataframe(resultado_df.toPandas())

def creacion_dataframe_check(spark,resultado):
    resultado_df = VerificationResult.successMetricsAsDataFrame(spark, resultado)
    st.write("Resultado de la validación:")
    st.dataframe(resultado_df.toPandas())

def main():
    ui()

if __name__ == "__main__":
    main()