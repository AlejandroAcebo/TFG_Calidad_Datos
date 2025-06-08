import datetime
import io
import os

from PIL.ImageOps import expand

os.environ["SPARK_VERSION"] = "3.5"

import pandas as pd
from Analisis_Generalizados.integridad_referencial import analizar_integridad_referencial
from Analisis_Generalizados.credibilidad import analizar_credibilidad
from Analisis_Generalizados.exactitud import analizar_exactitud
from Analisis_Generalizados.precision import analizar_precision
from Analisis_Generalizados.completitud import analizar_completitud
from Analisis_Generalizados.actualidad import analizar_actualidad
from io import BytesIO
from pyspark.sql.functions import concat, col, lit, current_timestamp


from pydeequ.verification import VerificationResult
from pydeequ.analyzers import AnalyzerContext
import streamlit as st
import json
from pyspark.sql import SparkSession


def conectar_bd(user, password, server, database):
    try:
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

        # Test de conexión real: intentar leer una tabla del sistema
        test_df = spark.read.jdbc(url, "INFORMATION_SCHEMA.TABLES", properties=properties)
        test_df.limit(1).collect()  # Fuerza una lectura para confirmar conexión

        return spark, url, properties

    except Exception as e:
        print(f"Error de conexión: {e}")
        return None


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
    global patron, tabla_seleccionada_2, columna_2, tipo_exactitud,\
        tipo_credibilidad, num_decimales,schema_guardar, tabla_guardar, tiempo_limite

    # Datos de conexión
    if "conectado_analisis" not in st.session_state:
        st.session_state["conectado_analisis"] = False

    if "conectado_guardado" not in st.session_state:
        st.session_state["conectado_guardado"] = False

    # Primera conexión a base de datos, si falla se indicará que las credenciales no son correctas.
    if not st.session_state["conectado_analisis"]:
        st.sidebar.title("DaqLity")
        st.sidebar.header("🔌 Conexión a Base de Datos")
        host = st.sidebar.text_input("Host", value="localhost")
        user = st.sidebar.text_input("Usuario")
        password = st.sidebar.text_input("Contraseña", type="password")
        database = st.sidebar.text_input("Base de Datos")
        st.warning("Actualmente solamente permite conexión con SQL Server")

        if st.sidebar.button("Conectar análisis"):
            conn = conectar_bd(user, password, host, database)
            if conn:
                st.session_state["conn"] = conn
                st.session_state["conectado_analisis"] = True
                st.sidebar.success("Conectado para análisis")
            else:
                st.sidebar.error("Error al conectar para análisis")

    # 2.Selección de tabla y columna
    if "conn" in st.session_state:
        spark, url, properties = st.session_state["conn"]

        st.sidebar.title("DaqLity")

        # Boton para visualizar resultados
        json_file = st.sidebar.file_uploader("Visualizar analisis previos", type=["json"])
        if json_file is not None:
            try:
                # Leer y parsear el contenido del archivo
                json_data = json.load(json_file)

                # Verificar que es una lista de diccionarios (registros)
                if isinstance(json_data, list) and all(isinstance(row, dict) for row in json_data):
                    df_cargado = pd.DataFrame(json_data)
                    if 'Fecha y hora de ejecución' in df_cargado.columns:
                        try:
                            df_cargado['Fecha y hora de ejecución'] = pd.to_datetime(df_cargado['Fecha y hora de ejecución'])
                        except Exception as e:
                            st.warning("Error al convertir la fecha")

                    st.success("Archivo cargado correctamente. Aquí están los resultados:")
                    st.dataframe(df_cargado)

                    # Guardar en session_state si quieres reutilizar más adelante
                    st.session_state["df_resultado_cargado"] = df_cargado
                else:
                    st.error("El JSON no tiene el formato esperado.")

            except json.JSONDecodeError:
                st.error("El archivo no es un JSON válido.")
            except Exception as e:
                st.error(f"Error al procesar el archivo: {e}")

        # Botón para guardar los resultados como un JSON
        df = st.session_state.get("df_resultado")

        if df is not None and not df.empty:
            if 'Fecha y hora de ejecución' in df.columns:
                df['Fecha y hora de ejecución'] = df['Fecha y hora de ejecución'].astype(str)
            json_str = df.to_json(orient="records", indent=2, force_ascii=False)
            json_bytes = io.BytesIO(json_str.encode("utf-8"))
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"resultado_analisis_{timestamp}.json"

            # Botón de descarga
            st.sidebar.download_button(
                label="Descargar resultados",
                data=json_bytes,
                file_name=file_name,
                mime="application/json"
            )
        else:
            st.sidebar.warning("Primero debes ejecutar el análisis para poder guardar los resultados.")

        # División en columnas
        col_izq, col_der = st.columns(2)

        with col_izq:
            st.header("Definición de pruebas")
            # SELECCIÓN Y MOSTRADO DE LOS DIFERENTES ESQUEMAS, TABLAS Y TIPO DE TEST
            schemas = listar_schemas(spark, url, properties)
            schema_seleccionado = st.selectbox("Selecciona un esquema", schemas)
            print(schema_seleccionado)
            tablas = listar_tablas(spark, url, properties, schema_seleccionado)
            tabla_seleccionada = st.selectbox("Selecciona una tabla", tablas)
            print(tabla_seleccionada)
            columnas = listar_columnas(spark, url, properties, f"{schema_seleccionado}.{tabla_seleccionada}")
            columna = st.selectbox("Selecciona una columna", columnas)
            print(columna)
            tipo_analisis = st.selectbox("Selecciona el tipo de análisis", ["Completitud","Credibilidad","Integridad Referencial"
                ,"Exactitud","Precision","Actualidad"])

            # INTRODUCCIÓN DE NUEVOS SELECCIONABLES O CUADROS DE TEXTO DEPENDIENDO DEL TIPO DE TEST
            match tipo_analisis:

                # En este caso se permite seleccionar si quiere un patrón o un conjunto de valores válidos
                case "Credibilidad":
                    tipos_credibilidad_opciones = ["Patron", "Conjunto valores"]
                    tipo_credibilidad = st.selectbox("Selecciona el tipo", tipos_credibilidad_opciones)
                    patron = st.text_input("Escribe el patrón a filtrar o posibles valores separados por comas")
                    st.caption("Ejemplo patron: ^(?=(?:\D*\d){9,})[^\p{L}]*$")
                    st.caption("Ejemplo posibles valores: Main Office,Shipping")

                # Dependiendo del tipo de exactitud que se desee seleccionar primeramente y segundo según el formato
                # rellenar el campo de texto del patrón o conjunto de valores válidos.
                case "Exactitud":
                    tipos_exactitud_opciones = ["Sintactica","Semantica"]
                    tipo_exactitud = st.selectbox("Selecciona el tipo",tipos_exactitud_opciones)
                    patron = st.text_input("Escribe el patrón a filtrar o posibles valores separados por comas")
                    st.caption("Ejemplo sintáctica, patron: ^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")
                    st.caption("Ejemplo semántica, valores: Main Office,Shipping")

                case"Precision":
                    num_decimales = st.text_input("Introduce la cantidad de decimales que debe tener la columna,"
                                                      "solo número entero")

                case "Integridad Referencial":
                    tablas_opciones = [t for t in tablas if t != tabla_seleccionada]
                    tabla_seleccionada_2 = st.selectbox("Selecciona segunda tabla", tablas_opciones)

                    columnas_opciones = listar_columnas(spark, url, properties, f"{schema_seleccionado}.{tabla_seleccionada_2}")
                    columna_2 = st.selectbox("Selecciona la segunda columna", columnas_opciones)

                case "Actualidad":
                    tiempo_limite = st.text_input("Introduce la fecha maxima que deberia tener la columna")
                    st.caption("Un ejemplo sería: 2006-01-01 00:00:00")


            # Boton para almacenar test en conjunto de pruebas
            if st.button("Guardar test"):
                test_config = {
                    "tipo": tipo_analisis,
                    "columna": columna,
                    "tabla": tabla_seleccionada,
                    "schema": schema_seleccionado
                }
                match tipo_analisis:
                    case "Completitud":
                        pass
                    case "Exactitud":
                        test_config["patron"] = patron
                        test_config["tipo_exactitud"] = tipo_exactitud
                    case "Credibilidad":
                        test_config["patron"] = patron
                        test_config["tipo_credibilidad"] = tipo_credibilidad
                    case "Precision":
                        test_config["num_decimales"] = num_decimales
                    case "Integridad Referencial":
                        test_config["columna_2"] = columna_2
                        test_config["tabla_2"] = tabla_seleccionada_2
                    case "Actualidad":
                        test_config["tiempo_limite"] = tiempo_limite

                # Guardar en session_state
                if "tests_seleccionados" not in st.session_state:
                    st.session_state["tests_seleccionados"] = []

                st.session_state["tests_seleccionados"].append(test_config)
                st.success(f"Prueba '{tipo_analisis}' guardada correctamente.")


        with col_der:
            st.header("Conjuntos de pruebas")
            # Boton para cargar un conjunto de pruebas en formato JSON
            archivo_test = st.file_uploader("Cargar conjunto de test", type="json")
            if archivo_test is not None and not st.session_state.get("tests_cargados_flag", False):
                try:
                    tests_cargados = json.load(archivo_test)
                    if isinstance(tests_cargados, list) and all(isinstance(t, dict) for t in tests_cargados):
                        if "tests_seleccionados" not in st.session_state:
                            st.session_state["tests_seleccionados"] = []
                        st.session_state["tests_seleccionados"].extend(tests_cargados)
                        st.success(
                            f"Se han añadido {len(tests_cargados)} tests correctamente. Total: {len(st.session_state['tests_seleccionados'])}")
                        st.session_state["tests_cargados_flag"] = True  # marca que ya se cargó
                    else:
                        st.error("El archivo JSON no contiene una lista válida de tests.")
                except Exception as e:
                    st.error(f"Error al leer el archivo JSON: {e}")

            # Boton para descargar el conjunto de pruebas que se han guardado
            if "tests_seleccionados" in st.session_state and st.session_state["tests_seleccionados"]:
                # Convertir directamente a lista y guardar en formato JSON adecuado
                tests_lista = list(st.session_state["tests_seleccionados"])  # Asegura que es una lista
                tests_json = json.dumps(tests_lista, indent=4)

                # Convertir a bytes para descargar
                buffer = BytesIO()
                buffer.write(tests_json.encode('utf-8'))
                buffer.seek(0)

                st.download_button(
                    label="Descargar conjunto de test",
                    data=buffer,
                    file_name="tests_guardados.json",
                    mime="application/json"
                )
            else:
                st.warning("No hay tests guardados.")

        # Boton para ejecutar todas las pruebas guardadas
        if st.button("Ejecutar todos los análisis"):
            resultado = pd.DataFrame()
            # DataFrame pandas vacío para acumular resultados
            if "tests_seleccionados" in st.session_state and st.session_state["tests_seleccionados"]:

                for test in st.session_state["tests_seleccionados"]:
                    tabla = test.get("tabla")
                    tabla_2 = test.get("tabla_2")
                    schema = test.get("schema")
                    tipo = test.get("tipo")
                    columna = test.get("columna")
                    patron = test.get("patron")
                    tipo_exactitud = test.get("tipo_exactitud")
                    tipo_credibilidad = test.get("tipo_credibilidad")
                    num_decimales = test.get("num_decimales")
                    columna_2 = test.get("columna_2")
                    tiempo_limite = test.get("tiempo_limite")

                    try:
                        df = spark.read.jdbc(url=url, table=f"{schema}.{tabla}", properties=properties)
                        df_resultado = None

                        match tipo:
                            case "Completitud":
                                res = analizar_completitud(spark, df, columna)
                                df_resultado = AnalyzerContext.successMetricsAsDataFrame(spark, res)

                            case "Exactitud":
                                res = analizar_exactitud(spark, df, columna, patron, tipo_exactitud)
                                df_resultado = AnalyzerContext.successMetricsAsDataFrame(spark, res)

                            case "Credibilidad":
                                res = analizar_credibilidad(spark, df, columna, patron, tipo_credibilidad)
                                df_resultado = AnalyzerContext.successMetricsAsDataFrame(spark, res)

                            case "Precision":
                                res = analizar_precision(spark, df, columna, num_decimales)
                                df_resultado = AnalyzerContext.successMetricsAsDataFrame(spark, res)

                            case "Integridad Referencial":
                                df_2 = spark.read.jdbc(url=url, table=f"{schema}.{tabla_2}", properties=properties)
                                res = analizar_integridad_referencial(spark, df, df_2, columna, columna_2)
                                df_resultado = VerificationResult.successMetricsAsDataFrame(spark, res)

                            case "Actualidad":
                                res = analizar_actualidad(spark, df, columna, tiempo_limite,tabla)
                                df_resultado = VerificationResult.successMetricsAsDataFrame(spark, res)

                        # Si df_resultado es None o vacío, se omite concatenar
                        if df_resultado and df_resultado.count() > 0:
                            df_resultado_formateado = creacion_dataframe_analyzer(spark, df_resultado)
                            df_pandas = df_resultado_formateado.toPandas()
                            resultado = pd.concat(
                                [resultado, df_pandas], ignore_index=True
                            )
                        else:
                            st.warning(f"No hay resultados para el test: {test}")
                    except Exception as e:
                        st.error(f"Error en la ejecución del test: {e}")

                if not resultado.empty:
                    st.dataframe(resultado)
                    st.session_state["df_resultado"] = resultado
                else:
                    st.warning("No se generaron resultados para mostrar.")
            else:
                st.warning("No hay tests guardados.")

# Metodo que añade porcentaje, fecha y hora y cambia el nombre de las columnas que no tiene sentido el nombre
def creacion_dataframe_analyzer(spark,df):
    df = df.withColumn(
        "Porcentaje",
        concat((col("value") * 100).cast("int").cast("String"), lit("%"))
    )
    df = df.withColumn("Fecha y hora de ejecución", current_timestamp())
    df = (df.withColumnRenamed("entity","Tipo test")
          .withColumnRenamed("instance","Nombre de indicador")
          .withColumnRenamed("name","Tipo test PyDeequ")
          .withColumnRenamed("value","Valor"))
    return df


def main():
    ui()

if __name__ == "__main__":
    main()