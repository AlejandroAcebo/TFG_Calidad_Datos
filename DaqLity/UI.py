import datetime
import io
import os
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
from pyspark.sql import functions as F
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

def cargar_archivo(archivo):
    try:
        # Iniciar SparkSession si no existe
        if 'spark' not in globals():
            spark = SparkSession.builder \
                .appName("Carga desde Archivo") \
                .config("spark.sql.shuffle.partitions", "8") \
                .getOrCreate()
        else:
            spark = globals()['spark']
        # Determinar tipo de archivo
        if archivo.name.endswith(".csv"):
            df_pandas = pd.read_csv(archivo)
        elif archivo.name.endswith(".json"):
            df_pandas = pd.read_json(archivo)
        else:
            raise ValueError("Formato de archivo no soportado")
        # Convertir a Spark DataFrame
        df_spark = spark.createDataFrame(df_pandas)

        # Limpieza porque PyDeequ en archivos CSV no detecta como nulos NaN, NULL, vacio o null
        for col_name in df_spark.columns:
            df_spark = df_spark.withColumn(
                col_name,
                F.when(
                    (F.col(f"`{col_name}`").isNull()) |
                    (F.col(f"`{col_name}`") == "") |
                    (F.col(f"`{col_name}`") == "null") |
                    (F.col(f"`{col_name}`") == "NULL") |
                    (F.col(f"`{col_name}`") == "NaN"),
                    F.lit(None)
                ).otherwise(F.col(f"`{col_name}`"))
            )
        # Simular properties para compatibilidad
        properties = {
            "driver": "pyspark.sql.DataFrame",
            "source": archivo.name,
            "format": archivo.type
        }
        return spark, df_spark, properties
    except Exception as e:
        print(f"Error al cargar archivo: {e}")
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
        tipo_credibilidad, num_decimales,schema_guardar, tabla_guardar, tiempo_limite, df_pandas, columnas, tabla_nombre, esquema_nombre, spark, url, properties, archivo, tipo_analisis_seleccionado, nombre_indicador_seleccionado

    # Titulo de la herramienta
    st.sidebar.title("DaqLity")

    default_session_state = {
        "conectado_analisis": False,
        "seleccionada_fuente": False,
        "nombre_archivo": False,
    }

    # Si no estan inicializadas las st.session se inicializan
    for key, default in default_session_state.items():
        if key not in st.session_state:
            st.session_state[key] = default

    # Selección tipo de fuente de datos, si no hay conexion todavia
    if not st.session_state["conectado_analisis"]:
        st.session_state["opcion_fuente"] = st.sidebar.selectbox(
            "Selecciona la fuente de datos",
            ["Base de datos", "Archivo CSV", "Archivo JSON"]
        )
        st.session_state["seleccionada_fuente"] = True

    opcion_fuente = st.session_state["opcion_fuente"]

    # Gestión de conexión según fuente de datos
    if st.session_state["seleccionada_fuente"]:
        # Fuente de datos base de datos
        if opcion_fuente == "Base de datos":
            if not st.session_state["conectado_analisis"]:
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
        # Fuente de datos desde archivo CSV o JSON
        elif opcion_fuente in ["Archivo CSV", "Archivo JSON"]:
            if not st.session_state["conectado_analisis"]:
                if opcion_fuente == "Archivo CSV":
                    archivo = st.sidebar.file_uploader("Sube un archivo", type=["csv"])
                elif opcion_fuente == "Archivo JSON":
                    archivo = st.sidebar.file_uploader("Sube un archivo", type=["json"])

                if archivo is not None:
                    st.session_state["nombre_archivo"] = archivo.name
                    resultado = cargar_archivo(archivo)
                    if resultado:
                        spark, df_spark, properties = resultado
                        st.session_state["df_archivo"] = df_spark
                        st.session_state["spark"] = spark
                        st.session_state["archivo_info"] = properties
                        st.success(f"Archivo '{archivo.name}' cargado correctamente.")
                        st.session_state["conectado_analisis"] = True
                    else:
                        st.error("Hubo un error al cargar el archivo.")

    # 2.Selección de tabla y columna
    if "conn" in st.session_state or "df_archivo" in st.session_state:
        if "conn" in st.session_state:
            spark, url, properties = st.session_state["conn"]

        elif "df_archivo" in st.session_state:
            df_spark = st.session_state["df_archivo"]
            df_pandas = df_spark.toPandas()
            columnas = df_pandas.columns.tolist()
            nombre_archivo = st.session_state["nombre_archivo"]
            tabla_nombre = os.path.splitext(nombre_archivo)[0]
            esquema_nombre = nombre_archivo

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
            # Si hay conexión a base de datos
            if "conn" in st.session_state:
                spark, url, properties = st.session_state["conn"]
                schemas = listar_schemas(spark, url, properties)
                schema_seleccionado = st.selectbox("Selecciona un esquema", schemas)
                tablas = listar_tablas(spark, url, properties, schema_seleccionado)
                tabla_seleccionada = st.selectbox("Selecciona una tabla", tablas)
                columnas = listar_columnas(spark, url, properties, f"{schema_seleccionado}.{tabla_seleccionada}")
                tabla_nombre = tabla_seleccionada
                esquema_nombre = schema_seleccionado

            # Si hay un archivo CSV/JSON cargado
            elif "df_cargado" in st.session_state:
                df_cargado = st.session_state["df_cargado"]
                columnas = df_cargado.columns.tolist()
                nombre_archivo = st.session_state["nombre_archivo"]
                tabla_nombre = os.path.splitext(nombre_archivo)[0]
                esquema_nombre = nombre_archivo

            # Seleccionar columna y tipo de análisis que lo tienen ambos
            columna = st.selectbox("Selecciona una columna", columnas)
            tipo_analisis = st.selectbox("Selecciona el tipo de análisis", [
                "Completitud", "Credibilidad", "Integridad Referencial",
                "Exactitud", "Precision", "Actualidad"
            ])

            test_config = {
                "tipo": tipo_analisis,
                "columna": columna,
                "tabla": tabla_nombre,
                "schema": esquema_nombre
            }

            valido = True

            # INTRODUCCIÓN DE NUEVOS SELECCIONABLES O CUADROS DE TEXTO DEPENDIENDO DEL TIPO DE TEST
            match tipo_analisis:
                case "Credibilidad":
                    tipos_credibilidad_opciones = ["Patron", "Conjunto valores"]
                    tipo_credibilidad = st.selectbox("Selecciona el tipo", tipos_credibilidad_opciones)
                    patron = st.text_input("Escribe el patrón a filtrar o posibles valores separados por comas")
                    st.caption("Ejemplo patrón: ^(?=(?:\\D*\\d){9,})[^\\p{L}]*$")
                    st.caption("Ejemplo posibles valores: Main Office,Shipping")
                    if not patron.strip():
                        st.warning("El campo 'patrón' no puede estar vacío.")
                        valido = False
                    else:
                        test_config.update({
                            "patron": patron,
                            "tipo_credibilidad": tipo_credibilidad
                        })

                case "Exactitud":
                    tipos_exactitud_opciones = ["Sintactica", "Semantica"]
                    tipo_exactitud = st.selectbox("Selecciona el tipo", tipos_exactitud_opciones)
                    patron = st.text_input("Escribe el patrón a filtrar o posibles valores separados por comas")
                    st.caption("Ejemplo sintáctica: ^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$")
                    st.caption("Ejemplo semántica: Main Office,Shipping")
                    if not patron.strip():
                        st.warning("El campo 'patrón' no puede estar vacío.")
                        valido = False
                    else:
                        test_config.update({
                            "patron": patron,
                            "tipo_exactitud": tipo_exactitud
                        })


                case "Precision":
                    num_decimales = st.text_input("Introduce la cantidad de decimales que debe tener la columna,"
                                                  " solo número entero")
                    if not num_decimales.isdigit():
                        st.warning("Debes introducir un número entero válido.")
                        valido = False
                    else:
                        test_config["num_decimales"] = num_decimales

                case "Integridad Referencial":
                    if "conn" in st.session_state:
                        tablas_opciones = [t for t in tablas if t != tabla_seleccionada]
                        tabla_seleccionada_2 = st.selectbox("Selecciona segunda tabla", tablas_opciones)
                        columnas_opciones = listar_columnas(spark, url, properties,
                                                            f"{schema_seleccionado}.{tabla_seleccionada_2}")
                        columna_2 = st.selectbox("Selecciona la segunda columna", columnas_opciones)
                        if not tabla_seleccionada_2 or not columna_2:
                            st.warning("Debes seleccionar una tabla y una columna válidas.")
                            valido = False
                        else:
                            test_config.update({
                                "columna_2": columna_2,
                                "tabla_2": tabla_seleccionada_2
                            })
                    else:
                        st.warning("La integridad referencial no aplica sobre archivos simples.")

                case "Actualidad":
                    tiempo_limite = st.text_input("Introduce la fecha máxima que debería tener la columna")
                    st.caption("Un ejemplo sería: 2006-01-01 00:00:00")
                    if not tiempo_limite.strip():
                        st.warning("La fecha límite no puede estar vacía.")
                        valido = False
                    else:
                        test_config["tiempo_limite"] = tiempo_limite

            # Guardar test
            if st.button("Guardar test", disabled= not valido):
                st.session_state.setdefault("tests_seleccionados", []).append(test_config)
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
                        # Determinamos si la fuente es BD o archivo
                        if "conn" in st.session_state:
                            spark, url, properties = st.session_state["conn"]
                            df = spark.read.jdbc(url=url, table=f"{schema}.{tabla}", properties=properties)
                        elif "df_archivo" in st.session_state:
                            # Como no se carga
                            if "spark" not in st.session_state:
                                spark = SparkSession.builder \
                                    .appName("DaqLity") \
                                    .config("spark.sql.shuffle.partitions", "8") \
                                    .getOrCreate()
                                st.session_state["spark"] = spark
                            else:
                                spark = st.session_state["spark"]
                            df = st.session_state["df_archivo"]
                            if columna not in df.columns:
                                st.warning(f"La columna '{columna}' no existe en el archivo. Saltando test.")
                                continue
                            if tabla_2 and columna_2 and tabla_2 != tabla:
                                st.warning("No se puede hacer integridad referencial entre archivos distintos.")
                                continue
                        else:
                            st.error("No hay fuente de datos conectada.")
                            continue

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
                                if "conn" in st.session_state:
                                    df_2 = spark.read.jdbc(url=url, table=f"{schema}.{tabla_2}", properties=properties)
                                    res = analizar_integridad_referencial(spark, df, df_2, columna, columna_2)
                                    df_resultado = VerificationResult.successMetricsAsDataFrame(spark, res)
                                else:
                                    st.warning("La integridad referencial no aplica sobre archivos simples.")
                            case "Actualidad":
                                res = analizar_actualidad(spark, df, columna, tiempo_limite, tabla)
                                df_resultado = VerificationResult.successMetricsAsDataFrame(spark, res)

                        if df_resultado and df_resultado.count() > 0:
                            df_resultado_formateado = creacion_dataframe_analyzer(spark, df_resultado)
                            df_pandas = df_resultado_formateado.toPandas()
                            resultado = pd.concat([resultado, df_pandas], ignore_index=True)
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
