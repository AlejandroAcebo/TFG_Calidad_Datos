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
from pyspark.sql.functions import concat, col, lit, current_timestamp, concat_ws
from pyspark.sql import functions as F
from pydeequ.verification import VerificationResult
from pydeequ.analyzers import AnalyzerContext
import streamlit as st
import json
from pyspark.sql import SparkSession

def ui():
    """
    Gestión de toda la parte de la interfaz sobre esta se define el comportamiento y la apariencia de la misma.
    """
    # Definicion de variables globales
    global patron, tabla_seleccionada_2, columna_2, tipo_exactitud,\
        tipo_credibilidad, num_decimales,schema_guardar, tabla_guardar, \
        tiempo_limite, df_pandas, columnas, tabla_nombre, esquema_nombre, \
        spark, url, properties, archivo, tipo_analisis_seleccionado, nombre_indicador_seleccionado

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

    # Selección tipo de fuente de datos, si no hay conexión todavía
    seleccion_conexion()

    # Selección de tabla y columna
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
        json_file = st.sidebar.file_uploader("🔍 Visualizar analisis previos", type=["json"])
        if json_file is not None:
            visualizar_resultados(json_file)

        # Botón para guardar los resultados como un JSON
        df = st.session_state.get("df_resultado")

        if df is not None and not df.empty:
            descargar_resultados(df)
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

            # Gestion de la interfaz de acuerdo al tipo de test seleccionado
            valido = True
            valido = gestion_tipo_test_ui(properties, schema_seleccionado, spark, tabla_seleccionada, tablas,
                                          test_config, tipo_analisis, url, valido)

            # Guardar test
            if st.button("💾 Guardar test", disabled= not valido):
                st.session_state.setdefault("tests_seleccionados", []).append(test_config)
                st.success(f"Prueba '{tipo_analisis}' guardada correctamente.")

        with col_der:
            st.header("Conjuntos de pruebas")
            # Boton para cargar un conjunto de pruebas en formato JSON
            archivo_test = st.file_uploader("📁 Cargar conjunto de test", type="json")
            if archivo_test is not None and not st.session_state.get("tests_cargados_flag", False):
                cargar_conjunto_test(archivo_test)

            # Boton para descargar el conjunto de pruebas que se han guardado
            if "tests_seleccionados" in st.session_state and st.session_state["tests_seleccionados"]:
                descargar_conjunto_test()
            else:
                st.warning("No hay tests guardados.")

        # Boton para ejecutar todas las pruebas guardadas
        if st.button("▶️ Ejecutar el conjunto de pruebas"):
            resultado = pd.DataFrame()
            if "tests_seleccionados" in st.session_state and st.session_state["tests_seleccionados"]:
                gestion_ejecucion_test(resultado)
            else:
                st.warning("No hay tests guardados.")


def gestion_ejecucion_test(resultado):
    """
    Gestiona la ejecución de los tests seleccionados, en caso de que haya un conjunto de tests guardados, procede a
    ejecutarlos de uno en uno teniendo en cuenta el tipo de test que se trata.

    Args:
        resultado (DataFrame): DataFrame sobre el que se cargan los resultados
    """
    global patron, tipo_exactitud, tipo_credibilidad, num_decimales, columna_2, tiempo_limite, spark, url, properties, df_pandas
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
                    df_resultado = generar_df_modificado(spark, res,
                                                         "Analyzer", tipo, tabla, columna)
                case "Exactitud":
                    res = analizar_exactitud(spark, df, columna, patron, tipo_exactitud)
                    df_resultado = generar_df_modificado(spark, res,
                                                         "Analyzer", tipo, tipo_exactitud, tabla, columna)
                case "Credibilidad":
                    res = analizar_credibilidad(spark, df, columna, patron, tipo_credibilidad)
                    df_resultado = generar_df_modificado(spark, res,
                                                         "Analyzer", tipo, tipo_credibilidad, tabla, columna)

                case "Precision":
                    res = analizar_precision(spark, df, columna, num_decimales)
                    df_resultado = generar_df_modificado(spark, res,
                                                         "Analyzer", tipo, tabla, columna)

                case "Integridad Referencial":
                    if "conn" in st.session_state:
                        df_2 = spark.read.jdbc(url=url, table=f"{schema}.{tabla_2}", properties=properties)
                        res = analizar_integridad_referencial(spark, df, df_2, columna, columna_2)
                        df_resultado = generar_df_modificado(spark, res,
                                                             "Verification", tipo, tabla, tabla_2, columna, columna_2)
                    else:
                        st.warning("La integridad referencial no aplica sobre archivos simples.")

                case "Actualidad":
                    res = analizar_actualidad(spark, df, columna, tiempo_limite, tabla)
                    df_resultado = generar_df_modificado(spark, res,
                                                         "Verification", tipo, tabla, columna)

            if df_resultado and df_resultado.count() > 0:
                df_resultado_formateado = creacion_dataframe_personalizado(spark, df_resultado)
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


def gestion_tipo_test_ui(properties, schema_seleccionado, spark, tabla_seleccionada, tablas, test_config, tipo_analisis,
                         url, valido):
    """
    Gestiona en la interfaz los nuevos desplegables o campos que deben aparecer de acuerdo al tipo de test
    seleccionado.

    Args:
        properties(str): Propiedades para conexión.
        schema_seleccionado (str): Esquema seleccionado para desplegar.
        spark (SparkSession): Sesión activa de Spark.
        tabla_seleccionada (str): Tabla sobre la que se realizará el análisis.
        tablas (list[str]): Lista de tablas disponibles.
        test_config (dict): Configuración de los tests a ejecutar.
        tipo_analisis (str): Tipo de análisis seleccionado (e.g. "Completitud", "Exactitud").
        url (str): URL de conexión JDBC a la base de datos.
        valido (bool): Indicador de si estan los campos extras rellenos y se puede proceder a guardar.

    Returns:
        Devuelve True en caso de que el test se haya definido de forma adecuada y False en caso de que no.
    """

    global tipo_credibilidad, patron, tipo_exactitud, num_decimales, tabla_seleccionada_2, columna_2, tiempo_limite
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
    return valido

def conectar_bd(user, password, server, database):
    """
    Conexión de la base de datos con la herramienta.

    Args:
        user (str): Nombre de usuario de la base de datos.
        password (str): Contraseña de usuario de la base de datos.
        server (str): Servidor de la base de datos.
        database (str): Nombre de la base de datos.

    Returns:
        Devuelve spark, la url y propiedades de la base de datos o None en caso de error de conexión.
    """

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
    """
    Carga el archivo en formato CSV o JSON y formatea los atributos que contengan valores que PyDeequ no detecta.

    Args:
        archivo (File): Archivo que se quiere cargar

    Returns:
        El spark, el dataframe de spark y las propiedades creadas para simular para poder ser utilizado en análisis.
    """

    try:
        # Iniciar SparkSession si no existe
        if 'spark' not in globals():
            spark = SparkSession.builder \
                .appName("📁 Carga desde Archivo") \
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
    """
    Mediante jdbc y spark hace la conexión con los parámetros pasados para retornar los esquemas que tiene la base
    de datos.

    Args:
        spark (SparkSession): Sesión activa de Spark.
        url (str): URL de conexión JDBC a la base de datos.
        props(str): Propiedades para conexión.

    Returns:
        Devuelve los esquemas de esa base de datos.
    """

    schemas_df = spark.read.jdbc(url, "INFORMATION_SCHEMA.SCHEMATA", properties=props)
    return schemas_df.select("SCHEMA_NAME").rdd.flatMap(lambda x: x).collect()


def listar_tablas(spark, url, props, schema):
    """
    Mediante jdbc y spark hace la conexión con los parámetros pasados para retornar las tablas que ese esquema
    tiene.

    Args:
        spark (SparkSession): Sesión activa de Spark.
        url (str): URL de conexión JDBC a la base de datos.
        props(str): Propiedades para conexión.
        schema (str): Esquema del que se quieren listar las tablas.

    Returns:
        Devuelve las tablas que esa esquema tiene.
    """

    query = f"(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}') AS tablas"
    tablas_df = spark.read.jdbc(url, table=query, properties=props)
    return tablas_df.rdd.flatMap(lambda x: x).collect()


def listar_columnas(spark, url, props, tabla):
    """
    Mediante jdbc y spark hace la conexión con los parámetros pasados para retornar las columnas que esa tabla
    tiene.

    Args:
        spark (SparkSession): Sesión activa de Spark.
        url (str): URL de conexión JDBC a la base de datos.
        props(str): Propiedades para conexión.
        tabla (str): Tabla de la que se quieren listar las columnas.

    Returns:
        Devuelve las columnas que esa tabla tiene.
    """

    df_leido = spark.read.jdbc(url=url, table=tabla, properties=props)

    return df_leido.columns


def seleccion_conexion():
    """
    De acuerdo a la selección del tipo de fuente de datos a analizar, la herramienta hace la conexión con la base de
    datos de acuerdo a los parametros introucidos por el usuario o permite la carga de un archivo CSV o JSON.
    Además, la herramienta se encarga de la definición de los diferentes parametros que más adelante se emplearán de
    acuerdo al tipo de fuente de datos seleccionada.
    """

    global archivo, spark, properties
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


def visualizar_resultados(json_file):
    """
    Muestra en la pantalla los resultados almacenados en un archivo JSON cargado. Si el archivo está bien
    estructurado y es válido entonces se procede a mostrar por pantalla sino indica que el archivos no es correcto.

    Args:
        json_file (archivo JSON): Archivo JSON con los resultados que se quieren mostrar.
    """

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


def descargar_resultados(df):
    """
    Descarga un archivo JSON con el conjunto de resultados obtenidos de ejecutar el conjunto de tests.
    """

    if 'Fecha y hora de ejecución' in df.columns:
        df['Fecha y hora de ejecución'] = df['Fecha y hora de ejecución'].astype(str)
    json_str = df.to_json(orient="records", indent=2, force_ascii=False)
    json_bytes = io.BytesIO(json_str.encode("utf-8"))
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"resultado_analisis_{timestamp}.json"
    # Botón de descarga
    st.sidebar.download_button(
        label="📤 Descargar resultados",
        data=json_bytes,
        file_name=file_name,
        mime="application/json"
    )


def cargar_conjunto_test(archivo_test):
    """
    Carga un archivo JSON que contiene un conjunto de tests, si los tests cargados tienen una estructura correcta y no
    contienen errores, entonces estos se añaden como nuevos tests cargados en la herramienta.

    Args:
       archivo_test (archivo JSON): Archivo JSON que contiene los test a cargar en la herramienta.
    """

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


def descargar_conjunto_test():
    """
    Descarga un archivo JSON con el conjunto de test definidos y guardados por el usuario final.
    """

    # Convertir directamente a lista y guardar en formato JSON adecuado
    tests_lista = list(st.session_state["tests_seleccionados"])  # Asegura que es una lista
    tests_json = json.dumps(tests_lista, indent=4)
    # Convertir a bytes para descargar
    buffer = BytesIO()
    buffer.write(tests_json.encode('utf-8'))
    buffer.seek(0)
    st.download_button(
        label="📤 Descargar conjunto de test",
        data=buffer,
        file_name="tests_guardados.json",
        mime="application/json"
    )


def generar_df_modificado(spark, res, tipo_ejecucion, tipo, *componentes):
    """
    Genera un DataFrame con las métricas calculadas por PyDeequ según el tipo de test,
    y personaliza la columna 'instance' con un nombre más descriptivo.

    Args:
        spark (SparkSession): Sesión activa de Spark.
        res (AnalyzerContext | VerificationResult): Resultado del análisis realizado con PyDeequ.
        tipo_ejecucion (str): Tipo de test de PyDeequ a ejecutar.
        tipo (str): Tipo de test de calidad de datos (e.g., "Completitud", "Exactitud").
        *componentes (str): Lista de elementos adicionales que se usan para construir el valor de 'instance'.

    Returns:
        DataFrame: DataFrame de métricas enriquecido con una columna 'instance' personalizada.

    Raises:
        ValueError: Si se proporciona un valor inválido para el parámetro tipo_ejecucion.
    """

    df_resultado = None
    if tipo_ejecucion == "Analyzer":
        df_resultado = AnalyzerContext.successMetricsAsDataFrame(spark,res)
    elif tipo_ejecucion == "Verification":
        df_resultado = VerificationResult.successMetricsAsDataFrame(spark,res)
    else:
        raise ValueError("Tipo de ejecucion no soportado")

    # Elimina la columna que genera PyDeequ de tipo de test de PyDeequ ya que para el usuario final no tiene sentido.
    df_resultado = df_resultado.drop("name")
    return df_resultado.withColumn(
        "instance",
        concat_ws("_", *[lit(str(c)) for c in (tipo, *componentes)])
    )


def creacion_dataframe_personalizado(spark,df):
    """
    Personaliza el dataframe que se le pasa como parámetro, modificando nombres pocos descriptivos y añadiendo y
    eliminando columnas.

    Args:
        spark (SparkSession): Sesión activa de Spark.
        df (DataFrame): Dataframe que se va a personalizar.

    Returns:
        df: Dataframe con los cambios personalizados.
    """

    df = df.withColumn(
        "Porcentaje",
        concat((col("value") * 100).cast("int").cast("String"), lit("%"))
    )
    df = df.withColumn("Fecha y hora de ejecución", current_timestamp())
    df = (df.withColumnRenamed("entity","Tipo test")
          .withColumnRenamed("instance","Nombre de indicador")
          .withColumnRenamed("value","Valor"))
    return df


def main():
    ui()

if __name__ == "__main__":
    main()
