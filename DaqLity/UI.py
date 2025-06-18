import datetime
import io
import os
from time import sleep

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
import plotly.express as px
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
        spark, url, properties, archivo, tipo_analisis_seleccionado, nombre_indicador_seleccionado,\
        archivos, tabla_seleccionada, nombre_test_calidad

    gestion_estilo_UI()

    if 'page' not in st.session_state:
        st.session_state.page = 'conexion_bd'

    if st.session_state.page == 'conexion_bd':
        col1, col2, col3 = st.columns([3, 4, 3])
        with col2:
            with st.container(border=True):
                default_session_state = {
                    "conectado_analisis": False,
                    "seleccionada_fuente": False,
                    "nombre_archivo": False,
                    "pruebas_ejecutadas": False,
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

    elif st.session_state.page == 'inicio':

        # División en columnas
        col_izq, col_medio, col_der = st.columns(3)

        with col_izq:
            st.header("Gestión visualización")
            st.subheader("Aquí se gestiona la visualización de análisis y la descarga")
            # Creacion de una nueva pagina solo para ver la evaluación
            if st.button("📊 Ir a evaluación", on_click=ir_evaluacion, use_container_width=True):
                st.session_state.page = 'evaluacion'

            # Botón para guardar los resultados como un JSON
            if "df_resultado" in st.session_state:
                descargar_resultados(st.session_state["df_resultado"])
            else:
                st.warning("Primero debes ejecutar el análisis para poder guardar los resultados.")

        with col_medio:
            st.header("Definición de plan de calidad")
            nombre_test_calidad = st.text_input("Nombre plan de calidad:")
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

            # Caso conexión a base de datos
            if "conn" in st.session_state:
                if all(x is not None for x in [spark, properties, url, esquema_nombre, tabla_seleccionada, tablas]):
                    valido = gestion_tipo_test_ui(
                        properties=properties,
                        schema_seleccionado=esquema_nombre,
                        spark=spark,
                        tabla_seleccionada=tabla_seleccionada,
                        tablas=tablas,
                        test_config=test_config,
                        tipo_analisis=tipo_analisis,
                        url=url,
                        valido=valido
                    )
                else:
                    st.error("Faltan parámetros requeridos para el análisis en base de datos.")

            # Caso archivo cargado
            elif "df_cargado" in st.session_state:
                if all(x is not None for x in [tabla_nombre, esquema_nombre, columnas]):
                    valido = gestion_tipo_test_ui(
                        properties=None,
                        schema_seleccionado=esquema_nombre,
                        spark=None,
                        tabla_seleccionada=tabla_nombre,
                        tablas=[],
                        test_config=test_config,
                        tipo_analisis=tipo_analisis,
                        url=None,
                        valido=valido
                    )
                else:
                    st.error("Faltan parámetros requeridos para el análisis sobre archivo cargado.")

            col1, col2 = st.columns(2)
            with col1:
                # Guardar test
                if st.button("💾 Guardar test", disabled= not valido,use_container_width=True):
                    st.session_state.setdefault("tests_seleccionados", []).append(test_config)
                    st.success(f"Prueba '{tipo_analisis}' guardada correctamente.")
            with col2:
                eliminar = False
                if "tests_seleccionados" in st.session_state and st.session_state["tests_seleccionados"]:
                    eliminar = True
                if st.button("🗑️ Eliminar test anterior",disabled= not eliminar,use_container_width=True):
                    if st.session_state["tests_seleccionados"]:
                        st.session_state.setdefault("tests_seleccionados", []).pop()
                        st.success(f"Ultimo test eliminado correctamente.")
                    else:
                        st.error("No hay tests guardados actualmente.")

            #Botón ejecución de pruebas
            if st.button("▶️ Ejecutar el conjunto de pruebas", use_container_width=True):
                if "tests_seleccionados" in st.session_state and st.session_state["tests_seleccionados"]:
                    resultado = pd.DataFrame()
                    st.session_state["df_resultado"] = gestion_ejecucion_test(resultado)
                else:
                    st.warning("No hay tests guardados.")
        with col_der:
            st.header("Manejo plan de calidad")
            # Boton para cargar un conjunto de pruebas en formato JSON
            archivo_test = st.file_uploader("📁 Cargar conjunto de test", type="json")
            if archivo_test is not None and not st.session_state.get("tests_cargados_flag", False):
                cargar_conjunto_test(archivo_test)

            if st.button("🧹 Eliminar todas las pruebas",use_container_width=True):
                if st.session_state["tests_seleccionados"]:
                    st.session_state["tests_seleccionados"].clear()
                    st.success("Todas las pruebas han sido eliminadas.")
                    sleep(1)
                    st.rerun()
                else:
                    st.warning("El conjunto de pruebas esta vacio")

            # Boton para descargar el conjunto de pruebas que se han guardado
            if "tests_seleccionados" in st.session_state and st.session_state["tests_seleccionados"]:
                descargar_conjunto_test()
            else:
                st.warning("No hay tests guardados.")



        # Mostrar los resultados del test
        if "df_resultado" in st.session_state:
            st.write("### Resultado de pruebas:")
            st.dataframe(st.session_state["df_resultado"], use_container_width=True)

    elif st.session_state.page == 'evaluacion':
        st.title("📊 Evaluación histórico de datos")
        # Funcionalidad para ver histórico cargando múltiples archivos
        columna_izquierda, columna_derecha = st.columns(2)
        with columna_izquierda:
            archivos = st.file_uploader("Sube tus archivos", type=["json"], accept_multiple_files=True)
            st.button("↩️ Volver atrás",on_click=ir_inicio)
        with columna_derecha:
            gestion_evolucion_analisis(archivos)


def gestion_estilo_UI():
    # Configuración de la vista de la ventana
    st.set_page_config(
        page_title="DaqLity",
        page_icon="https://i.imgur.com/ZTU70TS.png",
        layout="wide"
    )
    # Ocultar elementos por defecto de Streamlit
    st.markdown("""
        <style>
            #MainMenu, header, footer {visibility: hidden;}
            .block-container {
                padding-top: 2rem;
            }
        </style>
    """, unsafe_allow_html=True)
    # Personalización de tipo de fuente, colores y fondos
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;600&display=swap');

        /* Aplica a todo */
        * {
            font-family: 'Poppins', sans-serif !important;
        }

        /* Fondo y color texto */
        .stApp {
            background-color: #f5f7fa !important;
            color: #222222 !important;
        }

        /* Botones */
        div.stButton > button {
            background-color: #1E90FF !important;
            color: white !important;
            border-radius: 8px !important;
            padding: 10px 20px !important;
            font-weight: 500 !important;
            border: none !important;
            transition: background-color 0.3s ease !important;
            font-family: 'Poppins', sans-serif !important;
        }
        div.stButton > button:hover {
            background-color: #1C86EE !important;
            cursor: pointer !important;
        }
        
        /* Labels e inputs */
        label, input, select, textarea {
            font-family: 'Poppins', sans-serif !important;
            font-weight: 500 !important;
        }

        </style>
        """,
        unsafe_allow_html=True
    )
    # Personalizacion formulario
    st.markdown(
        """
        <style>
        .st-form-box {
            border: 2px solid #1E90FF;
            border-radius: 10px;
            padding: 20px;
            margin-top: 20px;
            background-color: white;
            box-shadow: 2px 2px 10px rgba(30,144,255,0.1);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    # Título con banner personalizado
    st.markdown(
        """
        <div style="text-align: center; margin-top: 0; margin-bottom: 0; padding-top: 0; padding-bottom: 0;">
            <img src="https://i.imgur.com/p9ASPIf.png" width="400" style="display: block; margin: 0 auto;">
        </div>
        """,
        unsafe_allow_html=True
    )

    # Pie de página
    st.markdown("""
        <style>
        .footer {
            position: fixed;
            bottom: 0;
            width: 100%;
            background-color: white;
            text-align: center;
            padding: 10px;
            font-size: 0.85em;
            color: gray;
        }
        </style>

        <div class="footer">
            © 2025 - Hecho por <a href="https://github.com/AlejandroAcebo" target="_blank">Alejandro Acebo</a>
        </div>
        """, unsafe_allow_html=True)


def ir_inicio():
    """
    Cambia de pagina a la pagina de inicio
    """
    st.session_state.page = "inicio"


def ir_evaluacion():
    """
    Cambia de pagina a la pagina de evaluacion
    """
    st.session_state.page = "evaluacion"


def gestion_evolucion_analisis(archivos):
    """
    Gestiona todos los archivos que se suben y recoge los parametros de "Nombre de indicador, Porcentaje y Fecha y hora
    de ejecucion" con el fin de crear una gráfica de líneas en el que cada color es un análisis diferente y en este el
    eje de las X es el nombre del indicador y el eje de las Y es el porcentaje obtenido del análisis. Luego tambien en
    el lateral esta una leyenda con la fecha del analisis.

    Args:
        archivos ([File json]): conjunto de archivos json con resultados de analisis.
    """

    global archivo
    if archivos:
        st.success(f"Se cargaron {len(archivos)} archivos")
        df_final = []
        for archivo in archivos:
            try:
                df = pd.read_json(archivo)
                if {"Nombre de indicador", "Porcentaje", "Fecha y hora de ejecución"}.issubset(df.columns):
                    df_filtrado = df[["Nombre de indicador", "Porcentaje", "Fecha y hora de ejecución"]].copy()

                    fecha_analisis = df_filtrado["Fecha y hora de ejecución"].iloc[0]
                    df_filtrado["Evaluacion"] = f"{fecha_analisis}"
                    df_final.append(df_filtrado)
                else:
                    st.warning(f"{archivo.name} no es correcto o no esta bien estructurado")
            except:
                st.error(f"Error al leer {archivo.name}")

        if df_final:
            df_combinado = pd.concat(df_final, ignore_index=True)

            # Gráfica de líneas
            fig = px.line(
                df_combinado,
                x="Nombre de indicador",
                y="Porcentaje",
                color="Evaluacion",
                markers=True,
                title="Evolución de Indicadores por Fecha de Evaluación"
            )

            fig.update_layout(
                xaxis_title="Nombre de indicador",
                yaxis_title="Porcentaje (%)",
                legend_title="Evaluacion",
                hovermode="x unified"
            )

            st.plotly_chart(fig, use_container_width=True)


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
                                                             "Verification", tipo, tabla, tabla_2, columna)
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
        return resultado
    else:
        st.warning("No se generaron resultados para mostrar.")


def gestion_tipo_test_ui(properties=None, schema_seleccionado=None, spark=None, tabla_seleccionada=None,
                         tablas=None, test_config=None, tipo_analisis=None, url=None, valido=True):

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
                tabla_seleccionada_2 = st.selectbox("Selecciona segunda tabla", tablas)
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

def conectar_bd(tipo, user, password, server, database):
    """
    Conexión de la base de datos con la herramienta.

    Args:
        tipo (str): Tipo de base de datos relacional.
        user (str): Nombre de usuario de la base de datos.
        password (str): Contraseña de usuario de la base de datos.
        server (str): Servidor de la base de datos.
        database (str): Nombre de la base de datos.

    Returns:
        Devuelve spark, la url y propiedades de la base de datos o None en caso de error de conexión.
    """

    tipo = tipo.lower()

    JARS_PATH = "/home/x/Desktop/TFG_Calidad_Datos/DaqLity/jars"

    config_bd = {
        "sqlserver": {
            "driver_class": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "default_port": 1433,
            "jar_file": "mssql-jdbc-12.10.0.jre8.jar",
            "jdbc_url": lambda s, p,
                               d: f"jdbc:sqlserver://{s}:{p};databaseName={d};encrypt=false;trustServerCertificate=true;"
        },
        "postgresql": {
            "driver_class": "org.postgresql.Driver",
            "default_port": 5432,
            "jar_file": "postgresql-42.6.0.jar",
            "jdbc_url": lambda s, p, d: f"jdbc:postgresql://{s}:{p}/{d}"
        },
        "mysql": {
            "driver_class": "com.mysql.cj.jdbc.Driver",
            "default_port": 3306,
            "jar_file": "mysql-connector-java-8.0.33.jar",
            "jdbc_url": lambda s, p, d: f"jdbc:mysql://{s}:{p}/{d}?useSSL=false"
        },
        "mariadb": {
            "driver_class": "org.mariadb.jdbc.Driver",
            "default_port": 3306,
            "jar_file": "mariadb-java-client-3.1.4.jar",
            "jdbc_url": lambda s, p, d: f"jdbc:mariadb://{s}:{p}/{d}"
        },
        "oracle": {
            "driver_class": "oracle.jdbc.OracleDriver",
            "default_port": 1521,
            "jar_file": "ojdbc11-21.9.0.0.jar",
            "jdbc_url": lambda s, p, d: f"jdbc:oracle:thin:@{s}:{p}:{d}"
        }
    }

    if tipo not in config_bd:
        print(f"Tipo de base de datos no soportado: {tipo}")
        return None

    conf = config_bd[tipo]
    port = conf["default_port"]
    jar_path = os.path.join(JARS_PATH, conf["jar_file"])

    if not os.path.exists(jar_path):
        print(f"No se encontró el driver JDBC en: {jar_path}")
        return None

    try:
        spark = (SparkSession.builder
                 .appName(f"{tipo.capitalize()} Connection with PySpark")
                 .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.7-spark-3.5")
                 .config("spark.jars", jar_path)
                 .getOrCreate())

        spark.sparkContext.setLogLevel("WARN")

        url = conf["jdbc_url"](server, port, database)

        properties = {
            "user": user,
            "password": password,
            "driver": conf["driver_class"]
        }

        test_table = {
            "sqlserver": "INFORMATION_SCHEMA.TABLES",
            "postgresql": "pg_catalog.pg_tables",
            "mysql": "information_schema.tables",
            "mariadb": "information_schema.tables",
            "oracle": "ALL_TABLES"
        }.get(tipo)

        if test_table:
            test_df = spark.read.jdbc(url, test_table, properties=properties)
            test_df.limit(1).collect()  # Fuerza la conexión

        return spark, url, properties

    except Exception as e:
        print(f"Error conectando a {tipo}: {e}")
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
        st.header("FORMULARIO CONEXIÓN:")
        st.session_state["opcion_fuente"] = st.selectbox(
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
                tipos_bd = {
                    "SQL Server": "sqlserver",
                    "PostgreSQL": "postgresql",
                    "MySQL": "mysql",
                    "MariaDB": "mariadb",
                    "Oracle": "oracle"
                }
                tipo_mostrar = st.selectbox("Tipo de base de datos", list(tipos_bd.keys()))
                tipo = tipos_bd[tipo_mostrar]
                host = st.text_input("Host", value="localhost")
                user = st.text_input("Usuario")
                password = st.text_input("Contraseña", type="password")
                database = st.text_input("Base de Datos")

                if st.button("Conectar análisis"):
                    conn = conectar_bd(tipo,user, password, host, database)
                    if conn:
                        st.session_state["conn"] = conn
                        st.session_state["conectado_analisis"] = True
                        st.success("Conectado para análisis")
                        st.session_state.page = 'inicio'
                        st.rerun()
                    else:
                        st.error("Error al conectar para análisis")
        # Fuente de datos desde archivo CSV o JSON
        elif opcion_fuente in ["Archivo CSV", "Archivo JSON"]:
            if not st.session_state["conectado_analisis"]:
                if opcion_fuente == "Archivo CSV":
                    archivo = st.file_uploader("Sube un archivo", type=["csv"])
                elif opcion_fuente == "Archivo JSON":
                    archivo = st.file_uploader("Sube un archivo", type=["json"])

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
                        st.session_state.page = 'inicio'
                        st.rerun()
                    else:
                        st.error("Hubo un error al cargar el archivo.")


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
    st.download_button(
        label="📤 Descargar resultados",
        data=json_bytes,
        file_name=file_name,
        mime="application/json",
        use_container_width = True
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
        file_name=nombre_test_calidad,
        mime="application/json",
        use_container_width = True

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
