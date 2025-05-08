import os

from pyspark.sql.functions import concat, col, lit, current_timestamp


os.environ["SPARK_VERSION"] = "3.5"
from Analisis_Generalizados.consistencia import analizar_consistencia
from Analisis_Generalizados.credibilidad import analizar_credibilidad
from Analisis_Generalizados.exactitud import analizar_exactitud
from Analisis_Generalizados.precision import analizar_precision
from Analisis_Generalizados.completitud import analizar_completitud
from Analisis_Generalizados.actualidad import analizar_actualidad


from pydeequ.verification import VerificationResult
from pydeequ.analyzers import AnalyzerContext
import streamlit as st
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
        print(f"❌ Error de conexión: {e}")
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
        tipo_credibilidad, num_decimales,df,schema_guardar, tabla_guardar, tiempo_limite

    # 1.Datos de conexión
    if "conectado_analisis" not in st.session_state:
        st.session_state["conectado_analisis"] = False

    if "conectado_guardado" not in st.session_state:
        st.session_state["conectado_guardado"] = False

    # Título principal
    st.title("Analizador calidad de datos")

    # Primera conexión a base de datos, si falla se indicará que las credenciales no son correctas.
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
                st.sidebar.success("✅ Conectado para análisis")
            else:
                st.sidebar.error("❌ Error al conectar para análisis")

    # Este segundo formulario aparece en el momento en el que la conexión con la base de datos a analizar ha
    # sido correcto
    if st.session_state["conectado_analisis"]:
        st.sidebar.subheader("💾 Guardar resultados")

        host_guardar = st.sidebar.text_input("Host", value="localhost", key="host_guardar")
        user_guardar = st.sidebar.text_input("Usuario", key="user_guardar")
        password_guardar = st.sidebar.text_input("Contraseña", type="password", key="pass_guardar")
        database_guardar = st.sidebar.text_input("Base de Datos", key="db_guardar")

        # Se comprueba que al darle a guardar es posible conectar a esa base de datos y que además no esta vació
        # el dataframe a guardar para no
        if st.sidebar.button("Conectar para guardar"):
            conn_guardar = conectar_bd(user_guardar, password_guardar, host_guardar, database_guardar)
            if conn_guardar:
                st.session_state["conn_guardar"] = conn_guardar
                st.session_state["conectado_guardado"] = True
                st.sidebar.success("✅ Conexión de guardado exitosa")
            else:
                st.sidebar.error("❌ Error en la conexión para guardar")

    if st.session_state.get("conectado_guardado"):
        spark_guardar, url_guardar, props_guardar = st.session_state["conn_guardar"]

        # Desplegable de esquemas
        try:
            schemas = listar_schemas(spark_guardar, url_guardar, props_guardar)
            schema_guardar = st.sidebar.selectbox("Selecciona el esquema", schemas)

            # Desplegable de tablas dentro del esquema
            tablas = listar_tablas(spark_guardar, url_guardar, props_guardar, schema_guardar)
            tabla_guardar = st.sidebar.selectbox("Selecciona la tabla", tablas)

            # Botón para guardar la selección en el estado
            if st.sidebar.button("Guardar selección"):
                st.session_state["schema_guardar"] = schema_guardar
                st.session_state["tabla_guardar"] = tabla_guardar
                st.sidebar.success(f"Esquema y tabla guardados: {schema_guardar}.{tabla_guardar}")

        except Exception as e:
            st.sidebar.error(f"❌ Error cargando esquemas/tablas: {e}")

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
            ,"Exactitud","Precision","Actualidad"])

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
                st.caption("Ejemplo sintáctica, patron: ^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")
                st.caption("Ejemplo semántica, valores: Main Office,Shipping")

            case"Precision":
                num_decimales = st.text_input("Introduce la cantidad de decimales que debe tener la columna,"
                                                  "solo número entero")

            case "Consistencia":
                tablas_opciones = [t for t in tablas if t != tabla_seleccionada]
                tabla_seleccionada_2 = st.selectbox("Selecciona segunda tabla", tablas_opciones)

                columnas_opciones = listar_columnas(spark, url, properties, f"{schema_seleccionado}.{tabla_seleccionada_2}")
                columna_2 = st.selectbox("Selecciona la segunda columna", columnas_opciones)

            case "Actualidad":
                tiempo_limite = st.text_input("Introduce la fecha maxima que deberia tener la columna")
                st.caption("Un ejemplo sería: ")

        # 3.Ejecuion del analisis seleccionado
        if st.button("Ejecutar Análisis"):
            df = spark.read.jdbc(url=url, table=schema_seleccionado + "." +tabla_seleccionada, properties=properties)
            match tipo_analisis:
                case "Completitud":
                    print(spark.read,df.count(),columna)
                    resultado = analizar_completitud(spark, df, columna)
                    df = AnalyzerContext.successMetricsAsDataFrame(spark, resultado)
                    df = creacion_dataframe_analyzer(spark, df)
                    st.dataframe(df.toPandas())


                case "Exactitud":
                    resultado = analizar_exactitud(spark, df, columna, patron, tipo_exactitud)
                    df = AnalyzerContext.successMetricsAsDataFrame(spark, resultado)
                    creacion_dataframe_analyzer(spark, df)
                    df = creacion_dataframe_analyzer(spark, df)
                    st.dataframe(df.toPandas())


                case "Credibilidad":
                    print(spark.read, df.count(), columna)
                    resultado = analizar_credibilidad(spark, df, columna,patron,tipo_credibilidad)
                    df = AnalyzerContext.successMetricsAsDataFrame(spark, resultado)
                    creacion_dataframe_analyzer(spark, df)
                    df = creacion_dataframe_analyzer(spark, df)
                    st.dataframe(df.toPandas())


                case "Precision":
                    resultado = analizar_precision(spark, df, columna,num_decimales)
                    df = AnalyzerContext.successMetricsAsDataFrame(spark, resultado)
                    creacion_dataframe_analyzer(spark, df)
                    df = creacion_dataframe_analyzer(spark, df)


                case "Consistencia":
                    df_2 = spark.read.jdbc(url=url, table=schema_seleccionado + "." +tabla_seleccionada_2, properties=properties)
                    resultado = analizar_consistencia(spark,df,df_2,columna, columna_2)
                    df = VerificationResult.successMetricsAsDataFrame(spark, resultado)
                    df = creacion_dataframe_analyzer(spark, df)
                    st.dataframe(df.toPandas())

                case "Actualidad":
                    resultado = analizar_actualidad(spark, df, columna,tiempo_limite)
                    df = VerificationResult.successMetricsAsDataFrame(spark, resultado)
                    df = creacion_dataframe_analyzer(spark, df)
                    st.dataframe(df.toPandas())

            
            st.session_state["df_resultado"] = df

        # Mostrar botón para guardar resultado
        if st.button("💾 Guardar resultado"):
            df = st.session_state.get("df_resultado")
            # Verificar si se ha seleccionado un esquema y una tabla
            if not st.session_state.get("schema_guardar") or not st.session_state.get("tabla_guardar"):
                st.sidebar.warning("Debes seleccionar un esquema y una tabla antes de guardar los datos.")
            elif df is not None and not df.rdd.isEmpty():

                # Comprobar si el DataFrame no está vacío
                if st.session_state.get("conectado_guardado"):
                    try:

                        registro_bd(df, st)
                    except Exception as e:
                        st.error(f"Error al guardar los datos: {e}")

                else:
                    st.warning("No hay conexión activa para guardar los datos.")

            else:
                st.warning("El DataFrame está vacío. No se puede guardar.")



def registro_bd(df, st):
    try:
        # Obtener parámetros de conexión
        spark_guardar, url_guardar, props_guardar = st.session_state["conn_guardar"]

        # Obtener tabla y esquema guardados del estado
        schema_guardar = st.session_state.get("schema_guardar")
        tabla_guardar = st.session_state.get("tabla_guardar")


        tabla_guardar_bd = f"{schema_guardar}.{tabla_guardar}"

        # Escribir el DataFrame
        df.write.jdbc(
            url=url_guardar,
            table=tabla_guardar_bd,
            mode="append",
            properties=props_guardar
        )
        st.success(f"Datos guardados correctamente en la tabla: {tabla_guardar_bd}")

    except Exception as e:
        st.error(f"Error al guardar los datos: {e}")


# Metodo que añade porcentaje, fecha y hora y cambia el nombre de las columnas que no tiene sentido el nombre
def creacion_dataframe_analyzer(spark,df):
    df = df.withColumn(
        "Porcentaje",
        concat((col("value") * 100).cast("int").cast("String"), lit("%"))
    )
    df = df.withColumn("Fecha & Hora", current_timestamp())
    df = (df.withColumnRenamed("entity","Tipo test")
          .withColumnRenamed("instance","Columna/Tabla")
          .withColumnRenamed("name","Tipo test PyDeequ")
          .withColumnRenamed("value","Valor"))
    st.write("Resultado de la validación:")
    return df


def main():
    ui()

if __name__ == "__main__":
    main()