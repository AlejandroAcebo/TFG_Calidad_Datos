import os
os.environ["SPARK_VERSION"] = "3.5"
from pydeequ.analyzers import AnalysisRunner, PatternMatch, Compliance
from pyspark.sql import functions as F

def analizar_credibilidad(spark,df,column,patron,tipo):
    global resultado
    # Esto es para prevenir que haya espacios invisibles
    patron = patron.strip()
    if (tipo == "Patron"):
        resultado = (
            AnalysisRunner(spark)
            .onData(df)
            .addAnalyzer(PatternMatch(column,patron))
            .run()
        )
    elif (tipo == "Conjunto valores"):
        lista_valores = [x.strip() for x in patron.split(",") if x.strip()]
        # Se ponen como valores independientes con una coma a modo de separacion
        valores_sql = ", ".join([f"'{val}'" for val in lista_valores])
        condicion = f"{column} IN ({valores_sql})"
        resultado = (
            AnalysisRunner(spark)
            .onData(df)
            .addAnalyzer(Compliance("Validos", condicion))
            .run()
        )
    return resultado