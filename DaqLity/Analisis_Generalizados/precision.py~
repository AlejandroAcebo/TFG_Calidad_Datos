import os

os.environ["SPARK_VERSION"] = "3.5"
from pyspark.sql.functions import col, trim
from pydeequ.analyzers import AnalysisRunner, PatternMatch


def analizar_precision(spark,df,column,decimales):
    # La columna es un numero, asi que se crea una nueva columna que almacenara el numero en string
    columna_string = f"{column}_string"
    df = df.withColumn(columna_string, col(column).cast("string"))
    df.select(column, columna_string).show(truncate=False)
    if decimales == "0":
        decimal_pattern = r"^[0-9]+$"
    else:
        decimal_pattern = rf'^\d+\.\d{{{decimales},}}$'

    resultado = (
        AnalysisRunner(spark)
        .onData(df)
        .addAnalyzer(PatternMatch(columna_string,decimal_pattern))
        .run()
    )
    return resultado