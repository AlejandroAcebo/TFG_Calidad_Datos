import os

from pyspark.sql.functions import col

os.environ["SPARK_VERSION"] = "3.5"
from pydeequ.analyzers import AnalysisRunner, PatternMatch


def analizar_precision(spark,df,column,decimales):
    # La columna es un numero, asi que se crea una nueva columna que almacenara el numero en string
    columna_string = f"{column}_string"
    df = df.withColumn(columna_string, col(column).cast("string"))
    decimal_pattern = rf'^\d+\.\d{{{decimales},}}$'

    resultado = (
        AnalysisRunner(spark)
        .onData(df)
        .addAnalyzer(PatternMatch(columna_string,decimal_pattern).withTag(""))
        .run()
    )
    return resultado