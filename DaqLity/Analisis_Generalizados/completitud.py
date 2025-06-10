import os
os.environ["SPARK_VERSION"] = "3.5"
from pydeequ.analyzers import AnalysisRunner, Completeness, AnalyzerContext
from pyspark.sql import functions as F

def analizar_completitud(spark, df, columna):

    resultado = (
        AnalysisRunner(spark)
        .onData(df)
        .addAnalyzer(Completeness(columna))
        .run()
    )
    return resultado