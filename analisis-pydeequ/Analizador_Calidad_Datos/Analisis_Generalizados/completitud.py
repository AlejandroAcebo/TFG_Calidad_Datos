import os
os.environ["SPARK_VERSION"] = "3.5"
from pydeequ.analyzers import AnalysisRunner, Completeness, AnalyzerContext


def analizar_completitud(spark, df, columna):

    resultado = (
        AnalysisRunner(spark)
        .onData(df)
        .addAnalyzer(Completeness(columna))
        .run()
    )
    return resultado