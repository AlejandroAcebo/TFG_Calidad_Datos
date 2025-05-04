import os
os.environ["SPARK_VERSION"] = "3.5"
from pydeequ.analyzers import AnalysisRunner, PatternMatch, Compliance


def analizar_exactitud(spark,df,column,patron,tipo):
    global resultado
    # Esto es para prevenir que haya espacios invisibles
    patron = patron.strip()
    if tipo == "Sintactica":
        resultado = (
            AnalysisRunner(spark)
            .onData(df)
            .addAnalyzer(PatternMatch(column,patron))
            .run()
        )
    if tipo == "Semantica":
        lista_valores = [x.strip() for x in patron.split(",") if x.strip()]
        condicion = f"{column} IN ({', '.join([f'\'{x}\'' for x in lista_valores])})"
        resultado = (
            AnalysisRunner(spark)
            .onData(df)
            .addAnalyzer(Compliance(column, condicion))
            .run()
        )

    return resultado