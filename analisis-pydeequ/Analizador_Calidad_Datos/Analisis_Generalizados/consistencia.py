import os

from pydeequ import Check, CheckLevel
from pydeequ.verification import VerificationSuite
from pyspark.sql.functions import col

os.environ["SPARK_VERSION"] = "3.5"
from pydeequ.analyzers import AnalysisRunner, Completeness, AnalyzerContext

def analizar_consistencia(spark, df_tabla1, df_tabla2, columna, columna2):

    df_validacion = (df_tabla1.join(df_tabla2, on=columna, how="left")
                     .withColumn("es_correcto",col(columna2).isNotNull()))

    existen = df_validacion.filter(df_validacion.es_correcto==True).count()

    porcentaje_validos = existen/df_tabla1.count()

    check_resultado_tablas = (Check(spark, CheckLevel.Warning
                            , f"Comprobacion consistencia f{df_tabla1} y f{df_tabla2}")
                            .satisfies(f"{porcentaje_validos} >= 0.0"
                            , "Porcentaje de ventas con productos que existen"))

    check_resultado = VerificationSuite(spark).onData(df_tabla1).addCheck(check_resultado_tablas).run()
    return check_resultado