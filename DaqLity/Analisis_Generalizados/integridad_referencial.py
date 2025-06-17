import os
os.environ["SPARK_VERSION"] = "3.5"

from pydeequ import Check, CheckLevel
from pydeequ.verification import VerificationSuite
from pyspark.sql.functions import col

from pydeequ.analyzers import AnalysisRunner, Completeness, AnalyzerContext

def analizar_integridad_referencial(spark, df_tabla1, df_tabla2, columna, columna2):

    df1 = df_tabla1.alias("t1")
    df2 = df_tabla2.alias("t2")

    df_validacion = (
        df1.join(df2, col(f"t1.{columna}") == col(f"t2.{columna}"), how="left")
        .withColumn("es_correcto", col(f"t2.{columna2}").isNotNull())
    )

    existen = df_validacion.filter(df_validacion.es_correcto==True).count()

    porcentaje_validos = existen/df_tabla1.count()

    check_resultado_tablas = (Check(spark, CheckLevel.Warning
                            , f"Comprobacion consistencia f{df_tabla1} y f{df_tabla2}")
                            .satisfies(f"{porcentaje_validos} >= 0.0"
                            , "Integridad referencial"))

    check_resultado = VerificationSuite(spark).onData(df_tabla1).addCheck(check_resultado_tablas).run()
    return check_resultado