import os

from pyspark.sql.functions import col, to_date

os.environ["SPARK_VERSION"] = "3.5"

from pydeequ import Check, CheckLevel
from pydeequ.verification import VerificationSuite

def analizar_actualidad(spark, df, columna, fecha_limite,tabla):

    formato_ddMMyyyy = df.filter(col("BoePublicationDate").rlike(r"^\d{2}/\d{2}/\d{4}$"))
    if formato_ddMMyyyy.count() > 0:
        df = df.withColumn(f"{columna}", to_date(col(f"{columna}"), "dd/MM/yyyy"))

    check_resultado_tablas = (
        Check(spark, CheckLevel.Warning, "Validación fecha")
        .satisfies(
            f"to_date({columna}) >= DATE('{fecha_limite}')",
            f"Fecha válida en la {tabla} y columna {columna}",
            lambda x: x >= 1.0
        )
    )

    check_resultado = VerificationSuite(spark).onData(df).addCheck(check_resultado_tablas).run()
    return check_resultado

