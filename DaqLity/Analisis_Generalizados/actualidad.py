import os

from pyspark.sql.functions import col, to_date, to_timestamp

os.environ["SPARK_VERSION"] = "3.5"

from pydeequ import Check, CheckLevel
from pydeequ.verification import VerificationSuite

def analizar_actualidad(spark, df, columna, fecha_limite,tabla,tipo_actualidad):

    check_resultado_tablas = None
    if tipo_actualidad == "Fecha":
        # Filtra valores que tengan exactamente el formato dd/MM/yyyy (sin hora)
        formato_ddMMyyyy = df.filter(col(columna).rlike(r"^\d{2}/\d{2}/\d{4}$"))
        if formato_ddMMyyyy.count() > 0:
            df = df.withColumn(columna, to_date(col(columna), "dd/MM/yyyy"))
        check_resultado_tablas = (
            Check(spark, CheckLevel.Warning, "Validación fecha")
            .satisfies(
                f"to_date({columna}) >= DATE('{fecha_limite}')",
                f"Fecha válida en la {tabla} y columna {columna}",
                lambda x: x >= 1.0
            )
        )
    elif tipo_actualidad == "Fecha y hora":
        formato_ddMMyyyy_hhmmss = df.filter(
            col(columna).rlike(r"^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}$")
        )
        if formato_ddMMyyyy_hhmmss.count() > 0:
            df = df.withColumn(columna, to_timestamp(col(columna), "dd/MM/yyyy HH:mm:ss"))

        check_resultado_tablas = (
            Check(spark, CheckLevel.Warning, "Validación fecha")
            .satisfies(f"{columna} >= TIMESTAMP('{fecha_limite}')",
                       f"Fecha válida en la {tabla} y columna {columna}",
                       lambda x: x >= 1.0)
        )

    check_resultado = VerificationSuite(spark).onData(df).addCheck(check_resultado_tablas).run()
    return check_resultado

