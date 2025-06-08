import os
os.environ["SPARK_VERSION"] = "3.5"

from pydeequ import Check, CheckLevel
from pydeequ.verification import VerificationSuite

def analizar_actualidad(spark, df, columna, fecha_limite,tabla):

    umbral = 0.8
    check_resultado_tablas = (
        Check(spark, CheckLevel.Warning, "Validación fecha")
        .satisfies(f"{columna} >= TIMESTAMP('{fecha_limite}')", f"Fecha válida en la {tabla} y columna {columna}",
                   lambda x: x >= umbral)
    )

    check_resultado = VerificationSuite(spark).onData(df).addCheck(check_resultado_tablas).run()
    return check_resultado

