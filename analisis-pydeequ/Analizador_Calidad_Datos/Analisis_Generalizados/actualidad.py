import os
os.environ["SPARK_VERSION"] = "3.5"

from pydeequ import Check, CheckLevel
from pydeequ.verification import VerificationSuite

def analizar_actualidad(spark, df, columna, fecha_limite):

    umbral = 0.8
    # Crear el check para Address
    check_resultado_tablas = (
        Check(spark, CheckLevel.Warning, "Validación Address ModifiedDate")
        .satisfies(f"{columna} >= TIMESTAMP('{fecha_limite}')", "Fecha válida address",
                   lambda x: x >= umbral)
    )
    check_resultado = VerificationSuite(spark).onData(df).addCheck(check_resultado_tablas).run()
    return check_resultado

