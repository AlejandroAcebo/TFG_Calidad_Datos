import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.checkpoint import UpdateDataDocsAction

context = gx.get_context()

# Configurar datasource para Postgres
PG_CONNECTION_STRING = "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/postgres"
pg_datasource = context.data_sources.add_postgres(name="pg_datasource", connection_string=PG_CONNECTION_STRING)

# Definir el asset (tabla clientes)
clientes_asset = pg_datasource.add_table_asset(name="clientes_asset", table_name="clientes")
batch = clientes_asset.add_batch_definition_whole_table("clientes_batch")

# Crear suite y validation definition
suite = context.suites.add(gx.ExpectationSuite("clientes_suite"))
vd = gx.ValidationDefinition(
    name="clientes_validation_definition",
    data=batch,
    suite=suite
)
context.validation_definitions.add(vd)

# Agregar expectations similares a tu pydeequ:

# 1) Completitud
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="id_cliente", result_format="COMPLETE"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="nombre", result_format="COMPLETE"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="email", result_format="COMPLETE"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="telefono", result_format="COMPLETE"))

# 2) Formato email y teléfono con regex
suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(
    column="email",
    regex=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
    mostly=1.0,
    result_format="COMPLETE"
))

suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(
    column="telefono",
    regex=r"^\d{3,4}-\d{4}$",
    mostly=1.0,
    result_format="COMPLETE"
))

# 3) Condición personalizada para fechas (fecha_registro <= fecha_baja o fecha_baja es NULL)
# Como Great Expectations no tiene expect_satisfies, podemos usar expect_select_column_values_to_be_in_set
# O crear expectation custom. Aquí usamos "expect_column_values_to_be_between" no aplica directamente,
# Por eso, podemos usar expect_sql_query para validar esta regla:

query_fecha = """
SELECT COUNT(*) AS invalid_count
FROM clientes
WHERE fecha_baja IS NOT NULL AND fecha_registro > fecha_baja
"""

suite.add_expectation(gxe.ExpectQueryToReturnRows(
    query=query_fecha,
    result_format="COMPLETE",
    meta={"Notas": "fecha_registro debe ser menor o igual que fecha_baja cuando esta no es NULL"},
    condition=lambda rows: rows.collect()[0]["invalid_count"] == 0
))

# 4) Valor único DNI
suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="dni", result_format="COMPLETE"))

# 5) Valores permitidos para tipo_cliente
valores_aceptados = ["normal", "vip"]
suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(
    column="tipo_cliente",
    value_set=valores_aceptados,
    mostly=1.0,
    result_format="COMPLETE"
))

# 6) Valor de numero_secuencia > 1
suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
    column="numero_secuencia",
    min_value=2,  # Strict min > 1
    max_value=None,
    result_format="COMPLETE"
))

# 7) Integridad referencial para pares (cp, poblacion)
# Primero obtenemos la lista válida con pandas y sqlalchemy

from sqlalchemy import create_engine
import pandas as pd

engine = create_engine(PG_CONNECTION_STRING)

query = """
SELECT cp, poblacion
FROM clientes
GROUP BY cp, poblacion
HAVING COUNT(DISTINCT poblacion) = 1
"""

valid_pairs_df = pd.read_sql(query, engine)
valid_pairs = list(valid_pairs_df.itertuples(index=False, name=None))

suite.add_expectation(gxe.ExpectColumnPairValuesToBeInSet(
    column_A="cp",
    column_B="poblacion",
    value_pairs_set=valid_pairs,
    mostly=1.0,
    result_format="COMPLETE"
))

# Crear y ejecutar el checkpoint
checkpoint = context.checkpoints.add(gx.Checkpoint(
    name="clientes_checkpoint",
    validation_definitions=[vd],
    actions=[UpdateDataDocsAction(name="update_data_docs")]
))

result = checkpoint.run()

print(result)
