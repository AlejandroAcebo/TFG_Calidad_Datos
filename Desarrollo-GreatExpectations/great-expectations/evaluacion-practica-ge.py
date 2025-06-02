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

# Completitud
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="id_cliente", result_format="COMPLETE"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="nombre", result_format="COMPLETE"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="email", result_format="COMPLETE"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="telefono", result_format="COMPLETE"))

# Credibilidad
suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(
    column="email",
    regex=r"^[\w\.-]+@[a-zA-Z]+\.[a-zA-Z]+$",
    mostly=1.0,
    result_format="COMPLETE"
))

# Consistencia, violación de dominio
suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
    column="numero_secuencia",
    min_value=2,  # Strict min > 1
    max_value=None,
    result_format="COMPLETE"
))

valores_aceptados = ["normal", "vip"]
suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(
    column="tipo_cliente",
    value_set=valores_aceptados,
    mostly=1.0,
    result_format="COMPLETE"
))

# Consistencia violación de restricción
# No hay ningún metodo en great-expectations que permite comparar fechas ni con sql, tampoco funciona la creacion
# de test personalizados

# Consistencia violación de valor único
suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="dni", result_format="COMPLETE"))


# Exactitud
suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(
    column="telefono",
    regex=r"^\d{9,}$",
    mostly=1.0,
    result_format="COMPLETE"
))

# Consistencia, violación integridad referencial
# No hay ningún metodo en great-expectations que permita hacer comparaciones entre tablas, tampoco funciona la creacion
# de test personalizados

# Crear y ejecutar el checkpoint
checkpoint = context.checkpoints.add(gx.Checkpoint(
    name="clientes_checkpoint",
    validation_definitions=[vd],
    actions=[UpdateDataDocsAction(name="update_data_docs")]
))

result = checkpoint.run()

print(result)
