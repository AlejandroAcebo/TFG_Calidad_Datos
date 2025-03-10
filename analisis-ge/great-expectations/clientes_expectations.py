#!/usr/bin/env python
# coding: utf-8
# In[1]:
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.checkpoint import UpdateDataDocsAction

context = gx.get_context(mode="file")

# In[2]:

## Connect to your data

PG_CONNECTION_STRING = "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/postgres"
pg_datasource = context.data_sources.add_postgres(name="pg_datasource", connection_string=PG_CONNECTION_STRING)
asset = pg_datasource.add_table_asset(name="pedidos_data", table_name="clientes")
bd = asset.add_batch_definition_whole_table("BD")

# In[3]:

## Create Expectations
suite = context.suites.add(gx.ExpectationSuite("Suite"))
vd = gx.ValidationDefinition(
    name="Validation Definition",
    data=bd,
    suite=suite
)

context.validation_definitions.add(vd)

## Completitud
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="id_cliente",
                                                        result_format="COMPLETE",
                                                        meta={"Notas": "Deberías revisar la columna id del cliente."}))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="nombre",result_format="COMPLETE"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="email",result_format="COMPLETE"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="telefono",
                                                        result_format="COMPLETE",
                                                        meta={"Notas": "Deberías revisar la columna telefono."}))

## Precision semantica ##
## Email cumple el formato 'letras@letras.letras'
suite.add_expectation(gxe.ExpectColumnValuesToNotMatchLikePattern(
    column="email",
    like_pattern=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
    result_format="COMPLETE"
))

## Telefono cumple el formato 'XXXX-XXXX' siendo X numeros
suite.add_expectation(gxe.ExpectColumnValuesToNotMatchLikePattern(
    column="telefono",
    like_pattern=r"^\d{3,4}-\d{4}$",  # acepta entre 3 o 4 dígitos antes del guion
    result_format="COMPLETE"
))


## Violacion de dominio
suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
    column="numero_secuencia",
    min_value=1,
    max_value= None,
    strict_min=True,
    strict_max=False,
    result_format="COMPLETE"
))

## Violacion de restricciones
## Comprobar que un atributo no nulo tiene un campo nulo
suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
    column="numero_secuencia",
    min_value=1,
    max_value= None,
    strict_min=True,
    strict_max=False,
    result_format="COMPLETE"
))

## Violacion valor unico
suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(
    column="dni",result_format="COMPLETE"))

## Pendiente de revision porque esto no funciona exactamente como deberia
## Tuplas aproximadamente duplicadas
suite.add_expectation(gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(
    column_list=["nombre", "email", "telefono", "cp", "poblacion"],
    mostly=0.80
))

## Filas con valores no esperados
valores_aceptados = ["normal", "vip"]

suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(
    column="tipo_cliente",
    value_set=valores_aceptados
))

# suite.add_expectation(gxe.ExpectColumnValuesToBeOver18(
#     column="fecha_nacimiento"
# ))


# In[4]:

from sqlalchemy import create_engine
import pandas as pd

engine = create_engine(PG_CONNECTION_STRING)

## Violacion integridad referencial
## Esto deberia revisarlo porque esto no va bien
query = """SELECT cp, poblacion 
FROM clientes
GROUP BY cp, poblacion
HAVING COUNT(DISTINCT poblacion) = 1"""

valid_pairs = pd.read_sql(query,engine)

valid_pairs = valid_pairs.drop_duplicates(subset=['cp'], keep='first')

valid_list = list(valid_pairs.itertuples(index = False, name = None))

## Esto tendria que ser not to be in set, pero no existe
suite.add_expectation(gxe.ExpectColumnPairValuesToBeInSet(
    column_A="cp",
    column_B="poblacion",
    value_pairs_set= valid_list,
    mostly=1.0
))


## Violacion integridad referencial
## Por algun motivo falla
# query = """SELECT id_cliente FROM clientes"""
#
# valid_clients_id = pd.read_sql(query,engine)
#
# suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(
#     column="id_cliente",
#     value_set=valid_clients_id
# ))




# In[6]:

## Validate your data
checkpoint = context.checkpoints.add(gx.Checkpoint(
    name="Checkpoint",
    validation_definitions=[vd],
    actions=[
        UpdateDataDocsAction(name="update_data_docs")
    ]
))

## Complete is for show all the data that fail the tests
checkpoint_result = checkpoint.run()




