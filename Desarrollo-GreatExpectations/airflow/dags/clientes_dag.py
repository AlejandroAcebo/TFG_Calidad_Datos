from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import great_expectations as gx

def run_gx_checkpoint():
    context = gx.get_context(mode="file", project_root_dir="../../great-expectations/")
    context.checkpoints.get("Checkpoint").run()

with DAG(
    dag_id='clientes_dag',
    start_date=datetime(2023, 11, 10),
    schedule_interval=None
) as dag:

    gx_run_audit = PythonOperator(
        task_id="gx_run_audit",
        python_callable=run_gx_checkpoint
    )

gx_run_audit

