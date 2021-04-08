"""Lineage Backend

An example DAG demonstrating the usage of DataHub's Airflow lineage backend.
"""

import time
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

try:
    from airflow.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python_operator import PythonOperator

from datahub.integrations.airflow.entities import Dataset

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "execution_timeout": timedelta(minutes=2),
}


def fn():
    with open(
        "/Users/hsheth/projects/datahub/metadata-ingestion/remove_run_log.txt", "a"
    ) as f:
        f.write(f"run at {time.time()}\n")


with DAG(
    "datahub_lineage_backend_demo_remove",
    default_args=default_args,
    description="An example DAG demonstrating the usage of DataHub's Airflow lineage backend.",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=["datahub-ingest"],
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="transform",
        dag=dag,
        python_callable=fn,
        inlets={
            "datasets": [
                Dataset("snowflake", "mydb2.schema.tableA"),
                Dataset("snowflake", "mydb2.schema.tableB"),
            ],
        },
        outlets={"datasets": [Dataset("snowflake", "mydb2.schema.tableC")]},
    )
