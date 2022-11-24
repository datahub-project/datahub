"""Generic DataHub Ingest via Recipe

This example demonstrates how to load any configuration file and run a
DataHub ingestion pipeline within an Airflow DAG.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datahub.configuration.config_loader import load_config_file
from datahub.ingestion.run.pipeline import Pipeline

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jdoe@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=120),
}


def datahub_recipe():
    config = load_config_file("path/to/recipe.yml")

    pipeline = Pipeline.create(config)
    pipeline.run()
    pipeline.raise_from_status()


with DAG(
    "datahub_ingest_using_recipe",
    default_args=default_args,
    description="An example DAG which runs a DataHub ingestion recipe",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    catchup=False,
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_using_recipe",
        python_callable=datahub_recipe,
    )
