"""MySQL DataHub Ingest DAG

This example demonstrates how to ingest metadata from MySQL into DataHub
from within an Airflow DAG. Note that the DB connection configuration is
embedded within the code.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator


def ingest_from_mysql():
    from datahub.ingestion.run.pipeline import Pipeline

    pipeline = Pipeline.create(
        # This configuration is analogous to a recipe configuration.
        {
            "source": {
                "type": "mysql",
                "config": {
                    # If you want to use Airflow connections, take a look at the snowflake_sample_dag.py example.
                    "username": "user",
                    "password": "pass",
                    "database": "db_name",
                    "host_port": "localhost:3306",
                },
            },
            "sink": {
                "type": "datahub-rest",
                "config": {"server": "http://localhost:8080"},
            },
        }
    )
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status()


with DAG(
    "datahub_mysql_ingest",
    default_args={
        "owner": "airflow",
    },
    description="An example DAG which ingests metadata from MySQL to DataHub",
    start_date=datetime(2022, 1, 1),
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    # While it is also possible to use the PythonOperator, we recommend using
    # the PythonVirtualenvOperator to ensure that there are no dependency
    # conflicts between DataHub and the rest of your Airflow environment.
    ingest_task = PythonVirtualenvOperator(
        task_id="ingest_from_mysql",
        requirements=[
            "acryl-datahub[mysql]",
        ],
        system_site_packages=False,
        python_callable=ingest_from_mysql,
    )
