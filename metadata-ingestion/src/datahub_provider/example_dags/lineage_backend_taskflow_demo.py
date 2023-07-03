"""Lineage Backend

An example DAG demonstrating the usage of DataHub's Airflow lineage backend using the TaskFlow API.
"""

from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from datahub_provider.entities import Dataset, Urn

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jdoe@example.com"],
    "email_on_failure": False,
    "execution_timeout": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    description="An example DAG demonstrating the usage of DataHub's Airflow lineage backend using the TaskFlow API.",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=["example_tag"],
    catchup=False,
)
def datahub_lineage_backend_taskflow_demo():
    @task(
        inlets=[
            Dataset("snowflake", "mydb.schema.tableA"),
            Dataset("snowflake", "mydb.schema.tableB", "DEV"),
            # You can also put dataset URNs in the inlets/outlets lists.
            Urn(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableC,PROD)"
            ),
        ],
        outlets=[Dataset("snowflake", "mydb.schema.tableD")],
    )
    def run_data_task():
        # This is where you might run your data tooling.
        pass

    run_data_task()


datahub_lineage_backend_taskflow_dag = datahub_lineage_backend_taskflow_demo()
