"""Lineage Backend

An example DAG demonstrating the usage of DataHub's Airflow lineage backend using the TaskFlow API.

This is the Airflow 3.0+ version.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task  # type: ignore[attr-defined]

from datahub_airflow_plugin.entities import Dataset, Urn

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
    start_date=datetime(2023, 1, 1),
    schedule=timedelta(days=1),
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
            Urn("urn:li:dataJob:(urn:li:dataFlow:(airflow,dag1,prod),task1)"),
        ],
        outlets=[Dataset("snowflake", "mydb.schema.tableD")],
    )
    def run_data_task():
        # This is where you might run your data tooling.
        pass

    run_data_task()


datahub_lineage_backend_taskflow_dag = datahub_lineage_backend_taskflow_demo()
