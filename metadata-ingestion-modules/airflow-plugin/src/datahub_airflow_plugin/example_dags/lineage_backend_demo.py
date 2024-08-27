"""Lineage Backend

An example DAG demonstrating the usage of DataHub's Airflow lineage backend.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from datahub_airflow_plugin.entities import Dataset, Urn

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jdoe@example.com"],
    "email_on_failure": False,
    "execution_timeout": timedelta(minutes=5),
}


with DAG(
    "datahub_lineage_backend_demo",
    default_args=default_args,
    description="An example DAG demonstrating the usage of DataHub's Airflow lineage backend.",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=["example_tag"],
    catchup=False,
    default_view="tree",
) as dag:
    task1 = BashOperator(
        task_id="run_data_task",
        dag=dag,
        bash_command="echo 'This is where you might run your data tooling.'",
        inlets=[
            Dataset(platform="snowflake", name="mydb.schema.tableA"),
            Dataset(platform="snowflake", name="mydb.schema.tableB", env="DEV"),
            Dataset(
                platform="snowflake",
                name="mydb.schema.tableC",
                platform_instance="cloud",
            ),
            # You can also put dataset URNs in the inlets/outlets lists.
            Urn(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableC,PROD)"
            ),
            Urn("urn:li:dataJob:(urn:li:dataFlow:(airflow,dag1,prod),task1)"),
        ],
        outlets=[Dataset("snowflake", "mydb.schema.tableD")],
    )
