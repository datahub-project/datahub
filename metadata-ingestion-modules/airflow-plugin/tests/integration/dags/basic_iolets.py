from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from datahub_airflow_plugin.entities import Dataset, Urn

with DAG(
    "basic_iolets",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    task = BashOperator(
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
            Urn(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableC,PROD)"
            ),
            Urn("urn:li:dataJob:(urn:li:dataFlow:(airflow,test_dag,PROD),test_task)"),
        ],
        outlets=[
            Dataset("snowflake", "mydb.schema.tableD"),
            Dataset("snowflake", "mydb.schema.tableE"),
        ],
    )
