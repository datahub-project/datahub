from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from datahub_airflow_plugin.entities import Dataset, Urn

with DAG(
    "dag_to_skip",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id="dag_to_skip_task_1",
        dag=dag,
        bash_command="echo 'dag_to_skip_task_1'",
        inlets=[
            Dataset(platform="snowflake", name="mydb.schema.tableA"),
            Urn(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableC,PROD)"
            ),
            Urn("urn:li:dataJob:(urn:li:dataFlow:(airflow,test_dag,PROD),test_task)"),
        ],
        outlets=[Dataset("snowflake", "mydb.schema.tableD")],
    )

    task2 = BashOperator(
        task_id="dag_to_skip_task_2",
        dag=dag,
        bash_command="echo 'dag_to_skip_task_2'",
    )

    task1 >> task2
