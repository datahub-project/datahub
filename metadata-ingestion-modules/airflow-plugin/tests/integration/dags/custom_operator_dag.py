from datetime import timedelta, datetime

from airflow import DAG
from custom_operator import CustomOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email": ["jdoe@example.com"],
    "email_on_failure": False,
    "execution_timeout": timedelta(minutes=5),
}


with DAG(
    "custom_operator_dag",
    default_args=default_args,
    description="An example dag with custom operator",
    schedule_interval=None,
    tags=["example_tag"],
    catchup=False,
    default_view="tree",
) as dag:
    custom_task = CustomOperator(
        task_id="custom_task_id",
        name="custom_name",
        dag=dag,
    )
