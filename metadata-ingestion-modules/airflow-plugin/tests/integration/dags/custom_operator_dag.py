from airflow import DAG
from custom_operator import CustomOperator

default_args = {
    "owner": "airflow",
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
    custom_task = CustomOperator(task_id="custom_task_id", name="custom_name")
