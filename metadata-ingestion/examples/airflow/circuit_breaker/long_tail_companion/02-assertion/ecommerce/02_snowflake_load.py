import pendulum
from airflow.models import DAG
from airflow.operators.bash import BashOperator

from datahub.api.graphql.operation import Operation
from datahub_provider.entities import Dataset
from datahub_provider.hooks.datahub import DatahubRestHook

dag = DAG(
    dag_id="snowflake_load",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval="0 0 * * *",
    catchup=False,
)


def report_operation(context):
    hook: DatahubRestHook = DatahubRestHook("datahub_longtail")
    host, password, timeout_sec = hook._get_config()
    reporter = Operation(datahub_host=host, datahub_token=password, timeout=timeout_sec)
    task = context["ti"].task
    for inlet in task._outlets:
        reporter.report_operation(urn=inlet.urn, operation_type="INSERT")


pet_profiles_load = BashOperator(
    task_id="load_s3_adoption_pet_profiles",
    dag=dag,
    inlets=[Dataset("s3", "longtail-core-data/mongo/adoption/pet_profiles")],
    outlets=[Dataset("snowflake", "long_tail_companions.adoption.pet_profiles")],
    bash_command="echo Dummy Task",
    on_success_callback=report_operation,
)

# Simple bash command as example to load great expectation tests
run_ge_tests = BashOperator(
    task_id="pet_profiles_ge_tests_run",
    inlets=[Dataset("snowflake", "long_tail_companions.adoption.pet_profiles")],
    bash_command="echo /usr/local/airflow/.local/bin/great_expectations checkpoint run pet_profiles",
)

pet_profiles_load.set_downstream(run_ge_tests)
