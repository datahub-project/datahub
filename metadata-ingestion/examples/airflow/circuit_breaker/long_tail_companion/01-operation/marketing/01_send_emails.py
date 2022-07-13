import datetime

import pendulum
from airflow.models import DAG
from airflow.operators.bash import BashOperator

from datahub_provider.entities import Dataset
from datahub_provider.operators.datahub_operation_sensor import (
    DataHubOperationCircuitBreakerSensor,
)

dag = DAG(
    dag_id="marketing-send_emails",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval="0 0 * * *",
    catchup=False,
)

# New DataHub Operation Circuit Breaker Sensor
pet_profiles_operation_sensor = DataHubOperationCircuitBreakerSensor(
    task_id="pet_profiles_operation_sensor",
    datahub_rest_conn_id="datahub_longtail",
    urn=[
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pet_profiles,PROD)"
    ],
    time_delta=datetime.timedelta(minutes=10),
)

send_email = BashOperator(
    task_id="send_emails",
    dag=dag,
    inlets=[Dataset("snowflake", "long_tail_companions.adoption.pet_profiles")],
    bash_command="echo Dummy Task",
)

pet_profiles_operation_sensor.set_downstream(send_email)
