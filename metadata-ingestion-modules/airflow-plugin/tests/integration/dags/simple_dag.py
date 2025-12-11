# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from datahub_airflow_plugin.entities import Dataset, Urn

with DAG(
    "simple_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="A simple DAG that runs a few fake data tasks.",
) as dag:
    task1 = BashOperator(
        task_id="task_1",
        dag=dag,
        bash_command="echo 'task 1'",
        inlets=[
            Dataset(platform="snowflake", name="mydb.schema.tableA"),
            Urn(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableC,PROD)"
            ),
            Urn(
                "urn:li:dataJob:(urn:li:dataFlow:(airflow,myairflow.test_dag,PROD),test_task)"
            ),
        ],
        outlets=[Dataset("snowflake", "mydb.schema.tableD")],
    )

    task2 = BashOperator(
        task_id="run_another_data_task",
        dag=dag,
        bash_command="echo 'task 2'",
    )

    task1 >> task2
