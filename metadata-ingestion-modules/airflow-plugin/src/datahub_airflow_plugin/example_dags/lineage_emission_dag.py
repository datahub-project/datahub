"""Lineage Emission

This example demonstrates how to emit lineage to DataHub within an Airflow DAG.
"""
from datetime import timedelta

import datahub.emitter.mce_builder as builder
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from datahub_airflow_plugin.operators.datahub import DatahubEmitterOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jdoe@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=120),
}


with DAG(
    "datahub_lineage_emission_example",
    default_args=default_args,
    description="An example DAG demonstrating lineage emission within an Airflow DAG.",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    catchup=False,
    default_view="tree",
) as dag:
    transformation_task = BashOperator(
        task_id="transformation_task",
        dag=dag,
        bash_command="echo 'This is where you might run your data tooling.'",
    )

    emit_lineage_task = DatahubEmitterOperator(
        task_id="emit_lineage",
        datahub_conn_id="datahub_rest_default",
        mces=[
            builder.make_lineage_mce(
                upstream_urns=[
                    builder.make_dataset_urn(
                        platform="snowflake", name="mydb.schema.tableA"
                    ),
                    builder.make_dataset_urn_with_platform_instance(
                        platform="snowflake",
                        name="mydb.schema.tableB",
                        platform_instance="cloud",
                    ),
                ],
                downstream_urn=builder.make_dataset_urn(
                    platform="snowflake", name="mydb.schema.tableC", env="DEV"
                ),
            )
        ],
    )

    transformation_task >> emit_lineage_task
