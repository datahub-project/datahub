"""Lineage Emission

This example demonstrates how to emit lineage to DataHub within an Airflow DAG.
"""

from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

import datahub.emitter.mce_builder as builder
from datahub.integrations.airflow.operators import DatahubEmitterOperator


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
    tags=["datahub-ingest"],
    catchup=False,
) as dag:
    run_transformation_task = DummyOperator(
        task_id="run_a_transformation",
    )

    emit_lineage_task = DatahubEmitterOperator(
        task_id="emit_lineage",
        datahub_rest_conn_id="datahub_rest_default",
        mces=[
            builder.make_lineage_mce(
                [
                    builder.make_dataset_urn("bigquery", "proj.schema.tableA"),
                    builder.make_dataset_urn("bigquery", "proj.schema.tableB"),
                ],
                builder.make_dataset_urn("bigquery", "proj.schema.tableC"),
            )
        ],
    )
