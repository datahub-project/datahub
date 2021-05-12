"""Lineage Emission

This example demonstrates how to emit lineage to DataHub within an Airflow DAG.
"""

from datetime import timedelta

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

import datahub.emitter.mce_builder as builder
from datahub_provider.operators.datahub import DatahubEmitterOperator

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
) as dag:
    # This example shows a SnowflakeOperator followed by a lineage emission. However, the
    # same DatahubEmitterOperator can be used to emit lineage in any context.

    sql = """CREATE OR REPLACE TABLE `mydb.schema.tableC` AS
            WITH some_table AS (
              SELECT * FROM `mydb.schema.tableA`
            ),
            some_other_table AS (
              SELECT id, some_column FROM `mydb.schema.tableB`
            )
            SELECT * FROM some_table
            LEFT JOIN some_other_table ON some_table.unique_id=some_other_table.id"""
    transformation_task = SnowflakeOperator(
        task_id="snowflake_transformation",
        dag=dag,
        snowflake_conn_id="snowflake_default",
        sql=sql,
    )

    emit_lineage_task = DatahubEmitterOperator(
        task_id="emit_lineage",
        datahub_conn_id="datahub_rest_default",
        mces=[
            builder.make_lineage_mce(
                upstream_urns=[
                    builder.make_dataset_urn("snowflake", "mydb.schema.tableA"),
                    builder.make_dataset_urn("snowflake", "mydb.schema.tableB"),
                ],
                downstream_urn=builder.make_dataset_urn(
                    "snowflake", "mydb.schema.tableC"
                ),
            )
        ],
    )

    transformation_task >> emit_lineage_task
