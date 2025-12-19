"""
Airflow 3.0 version of snowflake_operator.py

This DAG uses SQLExecuteQueryOperator instead of SnowflakeOperator,
which was removed in Airflow 3.0.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

SNOWFLAKE_COST_TABLE = "costs"
SNOWFLAKE_PROCESSED_TABLE = "processed_costs"


def _fake_snowflake_execute(*args, **kwargs):
    raise ValueError("mocked snowflake execute to not run queries")


with DAG(
    "snowflake_operator",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    # HACK: We don't want to send real requests to Snowflake. As a workaround,
    # we can simply monkey-patch the operator.
    SQLExecuteQueryOperator.execute = _fake_snowflake_execute  # type: ignore

    transform_cost_table = SQLExecuteQueryOperator(
        conn_id="my_snowflake",
        task_id="transform_cost_table",
        sql="""
        CREATE OR REPLACE TABLE {{ params.out_table_name }} AS
        SELECT
            id,
            month,
            total_cost,
            area,
            total_cost / area as cost_per_area
        FROM {{ params.in_table_name }}
        """,
        params={
            "in_table_name": SNOWFLAKE_COST_TABLE,
            "out_table_name": SNOWFLAKE_PROCESSED_TABLE,
        },
    )
