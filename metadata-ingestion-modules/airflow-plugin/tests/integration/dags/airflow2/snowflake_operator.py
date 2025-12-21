"""
Airflow 2.x version of snowflake_operator.py

This DAG supports both older and newer versions of apache-airflow-providers-snowflake:
- Older versions (< 6.3.0): Use SnowflakeOperator
- Newer versions (>= 6.3.0): Use SQLExecuteQueryOperator (SnowflakeOperator was removed)
"""

from datetime import datetime

from airflow import DAG

# Try to import SnowflakeOperator (available in older provider versions)
# Fall back to SQLExecuteQueryOperator if it's not available
try:
    from airflow.providers.snowflake.operators.snowflake import (
        SnowflakeOperator,  # type: ignore[attr-defined]
    )

    OPERATOR_CLASS = SnowflakeOperator
    CONN_ID_PARAM = "snowflake_conn_id"
except ImportError:
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    OPERATOR_CLASS = SQLExecuteQueryOperator  # type: ignore[assignment]
    CONN_ID_PARAM = "conn_id"

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
    OPERATOR_CLASS.execute = _fake_snowflake_execute  # type: ignore

    transform_cost_table = OPERATOR_CLASS(
        **{
            CONN_ID_PARAM: "my_snowflake",
            "task_id": "transform_cost_table",
            "sql": """
        CREATE OR REPLACE TABLE {{ params.out_table_name }} AS
        SELECT
            id,
            month,
            total_cost,
            area,
            total_cost / area as cost_per_area
        FROM {{ params.in_table_name }}
        """,
            "params": {
                "in_table_name": SNOWFLAKE_COST_TABLE,
                "out_table_name": SNOWFLAKE_PROCESSED_TABLE,
            },
        }
    )
