from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

ATHENA_COST_TABLE = "costs"
ATHENA_PROCESSED_TABLE = "processed_costs"


def _fake_athena_execute(*args, **kwargs):
    pass


with DAG(
    "athena_operator",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    # HACK: We don't want to send real requests to Athena. As a workaround,
    # we can simply monkey-patch the operator.
    AthenaOperator.execute = _fake_athena_execute  # type: ignore

    transform_cost_table = AthenaOperator(
        aws_conn_id="my_aws",
        task_id="transform_cost_table",
        database="athena_db",
        query="""
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
            "in_table_name": ATHENA_COST_TABLE,
            "out_table_name": ATHENA_PROCESSED_TABLE,
        },
        output_location="s3://athena-results-bucket/",
    )
