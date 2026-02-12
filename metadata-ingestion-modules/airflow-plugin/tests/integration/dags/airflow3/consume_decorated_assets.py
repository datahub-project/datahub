"""Test DAG that consumes @asset decorated outputs.

This DAG demonstrates cross-DAG lineage by consuming assets produced
by the @asset decorated functions in other files.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sdk.definitions.asset import Asset

# Reference to the decorated asset for use as inlet
# This Asset object matches the URI created by @asset decorator in decorated_asset_producer.py
decorated_asset_ref = Asset("decorated_asset_producer")


with DAG(
    "consume_decorated_assets",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    # Task that consumes the decorated asset as inlet
    task_consume_decorated = BashOperator(
        task_id="consume_decorated_assets_task",
        dag=dag,
        bash_command="echo 'Consuming decorated assets'",
        # This inlet references the asset created by @asset decorator
        inlets=[decorated_asset_ref],
        # Use s3:// scheme for realistic output asset
        outlets=[Asset("s3://my-bucket/final-output/result.parquet")],
    )
