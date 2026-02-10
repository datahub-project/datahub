"""Test DAG for native Airflow Asset/Dataset support.

This DAG tests the capture of native Airflow Assets/Datasets as DataHub lineage.
It covers:
1. Assets with URI schemes (s3://, gs://, postgresql://)
2. Plain name assets (like @asset decorator creates)
3. Mixed with DataHub-native entities

Note: Airflow Dataset was introduced in Airflow 2.4+.
This DAG will not be loaded on Airflow < 2.4.
"""

from datetime import datetime

import airflow
import packaging.version

# airflow.datasets was introduced in Airflow 2.4
# Skip this DAG on older versions
if packaging.version.parse(airflow.version.version) < packaging.version.parse("2.4.0"):
    raise ImportError("airflow.datasets requires Airflow 2.4+")

from airflow import DAG
from airflow.datasets import Dataset as AirflowDataset
from airflow.operators.bash import BashOperator

from datahub_airflow_plugin.entities import Dataset as DataHubDataset

# Native Airflow Assets with different URI schemes
s3_input = AirflowDataset("s3://my-bucket/input/data.parquet")
gcs_input = AirflowDataset("gs://analytics-bucket/raw/events.json")
postgres_input = AirflowDataset("postgresql://myhost/mydb/schema/source_table")

# Plain name asset (like @asset decorator creates)
plain_asset = AirflowDataset("my_plain_asset")

# Output assets
s3_output = AirflowDataset("s3://my-bucket/output/processed.parquet")
bigquery_output = AirflowDataset("bigquery://my-project/dataset/result_table")

with DAG(
    "airflow_asset_iolets",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    # Task using only native Airflow Assets
    task_native_assets = BashOperator(
        task_id="process_with_native_assets",
        dag=dag,
        bash_command="echo 'Processing with native Airflow assets'",
        inlets=[s3_input, gcs_input, postgres_input, plain_asset],
        outlets=[s3_output, bigquery_output],
    )

    # Task mixing DataHub entities with Airflow Assets
    task_mixed = BashOperator(
        task_id="process_mixed",
        dag=dag,
        bash_command="echo 'Processing with mixed entities'",
        inlets=[
            DataHubDataset(platform="snowflake", name="mydb.schema.source"),
            s3_input,
        ],
        outlets=[
            DataHubDataset(platform="snowflake", name="mydb.schema.target"),
            s3_output,
        ],
    )

    task_native_assets >> task_mixed
