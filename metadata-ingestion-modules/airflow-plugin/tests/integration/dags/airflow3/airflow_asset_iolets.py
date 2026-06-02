"""Test DAG for native Airflow Asset/Dataset support.

This DAG tests the capture of native Airflow Assets/Datasets as DataHub lineage.
It covers:
1. Assets with URI schemes (s3://, gs://, postgresql://)
2. Plain name assets (like @asset decorator creates)
3. Mixed with DataHub-native entities
4. AssetAlias with runtime resolution (actual Asset URI emitted at task run time)
5. Contrast between partitioned-file URI vs table-level URI in Asset.uri
"""

from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sdk.definitions.asset import Asset, AssetAlias

from datahub_airflow_plugin.entities import Dataset as DataHubDataset

# Native Airflow Assets with different URI schemes (using Airflow 3 Asset class)
s3_input = Asset("s3://my-bucket/input/data.parquet")
gcs_input = Asset("gs://analytics-bucket/raw/events.json")
postgres_input = Asset("postgresql://myhost/mydb/schema/source_table")

# Plain name asset (like @asset decorator creates)
plain_asset = Asset("my_plain_asset")

# Output assets
s3_output = Asset("s3://my-bucket/output/processed.parquet")
bigquery_output = Asset("bigquery://my-project/dataset/result_table")

with DAG(
    "airflow_asset_iolets",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    # Produce the input assets so Airflow marks them as "active".
    # Airflow 3.1+ raises AirflowInactiveAssetInInletOrOutletException for any
    # asset used as an inlet that has never been produced as an outlet.
    task_produce_inputs = BashOperator(
        task_id="produce_inputs",
        dag=dag,
        bash_command="echo 'Producing input assets'",
        outlets=[s3_input, gcs_input, postgres_input, plain_asset],
    )

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

    # Task using AssetAlias — Asset.uri is the full partitioned file path.
    # DataHub lineage points to the specific partition file, not the logical table.
    # This is the "wrong" pattern when you want table-level lineage.
    #
    # DataHub sees:
    #   urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/data/year=2024/month=01/day=01/data.parquet,PROD)
    def _emit_partitioned_uri(**context: Any) -> None:
        ds = context.get("ds", "2024-01-01")
        year, month, day = ds.split("-")
        context["outlet_events"][AssetAlias("partitioned_table")].add(  # type: ignore[index]
            Asset(
                f"s3://my-bucket/data/year={year}/month={month}/day={day}/data.parquet"
            )
        )

    task_partitioned_uri = PythonOperator(
        task_id="alias_with_partitioned_uri",
        dag=dag,
        python_callable=_emit_partitioned_uri,
        inlets=[s3_input],
        outlets=[AssetAlias("partitioned_table")],
    )

    # Task using AssetAlias — Asset.uri is the table-level S3 prefix; the specific
    # partition file is recorded in extra["output_uri"] for traceability but does
    # not affect DataHub lineage.
    # This is the correct pattern for table-level lineage.
    #
    # DataHub sees:
    #   urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/data,PROD)
    def _emit_table_level_uri(**context: Any) -> None:
        ds = context.get("ds", "2024-01-01")
        year, month, day = ds.split("-")
        partition_path = (
            f"s3://my-bucket/data/year={year}/month={month}/day={day}/data.parquet"
        )
        context["outlet_events"][AssetAlias("table_level")].add(  # type: ignore[index]
            Asset("s3://my-bucket/data"),
            extra={"output_uri": partition_path},
        )

    task_table_level_uri = PythonOperator(
        task_id="alias_with_table_level_uri",
        dag=dag,
        python_callable=_emit_table_level_uri,
        inlets=[s3_input],
        outlets=[AssetAlias("table_level")],
    )

    (
        task_produce_inputs
        >> task_native_assets
        >> task_mixed
        >> task_partitioned_uri
        >> task_table_level_uri
    )
