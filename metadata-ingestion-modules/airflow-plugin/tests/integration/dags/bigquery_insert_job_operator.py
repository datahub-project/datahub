from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

BQ_PROJECT_ID = "test_project"
BQ_DATASET_ID = "test_dataset"
BQ_SOURCE_TABLE = "costs"
BQ_DEST_TABLE = "processed_costs"


def _fake_bigquery_insert_job_execute(*args, **kwargs):
    pass


class FakeBigQueryJob:
    def __init__(self):
        self.job_id = "fake_job_id_12345"

    def result(self):
        return []

    def running(self):
        return False

    @property
    def state(self):
        return "DONE"

    @property
    def error_result(self):
        return None


def _fake_insert_job(*args, **kwargs):
    return FakeBigQueryJob()


with DAG(
    "bigquery_insert_job_operator",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    # HACK: We don't want to send real requests to BigQuery. As a workaround,
    # we can simply monkey-patch the operator and hook methods.
    BigQueryInsertJobOperator.execute = _fake_bigquery_insert_job_execute  # type: ignore

    # Also patch the hook's insert_job method
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    BigQueryHook.insert_job = _fake_insert_job  # type: ignore

    # Test case 1: WITH destinationTable
    select_with_destination_config = BigQueryInsertJobOperator(
        gcp_conn_id="my_bigquery",
        task_id="select_with_destination_config",
        configuration={
            "query": {
                "query": """
                SELECT
                    id,
                    month,
                    total_cost,
                    area,
                    total_cost / area as cost_per_area
                FROM {{ params.source_table }}
                WHERE total_cost > 50
                """,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": BQ_PROJECT_ID,
                    "datasetId": BQ_DATASET_ID,
                    "tableId": BQ_DEST_TABLE,
                },
            }
        },
        params={
            "source_table": f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_SOURCE_TABLE}`",
        },
    )

    # Test case 2: WITHOUT destinationTable
    insert_query_without_destination = BigQueryInsertJobOperator(
        gcp_conn_id="my_bigquery",
        task_id="insert_query_without_destination",
        configuration={
            "query": {
                "query": """
                INSERT INTO {{ params.dest_table }}
                (id, month, total_cost, area, cost_per_area)
                SELECT
                    id,
                    month,
                    total_cost,
                    area,
                    total_cost / area as cost_per_area
                FROM {{ params.source_table }}
                WHERE total_cost > 100
                """,
                "useLegacySql": False,
            }
        },
        params={
            "source_table": f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_SOURCE_TABLE}`",
            "dest_table": f"`{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_DEST_TABLE}`",
        },
    )
