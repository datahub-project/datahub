"""This example DAG demonstrates how to create and use a DataHubGraph client."""

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from datahub.ingestion.graph.client import DataHubGraph, RemovedStatusFilter
from datahub_airflow_plugin.hooks.datahub import DatahubRestHook


@dag(
    schedule_interval=timedelta(days=1),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def datahub_graph_usage_sample_dag():
    @task()
    def use_the_graph():
        graph: DataHubGraph = DatahubRestHook("my_datahub_rest_conn_id").make_graph()
        graph.test_connection()

        # Example usage: Find all soft-deleted BigQuery DEV entities
        # in DataHub, and hard delete them.
        for urn in graph.get_urns_by_filter(
            platform="bigquery",
            env="DEV",
            status=RemovedStatusFilter.ONLY_SOFT_DELETED,
        ):
            graph.hard_delete_entity(urn)

    use_the_graph()


datahub_graph_usage_sample_dag()
