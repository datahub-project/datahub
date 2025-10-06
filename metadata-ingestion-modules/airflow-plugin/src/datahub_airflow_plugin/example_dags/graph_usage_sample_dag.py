"""This example DAG demonstrates how to create and use a DataHubGraph client."""

from datetime import timedelta

# Create DAG decorator arguments conditionally for Airflow version compatibility
import airflow  # noqa: E402
import pendulum
from airflow.decorators import dag, task

from datahub.ingestion.graph.client import DataHubGraph, RemovedStatusFilter
from datahub_airflow_plugin.hooks.datahub import DatahubRestHook

dag_decorator_kwargs = {
    "start_date": pendulum.datetime(2021, 1, 1, tz="UTC"),
    "catchup": False,
}

# Handle schedule parameter change in Airflow 3.0
if hasattr(airflow, '__version__') and airflow.__version__.startswith(('3.', '2.10', '2.9', '2.8', '2.7')):
    # Use schedule for newer Airflow versions (2.7+)
    dag_decorator_kwargs["schedule"] = timedelta(days=1)
else:
    # Use schedule_interval for older versions
    dag_decorator_kwargs["schedule_interval"] = timedelta(days=1)

@dag(**dag_decorator_kwargs)
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
