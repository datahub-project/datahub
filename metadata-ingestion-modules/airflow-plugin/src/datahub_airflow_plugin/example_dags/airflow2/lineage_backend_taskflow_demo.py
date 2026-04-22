"""Lineage Backend

An example DAG demonstrating the usage of DataHub's Airflow lineage backend using the TaskFlow API.
"""

from datetime import timedelta

from airflow.decorators import dag, task  # type: ignore[attr-defined]

from datahub_airflow_plugin._airflow_version_specific import days_ago
from datahub_airflow_plugin.entities import Dataset, Urn

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jdoe@example.com"],
    "email_on_failure": False,
    "execution_timeout": timedelta(minutes=5),
}


# Create DAG decorator arguments conditionally for Airflow version compatibility
import airflow  # noqa: E402

dag_decorator_kwargs = {
    "default_args": default_args,
    "description": "An example DAG demonstrating the usage of DataHub's Airflow lineage backend using the TaskFlow API.",
    "start_date": days_ago(2),
    "tags": ["example_tag"],
    "catchup": False,
}

# Handle schedule parameter change in Airflow 3.0
if hasattr(airflow, "__version__") and airflow.__version__.startswith(
    ("3.", "2.10", "2.9", "2.8", "2.7")
):
    # Use schedule for newer Airflow versions (2.7+)
    dag_decorator_kwargs["schedule"] = timedelta(days=1)
else:
    # Use schedule_interval for older versions
    dag_decorator_kwargs["schedule_interval"] = timedelta(days=1)

# Add default_view only for older Airflow versions that support it
if hasattr(airflow, "__version__") and not airflow.__version__.startswith("3."):
    dag_decorator_kwargs["default_view"] = "tree"


@dag(**dag_decorator_kwargs)
def datahub_lineage_backend_taskflow_demo():
    @task(
        inlets=[
            Dataset("snowflake", "mydb.schema.tableA"),
            Dataset("snowflake", "mydb.schema.tableB", "DEV"),
            # You can also put dataset URNs in the inlets/outlets lists.
            Urn(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableC,PROD)"
            ),
            Urn("urn:li:dataJob:(urn:li:dataFlow:(airflow,dag1,prod),task1)"),
        ],
        outlets=[Dataset("snowflake", "mydb.schema.tableD")],
    )
    def run_data_task():
        # This is where you might run your data tooling.
        pass

    run_data_task()


datahub_lineage_backend_taskflow_dag = datahub_lineage_backend_taskflow_demo()
