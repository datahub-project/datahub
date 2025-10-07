"""Generic DataHub Ingest via Recipe

This example demonstrates how to load any configuration file and run a
DataHub ingestion pipeline within an Airflow DAG.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Conditional import for Airflow version compatibility
try:
    from airflow.utils.dates import days_ago
except ImportError:
    # days_ago was removed in Airflow 3.0, create a simple replacement
    def days_ago(n):
        return datetime(2025, 1, 1) - timedelta(days=n)


from datahub.configuration.config_loader import load_config_file
from datahub.ingestion.run.pipeline import Pipeline

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jdoe@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=120),
}


def datahub_recipe():
    # Note that this will also resolve environment variables in the recipe.
    config = load_config_file("path/to/recipe.yml")

    pipeline = Pipeline.create(config)
    pipeline.run()
    pipeline.raise_from_status()


# Create DAG arguments conditionally for Airflow version compatibility
import airflow  # noqa: E402

dag_kwargs = {
    "dag_id": "datahub_ingest_using_recipe",
    "default_args": default_args,
    "description": "An example DAG which runs a DataHub ingestion recipe",
    "start_date": days_ago(2),
    "catchup": False,
}

# Handle schedule parameter change in Airflow 3.0
if hasattr(airflow, "__version__") and airflow.__version__.startswith(
    ("3.", "2.10", "2.9", "2.8", "2.7")
):
    # Use schedule for newer Airflow versions (2.7+)
    dag_kwargs["schedule"] = timedelta(days=1)
else:
    # Use schedule_interval for older versions
    dag_kwargs["schedule_interval"] = timedelta(days=1)

# Add default_view only for older Airflow versions that support it
if hasattr(airflow, "__version__") and not airflow.__version__.startswith("3."):
    dag_kwargs["default_view"] = "tree"

with DAG(**dag_kwargs) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_using_recipe",
        python_callable=datahub_recipe,
    )
