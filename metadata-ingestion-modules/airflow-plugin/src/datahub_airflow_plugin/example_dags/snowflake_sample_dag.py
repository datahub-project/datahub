"""Snowflake DataHub Ingest DAG

This example demonstrates how to ingest metadata from Snowflake into DataHub
from within an Airflow DAG. In contrast to the MySQL example, this DAG
pulls the DB connection configuration from Airflow's connection store.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonVirtualenvOperator


def ingest_from_snowflake(snowflake_credentials, datahub_gms_server):
    from datahub.ingestion.run.pipeline import Pipeline

    pipeline = Pipeline.create(
        # This configuration is analogous to a recipe configuration.
        {
            "source": {
                "type": "snowflake",
                "config": {
                    **snowflake_credentials,
                    # Other Snowflake config can be added here.
                    "profiling": {"enabled": False},
                },
            },
            # Other ingestion features, like transformers, are also supported.
            # "transformers": [
            #     {
            #         "type": "simple_add_dataset_ownership",
            #         "config": {
            #             "owner_urns": [
            #                 "urn:li:corpuser:example",
            #             ]
            #         },
            #     }
            # ],
            "sink": {
                "type": "datahub-rest",
                "config": {"server": datahub_gms_server},
            },
        }
    )
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status()


with DAG(
    "datahub_snowflake_ingest",
    default_args={
        "owner": "airflow",
    },
    description="An example DAG which ingests metadata from Snowflake to DataHub",
    start_date=datetime(2022, 1, 1),
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    # This example pulls credentials from Airflow's connection store.
    # For this to work, you must have previously configured these connections in Airflow.
    # See the Airflow docs for details: https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html
    snowflake_conn = BaseHook.get_connection("snowflake_admin_default")
    datahub_conn = BaseHook.get_connection("datahub_rest_default")

    # While it is also possible to use the PythonOperator, we recommend using
    # the PythonVirtualenvOperator to ensure that there are no dependency
    # conflicts between DataHub and the rest of your Airflow environment.
    ingest_task = PythonVirtualenvOperator(
        task_id="ingest_from_snowflake",
        requirements=[
            "acryl-datahub[snowflake]",
        ],
        system_site_packages=False,
        python_callable=ingest_from_snowflake,
        op_kwargs={
            "snowflake_credentials": {
                "username": snowflake_conn.login,
                "password": snowflake_conn.password,
                "account_id": snowflake_conn.extra_dejson["account"],
                "warehouse": snowflake_conn.extra_dejson.get("warehouse"),
                "role": snowflake_conn.extra_dejson.get("role"),
            },
            "datahub_gms_server": datahub_conn.host,
        },
    )
