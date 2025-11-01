# metadata-ingestion/examples/library/datajob_create_basic.py
from datahub.metadata.urns import DataFlowUrn, DatasetUrn
from datahub.sdk import DataHubClient, DataJob

client = DataHubClient.from_env()

datajob = DataJob(
    name="transform_customer_data",
    flow_urn=DataFlowUrn(
        orchestrator="airflow",
        flow_id="daily_etl_pipeline",
        cluster="prod",
    ),
    description="Transforms raw customer data into analytics-ready format",
    inlets=[
        DatasetUrn(platform="postgres", name="raw.customers", env="PROD"),
        DatasetUrn(platform="postgres", name="raw.addresses", env="PROD"),
    ],
    outlets=[
        DatasetUrn(platform="snowflake", name="analytics.dim_customers", env="PROD"),
    ],
)

client.entities.upsert(datajob)
print(f"Created data job: {datajob.urn}")
