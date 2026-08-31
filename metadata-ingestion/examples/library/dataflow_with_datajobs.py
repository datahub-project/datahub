# metadata-ingestion/examples/library/dataflow_with_datajobs.py
from datahub.metadata.urns import DatasetUrn
from datahub.sdk import DataFlow, DataHubClient, DataJob

client = DataHubClient.from_env()

# Create the parent DataFlow
dataflow = DataFlow(
    platform="airflow",
    name="customer_360_pipeline",
    description="End-to-end pipeline for building customer 360 view",
    env="PROD",
)

# Create DataJobs that belong to this flow
extract_job = DataJob(
    name="extract_customer_data",
    flow=dataflow,
    description="Extracts customer data from operational databases",
    outlets=[
        DatasetUrn(platform="snowflake", name="staging.customers_raw", env="PROD"),
    ],
)

transform_job = DataJob(
    name="transform_customer_data",
    flow=dataflow,
    description="Transforms and enriches customer data",
    inlets=[
        DatasetUrn(platform="snowflake", name="staging.customers_raw", env="PROD"),
    ],
    outlets=[
        DatasetUrn(
            platform="snowflake", name="analytics.customers_enriched", env="PROD"
        ),
    ],
)

load_job = DataJob(
    name="load_customer_360",
    flow=dataflow,
    description="Loads final customer 360 view",
    inlets=[
        DatasetUrn(
            platform="snowflake", name="analytics.customers_enriched", env="PROD"
        ),
    ],
    outlets=[
        DatasetUrn(platform="snowflake", name="prod.customer_360", env="PROD"),
    ],
)

# Upsert all entities
client.entities.upsert(dataflow)
client.entities.upsert(extract_job)
client.entities.upsert(transform_job)
client.entities.upsert(load_job)

print(f"Created DataFlow: {dataflow.urn}")
print(f"  - Job 1: {extract_job.urn}")
print(f"  - Job 2: {transform_job.urn}")
print(f"  - Job 3: {load_job.urn}")
