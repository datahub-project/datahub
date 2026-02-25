# metadata-ingestion/examples/library/dataflow_comprehensive.py
from datetime import datetime, timezone

from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn, GlossaryTermUrn, TagUrn
from datahub.sdk import DataFlow, DataHubClient

client = DataHubClient.from_env()

# Create a DataFlow with comprehensive metadata
dataflow = DataFlow(
    platform="airflow",
    name="daily_sales_aggregation",
    display_name="Daily Sales Aggregation Pipeline",
    platform_instance="PROD-US-EAST",
    env="PROD",
    description="Aggregates daily sales data from multiple sources and updates reporting tables",
    external_url="https://airflow.company.com/dags/daily_sales_aggregation",
    custom_properties={
        "team": "analytics",
        "schedule": "0 2 * * *",
        "sla_hours": "4",
        "priority": "high",
    },
    created=datetime(2024, 1, 15, tzinfo=timezone.utc),
    last_modified=datetime.now(timezone.utc),
    subtype="ETL",
    owners=[
        CorpUserUrn("jdoe"),
        CorpGroupUrn("data-engineering"),
    ],
    tags=[
        TagUrn(name="production"),
        TagUrn(name="sales"),
        TagUrn(name="critical"),
    ],
    terms=[
        GlossaryTermUrn("Classification.Confidential"),
    ],
    domain="urn:li:domain:sales",
)

# Upsert the DataFlow
client.entities.upsert(dataflow)

print(f"Created DataFlow: {dataflow.urn}")
print(f"Display Name: {dataflow.display_name}")
print(f"Description: {dataflow.description}")
print(f"External URL: {dataflow.external_url}")
print(f"Custom Properties: {dataflow.custom_properties}")
