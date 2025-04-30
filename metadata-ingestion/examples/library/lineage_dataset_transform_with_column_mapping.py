from datahub.sdk import DataHubClient, DatasetUrn
from datahub.sdk.lineage_client import LineageClient

client = DataHubClient.from_env()
lineage_client = LineageClient(client=client)

source_dataset_urn = DatasetUrn(platform="snowflake", name="customer_data")
target_dataset_urn = DatasetUrn(platform="snowflake", name="customer_metrics")

column_mapping = {
    "customer_id": ["id"],
    "full_name": ["first_name", "last_name"],
    "total_spend": ["purchase_amount"],
    "loyalty_score": ["purchase_frequency", "purchase_amount"],
}

lineage_client.add_dataset_transform_lineage(
    upstream=source_dataset_urn,
    downstream=target_dataset_urn,
    column_lineage=column_mapping,
)
