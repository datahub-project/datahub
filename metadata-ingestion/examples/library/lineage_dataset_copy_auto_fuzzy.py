from datahub.sdk import DataHubClient, DatasetUrn
from datahub.sdk.lineage_client import LineageClient

client = DataHubClient.from_env()
lineage_client = LineageClient(client=client)

source_dataset_urn = DatasetUrn(platform="postgres", name="customer_data")
target_dataset_urn = DatasetUrn(platform="snowflake", name="customer_info")

lineage_client.add_dataset_copy_lineage(
    upstream=source_dataset_urn,
    downstream=target_dataset_urn,
    column_lineage="auto_fuzzy",
)
