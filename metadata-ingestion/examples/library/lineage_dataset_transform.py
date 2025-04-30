from datahub.sdk import DataHubClient, DatasetUrn
from datahub.sdk.lineage_client import LineageClient

client = DataHubClient.from_env()
lineage_client = LineageClient(client=client)

source_dataset_urn = DatasetUrn(platform="snowflake", name="source_table")
target_dataset_urn = DatasetUrn(platform="snowflake", name="target_table")

lineage_client.add_dataset_transform_lineage(
    upstream=source_dataset_urn, downstream=target_dataset_urn
)
