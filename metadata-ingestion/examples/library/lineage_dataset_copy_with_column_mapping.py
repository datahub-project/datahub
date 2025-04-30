from datahub.metadata.urns import DatasetUrn
from datahub.sdk.lineage_client import LineageClient
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()
lineage_client = LineageClient(client=client)

source_dataset_urn = DatasetUrn(platform="postgres", name="products")
target_dataset_urn = DatasetUrn(platform="snowflake", name="products_copy")

lineage_client.add_dataset_copy_lineage(
    upstream=source_dataset_urn,
    downstream=target_dataset_urn,
    column_lineage={"id": ["id"]},
)
