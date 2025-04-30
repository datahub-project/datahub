from datahub.metadata.urns import DatasetUrn
from datahub.sdk.lineage_client import LineageClient
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()
lineage_client = LineageClient(client=client)

lineage_client.add_dataset_copy_lineage(
    upstream=DatasetUrn(platform="postgres", name="customer_data"),
    downstream=DatasetUrn(platform="snowflake", name="customer_info"),
    column_lineage="auto_fuzzy",  # default
)
