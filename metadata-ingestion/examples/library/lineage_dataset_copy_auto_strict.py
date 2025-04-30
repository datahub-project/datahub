from datahub.metadata.urns import DatasetUrn
from datahub.sdk.lineage_client import LineageClient
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()
lineage_client = LineageClient(client=client)

lineage_client.add_dataset_copy_lineage(
    upstream=DatasetUrn(platform="mysql", name="transactions"),
    downstream=DatasetUrn(platform="snowflake", name="transactions_replica"),
    column_lineage="auto_strict",
)
