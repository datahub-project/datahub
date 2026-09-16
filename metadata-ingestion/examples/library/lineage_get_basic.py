from datahub.metadata.urns import DatasetUrn
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()

downstream_lineage = client.lineage.get_lineage(
    source_urn=DatasetUrn(platform="snowflake", name="sales_summary"),
    direction="downstream",
)

print(downstream_lineage)
