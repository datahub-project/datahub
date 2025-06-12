from datahub.metadata.urns import DatasetUrn
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()


upstream_urn = DatasetUrn(platform="snowflake", name="sales_raw")
downstream_urn = DatasetUrn(platform="snowflake", name="sales_cleaned")
client.lineage.add_lineage(upstream=upstream_urn, downstream=downstream_urn)
