from datahub.metadata.urns import DatasetUrn
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()


upstream_urn = DatasetUrn(platform="snowflake", name="upstream_table")
downstream_urn = DatasetUrn(platform="snowflake", name="downstream_table")
client.lineage.add_lineage(
    upstream=upstream_urn, downstream=downstream_urn, column_lineage="auto_fuzzy"
)
# column_lineage="auto_fuzzy" will try to match the column names in the upstream and downstream datasets
# column_lineage="auto_strict" will only match the column names in the upstream and downstream datasets

# you can also pass a dictionary with the column names in the form of {downstream_column_name: [upstream_column_name1, upstream_column_name2]}
# e.g. column_lineage={"id": ["id", "customer_id"]}
