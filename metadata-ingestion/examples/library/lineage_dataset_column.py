from datahub.metadata.urns import DatasetUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

client.lineage.add_lineage(
    upstream=DatasetUrn(platform="snowflake", name="sales_raw"),
    downstream=DatasetUrn(platform="snowflake", name="sales_cleaned"),
    column_lineage=True,  # same as "auto_fuzzy", which maps columns based on name similarity
)
