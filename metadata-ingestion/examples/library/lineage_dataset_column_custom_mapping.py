from datahub.metadata.urns import DatasetUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

client.lineage.add_lineage(
    upstream=DatasetUrn(platform="snowflake", name="sales_raw"),
    downstream=DatasetUrn(platform="snowflake", name="sales_cleaned"),
    # { downstream_column -> [upstream_columns] }
    column_lineage={
        "id": ["id"],
        "region": ["region", "region_id"],
        "total_revenue": ["revenue"],
    },
)
