from datahub.metadata.urns import DatasetUrn
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_filters import FilterDsl as F

client = DataHubClient.from_env()

dataset_urn = DatasetUrn(platform="snowflake", name="downstream_table")

# Get column lineage for the entire flow
downstream_column_lineage = client.lineage.get_lineage(
    source_urn=dataset_urn,
    source_column="id",
    direction="downstream",
    max_hops=1,
    filter=F.and_(
        F.platform("snowflake"),
        F.entity_type("dataset"),
    ),
)

print(downstream_column_lineage)
