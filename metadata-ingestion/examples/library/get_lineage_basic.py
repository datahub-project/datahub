from datahub.metadata.urns import DatasetUrn
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_filters import FilterDsl as F

client = DataHubClient.from_env()

downstream_lineage = client.lineage.get_lineage(
    source_urn=DatasetUrn(platform="snowflake", name="downstream_table"),
    direction="downstream",
    max_hops=2,
    filter=F.and_(
        F.platform("airflow"),
        F.entity_type("dataJob"),
    ),
)

print(downstream_lineage)
