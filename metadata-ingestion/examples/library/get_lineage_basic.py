from datahub.sdk.dataset import Dataset
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()

upstream_dataset = Dataset(
    platform="snowflake",
    name="upstream_table",
)

downstream_dataset = Dataset(
    platform="snowflake",
    name="downstream_table",
)

downstream2_dataset = Dataset(
    platform="snowflake",
    name="downstream2_table",
)

client.entities.upsert(upstream_dataset)
client.entities.upsert(downstream_dataset)
client.entities.upsert(downstream2_dataset)

client.lineage.add_lineage(
    upstream=upstream_dataset.urn, downstream=downstream_dataset.urn
)

client.lineage.add_lineage(
    upstream=downstream_dataset.urn, downstream=downstream2_dataset.urn
)

downstream_lineage = client.lineage.get_lineage(
    source_urn=upstream_dataset.urn,
    direction="downstream",
    max_hops=2,
)


upstream_lineage = client.lineage.get_lineage(
    source_urn=downstream2_dataset.urn,
    direction="upstream",
    filters={"platform": ["snowflake"], "entity_type": ["DATASET"]},
)

print(upstream_lineage)
