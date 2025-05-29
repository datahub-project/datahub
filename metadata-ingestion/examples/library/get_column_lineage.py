from datahub.sdk.dataset import Dataset
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()

upstream_dataset = Dataset(
    platform="snowflake",
    name="upstream_table",
    schema=[
        ("id", "number"),
        ("name", "string"),
        ("age", "number"),
        ("customer_id", "number"),
    ],
)

downstream_dataset = Dataset(
    platform="snowflake",
    name="downstream_table",
    schema=[
        ("id", "number"),
        ("name", "string"),
        ("age", "number"),
        ("customer_id", "number"),
    ],
)

downstream2_dataset = Dataset(
    platform="snowflake",
    name="downstream2_table",
    schema=[
        ("id", "number"),
        ("name", "string"),
        ("age", "number"),
        ("customer_id", "number"),
    ],
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

# Get column lineage for the entire flow
downstream_column_lineage = client.lineage.get_lineage(
    source_urn=upstream_dataset.urn,
    source_column="id",
    direction="downstream",
    max_hops=1,
)

downstream_column_lineage2 = client.lineage.get_lineage(
    source_urn=upstream_dataset.urn,
    source_column="id",
    direction="downstream",
    max_hops=2,
    filters={"platform": ["snowflake"]},
)
