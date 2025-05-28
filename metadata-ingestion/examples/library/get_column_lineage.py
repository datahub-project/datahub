from datahub.sdk.main_client import DataHubClient
from datahub.sdk.dataset import Dataset

client = DataHubClient.from_env()

# Table 1 (upstream_table)
upstream_dataset = Dataset(
    platform="snowflake",
    name="table_1",
    schema=[("col1", "number"), ("col2", "string"), ("col3", "number")],
)

# Table 2 (downstream_table)
downstream_dataset = Dataset(
    platform="snowflake",
    name="table_2",
    schema=[("col4", "number"), ("col5", "string"), ("col6", "number")],
)

# Table 3 (downstream2_table)
downstream2_dataset = Dataset(
    platform="snowflake",
    name="table_3",
    schema=[("col7", "number"), ("col8", "string"), ("col9", "number")],
)

# Upsert datasets
client.entities.upsert(upstream_dataset)
client.entities.upsert(downstream_dataset)
client.entities.upsert(downstream2_dataset)

# Create URNs for datasets
# Add column lineage to match the Mermaid diagram
client.lineage.add_lineage(
    upstream=upstream_dataset.urn, 
    downstream=downstream_dataset.urn, 
    column_lineage={
        "col6": ["col3"],
        "col4": ["col1"],
        "col5": ["col1"]
    }
)

client.lineage.add_lineage(
    upstream=downstream_dataset.urn, 
    downstream=downstream2_dataset.urn, 
    column_lineage={
        "col7": ["col4"],
        "col6": ["col9"]
    }
)

# Get downstream lineage
downstream_lineage = client.lineage.get_lineage(
    source_urn=upstream_dataset.urn, direction="downstream", max_hops=2
)

# Get column lineage for the entire flow
downstream_column_lineage = client.lineage.get_column_lineage(
    source_urn=upstream_dataset.urn, source_column="col1", direction="downstream", max_hops=1
)

downstream_column_lineage2 = client.lineage.get_column_lineage(
    source_urn=upstream_dataset.urn, source_column="col1", direction="downstream", max_hops=2
)

from pprint import pprint


for lineage in downstream_lineage:
    pprint(lineage)


print("--------------------------------")

print("Downstream Column Lineage:")
for lineage in downstream_column_lineage:
    pprint(lineage)

print("--------------------------------")
for lineage in downstream_column_lineage2:
    pprint(lineage)