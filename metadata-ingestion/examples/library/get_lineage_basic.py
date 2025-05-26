from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()

# upstream_dataset = Dataset(
#     platform="snowflake",
#     name="upstream_table",
#     schema=[("id", "number"), ("name", "string"), ("age", "number"), ("customer_id", "number")],
# )

# downstream_dataset = Dataset(
#     platform="snowflake",
#     name="downstream_table",
#     schema=[("id", "number"), ("name", "string"), ("age", "number"), ("customer_id", "number")],
# )

# downstream2_dataset = Dataset(
#     platform="snowflake",
#     name="downstream2_table",
#     schema=[("id", "number"), ("name", "string"), ("age", "number"), ("customer_id", "number")],
# )

# client.entities.upsert(upstream_dataset)
# client.entities.upsert(downstream_dataset)
# client.entities.upsert(downstream2_dataset)

upstream_dataset_urn = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,upstream_table,PROD)"
)
downstream_dataset_urn = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table,PROD)"
)
downstream2_dataset_urn = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream2_table,PROD)"
)

client.lineage.add_lineage(
    upstream=upstream_dataset_urn, downstream=downstream_dataset_urn
)
client.lineage.add_lineage(
    upstream=upstream_dataset_urn, downstream=downstream_dataset_urn
)
client.lineage.add_lineage(
    upstream=downstream_dataset_urn, downstream=downstream2_dataset_urn
)

downstream_lineage = client.lineage.get_lineage(
    source_urn=upstream_dataset_urn, direction="downstream", max_hops=2
)
print("downstream_lineage")
for lineage in downstream_lineage:
    print(lineage)

upstream_lineage = client.lineage.get_lineage(
    source_urn=upstream_dataset_urn, direction="upstream", max_hops=2
)
print("upstream_lineage")
for lineage in upstream_lineage:
    print(lineage)
