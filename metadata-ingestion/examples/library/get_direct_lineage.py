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

# direct_lineage = client.lineage.get_direct_lineage(
#     source_urn=downstream_dataset_urn, source_column="id", direction="downstream"
# )


# print(direct_lineage)

direct_lineage2 = client.lineage.get_direct_lineage(
    source_urn=upstream_dataset_urn, source_column="id", direction="upstream"
)

print(direct_lineage2)