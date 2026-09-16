from datahub.metadata.urns import DatasetUrn
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()

# Get column lineage for the entire flow
# you can pass source_urn and source_column to get lineage for a specific column
# alternatively, you can pass schemaFieldUrn to source_urn.
# e.g. source_urn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,downstream_table),id)"
downstream_column_lineage = client.lineage.get_lineage(
    source_urn=DatasetUrn(platform="snowflake", name="sales_summary"),
    source_column="id",
    direction="downstream",
)

print(downstream_column_lineage)
