from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()

# Get column lineage for the entire flow
results = client.lineage.get_lineage(
    source_urn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_summary,PROD),id)",
    direction="downstream",
)

print(list(results))
