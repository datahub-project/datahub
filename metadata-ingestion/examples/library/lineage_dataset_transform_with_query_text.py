from datahub.sdk import DataHubClient, DatasetUrn
from datahub.sdk.lineage_client import LineageClient

client = DataHubClient.from_env()
lineage_client = LineageClient(client=client)

source_dataset_urn = DatasetUrn(platform="snowflake", name="customers")
target_dataset_urn = DatasetUrn(platform="snowflake", name="high_value_customers")

sql_query = """
SELECT 
    customer_id,
    name,
    email,
    total_purchases,
    lifetime_value
FROM 
    customers
WHERE 
    lifetime_value > 10000
"""

lineage_client.add_dataset_transform_lineage(
    upstream=source_dataset_urn, downstream=target_dataset_urn, query_text=sql_query
)
