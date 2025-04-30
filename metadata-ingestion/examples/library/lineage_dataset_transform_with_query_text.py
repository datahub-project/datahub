from datahub.metadata.urns import DatasetUrn
from datahub.sdk.lineage_client import LineageClient
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()
lineage_client = LineageClient(client=client)

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
    upstream=DatasetUrn(platform="snowflake", name="customers"),
    downstream=DatasetUrn(platform="snowflake", name="high_value_customers"),
    query_text=sql_query,
)
