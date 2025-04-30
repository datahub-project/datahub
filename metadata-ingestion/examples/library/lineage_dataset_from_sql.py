from datahub.sdk import DataHubClient
from datahub.sdk.lineage_client import LineageClient

client = DataHubClient.from_env()
lineage_client = LineageClient(client=client)

sql_query = """
CREATE TABLE sales_summary AS
SELECT 
    p.product_name,
    c.customer_segment,
    SUM(s.quantity) as total_quantity,
    SUM(s.amount) as total_sales
FROM sales s
JOIN products p ON s.product_id = p.id
JOIN customers c ON s.customer_id = c.id
GROUP BY p.product_name, c.customer_segment
"""

lineage_client.add_dataset_lineage_from_sql(query_text=sql_query, platform="snowflake")
