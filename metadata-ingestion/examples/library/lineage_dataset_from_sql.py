from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()

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

# sales_summary will be assumed to be in the default db/schema
# e.g. prod_db.public.sales_summary
client.lineage.add_dataset_lineage_from_sql(
    query_text=sql_query,
    platform="snowflake",
    default_db="prod_db",
    default_schema="public",
)
