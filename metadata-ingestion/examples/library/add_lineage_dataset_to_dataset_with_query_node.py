from datahub.metadata.urns import DatasetUrn
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()

upstream_urn = DatasetUrn(platform="snowflake", name="upstream_table")
downstream_urn = DatasetUrn(platform="snowflake", name="downstream_table")

transformation_text = """
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HighValueFilter").getOrCreate()
df = spark.read.table("customers")
high_value = df.filter("lifetime_value > 10000")
high_value.write.saveAsTable("high_value_customers")
"""

client.lineage.add_lineage(
    upstream=upstream_urn,
    downstream=downstream_urn,
    transformation_text=transformation_text,
    column_lineage={"id": ["id", "customer_id"]},
)

# by passing the transformation_text, the query node will be created with the table level lineage.
# transformation_text can be any transformation logic e.g. a spark job, an airflow DAG, python script, etc.
# if you have a SQL query, we recommend using add_dataset_lineage_from_sql instead.
# note that transformation_text itself will not create a column level lineage.
