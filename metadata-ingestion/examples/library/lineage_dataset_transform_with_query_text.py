from datahub.metadata.urns import DatasetUrn
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()

# this can be any transformation logic e.g. a spark job, an airflow DAG, python script, etc.
# if you have a SQL query, we recommend using add_dataset_lineage_from_sql instead.

transformation_text = """
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HighValueFilter").getOrCreate()
df = spark.read.table("customers")
high_value = df.filter("lifetime_value > 10000")
high_value.write.saveAsTable("high_value_customers")
"""

client.lineage.add_dataset_transform_lineage(
    upstream=DatasetUrn(platform="snowflake", name="customers"),
    downstream=DatasetUrn(platform="snowflake", name="high_value_customers"),
    transformation_text=transformation_text,
)
