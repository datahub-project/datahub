"""Iceberg-on-Glue connection-instance scenario.

Reads/writes an Iceberg table through Iceberg's GlueCatalog pointed at a mock Glue + S3 (moto), so
OpenLineage emits the table with an ``arn:aws:glue:<region>:<account>`` namespace. The DataHub
listener (configured via spark-docker.conf + the ``connections`` --conf overrides on the
spark-submit line) then stamps the upstream URN with the platform_instance mapped for that catalog
ARN. test_e2e.py asserts the resulting ``glue,domain_a.…`` dataset exists in GMS.

This lives in the spark-submit harness (not the in-JVM unit suite) because OpenLineage only generates
the Iceberg→Glue catalog symlink under a real spark-submit execution, not for an in-process
local[*] SparkSession.

AWS credentials/region/endpoint and the checksum settings come from the container environment (see
setup_spark_smoke_test.sh); Iceberg packages come from --packages (see python_test_run.sh).
"""
import urllib.request

from pyspark.sql import SparkSession

MOTO_ENDPOINT = "http://moto:5000"
TEST_CASE_NAME = "PythonGlueIcebergConnectionInstance"

# S3FileIO does not create the warehouse bucket; moto accepts an unauthenticated path-style PUT.
try:
    urllib.request.urlopen(
        urllib.request.Request(MOTO_ENDPOINT + "/warehouse", method="PUT")
    )
except Exception as e:  # noqa: BLE001 - best-effort; bucket may already exist
    print("warehouse bucket create:", e)

spark = (
    SparkSession.builder.appName(TEST_CASE_NAME)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.glue_cat", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.glue_cat.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config("spark.sql.catalog.glue_cat.warehouse", "s3://warehouse/")
    .config("spark.sql.catalog.glue_cat.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_cat.s3.endpoint", MOTO_ENDPOINT)
    .config("spark.sql.catalog.glue_cat.s3.path-style-access", "true")
    .config("spark.sql.catalog.glue_cat.glue.endpoint", MOTO_ENDPOINT)
    .config("spark.sql.catalog.glue_cat.client.region", "us-east-1")
    .getOrCreate()
)

spark.sql("CREATE DATABASE IF NOT EXISTS glue_cat.my_glue_database")
spark.sql(
    "CREATE TABLE IF NOT EXISTS glue_cat.my_glue_database.my_glue_table "
    "(id INT, amount INT) USING iceberg"
)
spark.sql("INSERT INTO glue_cat.my_glue_database.my_glue_table VALUES (1, 100)")

# Read the Glue-catalog table and write it out: the read makes the Glue table an upstream the
# listener stamps with the per-connection platform_instance.
spark.table("glue_cat.my_glue_database.my_glue_table").write.mode("overwrite").csv(
    "../resources/data/" + TEST_CASE_NAME + "/out.csv"
)
spark.stop()
