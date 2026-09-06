from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# PURGE is omitted on purpose: the catalog's S3 signer rejects the remote-signing
# requests Spark's purge makes, and file cleanup is irrelevant to the test's purpose.
spark.sql("DROP TABLE IF EXISTS nyc.taxis")
