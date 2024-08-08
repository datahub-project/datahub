from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("DROP TABLE nyc.taxis PURGE")
