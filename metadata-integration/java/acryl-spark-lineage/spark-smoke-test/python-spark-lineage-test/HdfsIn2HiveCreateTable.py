from pyspark.sql import SparkSession

DATA_DIR = "../resources/data"
TEST_CASE_NAME = "PythonHdfsIn2HiveCreateTable"
TEST_DB = TEST_CASE_NAME

spark = SparkSession \
    .builder \
    .appName(TEST_CASE_NAME) \
    .enableHiveSupport() \
    .getOrCreate()

def tbl(tbl_name):
  return TEST_DB + "." + tbl_name

spark.sql("DROP DATABASE IF EXISTS " + TEST_DB + " CASCADE")  
spark.sql("CREATE DATABASE IF NOT EXISTS " + TEST_DB)
spark.sql("DROP TABLE IF EXISTS " + tbl("foo3"))

df1 = spark.read \
        .option("header", "true") \
        .csv(DATA_DIR + "/in1.csv") \
        .withColumnRenamed("c1", "a") \
        .withColumnRenamed("c2", "b")

df2 = spark.read \
        .option("header", "true") \
        .csv(DATA_DIR + "/in2.csv") \
        .withColumnRenamed("c1", "c") \
        .withColumnRenamed("c2", "d")

df = df1.join(df2, "id").drop("id")

df.write.mode('overwrite').saveAsTable(tbl("foo3")) # CreateDataSourceTableAsSelectCommand

spark.stop()


