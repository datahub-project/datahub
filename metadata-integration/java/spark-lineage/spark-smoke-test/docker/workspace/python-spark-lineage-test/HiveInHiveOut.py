from pyspark.sql import SparkSession

TEST_CASE_NAME = "PythonHiveInHiveOut"
DATA_DIR = "../resources/data"
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
spark.sql("DROP TABLE IF EXISTS " +tbl("hivetab"))
spark.sql("DROP TABLE IF EXISTS " +tbl("foo5"))

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

df1.createOrReplaceTempView("v1")
df2.createOrReplaceTempView("v2")

# CreateHiveTableAsSelectCommand
spark.sql(
        "create table " + tbl("foo5") + " as " + "(select v1.a, v1.b, v2.c, v2.d from v1 join v2 on v1.id = v2.id)")

# CreateHiveTableAsSelectCommand
spark.sql("create table " + tbl("hivetab") + " as " + "(select * from " + tbl("foo5") + ")");

# InsertIntoHiveTable
spark.sql("insert into " + tbl("hivetab") + " (select * from " + tbl("foo5") + ")");


df = spark.sql("select * from " + tbl("foo5"));

# InsertIntoHiveTable
df.write.insertInto(tbl("hivetab"));

spark.stop()


