## Quickstart Guide

### Follow below steps to get started on delta lake source

#### On local system
- create delta table ( sample code below) if you dont have already created table
```python 
import uuid
import random
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

def generate_data():
    return [(y, m, d, str(uuid.uuid4()), str(random.randrange(10000) % 26 + 65) * 3, random.random()*10000)
    for d in range(1, 29)
    for m in range(1, 13)
    for y in range(2000, 2021)]

jar_packages = ["org.apache.hadoop:hadoop-aws:3.2.3", "io.delta:delta-core_2.12:1.2.1"]
spark = SparkSession.builder \
    .appName("quickstart") \
    .master("local[*]") \
    .config("spark.jars.packages", ",".join(jar_packages)) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

table_path = "quickstart/my-table"
columns = ["year", "month", "day", "sale_id", "customer", "total_cost"]
spark.sparkContext.parallelize(generate_data()).toDF(columns).repartition(1).write.format("delta").save(table_path)

df = spark.read.format("delta").load(table_path)
df.show()

```

- create yml file with recipe

```recipe.yml
source:
  type: "delta-lake"
  config:
    base_path:  "quickstart/my-table"
    
sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```
Make sure you run the spark code as well as recipe from same folder otherwise use absolute paths.

- execute recipe with command:
```commandline
datahub ingest -c local_config.yml
```


#### On S3

- Create 'aws_creds' config file 
```
[my-creds]
aws_access_key_id: ######
aws_secret_access_key: ######
```
- create delta table ( sample code below) if you dont have already created table
```python
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from configparser import ConfigParser
import uuid
import random
def generate_data():
    return [(y, m, d, str(uuid.uuid4()), str(random.randrange(10000) % 26 + 65) * 3, random.random()*10000)
    for d in range(1, 29)
    for m in range(1, 13)
    for y in range(2000, 2021)]

jar_packages = ["org.apache.hadoop:hadoop-aws:3.2.3", "io.delta:delta-core_2.12:1.2.1"]
spark = SparkSession.builder \
    .appName("quickstart") \
    .master("local[*]") \
    .config("spark.jars.packages", ",".join(jar_packages)) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


config_object = ConfigParser()
config_object.read("aws_creds")
profile_info = config_object["my-creds"]
access_id = profile_info["aws_access_key_id"]
access_key = profile_info["aws_secret_access_key"]

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
hadoop_conf.set("fs.s3a.access.key", access_id)
hadoop_conf.set("fs.s3a.secret.key", access_key)

table_path = "s3a://my-bucket/my-folder/sales-table"
columns = ["year", "month", "day", "sale_id", "customer", "total_cost"]
spark.sparkContext.parallelize(generate_data()).toDF(columns).repartition(1).write.format("delta").save(table_path)
df = spark.read.format("delta").load(table_path)
df.show()

```
- create yml file with recipe

```yml
source:
  type: "delta-lake"
  config:
    base_path:  "s3://my-bucket/my-folder/sales-table"
    s3:
      aws_config:
        aws_access_key_id: <<Access key>>
        aws_secret_access_key: <<secret key>>
    
sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```
- execute recipe with command:
```commandline
datahub ingest -c s3_config.yml
```

### Notes

- Above recipes are minimal recipes. Please refer to Config Details section for full configuration.
