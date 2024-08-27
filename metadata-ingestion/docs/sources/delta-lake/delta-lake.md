## Usage Guide

If you are new to [Delta Lake](https://delta.io/) and want to test out a simple integration with Delta Lake and DataHub, you can follow this guide. 

### Delta Table on Local File System

#### Step 1 
Create a delta table using the sample PySpark code below if you don't have a delta table you can point to.

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

#### Step 2
Create a datahub ingestion yaml file (delta.dhub.yaml) to ingest metadata from the delta table you just created.

```yaml
source:
  type: "delta-lake"
  config:
    base_path:  "quickstart/my-table"
    
sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

Note: Make sure you run the Spark code as well as recipe from same folder otherwise use absolute paths.

#### Step 3 
Execute the ingestion recipe:
```shell
datahub ingest -c delta.dhub.yaml
```

### Delta Table on S3

#### Step 1 
Set up your AWS credentials by creating an AWS credentials config file; typically in '$HOME/.aws/credentials'.
```
[my-creds]
aws_access_key_id: ######
aws_secret_access_key: ######
```
Step 2: Create a Delta Table using the PySpark sample code below unless you already have Delta Tables on your S3. 
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
config_object.read("$HOME/.aws/credentials")
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

#### Step 3
Create a datahub ingestion yaml file (delta.s3.dhub.yaml) to ingest metadata from the delta table you just created.

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

#### Step 4
Execute the ingestion recipe:
```shell
datahub ingest -c delta.s3.dhub.yaml
```

### Note

The above recipes are minimal recipes. Please refer to [Config Details](#config-details) section for the full configuration.
