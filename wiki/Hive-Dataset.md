> This doc is for older versions (v0.2.1 and before) of WhereHows. Please refer to [this](../wherehows-etl/README.md) for the latest version.


Collect dataset metadata from Hive.

## Configuration
List of properties in the wh_etl_job_property table that are required for the Hive dataset ETL process:

| configuration key | description|
|---|---|
| hive.metastore.jdbc.url | hive metastore jdbc url|
| hive.metastore.jdbc.driver | hive metastore jdbc driver |
| hive.metastore.username | hive metastore user name |
| hive.metastore.password | hive metastore password|
| hive.schema_json_file | local file location to store the schema json file|
| hive.schema_csv_file | local file location to store the schema csv file |
| hive.field_metadata | local file location to store the field metadata csv file |


## Extract
Major related file: [HiveExtract.py](../wherehows-etl/src/main/resources/jython/HiveExtract.py)

Connect to Hive Metastore to get the Hive table/view information and store it in a local JSON file.

Major source tables: COLUMNS_V2, SERDE_PARAMS

## Transform
Major related file: [HiveTransform.py](../wherehows-etl/src/main/resources/jython/HiveTransform.py)

Transform the JSON output into CSV format for easy loading.

## Load
Major related file: [HiveLoad.py](../wherehows-etl/src/main/resources/jython/HiveLoad.py)

Load into MySQL database.

Related tables: dict_dataset
