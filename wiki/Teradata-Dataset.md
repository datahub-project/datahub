> This doc is for older versions (v0.2.1 and before) of WhereHows. Please refer to [this](../wherehows-etl/README.md) for the latest version.

Collect dataset metadata from Teradata.

## configuration
List of properties required for the ETL process:

| configuration key | description   |
|---                |---            |
|teradata.databases|the databases that will be collected, comma separated, e.g., financial,manufacturing|
|teradata.db.driver|Teradata database driver, e.g., com.teradata.jdbc.TeraDriver|
|teradata.db.jdbc.url|Teradata database JDBC URL (not including username and password), e.g., jdbc:teradata://localhost/DBS_PORT=1025|
|teradata.db.username| Teradata database username |
|teradata.db.password| Teradata database password |
|teradata.default_database|default database for connection|
|teradata.field_metadata|place to store the field metadata CSV file, e.g., var/tmp/wherehows/field_metadata |
|teradata.log|place to store the log file, e.g., /var/tmp/wherehows/teradata_log |
|teradata.metadata|place to store metadata CSV file|
|teradata.sample.skip.list| the tables you want to skip for sample data collecting (e.g., for security reasons)|
|teradata.sample_output|place to store sample data file|
|teradata.schema_output|place to store schema data file|

## Extract
Major related file: [TeradataExtract.py](../wherehows-etl/src/main/resources/jython/TeradataExtract.py)

Get metadata from Teradata DBC databases and store it in a local JSON file.

Major source tables: DBC.Tables, DBC.Columns, DBC.ColumnStatsV, DBC.TableSize, DBC.Indieces, DBC.IndexConstraints

'Select top 10' to get sample data.

## Transform
Major related file: [TeradataTransform.py](../wherehows-etl/src/main/resources/jython/TeradataTransform.py)

Transform the JSON output into CSV format for easy loading.

## Load
Major related file: [TeradataLoad.py](../wherehows-etl/src/main/resources/jython/TeradataLoad.py)

Load into MySQL database.

Related tables: dict_dataset, dict_field_detail, dict_dataset_sample
