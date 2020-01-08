> This doc is for older versions (v0.2.1 and before) of WhereHows. Please refer to [this](../wherehows-etl/README.md) for the latest version.

Collect dataset metadata from Oracle DB.

## Configuration
List of properties in the wh_etl_job_property table that are required for the Oracle dataset ETL process:

| configuration key | description|
|---|---|
| oracle.db.driver | oracle database jdbc driver |
| oracle.db.name | oracle database name |
| oracle.db.username | oracle database user name |
| oracle.db.password | oracle database password |
| oracle.db.jdbc.url | oracle database jdbc url |
| oracle.metadata | local file location to store the oracle datasets metadata csv file |
| oracle.field_metadata | local file location to store the oracle dataset fields csv file |
| oracle.sample_data | local file location to store the oracle dataset sample data csv file |
| oracle.load_sample | true/false whether to get sample data |
| oracle.exclude_db | list of excluded databases in oracle |

## Extract
Major related file: [OracleExtract.py](../wherehows-etl/src/main/resources/jython/OracleExtract.py)

Connect to Oracle database to get all the table/column/comments information excluding the databases in the exclude list. Extra table information including indices, constraints and partitions are also fetched. The results are formatted and stored in two CSV files, one for table records and the other for field records.

Major source tables: ALL_TABLES, ALL_TAB_COLUMNS, ALL_COL_COMMENTS, ALL_INDEXES, ALL_IND_COLUMNS, ALL_CONSTRAINTS, ALL_PART_KEY_COLUMNS

## Transform
Not needed.

## Load
Major related file: [OracleLoad.py](../wherehows-etl/src/main/resources/jython/OracleLoad.py)

Load into MySQL database, similar to HiveLoad or HdfsLoad.

Related tables: dict_dataset
