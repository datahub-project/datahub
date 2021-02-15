# SQL-Based Metadata Ingestion

This directory contains example ETL scripts that use [SQLAlchemy](https://www.sqlalchemy.org/) to ingest basic metadata
from a wide range of [commonly used SQL-based data systems](https://docs.sqlalchemy.org/en/13/dialects/index.html),
including MySQL, PostgreSQL, Oracle, MS SQL, Redshift, BigQuery, Snowflake, etc.

## Requirements
You'll need to install both the common requirements (`common.txt`) and the system-specific driver for the script (e.g.
`mysql_etl.txt` for `mysql_etl.py`). Some drivers also require additional dependencies to be installed so please check
the driver's official project page for more details. 

## Example
Here's an example on how to ingest metadata from MySQL.

Install requirements
```
pip install --user -r common.txt -r mysql_etl.txt
```

Modify these variables in `mysql_etl.py` to match your environment
```
URL       # Connection URL in the form of mysql+pymysql://username:password@hostname:port
OPTIONS   # Additional conenction options for the driver
```

Run the ETL script
```
python mysql_etl.py
```
