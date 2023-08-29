### Prerequisites

If you want to ingest MSSQL Jobs and stored procedures (with code) the user credentials needs the proper privileges.

Script for granting the privileges:
```
USE MSDB
GRANT SELECT ON OBJECT::msdb.dbo.sysjobsteps TO 'USERNAME'
GRANT SELECT ON OBJECT::msdb.dbo.sysjobs TO 'USERNAME'

USE 'DATA_DB_NAME'
GRANT VIEW DEFINITION TO 'USERNAME'
GRANT SELECT ON OBJECT::sys.sql_expression_dependencies TO 'USERNAME'
```