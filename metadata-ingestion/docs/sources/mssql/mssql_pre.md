### Prerequisites

If you want to ingest MSSQL Jobs the user credentials needs the proper privileges.

Script for granting the privileges:
```
USE MSDB
GRANT SELECT ON OBJECT::msdb.dbo.sysjobsteps TO 'USERNAME'
GRANT SELECT ON OBJECT::msdb.dbo.sysjobs TO 'USERNAME'
```