### Prerequisites

If you want to ingest jobs and stored procedures (with code), the user credentials need proper privileges depending on your SQL Server type:

#### For SQL Server (on-premises/IaaS):
For SQL Server Agent jobs:
```sql
USE MSDB
GRANT SELECT ON OBJECT::msdb.dbo.sysjobsteps TO 'USERNAME'
GRANT SELECT ON OBJECT::msdb.dbo.sysjobs TO 'USERNAME'

USE 'DATA_DB_NAME'
GRANT VIEW DEFINITION TO 'USERNAME'
GRANT SELECT ON OBJECT::sys.sql_expression_dependencies TO 'USERNAME'
```

#### For Azure SQL Database:
For Elastic Jobs:
```sql
-- On the Job database (usually named 'JobDatabase' or similar)
GRANT SELECT ON SCHEMA::jobs TO 'USERNAME'

-- On target databases where jobs run
USE 'DATA_DB_NAME'
GRANT VIEW DEFINITION TO 'USERNAME'
GRANT SELECT ON OBJECT::sys.sql_expression_dependencies TO 'USERNAME'
```

Note:
 - For Azure SQL Database Elastic Jobs, you need access to the jobs database where the Elastic Jobs are configured 
 - The jobs schema contains tables like jobs.jobs and jobs.jobsteps 
 - Stored procedure permissions are the same for both SQL Server and Azure SQL Database 
 - The user must have appropriate permissions on all target databases where jobs can run to see their metadata