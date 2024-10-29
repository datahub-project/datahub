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
### General metadata extrcation prerequisites
- Exctracting column description requires: SELECT on "sys.tables", "sys.all_columns", "sys.extended_properties". [It is also limited to securables that a user either owns, or on which the user was granted](https://learn.microsoft.com/en-us/sql/relational-databases/security/metadata-visibility-configuration?view=sql-server-ver16);
- Extracting databases information requires: SELECT on "master.sys.databases". [Membership in the **Public role** or **VIEW ANY DATABASE** permission](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-databases-transact-sql?view=sql-server-ver16#permissions);
- Extracting jobs information requires: SELECT on "msdb.dbo.sysjobs" & "msdb.dbo.sysjobsteps". [Membership in the **SQLAgentReaderRole** or all the **SQLAgentUserRole** permissions](https://learn.microsoft.com/en-us/sql/ssms/agent/sql-server-agent-fixed-database-roles?view=sql-server-ver16#sqlagentreaderrole-permissions);
- Extracting stored procedures information requires: SELECT on "sys.procedures" & "sys.schemas". [It is also limited to securables that a user either owns, or on which the user was granted](https://learn.microsoft.com/en-us/sql/relational-databases/security/metadata-visibility-configuration?view=sql-server-ver16);
- Extrcating stored procedure code requires: EXECUTE on "sp_helptext". [Membership in the **Public role**](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-helptext-transact-sql?view=sql-server-ver16#permissions);
- Extracting procedure`s input requires: SELECT on "sys.parameters". [It is also limited to securables that a user either owns, or on which the user was granted](https://learn.microsoft.com/en-us/sql/relational-databases/security/metadata-visibility-configuration?view=sql-server-ver16);
- Extracting procedure`s properties requires: SELECT on "sys.procedures". [It is also limited to securables that a user either owns, or on which the user was granted](https://learn.microsoft.com/en-us/sql/relational-databases/security/metadata-visibility-configuration?view=sql-server-ver16);
- Extracting table description requires: SELECT on "sys.tables" & "sys.extended_properties". [It is also limited to securables that a user either owns, or on which the user was granted](https://learn.microsoft.com/en-us/sql/relational-databases/security/metadata-visibility-configuration?view=sql-server-ver16).

### Lineage extraction prerequisites
- Extracting table-table, table-view lineage requires: SELECT on "sys.sql_expression_dependencies", "sys.objects". [Requires VIEW DEFINITION permission on the database and SELECT permission on sys.sql_expression_dependencies for the database](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-sql-expression-dependencies-transact-sql?view=sql-server-ver16#permissions). [It is also limited to securables that a user either owns, or on which the user was granted](https://learn.microsoft.com/en-us/sql/relational-databases/security/metadata-visibility-configuration?view=sql-server-ver16).
- Extracting procedure-table, procedure-view, procedure-procedure lineage requires: SELECT on "sys.procedures", "sys.objects", "sys.schemas", "sys.synonyms", "sys.dm_sql_referenced_entities". [VIEW DEFINITION permission on the referencing entity.](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-sql-referenced-entities-transact-sql?view=sql-server-ver16#permissions). [It is also limited to securables that a user either owns, or on which the user was granted](https://learn.microsoft.com/en-us/sql/relational-databases/security/metadata-visibility-configuration?view=sql-server-ver16).