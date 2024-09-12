### Prerequisites
1. Create a user which has access to the database you want to ingest.
    ```sql
    CREATE USER datahub FROM <database> AS PASSWORD = <password> PERM = 20000000;
    ```
2. Create a user with the following privileges:
    ```sql
GRANT SELECT ON DBC.DatabasesV TO datahub;
GRANT SELECT ON DBC.TablesV TO datahub;
GRANT SELECT ON DBC.ColumnsV TO datahub;
GRANT SELECT ON DBC.IndicesV TO datahub;
GRANT SELECT ON dbc.TableTextV TO datahub;
GRANT SELECT ON DBC.All_RI_ChildrenV TO datahub;

-- if lineage or usage extraction is enabled
GRANT SELECT ON dbc.dbqlogtbl TO datahub; 
GRANT SELECT ON dbc.QryLogV TO datahub;
GRANT SELECT ON dbc.QryLogSqlV TO datahub;
    ```
   
    If you want to run profiling, you need to grant select permission on all the tables you want to profile.

3. If lineage or usage extraction is enabled, please, check if query logging is enabled and it is set to size which
will fit for your queries (the default query text size Teradata captures is max 200 chars)
   An example how you can set it for all users and capture the SQL:
    ```sql
    REPLACE QUERY LOGGING WITH SQL LIMIT SQLTEXT=2000 ON ALL;
    ```
   See more here about query logging:
      [https://docs.teradata.com/r/Teradata-VantageCloud-Lake/Database-Reference/Database-Administration/Tracking-Query-Behavior-with-Database-Query-Logging-Operational-DBAs](https://docs.teradata.com/r/Teradata-VantageCloud-Lake/Database-Reference/Database-Administration/Tracking-Query-Behavior-with-Database-Query-Logging-Operational-DBAs)
