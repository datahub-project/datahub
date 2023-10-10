### Prerequisites
1. Create a user which has access to the database you want to ingest.
    ```sql
    CREATE USER datahub FROM <database> AS PASSWORD = <password> PERM = 20000000;
    ```
2. Create a user with the following privileges:
    ```sql
    GRANT SELECT ON dbc.columns TO datahub;
    GRANT SELECT ON dbc.databases TO datahub;
    GRANT SELECT ON dbc.tables TO datahub;
    GRANT SELECT ON DBC.All_RI_ChildrenV TO datahub;
    GRANT SELECT ON DBC.ColumnsV TO datahub;
    GRANT SELECT ON DBC.IndicesV TO datahub;
    GRANT SELECT ON dbc.TableTextV TO datahub;
    GRANT SELECT ON dbc.TablesV TO datahub;
    ```
   
    If you want to run profiling, you need to grant select permission on all the tables you want to profile.
