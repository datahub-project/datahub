from datahub.ingestion.source.powerbi.m_query.odbc import (
    extract_driver,
    extract_dsn,
    extract_platform,
    extract_server,
    normalize_platform_name,
)

test_connection_strings = [
    "Driver={SQL Server};Server=server;Database=database;Uid=sa;Pwd=pass",
    "Driver={Oracle ODBC Driver};Server=server.example.com:1521/ORCLPDB;Uid=username;Pwd=password;",
    "Driver={MySQL ODBC 3.51 driver};server=server;database=database;uid=user;pwd=pass;",
    "Driver={MySQL ODBC 3.51 driver};server=server;database=database;uid=user;pwd=pass;",
    "DRIVER={PostgreSQL};SERVER=server;DATABASE=database;UID=user;PWD=pass;",
    "Driver={ODBC Driver 17 for SQL Server};Server=server;Database=database;Uid=sa;Pwd=pass",
    "Driver={MySQL ODBC 8.0 Driver};Server=server;Port=3306;Database=database;User=username;Password=password;Option=3;",
    "Driver={SnowflakeDSIIDriver};Server=account.snowflakecomputing.com;Database=mydb;Warehouse=warehouse;UID=username;PWD=mypassword;Role=role;Schema=schema;",
    "Driver={Simba Spark ODBC Driver};Host=dbc-xxxxxxxx-xxxx.cloud.databricks.com;Port=443;HTTPPath=/sql/protocolv1/o/xxxxxxxxxx/xxxxxxxxxx;AuthMech=3;UID=token;PWD=dapi_xxxxxxxxxxxxxxxxxxxxxx;SSL=1;ThriftTransport=2;",
    "Driver={Simba Google BigQuery ODBC Driver};OAuthMechanism=0;Catalog=project;ProjectId=project;RefreshToken=refreshtoken;",
]

dsn_connection_strings = [
    "DSN=SalesDatabase;UID=sales_user;PWD=password123;",
    "DSN=FinanceWarehouse;",
    'DSN="Enterprise Reporting";UID=reporter;PWD=rep0rt!ng;',
    "DSN='Supply Chain';Encrypt=Yes;TrustServerCertificate=No;",
    "UID=user;PWD=pass;DSN=BackupServer;",
]

dsn_to_platform_map = {
    "SalesDatabase": "mssql",
    "FinanceWarehouse": "mysql",
    "Enterprise Reporting": "mysql",
    "Supply Chain": "mysql",
    "BackupServer": "mysql",
}

mapped_dsn_list = ["mssql", "mysql", "mysql", "mysql", "mysql"]

server_list = [
    "server",
    "server.example.com",
    "server",
    "server",
    "server",
    "server",
    "server",
    "account.snowflakecomputing.com",
    "dbc-xxxxxxxx-xxxx.cloud.databricks.com",
    None,
]

platform_list = [
    ("mssql", "SQL Server"),
    ("oracle", "Oracle"),
    ("mysql", "MySQL"),
    ("mysql", "MySQL"),
    ("postgres", "PostgreSQL"),
    ("mssql", "SQL Server"),
    ("mysql", "MySQL"),
    ("snowflake", "Snowflake"),
    ("databricks", "Databricks"),
    ("bigquery", "Google BigQuery"),
]

driver_list = [
    "SQL Server",
    "Oracle ODBC Driver",
    "MySQL ODBC 3.51 driver",
    "MySQL ODBC 3.51 driver",
    "PostgreSQL",
    "ODBC Driver 17 for SQL Server",
    "MySQL ODBC 8.0 Driver",
    "SnowflakeDSIIDriver",
    "Simba Spark ODBC Driver",
    "Simba Google BigQuery ODBC Driver",
]


def test_server_extraction():
    for connection_string, result in zip(test_connection_strings, server_list):
        assert extract_server(connection_string) == result


def test_platform_extraction():
    for connection_string, result in zip(test_connection_strings, platform_list):
        assert extract_platform(connection_string) == result


def test_driver_extraction():
    for connection_string, result in zip(test_connection_strings, driver_list):
        assert extract_driver(connection_string) == result


def test_dsn_mapping():
    for expected, connection_string in zip(mapped_dsn_list, dsn_connection_strings):
        dsn = extract_dsn(connection_string)
        assert dsn is not None
        mapped_platform, mapped_powerbi_platform = normalize_platform_name(
            dsn_to_platform_map[dsn]
        )
        assert mapped_platform == expected
