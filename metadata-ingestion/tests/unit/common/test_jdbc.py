import pytest

from datahub.ingestion.source.common.jdbc import (
    JdbcUrlInfo,
    normalize_jdbc_url,
    parse_jdbc_url,
    validate_jdbc_url,
)


@pytest.mark.parametrize(
    "url,expected",
    [
        ("jdbc:postgresql://host:5432/db", "postgresql://host:5432/db"),
        ("jdbc:mysql://host:3306/db", "mysql://host:3306/db"),
        ("jdbc:oracle:thin:@//host:1521/svc", "oracle://host:1521/svc"),
        (
            "oracle:thin:@host:1521/svc.corp.example.com",
            "oracle://host:1521/svc.corp.example.com",
        ),
        ("jdbc:oracle:thin:@host:1521:ORCL", "oracle://host:1521/ORCL"),
        ("jdbc:oracle:oci:@//host:1521/svc", "oracle://host:1521/svc"),
        (
            "jdbc:sqlserver://host:1433;databaseName=mydb;integratedSecurity=false",
            "mssql://host:1433/mydb",
        ),
        ("jdbc:sqlserver://host;databaseName=mydb", "mssql://host:1433/mydb"),
        ("jdbc:sqlserver://host:1433", "mssql://host:1433/"),
        ("jdbc:sqlserver://host;databaseName=;databaseName=db", "mssql://host:1433/"),
        (
            "jdbc:sqlserver://host:1433;databaseName=db;password=p@ss",
            "mssql://host:1433/db",
        ),
        (
            "jdbc:sqlserver://host:1433;databaseName=db;user=svc@tenant",
            "mssql://host:1433/db",
        ),
        ("jdbc:oracle:thin:@host:1521/svc;foo=bar", "oracle://host:1521/svc"),
        ("jdbc:jtds:sqlserver://host:1433/mydb", "mssql://host:1433/mydb"),
        ("jdbc:jtds:sqlserver://host:1433", "mssql://host:1433/"),
    ],
)
def test_normalize_jdbc_url(url: str, expected: str) -> None:
    assert normalize_jdbc_url(url) == expected


@pytest.mark.parametrize(
    "url,valid",
    [
        ("jdbc:postgresql://host:5432/db", True),
        ("oracle:thin:@host:1521/svc", True),
        ("jdbc:oracle:thin:@//host:1521/svc", True),
        ("jdbc:sqlserver://host:1433;databaseName=db", True),
        ("jdbc:sqlserver://host:1433;databaseName=db;password=p@ss", True),
        ("jdbc:jtds:sqlserver://host:1433/db", True),
        ("", False),
        ("not-a-url", False),
        ("http://host:8080/path", False),
    ],
)
def test_validate_jdbc_url(url: str, valid: bool) -> None:
    assert validate_jdbc_url(url) is valid


@pytest.mark.parametrize(
    "url,expected",
    [
        (
            "jdbc:postgresql://pg.example.com:5432/sales?ssl=true",
            JdbcUrlInfo(
                platform="postgres", host="pg.example.com", port=5432, database="sales"
            ),
        ),
        (
            "jdbc:postgresql://cluster.abc.us-east-1.redshift.amazonaws.com:5439/dev",
            JdbcUrlInfo(
                platform="redshift",
                host="cluster.abc.us-east-1.redshift.amazonaws.com",
                port=5439,
                database="dev",
            ),
        ),
        (
            "jdbc:redshift://rs.example.com:5439/dev",
            JdbcUrlInfo(
                platform="redshift", host="rs.example.com", port=5439, database="dev"
            ),
        ),
        (
            "jdbc:mysql://my.example.com:3306/shop?useSSL=false",
            JdbcUrlInfo(
                platform="mysql", host="my.example.com", port=3306, database="shop"
            ),
        ),
        (
            "jdbc:mariadb://maria.example.com/shop",
            JdbcUrlInfo(platform="mariadb", host="maria.example.com", database="shop"),
        ),
        (
            "jdbc:db2://db2.example.com:50000/SAMPLE:currentSchema=APP;",
            JdbcUrlInfo(
                platform="db2", host="db2.example.com", port=50000, database="SAMPLE"
            ),
        ),
        (
            "jdbc:sqlserver://sql.example.com:1433;encrypt=true;databaseName=sales",
            JdbcUrlInfo(
                platform="mssql", host="sql.example.com", port=1433, database="sales"
            ),
        ),
        (
            "jdbc:sqlserver://sql.example.com;database=sales",
            JdbcUrlInfo(platform="mssql", host="sql.example.com", database="sales"),
        ),
        (
            "jdbc:sqlserver://sql.example.com;databaseName=;database=sales",
            JdbcUrlInfo(platform="mssql", host="sql.example.com", database="sales"),
        ),
        (
            "jdbc:jtds:sqlserver://sql.example.com:1433/sales;domain=CORP",
            JdbcUrlInfo(
                platform="mssql", host="sql.example.com", port=1433, database="sales"
            ),
        ),
        (
            "jdbc:oracle:thin:@ora.example.com:1521:ORCL",
            JdbcUrlInfo(
                platform="oracle", host="ora.example.com", port=1521, database="ORCL"
            ),
        ),
        (
            "jdbc:oracle:thin:@//ora.example.com:1521/sales.example.com",
            JdbcUrlInfo(
                platform="oracle",
                host="ora.example.com",
                port=1521,
                database="sales.example.com",
            ),
        ),
        (
            "jdbc:snowflake://acct.snowflakecomputing.com/?db=ANALYTICS&schema=PUBLIC&warehouse=WH",
            JdbcUrlInfo(
                platform="snowflake",
                host="acct.snowflakecomputing.com",
                database="ANALYTICS",
                schema_name="PUBLIC",
            ),
        ),
        (
            "jdbc:sap://hana.example.com:30015/?databaseName=HXE",
            JdbcUrlInfo(
                platform="hana", host="hana.example.com", port=30015, database="HXE"
            ),
        ),
        (
            "jdbc:teradata://td.example.com/DATABASE=sales,CHARSET=UTF8",
            JdbcUrlInfo(platform="teradata", host="td.example.com", database="sales"),
        ),
        (
            "jdbc:clickhouse://ch.example.com:8123/events",
            JdbcUrlInfo(
                platform="clickhouse",
                host="ch.example.com",
                port=8123,
                database="events",
            ),
        ),
        (
            "jdbc:vertica://vt.example.com:5433/warehouse",
            JdbcUrlInfo(
                platform="vertica",
                host="vt.example.com",
                port=5433,
                database="warehouse",
            ),
        ),
        (
            "jdbc:trino://trino.example.com:8080/hive/sales",
            JdbcUrlInfo(
                platform="trino",
                host="trino.example.com",
                port=8080,
                database="hive",
                schema_name="sales",
            ),
        ),
        (
            "jdbc:presto://presto.example.com:8080/hive",
            JdbcUrlInfo(
                platform="presto", host="presto.example.com", port=8080, database="hive"
            ),
        ),
        (
            "jdbc:hive2://hive.example.com:10000/default;principal=hive/_HOST@REALM",
            JdbcUrlInfo(
                platform="hive", host="hive.example.com", port=10000, database="default"
            ),
        ),
        (
            "jdbc:mysql://h1.example.com:3306,h2.example.com:3306/shop",
            JdbcUrlInfo(platform="mysql", host="h1.example.com", database="shop"),
        ),
        (
            "jdbc:acme://acme.example.com:9999/db",
            JdbcUrlInfo(host="acme.example.com", port=9999),
        ),
        (
            "jdbc:mysqlx://my.example.com:33060/shop",
            JdbcUrlInfo(host="my.example.com", port=33060),
        ),
        (
            "jdbc:mysql:loadbalance://h1.example.com:3306,h2.example.com:3306/shop",
            JdbcUrlInfo(platform="mysql", host="h1.example.com", database="shop"),
        ),
        (
            "jdbc:redshift:iam://rs.example.com:5439/dev",
            JdbcUrlInfo(
                platform="redshift", host="rs.example.com", port=5439, database="dev"
            ),
        ),
        (
            "jdbc:clickhouse:https://ch.example.com:8443/events?ssl=true",
            JdbcUrlInfo(
                platform="clickhouse",
                host="ch.example.com",
                port=8443,
                database="events",
            ),
        ),
        (
            "JDBC:PostgreSQL://pg.example.com:5432/Sales",
            JdbcUrlInfo(
                platform="postgres", host="pg.example.com", port=5432, database="Sales"
            ),
        ),
        (
            "JDBC:SQLServer://sql.example.com;databaseName=Sales",
            JdbcUrlInfo(platform="mssql", host="sql.example.com", database="Sales"),
        ),
    ],
)
def test_parse_jdbc_url(url: str, expected: JdbcUrlInfo) -> None:
    assert parse_jdbc_url(url) == expected


@pytest.mark.parametrize(
    "url",
    [
        "",
        "not-a-url",
        "http://host:8080/path",
        "jdbc:",
        "jdbc:mysql://",
        "jdbc:mysql://[::1/db",
        "jdbc:mysql:unknownmode://my.example.com/shop",
    ],
)
def test_parse_jdbc_url_invalid(url: str) -> None:
    assert parse_jdbc_url(url) == JdbcUrlInfo()
