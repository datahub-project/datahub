import pytest

from datahub.ingestion.source.qlik_sense.script_lineage import (
    apply_preceding_loads,
    connection_lookup_names,
    infer_data_connection,
    parse_load_script,
)

SCRIPT = """
// Load from files, not a database
Sales: LOAD * FROM [lib://Files/sales.qvd] (qvd);

/* SQL Server
   source */
LIB CONNECT TO 'Finance:Sales SQL';
Orders:
NoConcatenate
SQL SELECT o.order_id, o.amount FROM dbo.orders o WHERE note <> 'a;b // c /* d */';

Customers:
LOAD customer_id, [From], upper(name) AS name;
SELECT customer_id, name FROM dbo.customers;

DISCONNECT;
ODBC CONNECT TO [Lakehouse];
SELECT * FROM main.sales.events;
"""


def test_parse_load_script_tracks_connections_and_labels() -> None:
    tables = parse_load_script(SCRIPT)

    assert [(t.label, t.connection) for t in tables] == [
        ("Orders", "Finance:Sales SQL"),
        ("Customers", "Finance:Sales SQL"),
        ("table_3", "Lakehouse"),
    ]
    assert tables[0].sql.startswith("SELECT o.order_id")
    assert "'a;b // c /* d */'" in tables[0].sql


def test_parse_load_script_resolves_variables_and_control_blocks() -> None:
    script = """
SET vLib = 'Warehouse SQL';
LET vSchema = 'dbo';
LET vComputed = 'lib://' & vLib;
IF vReload = 1 THEN
  LIB CONNECT TO '$(vLib)';
END IF
Orders:
SQL SELECT * FROM $(vSchema).orders;
IF vEnv = 'PROD' THEN
  LIB CONNECT TO '$(vLib)';
  Prod: SQL SELECT * FROM dbo.prod_only;
ELSE
  LIB CONNECT TO 'Test SQL';
END IF
FOR EACH vTable IN 'a', 'b'
  SQL SELECT * FROM $(vTable);
NEXT vTable
LIB CONNECT TO '$(vComputed)';
SELECT * FROM dbo.events;
"""
    tables = parse_load_script(script)

    assert [(t.label, t.connection, t.sql) for t in tables] == [
        # Skipping the IF leaves no connection, so the branches disagree.
        ("Orders", None, "SELECT * FROM dbo.orders"),
        ("Prod", "Warehouse SQL", "SELECT * FROM dbo.prod_only"),
        ("table_3", None, "SELECT * FROM $(vTable)"),
        ("table_4", "$(vComputed)", "SELECT * FROM dbo.events"),
    ]


def test_parse_load_script_keeps_connection_all_branches_agree_on() -> None:
    script = """
IF vEnv = 'PROD' THEN
  LIB CONNECT TO 'Warehouse SQL';
ELSEIF vEnv = 'UAT' THEN
  LIB CONNECT TO 'Warehouse SQL';
ELSE
  LIB CONNECT TO 'Warehouse SQL';
ENDIF
SQL SELECT * FROM dbo.orders;
"""
    assert [t.connection for t in parse_load_script(script)] == ["Warehouse SQL"]


def test_parse_load_script_merges_switch_cases_and_branch_variables() -> None:
    script = """
SWITCH vEnv
CASE 'PROD'
  LIB CONNECT TO 'Prod SQL';
  SET vSchema = 'dbo';
  SET vDb = 'sales';
CASE 'TEST'
  LIB CONNECT TO 'Test SQL';
  SET vSchema = 'test';
  SET vDb = 'sales';
DEFAULT
  LIB CONNECT TO 'Prod SQL';
  SET vSchema = 'dbo';
  SET vDb = 'sales';
END SWITCH
SQL SELECT * FROM $(vDb).$(vSchema).orders;
IF vMode = 'IF x THEN y' THEN
  LIB CONNECT TO 'Prod SQL';
  Inner: SQL SELECT * FROM dbo.inner_only;
END IF
"""
    assert [(t.label, t.connection, t.sql) for t in parse_load_script(script)] == [
        ("table_1", None, "SELECT * FROM sales.$(vSchema).orders"),
        ("Inner", "Prod SQL", "SELECT * FROM dbo.inner_only"),
    ]


def test_parse_load_script_switch_without_default_keeps_value_from_before() -> None:
    script = """
LIB CONNECT TO 'Prod SQL';
SWITCH vEnv
CASE 'PROD'
  LIB CONNECT TO 'Prod SQL';
CASE 'UAT'
  LIB CONNECT TO 'Prod SQL';
END SWITCH
Same: SQL SELECT * FROM dbo.orders;
LIB CONNECT TO 'Test SQL';
SWITCH vEnv
CASE 'PROD'
  LIB CONNECT TO 'Prod SQL';
END SWITCH
Differs: SQL SELECT * FROM dbo.orders;
"""
    # No CASE may match, so the connection from before SWITCH is an outcome too.
    assert [(t.label, t.connection) for t in parse_load_script(script)] == [
        ("Same", "Prod SQL"),
        ("Differs", None),
    ]


def test_infer_data_connection_reads_driver_and_database() -> None:
    sql_server = infer_data_connection(
        "Sales SQL",
        "OLEDB CONNECT TO [Provider=MSOLEDBSQL;Data Source=db;Initial Catalog=sales_db]",
    )
    assert (sql_server.platform, sql_server.database) == ("mssql", "sales_db")

    spaced = infer_data_connection(
        "Warehouse",
        "OLEDB CONNECT TO [Provider=MSOLEDBSQL;Initial Catalog=Sales Warehouse];OLEDB",
    )
    assert spaced.database == "Sales Warehouse"

    databricks = infer_data_connection("Lakehouse", "Simba Spark ODBC Databricks")
    assert databricks.platform == "databricks"

    assert infer_data_connection("Files", "folder").platform is None


@pytest.mark.parametrize(
    "name, platform",
    [
        ("MySQL Server", "mysql"),
        ("PostgreSQL Server", "postgres"),
        ("Sales SQL Server", "mssql"),
    ],
)
def test_infer_data_connection_matches_whole_platform_names(
    name: str, platform: str
) -> None:
    assert infer_data_connection(name, "").platform == platform


def test_connection_lookup_names_strips_cloud_space() -> None:
    assert connection_lookup_names("Finance:Sales SQL") == [
        "Finance:Sales SQL",
        "Sales SQL",
    ]
    assert connection_lookup_names("Sales SQL") == ["Sales SQL"]


PRECEDING_LOAD_SCRIPT = """
LIB CONNECT TO 'Sales SQL';
Orders:
LOAD OrderID AS [Order Id], [Net Sales],
     Year(order_date) & '-' & Month(order_date) AS Period,
     'ERP' AS Source, Upper(region)
WHERE amount > 0;
LOAD *, order_id AS OrderID, amount AS [Net Sales];
SQL SELECT order_id, amount, order_date, region FROM dbo.orders;
"""


def test_preceding_loads_rename_sql_columns() -> None:
    (table,) = parse_load_script(PRECEDING_LOAD_SCRIPT)

    assert table.label == "Orders"
    assert apply_preceding_loads(
        table.preceding_loads,
        {
            "ORDER_ID": ["order_id_urn"],
            "amount": ["amount_urn"],
            "order_date": ["order_date_urn"],
            "region": ["region_urn"],
        },
    ) == {
        "Order Id": ["order_id_urn"],
        "Net Sales": ["amount_urn"],
        "Period": ["order_date_urn"],
        "Source": [],
    }
