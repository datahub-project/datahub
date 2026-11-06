from typing import Dict, List, Optional

import pytest

from datahub.ingestion.source.qlik_sense.script_lineage import (
    apply_preceding_loads,
    connection_lookup_names,
    drop_condition_variables,
    expression_names,
    has_qualified_name,
    infer_data_connection,
    is_connector_sql,
    parse_load_script,
    redact_connection,
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


def test_infer_data_connection_prefers_the_driver() -> None:
    connection = "Driver={Snowflake};Server=sqlserver_sync.example.com;Database=sales"
    assert infer_data_connection(connection, connection).platform == "snowflake"


def test_parse_load_script_restores_connection_after_sub() -> None:
    script = """
LIB CONNECT TO 'A';
SUB LoadOther
  LIB CONNECT TO 'B';
  Inside: SQL SELECT * FROM db.s.inside;
END SUB
T: SQL SELECT * FROM db.s.t;
"""
    assert [(t.label, t.connection) for t in parse_load_script(script)] == [
        ("Inside", "B"),
        ("T", "A"),
    ]


def test_parse_load_script_skips_rem_comments() -> None:
    script = """
LIB CONNECT TO 'A';
REM Don't load the archive here;
rem another one;
T: SQL SELECT * FROM db.s.t;
"""
    assert [(t.label, t.connection) for t in parse_load_script(script)] == [("T", "A")]


def test_redact_connection_drops_credentials() -> None:
    connection = (
        "Driver={ODBC Driver 17 for SQL Server};Server=db.example.com;"
        "Database=sales;UID=svc_user;PWD={p;w}; User ID=other;XPassword=x;ApiKey=k"
    )

    redacted = redact_connection(connection)

    assert redacted == (
        "Driver={ODBC Driver 17 for SQL Server};Server=db.example.com;Database=sales"
    )
    assert redact_connection("Sales SQL") == "Sales SQL"


@pytest.mark.parametrize(
    "name, platform, instance, expected",
    [
        ("sales.dbo.orders", "mssql", None, True),
        ("sales.orders", "mssql", None, False),
        ("prod.sales.orders", "mssql", "prod", False),
        ("sales.orders", "mysql", None, True),
        ("orders", "oracle", None, True),
    ],
)
def test_has_qualified_name(
    name: str, platform: str, instance: Optional[str], expected: bool
) -> None:
    urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},PROD)"
    assert has_qualified_name(urn, platform, instance) == expected


def test_connection_lookup_names_strips_cloud_space() -> None:
    assert connection_lookup_names("Finance:Sales SQL") == [
        "Finance:Sales SQL",
        "Sales SQL",
    ]
    assert connection_lookup_names("Sales SQL") == ["Sales SQL"]


PRECEDING_LOAD_SCRIPT = """
LIB CONNECT TO 'Sales SQL';
Orders:
LOAD OrderID AS [Order Id], [Net Sales], %OrderKey,
     Year(order_date) & '-' & Month(order_date) AS Period,
     'ERP' AS Source, Upper(region)
WHERE amount > 0;
LOAD *, order_id AS OrderID, amount AS [Net Sales], order_id AS %OrderKey;
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
        "%OrderKey": ["order_id_urn"],
        "Period": ["order_date_urn"],
        "Source": [],
    }


@pytest.mark.parametrize(
    "field_list, expected",
    [
        pytest.param("$(vOrderFields)", {"order_id": ["order_id_urn"]}, id="variable"),
        pytest.param(
            "order_id AS OrderKey, $(vMoreFields)",
            {"order_id": ["order_id_urn"], "OrderKey": ["order_id_urn"]},
            id="variable_with_fields",
        ),
        pytest.param(
            "$(vFields) AS Renamed",
            {"order_id": ["order_id_urn"], "Renamed": []},
            id="variable_in_alias",
        ),
        pytest.param("Upper(order_id)", {}, id="unaliased_expression"),
    ],
)
def test_preceding_load_with_unknown_fields(
    field_list: str, expected: Dict[str, List[str]]
) -> None:
    (table,) = parse_load_script(
        f"""
LIB CONNECT TO 'Sales SQL';
Orders:
LOAD {field_list};
SQL SELECT order_id FROM dbo.orders;
"""
    )

    assert (
        apply_preceding_loads(table.preceding_loads, {"order_id": ["order_id_urn"]})
        == expected
    )


@pytest.mark.parametrize(
    "between",
    [
        pytest.param("Xml: LOAD Value FROM_FIELD (Raw, Payload);", id="from_field"),
        pytest.param(
            "Scores: LOAD Id, Score EXTENSION Model.Predict(Orders{Id});",
            id="extension",
        ),
        pytest.param("LOAD Value;\nLIB CONNECT TO 'Sales SQL';", id="connect"),
        pytest.param("LOAD Value;\nDISCONNECT;", id="disconnect"),
    ],
)
def test_load_that_does_not_precede_select_keeps_sql_columns(between: str) -> None:
    (table,) = parse_load_script(
        f"""
LIB CONNECT TO 'Sales SQL';
{between}
Orders:
SQL SELECT order_id FROM dbo.orders;
"""
    )

    assert table.preceding_loads == []
    assert apply_preceding_loads(
        table.preceding_loads, {"order_id": ["order_id_urn"]}
    ) == {"order_id": ["order_id_urn"]}


@pytest.mark.parametrize(
    "field_list",
    [
        pytest.param("order_id, from_field", id="field_named_from_field"),
        pytest.param("order_id, extension", id="field_named_extension"),
        pytest.param("order_id, '$(vReloadDate)' AS loaded", id="variable_in_string"),
    ],
)
def test_preceding_load_fields_that_look_like_clauses(field_list: str) -> None:
    (table,) = parse_load_script(
        f"""
LIB CONNECT TO 'Sales SQL';
Orders:
LOAD {field_list};
SQL SELECT order_id, from_field, extension, amount FROM dbo.orders;
"""
    )

    assert "amount" not in apply_preceding_loads(
        table.preceding_loads,
        {
            name: [f"{name}_urn"]
            for name in ("order_id", "from_field", "extension", "amount")
        },
    )


@pytest.mark.parametrize(
    "sql, expected",
    [
        ('SELECT "id" FROM JSON (wrap on) "root"', True),
        ('SELECT "id" FROM XML "root"', True),
        ('SELECT id FROM t WITH CONNECTION (URL "https://example.com")', True),
        ("SELECT id FROM json", False),
        ("SELECT id FROM t WHERE note = 'FROM JSON (x)'", False),
    ],
)
def test_is_connector_sql(sql: str, expected: bool) -> None:
    assert is_connector_sql(sql) == expected


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("ON a = b $(v) AND c = 1", "ON a = b  AND c = 1"),
        ("ON $(v) AND c = 1", "ON 1 = 1 AND c = 1"),
        ("WHERE ($(v) OR c = 1)", "WHERE (1 = 1 OR c = 1)"),
        ("WHERE c = 1 AND $(v)", "WHERE c = 1 AND 1 = 1"),
        ("WHERE $(v)", "WHERE 1 = 1"),
        ("WHERE c = $(v) AND d = 1", "WHERE c = $(v) AND d = 1"),
        ("FROM $(vTable) t WHERE c = 1", "FROM $(vTable) t WHERE c = 1"),
        ("FROM dbo.orders_$(vYear) AND", "FROM dbo.orders_$(vYear) AND"),
    ],
    ids=[
        "between_conditions",
        "join_start",
        "in_parentheses",
        "clause_end",
        "whole_where",
        "value",
        "table",
        "table_suffix",
    ],
)
def test_drop_condition_variables(sql: str, expected: str) -> None:
    assert drop_condition_variables(sql) == expected


@pytest.mark.parametrize(
    "expression, names",
    [
        pytest.param("=Sum(Year(OrderDate))", ["OrderDate"], id="nested_functions"),
        pytest.param("=Year (OrderDate)", ["OrderDate"], id="space_before_call"),
        pytest.param(
            "=[Year] + Year", ["Year", "Year"], id="field_named_like_function"
        ),
        pytest.param("Count({<Year={2024}>} Region)", ["Year", "Region"], id="set"),
    ],
)
def test_expression_names_skip_function_calls(
    expression: str, names: List[str]
) -> None:
    assert expression_names(expression) == names
