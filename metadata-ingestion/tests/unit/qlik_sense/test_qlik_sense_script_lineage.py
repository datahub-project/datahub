from typing import Dict, List, Optional

import pytest

from datahub.ingestion.source.qlik_sense.script_lineage import (
    apply_crosstable,
    apply_preceding_loads,
    connection_lookup_names,
    drop_condition_variables,
    expand_variables,
    expression_names,
    has_qualified_name,
    infer_data_connection,
    is_connector_sql,
    parse_load_script,
    redact_connection,
    select_list_size,
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
        ("main.sales.events", "Lakehouse"),
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
        ("dbo.events", "$(vComputed)", "SELECT * FROM dbo.events"),
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
    assert infer_data_connection("Warehouse", name).platform == platform


def test_infer_data_connection_ignores_the_connection_name() -> None:
    assert infer_data_connection("Snowflake Finance", "folder").platform is None


def test_infer_data_connection_reads_mariadb_driver() -> None:
    connection = "Driver={MariaDB ODBC 3.1 Driver};Server=db.example.com;Database=sales"
    assert infer_data_connection(connection, connection).platform == "mariadb"


def test_infer_data_connection_prefers_the_driver() -> None:
    connection = "Driver={Snowflake};Server=sqlserver_sync.example.com;Database=sales"
    assert infer_data_connection(connection, connection).platform == "snowflake"


def test_parse_load_script_skips_uncalled_sub() -> None:
    script = """
LIB CONNECT TO 'A';
Orders: SQL SELECT * FROM db.s.orders;
SUB LoadOther
  LIB CONNECT TO 'B';
  Inside: SQL SELECT * FROM db.s.inside;
END SUB
Concatenate SQL SELECT * FROM db.s.more_orders;
T: SQL SELECT * FROM db.s.t;
"""
    assert [(t.label, t.connection) for t in parse_load_script(script)] == [
        ("Orders", "A"),
        ("Orders", "A"),
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


def test_parse_load_script_skips_rem_comments_in_control_blocks() -> None:
    script = """
LIB CONNECT TO 'A';
IF vReload = 1 THEN
  REM Don't load the archive here;
  T: SQL SELECT * FROM db.s.t;
ELSE
  rem it's off;
END IF
U: SQL SELECT * FROM db.s.u;
"""
    assert [t.label for t in parse_load_script(script)] == ["T", "U"]


def test_redact_connection_keeps_only_location_properties() -> None:
    connection = (
        "Provider=MSOLEDBSQL;"
        "Data Source=https://svc_user:hunter2@db.example.com:443/x?sig=s3cr3t&sv=1;"
        'Extended Properties="UID=svc_user;PWD=hunter2;Encrypt=yes";'
        "Auth=Bearer t0k3n;Passphrase=p4ss;Initial Catalog=sales"
    )

    assert redact_connection(connection) == (
        "Provider=MSOLEDBSQL;Data Source=https://db.example.com:443/x;"
        "Initial Catalog=sales"
    )


def test_redact_connection_keeps_platform_inference() -> None:
    connection = "Driver={Snowflake};Server=acct.example.com;Token=t0k3n;Database=sales"

    redacted = redact_connection(connection)

    assert "t0k3n" not in redacted
    assert infer_data_connection(redacted, redacted).platform == "snowflake"


def test_parse_load_script_forgets_connection_after_include() -> None:
    script = """
LIB CONNECT TO 'A';
SET vSchema = 'dbo';
$(Must_Include=lib://Scripts/connect.qvs);
T: SQL SELECT * FROM $(vSchema).t;
"""
    (table,) = parse_load_script(script)

    assert table.connection is None
    assert table.sql == "SELECT * FROM $(vSchema).t"


def test_parse_load_script_call_replays_sub_with_call_site_state() -> None:
    script = """
SUB LoadOrders
  Orders: SQL SELECT * FROM $(vSchema).orders;
END SUB
LIB CONNECT TO 'A';
SET vSchema = 'dbo';
CALL LoadOrders;
LIB CONNECT TO 'B';
SET vSchema = 'archive';
Call LoadOrders;
"""
    assert [(t.connection, t.sql) for t in parse_load_script(script)] == [
        ("A", "SELECT * FROM dbo.orders"),
        ("B", "SELECT * FROM archive.orders"),
    ]


def test_parse_load_script_keeps_what_a_called_sub_sets() -> None:
    script = """
LIB CONNECT TO 'A';
SET vSchema = 'dbo';
SUB Setup
  LIB CONNECT TO 'B';
  SET vSchema = 'archive';
END SUB
CALL Setup;
T: SQL SELECT * FROM $(vSchema).t;
"""
    (table,) = parse_load_script(script)

    assert table.connection == "B"
    assert table.sql == "SELECT * FROM archive.t"


def test_parse_load_script_sub_calling_itself_is_not_replayed_forever() -> None:
    script = """
SUB Loop
  T: SQL SELECT * FROM db.s.t;
  CALL Loop;
END SUB
LIB CONNECT TO 'A';
CALL Loop;
"""
    assert [t.label for t in parse_load_script(script)] == ["T"]


def test_parse_load_script_call_of_unknown_sub_forgets_state() -> None:
    script = """
LIB CONNECT TO 'A';
SET vSchema = 'dbo';
Orders: SQL SELECT * FROM db.s.orders;
CALL FromInclude;
Concatenate SQL SELECT * FROM db.s.more_orders;
Left Join SQL SELECT * FROM db.s.regions;
T: SQL SELECT * FROM $(vSchema).t;
"""
    orders, table = parse_load_script(script)

    assert (orders.label, table.label) == ("Orders", "T")

    assert table.connection is None
    assert table.sql == "SELECT * FROM $(vSchema).t"


def test_parse_load_script_concatenate_after_include_is_skipped() -> None:
    script = """
LIB CONNECT TO 'A';
Orders: SQL SELECT * FROM db.s.orders;
$(Include=lib://Scripts/more.qvs);
LIB CONNECT TO 'A';
Concatenate SQL SELECT * FROM db.s.more;
"""
    assert [t.label for t in parse_load_script(script)] == ["Orders"]


def test_parse_load_script_concatenate_and_join_add_to_their_target() -> None:
    script = """
LIB CONNECT TO 'A';
Orders: SQL SELECT * FROM db.s.orders;
Concatenate (Orders) SQL SELECT * FROM db.s.old_orders;
Left Join ([Orders]) LOAD id, region;
SQL SELECT id, region FROM db.s.regions;
Concatenate SQL SELECT * FROM db.s.more_orders;
Customers: Left Keep (Orders) SQL SELECT * FROM db.s.customers;
Concatenate ([Orders (old)]) SQL SELECT * FROM db.s.archive;
"""
    assert [t.label for t in parse_load_script(script)] == [
        "Orders",
        "Orders",
        "Orders",
        "Orders",
        "Customers",
        "Orders (old)",
    ]


def test_parse_load_script_skips_concatenate_without_known_table() -> None:
    script = """
LIB CONNECT TO 'A';
LOAD * FROM [lib://Files/sales.qvd] (qvd);
Concatenate SQL SELECT * FROM db.s.orders;
"""
    assert parse_load_script(script) == []


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
        ("prod.sales.orders", "mssql", "Prod", False),
        ("sales.orders", "mysql", None, True),
        ("orders", "oracle", None, False),
        ("sales.orders", "oracle", None, True),
        ("orders", "mariadb", None, False),
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


def test_upper_load_reads_unaliased_expression_of_load_below() -> None:
    (table,) = parse_load_script(
        "Orders:\nLOAD [Upper(region)] AS Region;\nLOAD Upper(region);\n"
        "SQL SELECT region FROM dbo.orders;"
    )

    assert apply_preceding_loads(table.preceding_loads, {"region": ["region_urn"]}) == {
        "Region": ["region_urn"]
    }


def test_preceding_load_distinct_qualifier_is_not_a_column() -> None:
    (table,) = parse_load_script(
        "Orders:\nLOAD Count(DISTINCT customer_id) AS Customers, distinct AS Flag;\n"
        "SQL SELECT customer_id, distinct FROM dbo.orders;"
    )

    assert apply_preceding_loads(
        table.preceding_loads, {"customer_id": ["c"], "DISTINCT": ["d"]}
    ) == {"Customers": ["c"], "Flag": ["d"]}


def test_preceding_load_function_is_not_a_column() -> None:
    (table,) = parse_load_script(
        "Orders:\nLOAD Year(order_date) AS OrderYear;\n"
        "SQL SELECT Year, order_date FROM dbo.orders;"
    )

    assert apply_preceding_loads(
        table.preceding_loads,
        {"Year": ["year_urn"], "order_date": ["order_date_urn"]},
    ) == {"OrderYear": ["order_date_urn"]}


def test_preceding_load_interpretation_function_is_not_a_column() -> None:
    (table,) = parse_load_script(
        "Orders:\nLOAD Date#(order_date, 'YYYYMMDD') AS OrderDay;\n"
        "SQL SELECT Date, order_date FROM dbo.orders;"
    )

    assert apply_preceding_loads(
        table.preceding_loads,
        {"Date": ["date_urn"], "order_date": ["order_date_urn"]},
    ) == {"OrderDay": ["order_date_urn"]}


@pytest.mark.parametrize(
    "field_list, expected",
    [
        pytest.param("$(vOrderFields)", {"order_id": ["order_id_urn"]}, id="variable"),
        pytest.param(
            "$(vList(Orders))",
            {"order_id": ["order_id_urn"]},
            id="parameterized_variable",
        ),
        pytest.param(
            "$(=Concat(Field, ',')) AS Fields",
            {"order_id": ["order_id_urn"], "Fields": []},
            id="expression_expansion_in_alias",
        ),
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
        pytest.param(
            "$(vKeys)$(vMeasures)",
            {"order_id": ["order_id_urn"]},
            id="joined_variables",
        ),
        pytest.param("Upper(order_id)", {}, id="unaliased_expression"),
        pytest.param(
            "order_id AS [$(vName)]",
            {"order_id": ["order_id_urn"]},
            id="variable_in_bracketed_alias",
        ),
        pytest.param(
            "order_id AS $(vName)",
            {"order_id": ["order_id_urn"]},
            id="variable_as_alias",
        ),
        pytest.param(
            "[$(vFields)], order_id AS OrderKey",
            {"order_id": ["order_id_urn"], "OrderKey": ["order_id_urn"]},
            id="bracketed_variable_field",
        ),
        pytest.param(
            "order_id * $(vRate) AS Net",
            {"Net": ["order_id_urn"]},
            id="variable_inside_expression",
        ),
        pytest.param(
            "order_id * $(order_id) AS Net, '$(vSource)' AS Source",
            {"Net": ["order_id_urn"], "Source": []},
            id="variable_name_is_not_a_source",
        ),
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
            "Sum(Amount) // excludes Returns\n+ Sum([Tax]) /* Returns */",
            ["Amount", "Tax"],
            id="comments",
        ),
        pytest.param(
            "Count({<Url={'http://example.com'}>} [a/*b])",
            ["Url", "a/*b"],
            id="comment_markers_in_quotes",
        ),
        pytest.param(
            "=Date#(OrderDate, 'YYYYMMDD') + Num# (Amount)",
            ["OrderDate", "Amount"],
            id="interpretation_functions",
        ),
        pytest.param(
            "=[Year] + Year", ["Year", "Year"], id="field_named_like_function"
        ),
        pytest.param("Count({<Year={2024}>} Region)", ["Year", "Region"], id="set"),
        pytest.param(
            "Sum($(vSales)) + Sum($(=Max(Year))) + Sum(Amount)",
            ["Amount"],
            id="expansions",
        ),
        pytest.param(
            "={$<Year={2024}, [Sales Region]-={'East'}>} Sum(Sales)",
            ["Year", "Sales Region", "Sales"],
            id="set_after_leading_equals",
        ),
        pytest.param(
            "Count({<Region={'A'}+{\"B*\"}-{$(vSkip)}>} Amount)",
            ["Region", "Amount"],
            id="combined_set_values",
        ),
        pytest.param(
            "Sum(Total) + Count(DISTINCT Total)", ["Total", "Total"], id="total_field"
        ),
        pytest.param(
            "Sum(DISTINCT TOTAL <Region> Amount) and not Flag",
            ["Region", "Amount", "Flag"],
            id="keywords",
        ),
        pytest.param(
            'Count({<Region={"A)"}, Year={"}"}>} Store) + Sum($(=Concat(")", ","))) + Amount',
            ["Region", "Year", "Store", "Amount"],
            id="delimiters_inside_quotes",
        ),
        pytest.param(
            "Count([Date].autoCalendar.Year)",
            ["Date.autoCalendar.Year"],
            id="bracketed_derived_field",
        ),
    ],
)
def test_expression_names_skip_function_calls(
    expression: str, names: List[str]
) -> None:
    assert expression_names(expression) == names


def test_preceding_load_keeps_columns_named_like_chart_qualifiers() -> None:
    (table,) = parse_load_script(
        "Orders:\nLOAD total AS Total, distinct AS Kind;\n"
        "SQL SELECT total, [distinct] FROM dbo.orders;"
    )

    assert apply_preceding_loads(
        table.preceding_loads,
        {"total": ["total_urn"], "distinct": ["distinct_urn"]},
    ) == {"Total": ["total_urn"], "Kind": ["distinct_urn"]}


def test_parse_load_script_ignores_sql_line_comments() -> None:
    script = """
LIB CONNECT TO 'A';
T: SQL SELECT a -- don't
FROM db.dbo.t1;
ODBC CONNECT TO [Driver={ODBC Driver 17 for SQL Server};Server=h;Database=d];
U: SQL SELECT * FROM d.dbo.u1;
"""
    assert [(t.label, t.sql) for t in parse_load_script(script)] == [
        ("T", "SELECT a \nFROM db.dbo.t1"),
        ("U", "SELECT * FROM d.dbo.u1"),
    ]


def test_parse_load_script_ignores_sql_comments_in_bare_with_queries() -> None:
    script = """
LIB CONNECT TO 'A';
T: WITH recent AS ( -- don't count returns
  SELECT a FROM db.s.t1
)
SELECT a FROM recent;
U: LOAD 'SELECT' AS kind, a--b AS diff RESIDENT T;
V: SQL SELECT * FROM db.s.v1;
"""
    assert [t.label for t in parse_load_script(script)] == ["T", "V"]


def test_parse_load_script_binds_sub_parameters() -> None:
    script = """
SET vTable = 'db.s.global';
SUB LoadT(vTable)
  T: SQL SELECT * FROM $(vTable);
END SUB
LIB CONNECT TO 'A';
CALL LoadT('db.s.param_b');
CALL LoadT(42);
CALL LoadT(vOther);
U: SQL SELECT * FROM $(vTable);
"""
    assert [t.sql for t in parse_load_script(script)] == [
        "SELECT * FROM db.s.param_b",
        "SELECT * FROM 42",
        "SELECT * FROM $(vTable)",
        "SELECT * FROM db.s.global",
    ]


def test_parse_load_script_for_loop_variable_is_unknown() -> None:
    script = """
SET vTable = 'db.s.before';
LIB CONNECT TO 'A';
FOR EACH vTable IN 'db.s.a', 'db.s.b'
  T: SQL SELECT * FROM $(vTable);
NEXT vTable
"""
    assert [t.sql for t in parse_load_script(script)] == ["SELECT * FROM $(vTable)"]


def test_parse_load_script_stops_at_exit_script() -> None:
    script = """
LIB CONNECT TO 'A';
T: SQL SELECT * FROM db.s.t;
IF vDebug = 1 THEN
  EXIT SCRIPT;
END IF
U: SQL SELECT * FROM db.s.u;
EXIT SCRIPT;
Legacy: SQL SELECT * FROM db.s.legacy;
"""
    assert [t.label for t in parse_load_script(script)] == ["T", "U"]


def test_parse_load_script_label_of_preceding_load_stack() -> None:
    script = "LIB CONNECT TO 'A';\nT: LOAD a;\nLOAD a, b;\nSQL SELECT a, b FROM db.s.t;"
    assert [t.label for t in parse_load_script(script)] == ["T"]


@pytest.mark.parametrize(
    "sql, label",
    [
        pytest.param('SELECT * FROM "dbo"."orders"', "dbo.orders", id="quoted"),
        pytest.param(
            "SELECT * FROM [sales db].dbo.orders o",
            "sales db.dbo.orders",
            id="bracketed",
        ),
        pytest.param(
            "SELECT * FROM a_database_with_a_long_name.dbo.orders",
            "a_database_with_a_long_name.dbo.",
            id="cut_to_32_characters",
        ),
        pytest.param("SELECT 1 AS one", "table_1", id="no_from"),
        pytest.param(
            "SELECT 'x FROM y' AS note, a FROM db.s.t", "db.s.t", id="string_literal"
        ),
        pytest.param(
            "SELECT * FROM (SELECT a FROM db.s.inner_t) AS sub",
            "table_1",
            id="subquery",
        ),
        pytest.param(
            "SELECT EXTRACT(YEAR FROM d) AS y FROM db.s.dates",
            "db.s.dates",
            id="from_inside_function",
        ),
    ],
)
def test_parse_load_script_names_unlabelled_tables_after_from(
    sql: str, label: str
) -> None:
    (table,) = parse_load_script(f"LIB CONNECT TO 'A';\nSQL {sql};")
    assert table.label == label


@pytest.mark.parametrize(
    "connection",
    [
        pytest.param(
            "Server=svc:Hunter2Secret@db.example.com;Database=d", id="userinfo"
        ),
        pytest.param(
            "Server=db.example.com?token=Hunter2Secret;Database=d", id="query"
        ),
        pytest.param(
            "Server=db.example.com\nPWD=Hunter2Secret;Database=d", id="newline"
        ),
    ],
)
def test_redact_connection_scrubs_allow_listed_values(connection: str) -> None:
    redacted = redact_connection(connection)

    assert "Hunter2Secret" not in redacted
    assert "db.example.com" in redacted
    assert "Database=d" in redacted


def test_parse_load_script_marks_mapping_tables() -> None:
    script = """
LIB CONNECT TO 'A';
Map: MAPPING LOAD k, v;
SQL SELECT k, v FROM db.s.codes;
Direct: Mapping SQL SELECT k, v FROM db.s.other_codes;
T: SQL SELECT * FROM db.s.t;
[Mapping]: SQL SELECT * FROM db.s.named_mapping;
Concatenate ([Mapping]) SQL SELECT * FROM db.s.more;
[Mapping (old)]: SQL SELECT * FROM db.s.old;
Concatenate ([Mapping (old)]) SQL SELECT * FROM db.s.older;
"""
    assert [(t.label, t.mapping) for t in parse_load_script(script)] == [
        ("Map", True),
        ("Direct", True),
        ("T", False),
        ("Mapping", False),
        ("Mapping", False),
        ("Mapping (old)", False),
        ("Mapping (old)", False),
    ]


@pytest.mark.parametrize(
    "columns, expected",
    [
        pytest.param(
            {"Amount": ["exact_urn"], "amount": ["lower_urn"]},
            ["exact_urn"],
            id="exact",
        ),
        pytest.param({"AMOUNT": ["upper_urn"]}, ["upper_urn"], id="other_case"),
        pytest.param(
            {"AMOUNT": ["upper_urn"], "amount": ["lower_urn"]}, [], id="ambiguous"
        ),
    ],
)
def test_preceding_load_source_column_case(
    columns: Dict[str, List[str]], expected: List[str]
) -> None:
    (table,) = parse_load_script(
        "Orders:\nLOAD Amount AS Net;\nSQL SELECT * FROM dbo.orders;"
    )

    assert apply_preceding_loads(table.preceding_loads, columns)["Net"] == expected


@pytest.mark.parametrize(
    "script",
    [
        pytest.param(
            "T: CrossTable(Month, Sales) LOAD Region, Jan, Feb;\n"
            "SQL SELECT Region, Jan, Feb FROM dbo.sales;",
            id="crosstable_load",
        ),
        pytest.param(
            "T: Generic SQL SELECT id, attr, val FROM dbo.facts;", id="generic"
        ),
    ],
)
def test_parse_load_script_marks_reshaped_tables(script: str) -> None:
    (table,) = parse_load_script(script)
    assert table.reshaped


@pytest.mark.parametrize(
    "script, stacked",
    [
        pytest.param(
            "T:\nLOAD order_id AS OrderID;\nIF 1 = 1 THEN\n"
            "SQL SELECT order_id FROM dbo.orders;\nEND IF",
            False,
            id="load_above_control_statement",
        ),
        pytest.param(
            "IF 1 = 1 THEN\nT:\nLOAD order_id AS OrderID;\n"
            "SQL SELECT order_id FROM dbo.orders;\nEND IF",
            True,
            id="load_and_select_in_one_block",
        ),
    ],
)
def test_preceding_load_stacks_only_on_the_statement_below(
    script: str, stacked: bool
) -> None:
    (table,) = parse_load_script(script)

    assert bool(table.preceding_loads) is stacked


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT region, Sum(jan, feb), 'a, b' FROM db.s.t", 3),
        ("SELECT DISTINCT TOP 10 region, jan FROM (SELECT * FROM db.s.t) x", 2),
        ("SELECT t.*, jan FROM db.s.t t", None),
        ("SELECT `a,b`, jan FROM db.s.t", 2),
        ("SELECT `from`, jan FROM db.s.t", 2),
        ("WITH x AS (SELECT 1 AS a) SELECT a FROM x", None),
    ],
)
def test_select_list_size(sql: str, expected: Optional[int]) -> None:
    assert select_list_size(sql) == expected


@pytest.mark.parametrize(
    "script, expected",
    [
        pytest.param(
            "T: CrossTable(Month, Sales, 2) SQL SELECT region, store, jan FROM db.s.t;",
            {"region": ["r"], "store": ["s"], "Month": [], "Sales": ["j"]},
            id="two_qualifiers",
        ),
        pytest.param(
            "T: CrossTable([Month)], 'Sales, net', 1) "
            "SQL SELECT region, store, jan FROM db.s.t;",
            {"region": ["r"], "Month)": [], "Sales, net": ["s", "j"]},
            id="names_with_delimiters",
        ),
        pytest.param(
            "T: CrossTable(Month, Sales) LOAD Upper(region), store, jan;\n"
            "SQL SELECT region, store, jan FROM db.s.t;",
            {"Month": [], "Sales": ["s", "j"]},
            id="unaliased_qualifier_keeps_its_slot",
        ),
        pytest.param(
            "T: CrossTable(Month, Sales) LOAD region, Upper(store), jan;\n"
            "SQL SELECT region, store, jan FROM db.s.t;",
            {"region": ["r"], "Month": [], "Sales": ["s", "j"]},
            id="unaliased_data_field",
        ),
        pytest.param(
            "T: CrossTable(Month, Sales) LOAD *;\nSQL SELECT region, jan FROM db.s.t;",
            None,
            id="field_order_unknown",
        ),
        pytest.param(
            "T: CrossTable(Month, Sales) LOAD region, jan;\n"
            "Generic LOAD region, jan;\nSQL SELECT region, jan FROM db.s.t;",
            None,
            id="reshaped_below_the_top",
        ),
    ],
)
def test_crosstable_fields(
    script: str, expected: Optional[Dict[str, List[str]]]
) -> None:
    (table,) = parse_load_script(script)
    columns = {"region": ["r"], "store": ["s"], "jan": ["j"]}

    assert table.reshaped
    assert (
        apply_crosstable(table.crosstable, table.preceding_loads, columns) == expected
    )


@pytest.mark.parametrize(
    "expression, expanded",
    [
        ("Aggr(Sum(Sales), Day, $(vGrain))", "Aggr(Sum(Sales), Day, HOUR)"),
        # A variable can name another variable.
        ("Sum($(vMeasure))", "Sum(Sales)"),
        # An = definition expands to its calculated value, which is unknown.
        ("Max($(vMax))", "Max($(vMax))"),
        ("$(vUnknown)", "$(vUnknown)"),
        ("$(=Max(Year))", "$(=Max(Year))"),
        ("$(vSelf)", "$(vSelf)"),
    ],
)
def test_variables_expand_to_their_definition(expression: str, expanded: str) -> None:
    variables = {
        "vGrain": "HOUR",
        "vMeasure": "$(vField)",
        "vField": "Sales",
        "vMax": "=Max(Year)",
        "vSelf": "$(vSelf)",
    }
    assert expand_variables(expression, variables) == expanded
