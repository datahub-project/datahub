"""End-to-end M-Query lineage tests for the pattern handlers.

Each test feeds a realistic M expression through the real parser bridge
(``parser.get_upstream_tables``) and asserts on the resulting upstream URNs
and reporter counters. Only the HTTP-free parts of the pipeline are involved,
so nothing is mocked: the behaviours covered here are the ones a Power BI
tenant would exercise with its generated M-Query.
"""

from typing import Any, Dict

import pytest

from datahub.ingestion.source.powerbi.config import Constant
from tests.unit.powerbi.conftest import (
    m_query_lineages,
    make_column,
    upstream_urns,
)

# --- Redshift ---------------------------------------------------------------


def test_redshift_navigation_resolves_three_part_urn() -> None:
    lineages, _ = m_query_lineages(
        'let\n Source = AmazonRedshift.Database("redshift-url","dev"),\n'
        ' public = Source{[Name="public"]}[Data],\n'
        ' category1 = public{[Name="category"]}[Data]\nin\n category1'
    )
    assert upstream_urns(lineages) == [
        "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.public.category,PROD)"
    ]


def test_redshift_without_table_step_yields_no_lineage() -> None:
    lineages, reporter = m_query_lineages(
        'let\n Source = AmazonRedshift.Database("redshift-url","dev"),\n'
        ' public = Source{[Name="public"]}[Data]\nin\n public'
    )
    assert lineages == []
    assert reporter.m_query_resolver_no_lineage == 1


# --- MySQL ------------------------------------------------------------------


def test_mysql_navigation_resolves_schema_qualified_urn() -> None:
    """Navigation lineage copies every Power BI field onto the upstream side."""
    lineages, _ = m_query_lineages(
        'let\n Source = MySQL.Database("localhost:3306", "shop"),\n'
        ' orders = Source{[Schema="shop",Item="orders"]}[Data]\nin\n orders',
        columns=[make_column("id")],
    )
    assert upstream_urns(lineages) == [
        "urn:li:dataset:(urn:li:dataPlatform:mysql,shop.orders,PROD)"
    ]
    assert [c.downstream.column for c in lineages[0].column_lineage] == ["id"]


def test_mysql_with_unresolved_server_parameter_yields_no_lineage() -> None:
    lineages, _ = m_query_lineages(
        'let\n Source = MySQL.Database(ServerParam, "shop"),\n'
        ' orders = Source{[Schema="shop",Item="orders"]}[Data]\nin\n orders'
    )
    assert lineages == []


# --- PostgreSQL / Databricks ----------------------------------------------------


def test_postgres_navigation_resolves_database_qualified_urn() -> None:
    """Whitespace around the connector arguments is what Power BI Desktop
    emits for hand-edited queries and must not change the resolved table."""
    lineages, _ = m_query_lineages(
        'let\n    Source = PostgreSQL.Database("localhost"  ,   "mics"      ),\n'
        '  public_order_date =    Source{[Schema="public",Item="order_date"]}[Data] \n'
        " in \n public_order_date"
    )
    assert upstream_urns(lineages) == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mics.public.order_date,PROD)"
    ]


def test_databricks_single_step_navigation_with_metastore_prefix() -> None:
    """Power BI Desktop can emit catalog, schema and table in one navigation
    record; a configured Unity Catalog metastore is prepended to the URN."""
    lineages, _ = m_query_lineages(
        'let\n Source = DatabricksMultiCloud.Catalogs("abc.cloud.databricks.com", '
        '"/sql/abc", [Catalog="data_analysis", Database="summary"]),\n'
        ' committee = Source{[Item="committee_data",Schema="summary",Catalog="data_analysis"]}[Data]\n'
        "in\n committee",
        config_overrides={
            "server_to_platform_instance": {
                "abc.cloud.databricks.com": {"metastore": "primary_metastore"}
            }
        },
    )
    assert upstream_urns(lineages) == [
        "urn:li:dataset:(urn:li:dataPlatform:databricks,"
        "primary_metastore.data_analysis.summary.committee_data,PROD)"
    ]


# --- MS SQL -----------------------------------------------------------------


def test_mssql_navigation_resolves_database_qualified_urn() -> None:
    lineages, _ = m_query_lineages(
        'let\n Source = Sql.Database("localhost", "library"),\n'
        ' dbo_book_issue = Source{[Schema="dbo",Item="book_issue"]}[Data]\n'
        "in\n dbo_book_issue"
    )
    assert upstream_urns(lineages) == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,library.dbo.book_issue,PROD)"
    ]


def test_mssql_multi_database_navigation_resolves_urn() -> None:
    lineages, _ = m_query_lineages(
        'let\n Source = Sql.Databases("sql-host"),\n'
        ' db = Source{[Name="library"]}[Data],\n'
        ' tbl = db{[Schema="dbo",Item="book_issue"]}[Data]\nin\n tbl'
    )
    assert upstream_urns(lineages) == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,library.dbo.book_issue,PROD)"
    ]


def test_mssql_multi_database_requires_two_navigation_steps() -> None:
    lineages, _ = m_query_lineages(
        'let\n Source = Sql.Databases("sql-host"),\n'
        ' db = Source{[Name="library"]}[Data]\nin\n db'
    )
    assert lineages == []


def test_mssql_multi_database_with_unresolved_server_yields_no_lineage() -> None:
    lineages, _ = m_query_lineages(
        "let\n Source = Sql.Databases(HostParam),\n"
        ' db = Source{[Name="library"]}[Data],\n'
        ' tbl = db{[Schema="dbo",Item="book_issue"]}[Data]\nin\n tbl'
    )
    assert lineages == []


@pytest.mark.parametrize(
    "platform, navigation",
    [
        ("mssql", 'db{[Name="book_issue"]}[Data]'),
        ("redshift", 'db{[Kind="Table"]}[Data]'),
    ],
    ids=["mssql-multi-database", "redshift"],
)
def test_navigation_record_without_expected_keys_is_no_lineage_not_an_error(
    platform: str, navigation: str
) -> None:
    """A navigation step whose record lacks the keys the handler reads
    (``Schema``/``Item`` for Sql.Databases, ``Name`` for Redshift) used to
    raise ``KeyError`` out of the handler, which the parser reported as a
    misleading "Unknown M-Query Pattern". It is simply a shape the handler
    cannot resolve, so it must count as no lineage."""
    source = {
        "mssql": 'Sql.Databases("h"),\n db = Source{[Name="library"]}[Data]',
        "redshift": 'AmazonRedshift.Database("h","dev"),\n db = Source{[Name="public"]}[Data]',
    }[platform]
    lineages, reporter = m_query_lineages(
        f"let\n Source = {source},\n tbl = {navigation}\nin\n tbl"
    )
    assert lineages == []
    assert reporter.m_query_resolver_errors == 0
    assert reporter.m_query_resolver_no_lineage == 1
    assert not reporter.warnings


def test_mssql_unresolved_server_parameter_emits_partial_lineage() -> None:
    """The host is a dataset parameter that was not supplied; the table is
    still identifiable, so lineage is emitted without a server host."""
    lineages, _ = m_query_lineages(
        'let\n Source = Sql.Database(ServerName, "library"),\n'
        ' tbl = Source{[Schema="dbo",Item="book_issue"]}[Data]\nin\n tbl'
    )
    assert upstream_urns(lineages) == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,library.dbo.book_issue,PROD)"
    ]


def test_mssql_inline_query_uses_legacy_parser_when_advanced_lineage_is_off() -> None:
    lineages, _ = m_query_lineages(
        'let\n Source = Sql.Database("sql-host", "library", '
        '[Query="SELECT * FROM dbo.book_issue JOIN authors ON authors.id = dbo.book_issue.author_id"])\n'
        "in\n Source"
    )
    assert sorted(upstream_urns(lineages)) == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,library.dbo.authors,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:mssql,library.dbo.book_issue,PROD)",
    ]


@pytest.mark.xfail(
    strict=True,
    reason=(
        "native_sql_parser.get_tables takes the FROM-clause identifier's raw text "
        "and only strips a lowercase ' as ' suffix (`value.split('as')[0]`), so a "
        "bare alias (`book_issue b`), an upper-case `AS b` and any table whose "
        "name contains 'as' (`assets` -> ``) all leak into the URN."
    ),
)
@pytest.mark.parametrize(
    "from_clause, expected_table",
    [
        ("dbo.book_issue b", "library.dbo.book_issue"),
        ("dbo.book_issue AS b", "library.dbo.book_issue"),
        ("dbo.assets", "library.dbo.assets"),
    ],
    ids=["bare-alias", "upper-case-as", "name-containing-as"],
)
def test_legacy_parser_strips_table_aliases(
    from_clause: str, expected_table: str
) -> None:
    lineages, _ = m_query_lineages(
        'let\n Source = Sql.Database("sql-host", "library", '
        f'[Query="SELECT * FROM {from_clause}"])\nin\n Source'
    )
    assert upstream_urns(lineages) == [
        f"urn:li:dataset:(urn:li:dataPlatform:mssql,{expected_table},PROD)"
    ]


# --- Three-step navigation (Snowflake / Trino / BigQuery) -------------------


def test_snowflake_navigation_resolves_three_part_urn() -> None:
    lineages, _ = m_query_lineages(
        'let\n Source = Snowflake.Databases("acct.snowflakecomputing.com","WH",[Role="PBI_MEMBER"]),\n'
        ' db = Source{[Name="SALES_DB",Kind="Database"]}[Data],\n'
        ' sch = db{[Name="PUBLIC",Kind="Schema"]}[Data],\n'
        ' tbl = sch{[Name="ORDERS",Kind="Table"]}[Data]\nin\n tbl'
    )
    assert upstream_urns(lineages) == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.public.orders,PROD)"
    ]


def test_snowflake_navigation_short_of_table_step_yields_no_lineage() -> None:
    lineages, _ = m_query_lineages(
        'let\n Source = Snowflake.Databases("acct.snowflakecomputing.com","WH"),\n'
        ' db = Source{[Name="SALES_DB",Kind="Database"]}[Data],\n'
        ' sch = db{[Name="PUBLIC",Kind="Schema"]}[Data]\nin\n sch'
    )
    assert lineages == []


@pytest.mark.parametrize(
    "source_call, expected_urn",
    [
        (
            'StarburstAad.Contents("starburst.example.com", 443, '
            "[Role=null, Catalog=null, UseSystemProxy=null, ExtraConnectionString=null])",
            "urn:li:dataset:(urn:li:dataPlatform:trino,hive.analytics.events,PROD)",
        ),
        (
            'StarburstPresto.Contents("trino.example.com", 8443, [Catalog=null])',
            "urn:li:dataset:(urn:li:dataPlatform:trino,hive.analytics.events,PROD)",
        ),
        # Catalog selected in the connector's advanced options. Unlike
        # Databricks' record-based [Catalog=...] (which collapses navigation to
        # schema -> table), Starburst's .Contents is a navigation-table
        # connector: the option only scopes the navigator, so Power BI still
        # emits the full catalog -> schema -> table navigation and lineage
        # must not be dropped.
        (
            'StarburstAad.Contents("starburst.example.com", 443, [Role=null, Catalog="hive"])',
            "urn:li:dataset:(urn:li:dataPlatform:trino,hive.analytics.events,PROD)",
        ),
    ],
    ids=["starburst-aad", "starburst-presto", "starburst-aad-with-catalog-option"],
)
def test_starburst_connectors_resolve_to_trino(
    source_call: str, expected_urn: str
) -> None:
    lineages, _ = m_query_lineages(
        f"let\n Source = {source_call},\n"
        ' cat = Source{[Name="hive",Kind="Database"]}[Data],\n'
        ' sch = cat{[Name="analytics",Kind="Schema"]}[Data],\n'
        ' tbl = sch{[Name="events",Kind="Table"]}[Data]\nin\n tbl'
    )
    assert upstream_urns(lineages) == [expected_urn]


def test_bigquery_project_and_schema_come_from_navigation_and_parameters() -> None:
    """The billing project, the project navigation step and the schema step
    are all dataset parameters; the server_to_platform_instance lookup must
    use the resolved project name."""
    lineages, _ = m_query_lineages(
        'let\n Source = GoogleBigQuery.Database([BillingProject = #"Parameter - Source"]),\n'
        ' proj = Source{[Name=#"Parameter - Source"]}[Data],\n'
        ' sch = proj{[Name=#"My bq project",Kind="Schema"]}[Data],\n'
        ' tbl = sch{[Name="COST",Kind="Table"]}[Data]\nin\n tbl',
        parameters={"Parameter - Source": "gcp-project", "My bq project": "billing"},
        config_overrides={
            "server_to_platform_instance": {
                "gcp-project": {"platform_instance": "prod-bq", "env": "DEV"}
            }
        },
    )
    assert upstream_urns(lineages) == [
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,prod-bq.gcp-project.billing.cost,DEV)"
    ]


# --- Value.NativeQuery ------------------------------------------------------

_SNOWFLAKE_NATIVE_SOURCE = (
    'Snowflake.Databases("acct.snowflakecomputing.com","WH")'
    '{[Name="SALES_DB",Kind="Database"]}[Data]'
)


def test_native_query_legacy_parser_keeps_only_fully_qualified_tables() -> None:
    """Without the sqlglot path there is no database to default to, so a
    two-part table reference cannot be resolved and is dropped."""
    lineages, _ = m_query_lineages(
        f"let\n Source = Value.NativeQuery({_SNOWFLAKE_NATIVE_SOURCE}, "
        '"select * from SALES_DB.PUBLIC.ORDERS join PUBLIC.CUSTOMERS on ORDERS.cid = CUSTOMERS.id", '
        "null, [EnableFolding=true])\nin\n Source",
        config_overrides={"native_query_parsing": True},
        columns=[make_column("cid")],
    )
    assert upstream_urns(lineages) == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.public.orders,PROD)"
    ]
    assert [c.downstream.column for c in lineages[0].column_lineage] == ["cid"]


def test_native_query_with_fewer_than_two_arguments_yields_no_lineage() -> None:
    lineages, _ = m_query_lineages(
        f"let\n Source = Value.NativeQuery({_SNOWFLAKE_NATIVE_SOURCE})\nin\n Source",
        config_overrides={"native_query_parsing": True},
    )
    assert lineages == []


def test_native_query_with_non_literal_sql_yields_no_lineage() -> None:
    lineages, _ = m_query_lineages(
        f"let\n Source = Value.NativeQuery({_SNOWFLAKE_NATIVE_SOURCE}, SqlParam)\n"
        "in\n Source",
        config_overrides={"native_query_parsing": True},
    )
    assert lineages == []


def test_native_query_on_unsupported_connector_yields_no_lineage() -> None:
    lineages, _ = m_query_lineages(
        'let\n Source = Value.NativeQuery(Oracle.Database("orcl"), '
        '"select * from sales.orders")\nin\n Source',
        config_overrides={
            "native_query_parsing": True,
            "enable_advance_lineage_sql_construct": True,
        },
    )
    assert lineages == []


@pytest.mark.parametrize(
    ("record", "expected_db"),
    [
        ('[Catalog="cat", Database="explicit_db"]', "explicit_db"),
        ('[Catalog="cat", Database=null]', "nav_db"),
    ],
)
def test_databricks_native_query_database_precedence(
    record: str, expected_db: str
) -> None:
    """An explicit Database argument wins; a null one falls back to the
    navigation step's Name."""
    lineages, _ = m_query_lineages(
        'let\n Source = Value.NativeQuery(Databricks.Catalogs("adb.azuredatabricks.net", '
        f'"/sql/1.0/endpoints/abc", {record}){{[Name="nav_db",Kind="Database"]}}[Data], '
        '"select * from public.events", null, [EnableFolding=true])\nin\n Source',
        config_overrides={
            "native_query_parsing": True,
            "enable_advance_lineage_sql_construct": True,
        },
    )
    assert upstream_urns(lineages) == [
        f"urn:li:dataset:(urn:li:dataPlatform:databricks,{expected_db}.public.events,PROD)"
    ]


# --- ODBC -------------------------------------------------------------------

_MYSQL_ODBC = (
    "driver={MySQL ODBC 9.2 Unicode Driver};server=10.1.10.1;"
    "database=employees;dsn=testdb01"
)


def test_odbc_datasource_navigation_builds_database_table_urn() -> None:
    lineages, _ = m_query_lineages(
        f'let\n Source = Odbc.DataSource("{_MYSQL_ODBC}", [HierarchicalNavigation=true]),\n'
        ' db = Source{[Name="employees",Kind="Database"]}[Data],\n'
        ' tbl = db{[Name="employees",Kind="Table"]}[Data]\nin\n tbl'
    )
    assert upstream_urns(lineages) == [
        "urn:li:dataset:(urn:li:dataPlatform:mysql,employees.employees,PROD)"
    ]


def test_odbc_datasource_without_table_step_warns() -> None:
    lineages, reporter = m_query_lineages(
        f'let\n Source = Odbc.DataSource("{_MYSQL_ODBC}", [HierarchicalNavigation=true]),\n'
        ' db = Source{[Name="employees",Kind="Database"]}[Data]\nin\n db'
    )
    assert lineages == []
    assert any(
        w.title == "Can not determine qualified table name" for w in reporter.warnings
    )


def test_odbc_query_applies_dsn_database_and_schema_defaults() -> None:
    """The platform comes from the ODBC driver string while the database and
    schema an unqualified table lives in come from the DSN mapping."""
    lineages, _ = m_query_lineages(
        f'let\n Source = Odbc.Query("{_MYSQL_ODBC}", '
        '"SELECT transaction_id FROM transaction")\nin\n Source',
        config_overrides={
            "enable_advance_lineage_sql_construct": True,
            "dsn_to_database_schema": {"testdb01": "bank_demo.ledger"},
        },
    )
    assert upstream_urns(lineages) == [
        "urn:li:dataset:(urn:li:dataPlatform:mysql,bank_demo.ledger.transaction,PROD)"
    ]


def test_odbc_dsn_only_connect_string_resolves_platform_via_mapping() -> None:
    lineages, _ = m_query_lineages(
        'let\n Source = Odbc.DataSource("DSN=SalesWarehouse", [HierarchicalNavigation=true]),\n'
        ' db = Source{[Name="sales",Kind="Database"]}[Data],\n'
        ' sch = db{[Name="dbo",Kind="Schema"]}[Data],\n'
        ' tbl = sch{[Name="orders",Kind="Table"]}[Data]\nin\n tbl',
        config_overrides={"dsn_to_platform_name": {"SalesWarehouse": "mssql"}},
    )
    assert upstream_urns(lineages) == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,sales.dbo.orders,PROD)"
    ]


def test_odbc_unknown_platform_warns_and_skips() -> None:
    lineages, reporter = m_query_lineages(
        'let\n Source = Odbc.DataSource("DSN=Mystery", [HierarchicalNavigation=true]),\n'
        ' db = Source{[Name="x",Kind="Database"]}[Data]\nin\n db'
    )
    assert lineages == []
    assert any(w.title == "Can not determine ODBC platform" for w in reporter.warnings)


def test_odbc_connect_string_without_dsn_warns_and_skips() -> None:
    lineages, reporter = m_query_lineages(
        'let\n Source = Odbc.DataSource("driver={MySQL ODBC 9.2 Unicode Driver};'
        'server=10.1.10.1", [HierarchicalNavigation=true]),\n'
        ' db = Source{[Name="x",Kind="Database"]}[Data]\nin\n db'
    )
    assert lineages == []
    assert any(w.title == "Can not determine ODBC DSN" for w in reporter.warnings)


def test_odbc_unresolved_connect_string_parameter_warns() -> None:
    lineages, reporter = m_query_lineages(
        "let\n Source = Odbc.DataSource(ConnParam, [HierarchicalNavigation=true]),\n"
        ' db = Source{[Name="x",Kind="Database"]}[Data]\nin\n db'
    )
    assert lineages == []
    assert any(
        w.title == "Can not extract ODBC connect string" for w in reporter.warnings
    )


# --- BigQuery EXTERNAL_QUERY federation ---------------------------------------


def _bq_native_query(sql: str) -> str:
    return (
        'let\n Source = Value.NativeQuery(GoogleBigQuery.Database([BillingProject="my_project"])'
        f'{{[Name="my_project"]}}[Data], "{sql}", null, [EnableFolding=true])\nin\n Source'
    )


_FEDERATION_CONFIG: Dict[str, Any] = {
    "native_query_parsing": True,
    "enable_advance_lineage_sql_construct": True,
    "bigquery_external_query_connection_to_platform": {
        "my_project.us-east1.pg_conn": {
            "platform": "postgres",
            "default_database": "ext_db",
        },
        "my_project.us-east1.mssql_conn": {
            "platform": "mssql",
            "default_database": "ext_mssql",
        },
    },
}

_POSTGRES_UPSTREAM = (
    "urn:li:dataset:(urn:li:dataPlatform:postgres,ext_db.ext_schema.usage_report,PROD)"
)


def test_external_query_resolves_federated_and_native_tables() -> None:
    """The federation is stripped and resolved against the external platform
    while the remaining native table is parsed normally; both upstreams must
    be emitted."""
    sql = (
        "select n.account_id, e.account_name from my_project.ds.native_tbl n join "
        'EXTERNAL_QUERY(""my_project.us-east1.pg_conn"", ""SELECT account_name, account_id '
        "FROM ext_schema.usage_report"
        '"") e on e.account_id = n.account_id'
    )
    lineages, reporter = m_query_lineages(
        _bq_native_query(sql), config_overrides=_FEDERATION_CONFIG
    )
    assert sorted(upstream_urns(lineages)) == [
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.ds.native_tbl,PROD)",
        _POSTGRES_UPSTREAM,
    ]
    assert reporter.m_query_external_query_connections_resolved == 1
    assert reporter.m_query_external_query_failures == 0


def test_external_query_duplicate_federations_are_deduplicated() -> None:
    """Two federations resolving to the same external table produce a single
    upstream edge (nothing downstream collapses duplicate URNs), while both
    are still counted as resolved."""
    inner = '""SELECT account_name FROM ext_schema.usage_report""'
    sql = (
        f'select a.account_name from EXTERNAL_QUERY(""my_project.us-east1.pg_conn"", {inner}) a '
        f'union all select b.account_name from EXTERNAL_QUERY(""my_project.us-east1.pg_conn"", {inner}) b'
    )
    lineages, reporter = m_query_lineages(
        _bq_native_query(sql), config_overrides=_FEDERATION_CONFIG
    )
    assert upstream_urns(lineages) == [_POSTGRES_UPSTREAM]
    assert reporter.m_query_external_query_connections_resolved == 2


def test_external_query_unmapped_connection_is_reported_not_fatal() -> None:
    """An unmapped connection must not emit a bogus BigQuery URN: the lineage
    is skipped with an actionable info entry telling the operator to map the
    connection, and no SQL-parsing warning is raised for this expected case."""
    sql = (
        'select * from EXTERNAL_QUERY(""my_project.us-east1.unknown_conn"", '
        '""SELECT 1 FROM ext_schema.usage_report"")'
    )
    lineages, reporter = m_query_lineages(
        _bq_native_query(sql), config_overrides=_FEDERATION_CONFIG
    )
    assert lineages == []
    assert reporter.m_query_external_query_connections_unmapped == 1
    assert any(i.title == Constant.EXTERNAL_QUERY_NOT_MAPPED for i in reporter.infos)
    assert not reporter.warnings


def test_external_query_inner_sql_without_table_is_a_failure() -> None:
    """Inner SQL that parses but references no table still drops the
    federated lineage, so it is a reported failure, not a resolved federation."""
    sql = (
        'select * from EXTERNAL_QUERY(""my_project.us-east1.pg_conn"", '
        '""SELECT 1 AS one"")'
    )
    lineages, reporter = m_query_lineages(
        _bq_native_query(sql), config_overrides=_FEDERATION_CONFIG
    )
    assert lineages == []
    assert reporter.m_query_external_query_failures == 1
    assert reporter.m_query_external_query_connections_resolved == 0
    assert any(w.title == Constant.SQL_PARSING_FAILURE for w in reporter.warnings)


@pytest.mark.parametrize(
    "inner_preamble",
    ["SET NOCOUNT ON; ", "USE Reports#(lf)GO#(lf)DROP TABLE IF EXISTS #tmp;#(lf)"],
    ids=["set-option", "use-go-drop"],
)
def test_external_query_mssql_inner_sql_is_tsql_cleaned_after_extraction(
    inner_preamble: str,
) -> None:
    """The T-SQL cleanup's USE/GO/SET/DROP regexes ignore string boundaries,
    so it must run on the inner SQL only after EXTERNAL_QUERY extraction;
    running it on the outer text first would rewrite the federated literal.
    Cloud SQL Server federations commonly embed these control statements and
    must still resolve to the real mssql table."""
    sql = (
        'select * from EXTERNAL_QUERY(""my_project.us-east1.mssql_conn"", '
        f'""{inner_preamble}SELECT id FROM dbo.ext_table"")'
    )
    lineages, reporter = m_query_lineages(
        _bq_native_query(sql), config_overrides=_FEDERATION_CONFIG
    )
    assert upstream_urns(lineages) == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,ext_mssql.dbo.ext_table,PROD)"
    ]
    assert reporter.m_query_external_query_connections_resolved == 1
    assert reporter.m_query_external_query_failures == 0


def test_external_query_untokenizable_sql_is_reported_as_failure() -> None:
    """An unterminated comment makes sqlglot's tokenizer fail, so the
    federation detector cannot tell whether the EXTERNAL_QUERY call is real.
    The lineage is dropped either way, but it must not be dropped silently:
    a federation failure and a warning have to surface in the report."""
    sql = (
        "/* unterminated comment select * from "
        'EXTERNAL_QUERY(""my_project.us-east1.pg_conn"", ""SELECT 1"")'
    )
    lineages, reporter = m_query_lineages(
        _bq_native_query(sql), config_overrides=_FEDERATION_CONFIG
    )
    assert lineages == []
    assert reporter.m_query_external_query_failures == 1
    assert any(w.title == Constant.SQL_PARSING_FAILURE for w in reporter.warnings)
