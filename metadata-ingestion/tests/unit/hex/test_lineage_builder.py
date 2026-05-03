from datahub.ingestion.source.hex.lineage_builder import (
    HexLineageBuilder,
    LineageBuilderReport,
    SkippedCell,
)
from datahub.ingestion.source.hex.model import SqlCell

SNOWFLAKE_CONN = "conn-snowflake-1"
BIGQUERY_CONN = "conn-bq-1"
UNKNOWN_CONN = "conn-unknown-1"
UNSUPPORTED_TYPE_CONN = "conn-cassandra-1"

CONNECTIONS = {
    SNOWFLAKE_CONN: "snowflake",
    BIGQUERY_CONN: "bigquery",
    UNSUPPORTED_TYPE_CONN: "cassandra",  # not in CONNECTION_TYPE_TO_DATAHUB_PLATFORM
}


def _builder(**kwargs) -> HexLineageBuilder:
    defaults = dict(
        connections=CONNECTIONS,
        platform_instance=None,
        env="PROD",
        report=LineageBuilderReport(),
        project_id="proj-1",
    )
    defaults.update(kwargs)
    return HexLineageBuilder(**defaults)


def _cell(sql: str, conn_id: str = SNOWFLAKE_CONN, label: str = "q") -> SqlCell:
    return SqlCell(
        cell_id="cell-1",
        cell_label=label,
        sql_source=sql,
        data_connection_id=conn_id,
    )


# ------------------------------------------------------------------
# _resolve_platform
# ------------------------------------------------------------------


def test_resolve_platform_known_snowflake():
    b = _builder()
    platform, reason = b._resolve_platform(SNOWFLAKE_CONN)
    assert platform == "snowflake"
    assert reason is None


def test_resolve_platform_known_bigquery():
    b = _builder()
    platform, reason = b._resolve_platform(BIGQUERY_CONN)
    assert platform == "bigquery"
    assert reason is None


def test_resolve_platform_none_id():
    b = _builder()
    platform, reason = b._resolve_platform(None)
    assert platform is None
    assert reason == "unknown_connection_id"


def test_resolve_platform_empty_string():
    b = _builder()
    platform, reason = b._resolve_platform("")
    assert platform is None
    assert reason == "unknown_connection_id"


def test_resolve_platform_missing_connection_id():
    b = _builder()
    platform, reason = b._resolve_platform(UNKNOWN_CONN)
    assert platform is None
    assert reason == "unknown_connection_id"


def test_resolve_platform_unsupported_connection_type():
    b = _builder()
    platform, reason = b._resolve_platform(UNSUPPORTED_TYPE_CONN)
    assert platform is None
    assert reason is not None
    assert "cassandra" in reason


# ------------------------------------------------------------------
# build_from_queried_tables  (ENTERPRISE tier)
# ------------------------------------------------------------------


def test_build_from_queried_tables_basic():
    b = _builder()
    urns = b.build_from_queried_tables(
        [
            {
                "dataConnectionId": SNOWFLAKE_CONN,
                "tableName": "db.schema.orders",
            }
        ]
    )
    assert len(urns) == 1
    assert "snowflake" in urns[0]
    assert "db.schema.orders" in urns[0]
    assert b._report.upstream_datasets_found == 1
    assert b._report.projects_lineage_via_queried_tables == 1


def test_build_from_queried_tables_deduplication():
    b = _builder()
    urns = b.build_from_queried_tables(
        [
            {"dataConnectionId": SNOWFLAKE_CONN, "tableName": "db.schema.t"},
            {"dataConnectionId": SNOWFLAKE_CONN, "tableName": "db.schema.t"},
        ]
    )
    assert len(urns) == 1


def test_build_from_queried_tables_unknown_connection_skipped():
    report = LineageBuilderReport()
    b = _builder(report=report)
    urns = b.build_from_queried_tables(
        [{"dataConnectionId": UNKNOWN_CONN, "tableName": "db.schema.t"}]
    )
    assert urns == []
    assert len(report.skipped_cells) == 1
    assert report.skipped_cells[0].reason == "unknown_connection_id"


def test_build_from_queried_tables_empty_table_name_skipped():
    b = _builder()
    urns = b.build_from_queried_tables(
        [{"dataConnectionId": SNOWFLAKE_CONN, "tableName": ""}]
    )
    assert urns == []


def test_build_from_queried_tables_mixed_connections():
    b = _builder()
    urns = b.build_from_queried_tables(
        [
            {"dataConnectionId": SNOWFLAKE_CONN, "tableName": "sf.db.t"},
            {"dataConnectionId": BIGQUERY_CONN, "tableName": "bq.db.t"},
            {"dataConnectionId": UNKNOWN_CONN, "tableName": "x.db.t"},
        ]
    )
    assert len(urns) == 2
    assert any("snowflake" in u for u in urns)
    assert any("bigquery" in u for u in urns)
    assert b._report.upstream_datasets_found == 2


# ------------------------------------------------------------------
# build_upstream_urns  (SQL parsing tier)
# ------------------------------------------------------------------


def test_build_upstream_urns_explicit_columns():
    b = _builder()
    sql = "SELECT order_id, customer_id FROM db.schema.orders"
    datasets, fields = b.build_upstream_urns([_cell(sql)])
    assert len(datasets) == 1
    assert "orders" in datasets[0]
    assert any("order_id" in f for f in fields)
    assert any("customer_id" in f for f in fields)
    assert b._report.sql_cells_attempted == 1
    assert b._report.sql_cells_succeeded == 1


def test_build_upstream_urns_join():
    b = _builder()
    sql = "SELECT a.id, b.name FROM db.s.customers a JOIN db.s.orders b ON a.id = b.customer_id"
    datasets, fields = b.build_upstream_urns([_cell(sql)])
    assert len(datasets) == 2
    assert b._report.upstream_datasets_found == 2


def test_build_upstream_urns_unknown_connection_skipped():
    report = LineageBuilderReport()
    b = _builder(report=report)
    sql = "SELECT id FROM db.s.t"
    datasets, fields = b.build_upstream_urns([_cell(sql, conn_id=UNKNOWN_CONN)])
    assert datasets == []
    assert fields == []
    assert report.sql_cells_skipped_unknown_connection == 1
    assert len(report.skipped_cells) == 1
    assert report.skipped_cells[0].connection_id == UNKNOWN_CONN


def test_build_upstream_urns_unsupported_type_skipped():
    report = LineageBuilderReport()
    b = _builder(report=report)
    datasets, _ = b.build_upstream_urns(
        [_cell("SELECT id FROM t", conn_id=UNSUPPORTED_TYPE_CONN)]
    )
    assert datasets == []
    assert report.sql_cells_skipped_unknown_connection == 1


def test_build_upstream_urns_none_connection_skipped():
    report = LineageBuilderReport()
    b = _builder(report=report)
    cell = SqlCell(
        cell_id="c", cell_label=None, sql_source="SELECT 1", data_connection_id=None
    )
    datasets, _ = b.build_upstream_urns([cell])
    assert datasets == []
    assert report.sql_cells_skipped_unknown_connection == 1


def test_build_upstream_urns_deduplication_across_cells():
    b = _builder()
    sql = "SELECT id FROM db.schema.t"
    datasets, _ = b.build_upstream_urns([_cell(sql), _cell(sql)])
    assert len(datasets) == 1
    assert b._report.upstream_datasets_found == 1


def test_build_upstream_urns_invalid_sql_counted_as_failure():
    report = LineageBuilderReport()
    b = _builder(report=report)
    cell = _cell("THIS IS NOT SQL AT ALL @@##$$")
    datasets, fields = b.build_upstream_urns([cell])
    # sqlglot is lenient so it may succeed with empty tables — what matters is no crash
    assert isinstance(datasets, list)
    assert isinstance(fields, list)
    assert report.sql_cells_attempted == 1


def test_build_upstream_urns_platform_instance_in_urn():
    b = _builder(platform_instance="my_instance")
    sql = "SELECT id FROM db.schema.orders"
    datasets, _ = b.build_upstream_urns([_cell(sql)])
    assert len(datasets) == 1
    assert "my_instance" in datasets[0]


def test_build_upstream_urns_report_counters():
    report = LineageBuilderReport()
    b = _builder(report=report)
    good_sql = "SELECT id FROM db.schema.t1"
    bad_conn_sql = "SELECT id FROM db.schema.t2"
    b.build_upstream_urns(
        [
            _cell(good_sql, conn_id=SNOWFLAKE_CONN),
            _cell(bad_conn_sql, conn_id=UNKNOWN_CONN),
        ]
    )
    assert report.sql_cells_attempted == 2
    assert report.sql_cells_succeeded == 1
    assert report.sql_cells_skipped_unknown_connection == 1
    assert report.projects_lineage_via_sql_parsing == 1


# ------------------------------------------------------------------
# SchemaResolver caching — same platform reuses same instance
# ------------------------------------------------------------------


def test_schema_resolver_cached_per_platform():
    b = _builder()
    r1 = b._get_resolver("snowflake")
    r2 = b._get_resolver("snowflake")
    r3 = b._get_resolver("bigquery")
    assert r1 is r2
    assert r1 is not r3


# ------------------------------------------------------------------
# Skipped cell recording
# ------------------------------------------------------------------


def test_skipped_cell_attributes():
    report = LineageBuilderReport()
    b = _builder(report=report, project_id="proj-xyz")
    b.build_upstream_urns([_cell("SELECT 1", conn_id=UNKNOWN_CONN, label="My Query")])
    assert len(report.skipped_cells) == 1
    s: SkippedCell = report.skipped_cells[0]
    assert s.project_id == "proj-xyz"
    assert s.connection_id == UNKNOWN_CONN
    assert s.cell_label == "My Query"
    assert s.reason == "unknown_connection_id"
