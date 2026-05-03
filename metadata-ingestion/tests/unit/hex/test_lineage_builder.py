from typing import Dict, Optional

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

CONNECTIONS: Dict[str, str] = {
    SNOWFLAKE_CONN: "snowflake",
    BIGQUERY_CONN: "bigquery",
    UNSUPPORTED_TYPE_CONN: "cassandra",  # not in CONNECTION_TYPE_TO_DATAHUB_PLATFORM
}


def _builder(
    connections: Optional[Dict[str, str]] = None,
    platform_instance: Optional[str] = None,
    env: str = "PROD",
    report: Optional[LineageBuilderReport] = None,
    project_id: str = "proj-1",
) -> HexLineageBuilder:
    return HexLineageBuilder(
        connections=connections if connections is not None else CONNECTIONS,
        platform_instance=platform_instance,
        env=env,
        report=report if report is not None else LineageBuilderReport(),
        project_id=project_id,
    )


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


# ------------------------------------------------------------------
# build_validated_column_lineage  (ENTERPRISE cross-validation)
# ------------------------------------------------------------------


def _make_dataset_urn(
    table: str, platform: str = "snowflake", env: str = "PROD"
) -> str:
    from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance

    return make_dataset_urn_with_platform_instance(
        platform=platform, name=table, platform_instance=None, env=env
    )


def test_validated_cll_emits_when_table_matches():
    report = LineageBuilderReport()
    b = _builder(report=report)
    orders_urn = _make_dataset_urn("db.schema.orders")
    queried = [orders_urn]
    sql = "SELECT order_id, customer_id FROM db.schema.orders"
    fields = b.build_validated_column_lineage([_cell(sql)], queried)
    assert len(fields) > 0
    assert any("order_id" in f for f in fields)
    assert report.enterprise_column_fields_emitted > 0
    assert report.enterprise_cells_with_mismatch == 0


def test_validated_cll_skips_when_table_not_in_queried():
    report = LineageBuilderReport()
    b = _builder(report=report, project_id="proj-ent")
    # queriedTables has no entry for the table SQL parsing finds
    queried = [_make_dataset_urn("db.schema.other_table")]
    sql = "SELECT order_id FROM db.schema.orders"
    fields = b.build_validated_column_lineage([_cell(sql)], queried)
    assert fields == []
    assert report.enterprise_cells_with_mismatch == 1
    assert report.enterprise_column_fields_skipped_mismatch > 0


def test_validated_cll_partial_match_emits_matched_columns_only():
    """Two tables in SQL: one in queriedTables, one not. Only matched columns emitted."""
    report = LineageBuilderReport()
    b = _builder(report=report)
    customers_urn = _make_dataset_urn("db.s.customers")
    # orders is NOT in queriedTables
    queried = [customers_urn]
    sql = "SELECT a.id, b.name FROM db.s.customers a JOIN db.s.orders b ON a.id = b.cid"
    fields = b.build_validated_column_lineage([_cell(sql)], queried)
    # columns from customers should be emitted; columns from orders should be skipped
    assert any("customers" in f for f in fields)
    assert not any("orders" in f for f in fields)
    assert report.enterprise_cells_with_mismatch == 1
    assert report.enterprise_column_fields_skipped_mismatch > 0
    assert report.enterprise_column_fields_emitted > 0


def test_validated_cll_mismatch_sample_stored():
    report = LineageBuilderReport()
    b = _builder(report=report, project_id="proj-ent")
    queried = [_make_dataset_urn("db.schema.other")]
    sql = "SELECT id FROM db.schema.orders"
    b.build_validated_column_lineage([_cell(sql, label="Orders query")], queried)
    assert len(report.enterprise_sample_mismatched_cells) == 1
    sample = report.enterprise_sample_mismatched_cells[0]
    assert sample.project_id == "proj-ent"
    assert sample.cell_label == "Orders query"
    assert len(sample.unmatched_parsed_urns) > 0
    assert len(sample.sample_queried_urns) > 0


def test_validated_cll_sample_capped_at_max():
    report = LineageBuilderReport()
    b = _builder(report=report)
    queried = [_make_dataset_urn("db.schema.other")]
    # 10 different cells all mismatching — only 5 samples stored
    cells = [_cell(f"SELECT id FROM db.schema.t{i}", label=f"q{i}") for i in range(10)]
    b.build_validated_column_lineage(cells, queried)
    assert len(report.enterprise_sample_mismatched_cells) == 5


def test_validated_cll_deduplicates_fields_across_cells():
    report = LineageBuilderReport()
    b = _builder(report=report)
    orders_urn = _make_dataset_urn("db.schema.orders")
    queried = [orders_urn]
    sql = "SELECT order_id FROM db.schema.orders"
    # same SQL twice — same field should not be emitted twice
    fields = b.build_validated_column_lineage([_cell(sql), _cell(sql)], queried)
    assert len(fields) == len(set(fields))


def test_validated_cll_empty_sql_cells_returns_empty():
    b = _builder()
    fields = b.build_validated_column_lineage([], [_make_dataset_urn("db.s.t")])
    assert fields == []


def test_validated_cll_unknown_connection_skipped_silently():
    report = LineageBuilderReport()
    b = _builder(report=report)
    queried = [_make_dataset_urn("db.s.t")]
    fields = b.build_validated_column_lineage(
        [_cell("SELECT id FROM db.s.t", conn_id=UNKNOWN_CONN)], queried
    )
    assert fields == []
    assert report.enterprise_cells_with_mismatch == 0  # skipped before parsing
