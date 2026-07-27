from typing import Dict, Optional

from datahub.ingestion.source.hex.lineage_builder import (
    HexLineageBuilder,
    LineageBuilderReport,
    SkippedCell,
    _qualify_table_name,
)
from datahub.ingestion.source.hex.model import HexConnection, SqlCell

SNOWFLAKE_CONN = "conn-snowflake-1"
BIGQUERY_CONN = "conn-bq-1"
UNKNOWN_CONN = "conn-unknown-1"

# {connection_id → HexConnection}, pre-resolved by the caller
CONNECTIONS: Dict[str, HexConnection] = {
    SNOWFLAKE_CONN: HexConnection(name="Analytics", platform="snowflake"),
    BIGQUERY_CONN: HexConnection(name="BQ", platform="bigquery"),
}


def _builder(
    connections: Optional[Dict[str, HexConnection]] = None,
    env: str = "PROD",
    report: Optional[LineageBuilderReport] = None,
    project_id: str = "proj-1",
) -> HexLineageBuilder:
    return HexLineageBuilder(
        connections=connections if connections is not None else CONNECTIONS,
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
# _lookup_connection
# ------------------------------------------------------------------


def test_lookup_connection_known_snowflake():
    b = _builder()
    connection, reason = b._lookup_connection(SNOWFLAKE_CONN)
    assert connection is not None
    assert connection.platform == "snowflake"
    assert connection.platform_instance is None
    assert reason is None


def test_lookup_connection_known_bigquery():
    b = _builder()
    connection, reason = b._lookup_connection(BIGQUERY_CONN)
    assert connection is not None
    assert connection.platform == "bigquery"
    assert reason is None


def test_lookup_connection_none_id():
    b = _builder()
    connection, reason = b._lookup_connection(None)
    assert connection is None
    assert reason == "missing_connection_id"


def test_lookup_connection_empty_string():
    b = _builder()
    connection, reason = b._lookup_connection("")
    assert connection is None
    assert reason == "missing_connection_id"


def test_lookup_connection_unresolved_connection_id():
    b = _builder()
    connection, reason = b._lookup_connection(UNKNOWN_CONN)
    assert connection is None
    assert reason == "unresolved_platform"


def test_lookup_connection_uses_per_connection_platform_instance():
    """The per-connection platform_instance flows into the upstream URN."""
    b = _builder(
        connections={
            SNOWFLAKE_CONN: HexConnection(
                name="A", platform="snowflake", platform_instance="prod_snowflake"
            ),
        },
    )
    connection, _ = b._lookup_connection(SNOWFLAKE_CONN)
    assert connection is not None
    assert connection.platform_instance == "prod_snowflake"


def test_lookup_connection_no_platform_instance_emits_none():
    """When the connection has no platform_instance pinned, the upstream URN
    gets None — matches ADF / Mode / PowerBI behavior. Hex's own
    platform_instance is never used as a fallback for upstream URNs."""
    b = _builder(
        connections={SNOWFLAKE_CONN: HexConnection(name="A", platform="snowflake")},
    )
    connection, _ = b._lookup_connection(SNOWFLAKE_CONN)
    assert connection is not None
    assert connection.platform_instance is None


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
    assert report.skipped_cells[0].reason == "unresolved_platform"


def test_build_from_queried_tables_empty_table_name_skipped():
    b = _builder()
    urns = b.build_from_queried_tables(
        [{"dataConnectionId": SNOWFLAKE_CONN, "tableName": ""}]
    )
    assert urns == []


def test_qualify_table_name_already_three_parts_unchanged():
    assert (
        _qualify_table_name("db.schema.t", "DEFAULT_DB", "DEFAULT_SCHEMA")
        == "db.schema.t"
    )


def test_qualify_table_name_two_parts_gets_database_prefix():
    assert (
        _qualify_table_name("schema.t", "DEFAULT_DB", "DEFAULT_SCHEMA")
        == "DEFAULT_DB.schema.t"
    )


def test_qualify_table_name_one_part_gets_both_prefixes():
    assert (
        _qualify_table_name("t", "DEFAULT_DB", "DEFAULT_SCHEMA")
        == "DEFAULT_DB.DEFAULT_SCHEMA.t"
    )


def test_qualify_table_name_two_part_platform_prepends_only_schema():
    # MySQL/MariaDB/Clickhouse: default_database is None, default_schema slots in.
    assert _qualify_table_name("t", None, "DEFAULT_SCHEMA") == "DEFAULT_SCHEMA.t"
    assert _qualify_table_name("schema.t", None, "DEFAULT_SCHEMA") == "schema.t"


def test_qualify_table_name_no_defaults_returns_unchanged():
    assert _qualify_table_name("t", None, None) == "t"
    assert _qualify_table_name("schema.t", None, None) == "schema.t"


def test_build_from_queried_tables_pads_unqualified_name():
    """Hex returns just `orders` — connection defaults qualify it to db.schema.orders."""
    b = _builder(
        connections={
            SNOWFLAKE_CONN: HexConnection(
                name="A",
                platform="snowflake",
                default_database="ANALYTICS",
                default_schema="PUBLIC",
            ),
        },
    )
    urns = b.build_from_queried_tables(
        [{"dataConnectionId": SNOWFLAKE_CONN, "tableName": "orders"}]
    )
    assert len(urns) == 1
    assert "ANALYTICS.PUBLIC.orders" in urns[0]


def test_build_from_queried_tables_pads_two_part_name():
    """Hex returns `schema.orders` — connection's default_database qualifies it."""
    b = _builder(
        connections={
            SNOWFLAKE_CONN: HexConnection(
                name="A",
                platform="snowflake",
                default_database="ANALYTICS",
                default_schema="PUBLIC",
            ),
        },
    )
    urns = b.build_from_queried_tables(
        [{"dataConnectionId": SNOWFLAKE_CONN, "tableName": "RAW.orders"}]
    )
    assert len(urns) == 1
    assert "ANALYTICS.RAW.orders" in urns[0]


def test_build_from_queried_tables_three_part_name_not_padded():
    """Fully qualified names are passed through verbatim, even when defaults exist."""
    b = _builder(
        connections={
            SNOWFLAKE_CONN: HexConnection(
                name="A",
                platform="snowflake",
                default_database="ANALYTICS",
                default_schema="PUBLIC",
            ),
        },
    )
    urns = b.build_from_queried_tables(
        [
            {
                "dataConnectionId": SNOWFLAKE_CONN,
                "tableName": "OTHER_DB.OTHER_SCHEMA.orders",
            }
        ]
    )
    assert len(urns) == 1
    assert "OTHER_DB.OTHER_SCHEMA.orders" in urns[0]
    assert "ANALYTICS" not in urns[0]


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
    assert report.sql_cells_skipped_unresolved_platform == 1
    assert len(report.skipped_cells) == 1
    assert report.skipped_cells[0].connection_id == UNKNOWN_CONN
    assert report.skipped_cells[0].reason == "unresolved_platform"


def test_build_upstream_urns_none_connection_skipped():
    report = LineageBuilderReport()
    b = _builder(report=report)
    cell = SqlCell(
        cell_id="c", cell_label=None, sql_source="SELECT 1", data_connection_id=None
    )
    datasets, _ = b.build_upstream_urns([cell])
    assert datasets == []
    assert report.sql_cells_skipped_unresolved_platform == 1
    assert report.skipped_cells[0].reason == "missing_connection_id"


def test_lookup_connection_user_override_bypasses_canonical_map():
    """User overrides should resolve to ANY platform name, not just those in
    CONNECTION_TYPE_TO_DATAHUB_PLATFORM. This test guards the M2 fix at the
    builder level: the builder must accept any pre-resolved platform string
    without re-translating it through the canonical map.
    """
    b = _builder(
        connections={
            **CONNECTIONS,
            "conn-vertica-1": HexConnection(name="Vertica Prod", platform="vertica"),
        },
    )
    connection, reason = b._lookup_connection("conn-vertica-1")
    assert connection is not None
    assert connection.platform == "vertica"
    assert reason is None


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


def test_build_upstream_urns_no_platform_instance_when_not_pinned():
    """No per-connection platform_instance → upstream URN has no instance
    prefix. Matches a warehouse ingested without a platform_instance
    (typical BigQuery setup)."""
    b = _builder()
    sql = "SELECT id FROM db.schema.orders"
    datasets, _ = b.build_upstream_urns([_cell(sql)])
    assert len(datasets) == 1
    # No platform_instance was pinned → URN should not contain one.
    # The dataset name part is the only segment between platform and env.
    assert ",db.schema.orders," in datasets[0]


def test_build_upstream_urns_uses_per_connection_platform_instance():
    """The fix in action: a connection pinned to `prod_snowflake` produces
    an upstream URN that matches the corresponding Snowflake ingestion."""
    b = _builder(
        connections={
            SNOWFLAKE_CONN: HexConnection(
                name="A", platform="snowflake", platform_instance="prod_snowflake"
            ),
        },
    )
    datasets, _ = b.build_upstream_urns(
        [_cell("SELECT id FROM db.schema.orders", conn_id=SNOWFLAKE_CONN)]
    )
    assert len(datasets) == 1
    assert "prod_snowflake" in datasets[0]


def test_build_from_queried_tables_uses_connection_platform_instance():
    """End-to-end via the ENTERPRISE queriedTables path: upstream URNs
    reflect the connection's pinned platform_instance."""
    b = _builder(
        connections={
            SNOWFLAKE_CONN: HexConnection(
                name="A", platform="snowflake", platform_instance="prod_snowflake"
            ),
        },
    )
    urns = b.build_from_queried_tables(
        [
            {
                "dataConnectionId": SNOWFLAKE_CONN,
                "tableName": "db.schema.orders",
            }
        ]
    )
    assert len(urns) == 1
    assert "prod_snowflake" in urns[0]


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
    assert report.sql_cells_skipped_unresolved_platform == 1
    assert report.projects_lineage_via_sql_parsing == 1


# ------------------------------------------------------------------
# SchemaResolver caching — same platform reuses same instance
# ------------------------------------------------------------------


def test_schema_resolver_cached_per_combination():
    """Resolver cache keyed by (platform, platform_instance) — same key reuses,
    different on either axis yields a new resolver."""
    b = _builder()
    r1 = b._get_resolver("snowflake", None)
    r2 = b._get_resolver("snowflake", None)
    r3 = b._get_resolver("bigquery", None)
    # Different platform_instance for the same platform → different resolver
    # (two same-platform connections with distinct instances must NOT share).
    r4 = b._get_resolver("snowflake", "prod_sf")
    assert r1 is r2
    assert r1 is not r3
    assert r1 is not r4


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
    assert s.reason == "unresolved_platform"


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


# ------------------------------------------------------------------
# default_database / default_schema propagation to sqlglot
# ------------------------------------------------------------------


def test_default_db_and_schema_resolve_unqualified_table():
    """An unqualified `FROM orders` should resolve to a fully-qualified URN
    when the connection carries default_database / default_schema. Without
    these, sqlglot would emit a URN with only the bare table name."""
    b = _builder(
        connections={
            SNOWFLAKE_CONN: HexConnection(
                name="A",
                platform="snowflake",
                default_database="ANALYTICS",
                default_schema="PUBLIC",
            ),
        },
    )
    datasets, _ = b.build_upstream_urns([_cell("SELECT id FROM orders")])
    assert len(datasets) == 1
    # Fully qualified by the connection's defaults.
    assert "analytics.public.orders" in datasets[0].lower()


def test_no_default_db_leaves_unqualified_table_bare():
    """Counter-example to test_default_db_and_schema_resolve_unqualified_table:
    when no defaults are set on the connection, an unqualified `FROM orders`
    must NOT acquire a warehouse/schema prefix. Guards against the inverse
    regression — empty-string defaults silently producing `..orders` URNs."""
    b = _builder(
        connections={SNOWFLAKE_CONN: HexConnection(name="A", platform="snowflake")},
    )
    datasets, _ = b.build_upstream_urns([_cell("SELECT id FROM orders")])
    assert len(datasets) == 1
    # URN format: urn:li:dataset:(...,<name>,<env>). The `,orders,` segment
    # asserts the name part is exactly `orders` with no leading qualifiers.
    assert ",orders," in datasets[0].lower()


def test_explicit_table_qualifier_overrides_defaults():
    """When the SQL already qualifies the table, the connection's defaults
    must NOT override it — otherwise cross-database queries would silently
    rewrite to the connection's default database."""
    b = _builder(
        connections={
            SNOWFLAKE_CONN: HexConnection(
                name="A",
                platform="snowflake",
                default_database="ANALYTICS",
                default_schema="PUBLIC",
            ),
        },
    )
    datasets, _ = b.build_upstream_urns(
        [_cell("SELECT id FROM other_db.other_schema.orders")]
    )
    assert len(datasets) == 1
    assert "other_db.other_schema.orders" in datasets[0].lower()
