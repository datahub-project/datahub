import pytest
from sqlglot.errors import SqlglotError

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.source.informix.lineage import build_view_upstream_lineage
from datahub.ingestion.source.informix.report import InformixSourceReport
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage


def _resolver() -> SchemaResolver:
    sr = SchemaResolver(platform="informix", env="PROD")

    def urn(n: str) -> str:
        return make_dataset_urn("informix", f"testdb.informix.{n}", "PROD")

    sr.add_raw_schema_info(
        urn("customers"), {"id": "INTEGER", "name": "VARCHAR", "email": "VARCHAR"}
    )
    sr.add_raw_schema_info(
        urn("orders"),
        {"order_id": "INTEGER", "customer_id": "INTEGER", "amount": "DECIMAL"},
    )
    return sr


def test_view_lineage_join_table_and_column_level() -> None:
    sql = (
        'create view "informix".customer_orders (customer_id,customer_name,order_id,amount) as '
        "select x0.id ,x0.name ,x1.order_id ,x1.amount from "
        '("informix".customers x0 join "informix".orders x1 on (x0.id = x1.customer_id ) )'
    )
    view_urn = make_dataset_urn("informix", "testdb.informix.customer_orders", "PROD")
    report = InformixSourceReport()
    up = build_view_upstream_lineage(
        view_urn,
        sql,
        _resolver(),
        "testdb",
        "informix",
        report,
        ["customer_id", "customer_name", "order_id", "amount"],
    )
    assert up is not None
    assert report.view_column_remap_mismatches == 0
    upstream_names = sorted(u.dataset.split(",")[-2] for u in up.upstreams)
    assert upstream_names == ["testdb.informix.customers", "testdb.informix.orders"]
    assert up.fineGrainedLineages

    # Downstream field names must be the view's DECLARED columns (customer_id,
    # customer_name), not the inner SELECT projection names (id, name) that
    # sqlglot reports for Informix's alias-stripped normalized view text.
    def _down(fgl: object) -> str:
        return fgl.downstreams[0].split(",")[-1].rstrip(")")  # type: ignore[attr-defined]

    def _up(fgl: object) -> str:
        return fgl.upstreams[0].split(",")[-1].rstrip(")")  # type: ignore[attr-defined]

    lineage_map = {_down(fgl): _up(fgl) for fgl in up.fineGrainedLineages}
    assert lineage_map["customer_id"] == "id"
    assert lineage_map["customer_name"] == "name"
    assert "id" not in lineage_map  # the inner projection name must NOT leak downstream


def test_view_lineage_raises_on_unparseable_sql() -> None:
    # A table-level parse error must propagate (so the source records a warning +
    # view_lineage_failures) rather than being silently swallowed into None.
    view_urn = make_dataset_urn("informix", "testdb.informix.bad", "PROD")
    with pytest.raises(SqlglotError):
        build_view_upstream_lineage(
            view_urn,
            "not valid sql at all ((",
            _resolver(),
            "testdb",
            "informix",
            InformixSourceReport(),
        )


def test_view_lineage_counts_declared_column_count_mismatch() -> None:
    # Fewer declared columns than parsed projections means the positional remap is
    # unsafe; lineage still emits, but the mismatch must be visible in the report.
    sql = (
        'create view "informix".partial (only_one) as '
        'select x0.id ,x0.name from "informix".customers x0'
    )
    view_urn = make_dataset_urn("informix", "testdb.informix.partial", "PROD")
    report = InformixSourceReport()
    up = build_view_upstream_lineage(
        view_urn, sql, _resolver(), "testdb", "informix", report, ["only_one"]
    )
    assert up is not None
    assert report.view_column_remap_mismatches == 1


def test_view_lineage_comma_join_from_sysviews_style() -> None:
    # Informix often normalizes joins to comma form in sysviews.viewtext.
    sql = (
        'create view "informix".customer_orders (customer_id,customer_name,order_id,amount) as '
        "select x0.id ,x0.name ,x1.order_id ,x1.amount from "
        '"informix".customers x0 ,"informix".orders x1 where (x0.id = x1.customer_id )'
    )
    view_urn = make_dataset_urn("informix", "testdb.informix.customer_orders", "PROD")
    report = InformixSourceReport()
    up = build_view_upstream_lineage(
        view_urn,
        sql,
        _resolver(),
        "testdb",
        "informix",
        report,
        ["customer_id", "customer_name", "order_id", "amount"],
    )
    assert up is not None
    assert report.view_column_remap_mismatches == 0
    upstream_names = sorted(u.dataset.split(",")[-2] for u in up.upstreams)
    assert upstream_names == ["testdb.informix.customers", "testdb.informix.orders"]
    assert up.fineGrainedLineages

    def _down(fgl: object) -> str:
        return fgl.downstreams[0].split(",")[-1].rstrip(")")  # type: ignore[attr-defined]

    def _up(fgl: object) -> str:
        return fgl.upstreams[0].split(",")[-1].rstrip(")")  # type: ignore[attr-defined]

    lineage_map = {_down(fgl): _up(fgl) for fgl in up.fineGrainedLineages}
    assert lineage_map["customer_id"] == "id"
    assert lineage_map["customer_name"] == "name"
    assert lineage_map["order_id"] == "order_id"
    assert lineage_map["amount"] == "amount"


def test_central_dialect_map_parses_informix_without_override() -> None:
    # Cross-source path: SchemaResolver(platform="informix") must resolve via
    # get_dialect_str → postgres without the connector's override_dialect.
    sql = (
        'create view "informix".customer_names (name) as '
        'select x0.name from "informix".customers x0'
    )
    result = sqlglot_lineage(
        sql,
        schema_resolver=_resolver(),
        default_db="testdb",
        default_schema="informix",
    )
    assert result.debug_info.table_error is None
    assert any("customers" in urn for urn in result.in_tables)


def test_view_lineage_warns_when_only_column_parsing_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Table-level lineage resolved, column-level did not. The coarse lineage must
    # still be emitted, but the column failure has to surface rather than looking
    # like a view that simply has no column lineage.
    sql = (
        'create view "informix".customer_names (name) as '
        'select x0.name from "informix".customers x0'
    )
    view_urn = make_dataset_urn("informix", "testdb.informix.customer_names", "PROD")
    report = InformixSourceReport()

    def _with_column_error(*args, **kwargs):
        result = sqlglot_lineage(*args, **kwargs)
        result.debug_info.column_error = SqlglotError("column resolution failed")
        return result

    monkeypatch.setattr(
        "datahub.ingestion.source.informix.lineage.sqlglot_lineage",
        _with_column_error,
    )

    up = build_view_upstream_lineage(
        view_urn=view_urn,
        view_sql=sql,
        schema_resolver=_resolver(),
        database="testdb",
        owner="informix",
        report=report,
    )

    assert up is not None
    assert up.upstreams
    assert report.view_column_lineage_failures == 1
    assert any("column lineage" in str(w.title).lower() for w in report.warnings)
