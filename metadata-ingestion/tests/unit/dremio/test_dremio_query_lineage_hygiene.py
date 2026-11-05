"""Query lineage from Dremio job history must not override the catalog walk:
the catalog's current view definition is authoritative for a view, and a
table that is in scope but no longer in the catalog (dropped, temporary)
must not come back as a live entity."""

from typing import List, Optional
from unittest.mock import Mock

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.dremio.dremio_api import DremioEdition
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_entities import (
    DremioCatalog,
    DremioDataset,
    DremioDatasetType,
    DremioQuery,
)
from datahub.ingestion.source.dremio.dremio_source import DremioSource
from datahub.metadata.schema_classes import UpstreamLineageClass


def _urn(name: str, platform_instance: Optional[str] = None) -> str:
    return make_dataset_urn_with_platform_instance(
        "dremio", f"dremio.{name}", platform_instance
    )


@pytest.fixture(params=[None, "my_instance"])
def source(monkeypatch, request):
    mock_session = Mock()
    monkeypatch.setattr("requests.Session", Mock(return_value=mock_session))
    mock_session.post.return_value.json.return_value = {"token": "dummy-token"}
    mock_session.post.return_value.status_code = 200

    config = DremioSourceConfig(
        hostname="dummy-host",
        port=9047,
        tls=False,
        include_query_lineage=True,
        platform_instance=request.param,
        schema_pattern=AllowDenyPattern(
            allow=["^mysys($|\\..*)", "^my_s3($|\\..*)", "^src_db($|\\..*)"]
        ),
    )
    ctx = Mock()
    ctx.run_id = "test-run-id"
    ctx.graph = None

    src = DremioSource(config, ctx)
    src.dremio_catalog = Mock(spec=DremioCatalog)
    # What the catalog walk found: orders_blue has been dropped since the
    # jobs below ran, so it is absent.
    src.catalog_dataset_names = {
        "mysys.vdata.orders",
        "my_s3.materialize.orders_green",
        "src_db.sales.orders",
        "mysys.vdata.no_definition",
    }
    src._views_with_definition = {"mysys.vdata.orders"}
    return src


def _instance(source: DremioSource) -> Optional[str]:
    return source.config.platform_instance


def _run_query(source: DremioSource, sql: str, ts: str) -> None:
    source.process_query(
        DremioQuery(
            job_id="job",
            username="dbt_user",
            submitted_ts=ts,
            query=sql,
            queried_datasets="[]",
        )
    )


def _mcps(source: DremioSource) -> List[MetadataChangeProposalWrapper]:
    return [
        mcp
        for mcp in source.sql_parsing_aggregator.gen_metadata()
        if isinstance(mcp, MetadataChangeProposalWrapper)
    ]


def _upstreams(mcps: List[MetadataChangeProposalWrapper], urn: str) -> List[str]:
    return [
        u.dataset
        for mcp in mcps
        if mcp.entityUrn == urn and isinstance(mcp.aspect, UpstreamLineageClass)
        for u in mcp.aspect.upstreams
    ]


@pytest.mark.parametrize(
    "view_ddl",
    [
        "CREATE OR REPLACE VIEW MySys.vdata.Orders AS "
        "SELECT * FROM my_s3.materialize.orders_blue",
        "create view MySys.vdata.Orders as select * from my_s3.materialize.orders_blue",
        # dbt prefixes its statements with a comment header.
        '/* {"app": "dbt", "node_id": "model.orders"} */\n'
        "-- deploy\n  create or replace view MySys.vdata.Orders as "
        "select * from my_s3.materialize.orders_blue",
        # A column list wraps the view's name in a Schema.
        "CREATE OR REPLACE VIEW MySys.vdata.Orders (id) AS "
        "SELECT id FROM my_s3.materialize.orders_blue",
    ],
)
def test_catalog_view_definition_wins_over_observed_view_ddl(source, view_ddl):
    view = _urn("mysys.vdata.orders", _instance(source))
    source.sql_parsing_aggregator.add_view_definition(
        view_urn=view,
        view_definition="SELECT * FROM my_s3.materialize.orders_green",
        default_db="dremio",
    )
    _run_query(source, view_ddl, "2024-01-01 12:00:00.000")

    assert _upstreams(_mcps(source), view) == [
        _urn("my_s3.materialize.orders_green", _instance(source))
    ]
    assert source.report.num_view_ddl_queries_skipped == 1


def test_view_without_catalog_definition_keeps_observed_ddl(source):
    # No registered definition (e.g. its fetch failed): job history is the
    # only lineage this view has.
    _run_query(
        source,
        "CREATE VIEW mysys.vdata.no_definition AS "
        "SELECT * FROM my_s3.materialize.orders_green",
        "2024-01-01 12:00:00.000",
    )

    assert _upstreams(
        _mcps(source), _urn("mysys.vdata.no_definition", _instance(source))
    ) == [_urn("my_s3.materialize.orders_green", _instance(source))]
    assert source.report.num_view_ddl_queries_skipped == 0


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE my_s3.materialize.orders_blue AS SELECT * FROM SRC_DB.sales.orders",
        "INSERT INTO my_s3.materialize.orders_blue SELECT * FROM SRC_DB.sales.orders",
    ],
)
def test_dropped_in_scope_table_is_not_resurrected(source, sql):
    _run_query(source, sql, "2024-01-01 12:00:00.000")

    mcps = _mcps(source)

    blue = _urn("my_s3.materialize.orders_blue", _instance(source))
    assert [m.aspectName for m in mcps if m.entityUrn == blue] == []
    # Names, once each, however often the aggregator asks.
    assert list(source.report.lineage_dropped_not_in_catalog) == [
        "my_s3.materialize.orders_blue"
    ]


def test_dropped_table_stays_an_upstream_of_a_live_table(source):
    _run_query(
        source,
        "INSERT INTO my_s3.materialize.orders_green "
        "SELECT * FROM my_s3.materialize.orders_blue",
        "2024-01-01 12:00:00.000",
    )

    mcps = _mcps(source)
    blue = _urn("my_s3.materialize.orders_blue", _instance(source))
    green = _urn("my_s3.materialize.orders_green", _instance(source))
    assert _upstreams(mcps, green) == [blue]
    # But it gets no usage or operations of its own.
    assert [m.aspectName for m in mcps if m.entityUrn == blue] == []


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE my_s3.materialize.orders_green AS SELECT * FROM SRC_DB.sales.orders",
        "INSERT INTO my_s3.materialize.orders_green SELECT * FROM SRC_DB.sales.orders",
    ],
)
def test_catalog_table_keeps_query_lineage(source, sql):
    _run_query(source, sql, "2024-01-01 12:00:00.000")

    assert _upstreams(
        _mcps(source), _urn("my_s3.materialize.orders_green", _instance(source))
    ) == [_urn("src_db.sales.orders", _instance(source))]
    assert list(source.report.lineage_dropped_not_in_catalog) == []


def test_is_allowed_table_normalises_case_and_prefix(source):
    assert source._is_allowed_table("dremio.MY_S3.Materialize.Orders_Green")
    assert source._is_allowed_table("my_s3.materialize.orders_green")


def test_out_of_scope_name_still_counted_as_filtered(source):
    assert not source._is_allowed_table("dremio.other_source.sales.orders")
    assert source.report.lineage_dropped_filtered == 1
    assert list(source.report.lineage_dropped_not_in_catalog) == []


@pytest.mark.parametrize("cause", ["fetch_failed", "empty_catalog"])
def test_incomplete_dataset_fetch_falls_back_to_filters(source, cause):
    # A missing name may just be one the failed fetch never returned; dropping
    # its lineage would lose it for good once the query time window advances.
    if cause == "fetch_failed":
        source.report.dataset_fetch_incomplete = True
    else:
        source.catalog_dataset_names = set()

    _run_query(
        source,
        "CREATE TABLE my_s3.materialize.orders_blue AS SELECT * FROM SRC_DB.sales.orders",
        "2024-01-01 12:00:00.000",
    )

    assert _upstreams(
        _mcps(source), _urn("my_s3.materialize.orders_blue", _instance(source))
    ) == [_urn("src_db.sales.orders", _instance(source))]
    assert not source._is_allowed_table("dremio.other_source.sales.orders")


def test_walk_with_only_column_less_datasets_is_complete(source):
    # The walk returned rows, so an empty catalog here is not a silent failure.
    source.catalog_dataset_names = set()
    source.dremio_api.datasets_without_columns.add("my_s3.materialize.no_columns")

    assert source._is_allowed_table("dremio.my_s3.materialize.no_columns")
    assert not source._is_allowed_table("dremio.my_s3.materialize.orders_blue")
    assert list(source.report.lineage_dropped_not_in_catalog) == [
        "my_s3.materialize.orders_blue"
    ]


def _warning_titles(source: DremioSource) -> List[str]:
    return [w.title for w in source.report.warnings if w.title]


@pytest.mark.parametrize("fetch_incomplete", [False, True])
def test_system_tables_are_filtered_not_reported_as_dropped(source, fetch_incomplete):
    # The connector's own metadata queries are in job history; the catalog
    # query never returns system tables, whatever include_system_tables says.
    # The default allow-all schema_pattern would otherwise let them through.
    source.config.schema_pattern = AllowDenyPattern.allow_all()
    source.report.dataset_fetch_incomplete = fetch_incomplete
    for name in ["information_schema.tables", "sys.jobs_recent", "SYS.JOBS"]:
        assert not source._is_allowed_table(f"dremio.{name}")
    assert source.report.lineage_dropped_filtered == 3
    assert list(source.report.lineage_dropped_not_in_catalog) == []


def test_double_prefixed_name_is_reported_apart_from_dropped_tables(source):
    # A real space named dremio is in the catalog and keeps its lineage.
    source.catalog_dataset_names.add("dremio.sales.orders")
    assert source._is_allowed_table("dremio.dremio.sales.orders")
    # A two-part reference reaches the callback as dremio.dremio.<path>.
    assert not source._is_allowed_table("dremio.dremio.src_db.sales.orders")
    assert not source._is_allowed_table("dremio.dremio.src_db.sales.orders")
    assert list(source.report.lineage_dropped_unresolved_names) == [
        "dremio.src_db.sales.orders"
    ]
    assert list(source.report.lineage_dropped_not_in_catalog) == []


def test_dataset_without_columns_keeps_query_lineage(source):
    # Returned by the fetch with no column rows (e.g. its source is offline):
    # it exists, so it isn't treated as dropped.
    source.dremio_api.datasets_without_columns.add("my_s3.materialize.no_columns")
    assert source._is_allowed_table("dremio.my_s3.materialize.no_columns")
    # The filters still apply to it.
    source.config.dataset_pattern = AllowDenyPattern(deny=[".*\\.no_columns$"])
    assert not source._is_allowed_table("dremio.my_s3.materialize.no_columns")
    assert list(source.report.lineage_dropped_not_in_catalog) == []


@pytest.mark.parametrize(
    "sql",
    [
        # Only a statement that is itself a CREATE VIEW is skipped, not one
        # that mentions it in a comment or a string.
        "/* hdr */ SELECT '*/ create view x' AS note FROM src_db.sales.orders",
        # Unparsable text is left to the aggregator, which reports it.
        "CREATE VIEW (",
    ],
)
def test_non_view_ddl_is_not_skipped(source, sql):
    _run_query(source, sql, "2024-01-01 12:00:00.000")

    assert source.report.num_view_ddl_queries_skipped == 0
    assert source.sql_parsing_aggregator.report.num_observed_queries == 1


def test_parse_errors_other_than_parse_error_reach_the_aggregator(source, monkeypatch):
    # parse_statement can raise TypeError or RecursionError too; deciding
    # whether to skip must never fail the query.
    monkeypatch.setattr(
        "datahub.ingestion.source.dremio.dremio_source.parse_statement",
        Mock(side_effect=RecursionError),
    )
    _run_query(
        source,
        "CREATE VIEW mysys.vdata.orders AS SELECT 1",
        "2024-01-01 12:00:00.000",
    )

    assert source.report.num_view_ddl_queries_skipped == 0
    assert source.sql_parsing_aggregator.report.num_observed_queries == 1


def test_statements_without_view_are_not_parsed(source, monkeypatch):
    parse = Mock()
    monkeypatch.setattr(
        "datahub.ingestion.source.dremio.dremio_source.parse_statement", parse
    )
    _run_query(source, "SELECT * FROM src_db.sales.orders", "2024-01-01 12:00:00.000")

    parse.assert_not_called()


def _dataset(
    path: List[str],
    name: str,
    dataset_type: DremioDatasetType,
    sql_definition: Optional[str] = None,
) -> DremioDataset:
    dataset = Mock(spec=DremioDataset)
    dataset.path = path
    dataset.resource_name = name
    dataset.dataset_type = dataset_type
    dataset.sql_definition = sql_definition
    dataset.default_schema = None
    dataset.parents = None
    dataset.glossary_terms = []
    return dataset


def _query(sql: str, ts: str) -> DremioQuery:
    return DremioQuery(
        job_id="job",
        username="dbt_user",
        submitted_ts=ts,
        query=sql,
        queried_datasets="[]",
    )


def _run_ingestion(
    source: DremioSource, datasets: List[DremioDataset], queries: List[DremioQuery]
) -> List[MetadataChangeProposalWrapper]:
    source.catalog_dataset_names = set()
    source._views_with_definition = set()
    catalog = Mock(spec=DremioCatalog)
    catalog.edition = DremioEdition.COMMUNITY
    catalog.get_sources.return_value = []
    catalog.get_containers.return_value = []
    catalog.get_datasets.return_value = iter(datasets)
    catalog.get_queries.return_value = queries
    source.dremio_catalog = catalog
    aspects = Mock()
    aspects.populate_dataset_mcp.return_value = []
    source.dremio_aspects = aspects
    return [
        wu.metadata
        for wu in source.get_workunits_internal()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
    ]


def test_end_to_end_blue_green_deploy(source):
    # The catalog walk sees the view (now on green) and green; blue was
    # dropped, but job history still has its CTAS and the view's old DDL.
    mcps = _run_ingestion(
        source,
        datasets=[
            _dataset(
                ["MySys", "vdata"],
                "Orders",
                DremioDatasetType.VIEW,
                "SELECT * FROM my_s3.materialize.orders_green",
            ),
            _dataset(["my_s3", "materialize"], "orders_green", DremioDatasetType.TABLE),
            _dataset(["src_db", "sales"], "orders", DremioDatasetType.TABLE),
        ],
        queries=[
            _query(
                "CREATE TABLE my_s3.materialize.orders_blue AS "
                "SELECT * FROM src_db.sales.orders",
                "2024-01-01 12:00:00.000",
            ),
            _query(
                "CREATE OR REPLACE VIEW MySys.vdata.Orders AS "
                "SELECT * FROM my_s3.materialize.orders_blue",
                "2024-01-01 12:05:00.000",
            ),
        ],
    )

    instance = _instance(source)
    view = _urn("mysys.vdata.orders", instance)
    blue = _urn("my_s3.materialize.orders_blue", instance)
    assert _upstreams(mcps, view) == [_urn("my_s3.materialize.orders_green", instance)]
    assert [m.aspectName for m in mcps if m.entityUrn == blue] == []
    assert source.report.num_view_ddl_queries_skipped == 1
    assert list(source.report.lineage_dropped_not_in_catalog) == [
        "my_s3.materialize.orders_blue"
    ]
    assert _warning_titles(source) == [
        "Query lineage dropped for tables not in the catalog"
    ]


def test_end_to_end_incomplete_fetch_warns(source):
    source.report.dataset_fetch_incomplete = True
    mcps = _run_ingestion(
        source,
        datasets=[_dataset(["src_db", "sales"], "orders", DremioDatasetType.TABLE)],
        queries=[
            _query(
                "CREATE TABLE my_s3.materialize.orders_blue AS "
                "SELECT * FROM src_db.sales.orders",
                "2024-01-01 12:00:00.000",
            )
        ],
    )

    assert _upstreams(
        mcps, _urn("my_s3.materialize.orders_blue", _instance(source))
    ) == [_urn("src_db.sales.orders", _instance(source))]
    assert _warning_titles(source) == ["Query lineage not limited to the catalog"]


def test_no_query_lineage_scope_warning_without_query_lineage(source):
    source.config.include_query_lineage = False
    # State that would warn if the helper ran.
    source._names_not_in_catalog.add("my_s3.materialize.orders_blue")
    _run_ingestion(source, datasets=[], queries=[])

    assert _warning_titles(source) == []


def test_mixed_case_platform_instance_is_stripped(source):
    # The schema resolver lowercases the instance; the aggregator's strip is
    # case-sensitive, so the callback can receive it still prefixed.
    source.config.platform_instance = "MyInst"
    source.dremio_api.datasets_without_columns.add("my_s3.materialize.no_columns")

    assert source._is_allowed_table("myinst.dremio.my_s3.materialize.no_columns")
    assert source._is_allowed_table("myinst.dremio.my_s3.materialize.orders_green")
    assert not source._is_allowed_table("myinst.dremio.dremio.src_db.sales.orders")
    assert list(source.report.lineage_dropped_unresolved_names) == [
        "dremio.src_db.sales.orders"
    ]
    assert list(source.report.lineage_dropped_not_in_catalog) == []


def test_no_fallback_warning_when_nothing_took_the_fallback(source):
    # Patterns that exclude everything also leave the catalog empty.
    _run_ingestion(source, datasets=[], queries=[])

    assert _warning_titles(source) == []
