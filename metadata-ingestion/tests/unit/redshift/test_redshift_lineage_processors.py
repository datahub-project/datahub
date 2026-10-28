"""Behavioural tests for the per-row lineage processors of RedshiftSqlLineage.

Each processor is fed a LineageRow (what the Redshift system tables return) and
the resulting lineage is read back through the real SqlParsingAggregator, so a
regression in how rows are turned into upstream/downstream edges is caught at
the emitted-aspect level rather than at the method-call level.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Union, cast
from unittest.mock import MagicMock

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.redshift.config import (
    LineageMode,
    RedshiftConfig,
    S3LineageProviderConfig,
)
from datahub.ingestion.source.redshift.lineage import (
    LineageCollectorType,
    LineageDataset,
    LineageDatasetPlatform,
    LineageItem,
    RedshiftSqlLineage,
)
from datahub.ingestion.source.redshift.query import RedshiftServerlessQuery
from datahub.ingestion.source.redshift.redshift_schema import (
    LineageRow,
    RedshiftDataDictionary,
    RedshiftSchema,
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    UpstreamLineageClass,
)

DB = "dev"
START = datetime(2024, 1, 1, tzinfo=timezone.utc)
END = datetime(2024, 1, 10, tzinfo=timezone.utc)


def _config(**overrides: object) -> RedshiftConfig:
    base: Dict[str, object] = dict(
        host_port="localhost:5439",
        database=DB,
        start_time=START,
        end_time=END,
    )
    base.update(overrides)
    return RedshiftConfig.model_validate(base)


def _extractor(
    config: Optional[RedshiftConfig] = None,
    known_tables: Optional[List[str]] = None,
    redundant_run_skip_handler: Optional[MagicMock] = None,
) -> RedshiftSqlLineage:
    config = config or _config()
    extractor = RedshiftSqlLineage(
        config,
        RedshiftReport(),
        PipelineContext(run_id="test"),
        config.database,
        redundant_run_skip_handler=redundant_run_skip_handler,
    )
    extractor.known_urns = {
        make_dataset_urn("redshift", f"{DB}.{name}") for name in (known_tables or [])
    }
    return extractor


def _row(**kwargs: object) -> LineageRow:
    base: Dict[str, object] = dict(
        source_schema=None,
        source_table=None,
        target_schema=None,
        target_table=None,
        ddl=None,
        filename=None,
        timestamp=datetime(2024, 1, 2, tzinfo=timezone.utc),
        session_id="42",
    )
    base.update(kwargs)
    return LineageRow(**base)  # type: ignore[arg-type]


def _upstream_edges(extractor: RedshiftSqlLineage) -> Dict[str, Set[str]]:
    """downstream urn -> set of upstream urns from everything the aggregator emits."""
    edges: Dict[str, Set[str]] = {}
    for wu in extractor.generate():
        aspect = getattr(wu.metadata, "aspect", None)
        if isinstance(aspect, UpstreamLineageClass):
            urn = str(getattr(wu.metadata, "entityUrn", ""))
            edges.setdefault(urn, set()).update(u.dataset for u in aspect.upstreams)
    return edges


def _urn(table: str) -> str:
    return make_dataset_urn("redshift", f"{DB}.{table}")


# --------------------------------------------------------------------------- #
# LineageItem / construction
# --------------------------------------------------------------------------- #


def test_lineage_item_maps_collector_type_to_dataset_lineage_type() -> None:
    dataset = LineageDataset(platform=LineageDatasetPlatform.REDSHIFT, urn=_urn("t"))

    def item(collector: LineageCollectorType) -> LineageItem:
        return LineageItem(
            dataset=dataset, upstreams=set(), cll=None, collector_type=collector
        )

    assert (
        item(LineageCollectorType.COPY).dataset_lineage_type
        == DatasetLineageTypeClass.COPY
    )
    assert (
        item(LineageCollectorType.VIEW).dataset_lineage_type
        == DatasetLineageTypeClass.VIEW
    )
    assert (
        item(LineageCollectorType.VIEW_DDL_SQL_PARSING).dataset_lineage_type
        == DatasetLineageTypeClass.VIEW
    )
    assert (
        item(LineageCollectorType.QUERY_SCAN).dataset_lineage_type
        == DatasetLineageTypeClass.TRANSFORMED
    )


def test_serverless_config_uses_serverless_queries() -> None:
    extractor = _extractor(_config(is_serverless=True))
    assert isinstance(extractor.queries, RedshiftServerlessQuery)


def test_time_window_comes_from_skip_handler_when_stateful() -> None:
    handler = MagicMock()
    suggested = (START, datetime(2024, 1, 5, tzinfo=timezone.utc))
    handler.suggest_run_time_window.return_value = suggested
    extractor = _extractor(redundant_run_skip_handler=handler)

    assert (extractor.start_time, extractor.end_time) == suggested
    assert extractor.report.stateful_lineage_ingestion_enabled

    extractor.report_status("extract-x", False)
    handler.report_current_run_status.assert_called_once_with("extract-x", False)


# --------------------------------------------------------------------------- #
# S3 path resolution
# --------------------------------------------------------------------------- #


def _s3_config(**s3: object) -> RedshiftConfig:
    return _config(s3_lineage_config=S3LineageProviderConfig(**s3))  # type: ignore[arg-type]


def test_get_s3_path_uses_matching_path_spec_table_path() -> None:
    extractor = _extractor(
        _s3_config(
            path_specs=[PathSpec(include="s3://bucket/{table}/*.csv")],
        )
    )
    assert extractor._get_s3_path("s3://bucket/orders/file.csv") == "s3://bucket/orders"


def test_get_s3_path_drops_non_matching_path_when_configured() -> None:
    extractor = _extractor(
        _s3_config(
            path_specs=[PathSpec(include="s3://bucket/{table}/*.csv")],
            ignore_non_path_spec_path=True,
        )
    )
    assert extractor._get_s3_path("s3://other/orders/file.parquet") is None
    assert extractor.report.num_lineage_dropped_s3_path == 1


def test_get_s3_path_keeps_full_path_when_strip_urls_disabled() -> None:
    extractor = _extractor(_s3_config(strip_urls=False))
    assert (
        extractor._get_s3_path("s3://bucket/orders/file.csv")
        == "s3://bucket/orders/file.csv"
    )


def test_get_sources_copy_rejects_non_s3_source() -> None:
    extractor = _extractor()
    sources, cll = extractor._get_sources(
        lineage_type=LineageCollectorType.COPY,
        db_name=DB,
        source_schema=None,
        source_table=None,
        ddl=None,
        filename="file:///tmp/data.csv",
    )
    assert sources == []
    assert cll is None
    assert extractor.report.num_lineage_dropped_not_support_copy_path == 1


def test_get_sources_copy_returns_nothing_for_ignored_path() -> None:
    extractor = _extractor(
        _s3_config(
            path_specs=[PathSpec(include="s3://bucket/{table}/*.csv")],
            ignore_non_path_spec_path=True,
        )
    )
    sources, _ = extractor._get_sources(
        lineage_type=LineageCollectorType.COPY,
        db_name=DB,
        source_schema=None,
        source_table=None,
        ddl=None,
        filename="s3://elsewhere/x.csv",
    )
    assert sources == []


def test_get_sources_copy_uses_platform_instance_map_for_s3() -> None:
    extractor = _extractor(_config(platform_instance_map={"s3": "lake"}))
    sources, _ = extractor._get_sources(
        lineage_type=LineageCollectorType.COPY,
        db_name=DB,
        source_schema=None,
        source_table=None,
        ddl=None,
        filename="s3://bucket/orders/file.csv",
    )
    assert (
        sources[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:s3,lake.bucket/orders,PROD)"
    )


def test_get_sources_without_schema_or_file_returns_empty() -> None:
    extractor = _extractor()
    sources, _ = extractor._get_sources(
        lineage_type=LineageCollectorType.QUERY_SCAN,
        db_name=DB,
        source_schema=None,
        source_table=None,
        ddl=None,
        filename=None,
    )
    assert sources == []


def test_get_sources_from_query_unparseable_ddl_is_counted_not_raised() -> None:
    extractor = _extractor()
    sources, cll = extractor._get_sources(
        lineage_type=LineageCollectorType.QUERY_SQL_PARSER,
        db_name=DB,
        source_schema=None,
        source_table=None,
        ddl="this is ))) not sql",
        filename=None,
    )
    assert sources == []
    assert cll is None


def test_get_sources_from_query_returns_none_cll_when_disabled() -> None:
    extractor = _extractor(_config(include_view_column_lineage=False))
    sources, cll = extractor._get_sources_from_query(
        db_name=DB, query="select a from public.src"
    )
    assert [s.urn for s in sources] == [_urn("public.src")]
    assert cll is None


# --------------------------------------------------------------------------- #
# Target resolution
# --------------------------------------------------------------------------- #


def test_get_target_lineage_drops_target_filtered_by_table_pattern() -> None:
    extractor = _extractor(_config(table_pattern={"deny": [f"{DB}.public.secret"]}))
    row = _row(target_schema="public", target_table="secret")
    assert (
        extractor._get_target_lineage(DB, row, LineageCollectorType.QUERY_SCAN, {})
        is None
    )


def test_get_target_lineage_keeps_renamed_table_despite_pattern() -> None:
    # A renamed table may not match table_pattern anymore but is still tracked
    # through all_tables_set; its lineage must not be dropped.
    extractor = _extractor(_config(table_pattern={"deny": [f"{DB}.public.secret"]}))
    row = _row(target_schema="public", target_table="secret")
    item = extractor._get_target_lineage(
        DB,
        row,
        LineageCollectorType.QUERY_SCAN,
        {DB: {"public": {"secret"}}},
    )
    assert item is not None
    assert item.dataset.urn == _urn("public.secret")


def test_get_target_lineage_unload_to_non_s3_is_warned_and_dropped() -> None:
    extractor = _extractor()
    row = _row(filename="hdfs://cluster/out/part-0")
    assert (
        extractor._get_target_lineage(DB, row, LineageCollectorType.UNLOAD, {}) is None
    )
    assert extractor.report.warnings


def test_get_target_lineage_unload_ignored_path_returns_none() -> None:
    extractor = _extractor(
        _s3_config(
            path_specs=[PathSpec(include="s3://bucket/{table}/*.csv")],
            ignore_non_path_spec_path=True,
        )
    )
    row = _row(filename="s3://elsewhere/out/part-0")
    assert (
        extractor._get_target_lineage(DB, row, LineageCollectorType.UNLOAD, {}) is None
    )


# --------------------------------------------------------------------------- #
# Row processors, verified through the emitted lineage
# --------------------------------------------------------------------------- #


def test_sql_parser_lineage_row_produces_edge_from_ddl() -> None:
    extractor = _extractor(known_tables=["public.src", "public.tgt"])
    extractor._process_sql_parser_lineage(
        _row(ddl="insert into public.tgt select * from public.src")
    )
    assert _upstream_edges(extractor) == {_urn("public.tgt"): {_urn("public.src")}}


def test_sql_parser_lineage_row_without_ddl_is_ignored() -> None:
    extractor = _extractor(known_tables=["public.tgt"])
    extractor._process_sql_parser_lineage(_row(ddl=None))
    assert _upstream_edges(extractor) == {}


def test_stl_scan_lineage_row_links_source_to_target() -> None:
    extractor = _extractor(known_tables=["public.src", "public.tgt"])
    extractor._process_stl_scan_lineage(
        _row(
            source_schema="public",
            source_table="src",
            target_schema="public",
            target_table="tgt",
            ddl="insert into public.tgt select * from public.src",
        )
    )
    assert _upstream_edges(extractor) == {_urn("public.tgt"): {_urn("public.src")}}


def test_stl_scan_lineage_skips_unknown_target_and_missing_ddl() -> None:
    extractor = _extractor(known_tables=["public.src", "public.tgt"])
    # Target is not a known (permanent) table -> treated as temp, skipped.
    extractor._process_stl_scan_lineage(
        _row(
            source_schema="public",
            source_table="src",
            target_schema="public",
            target_table="not_known",
            ddl="insert into public.not_known select * from public.src",
        )
    )
    # Known target but no query text -> nothing to attach the edge to.
    extractor._process_stl_scan_lineage(
        _row(
            source_schema="public",
            source_table="src",
            target_schema="public",
            target_table="tgt",
            ddl=None,
        )
    )
    assert _upstream_edges(extractor) == {}


def test_view_lineage_row_parses_view_definition() -> None:
    extractor = _extractor(known_tables=["public.src", "public.v"])
    extractor._process_view_lineage(
        _row(
            target_schema="public",
            target_table="v",
            ddl="create view public.v as select a from public.src",
        )
    )
    assert _upstream_edges(extractor) == {_urn("public.v"): {_urn("public.src")}}


def test_view_lineage_row_skips_missing_ddl_or_unknown_view() -> None:
    extractor = _extractor(known_tables=["public.src"])
    extractor._process_view_lineage(_row(target_schema="public", target_table="v"))
    extractor._process_view_lineage(
        _row(
            target_schema="public",
            target_table="v",
            ddl="create view public.v as select a from public.src",
        )
    )
    assert _upstream_edges(extractor) == {}


def test_copy_command_links_s3_path_to_table() -> None:
    extractor = _extractor(known_tables=["public.tgt"])
    extractor._process_copy_command(
        _row(
            target_schema="public",
            target_table="tgt",
            filename="s3://bucket/orders/part-0.csv",
        )
    )
    assert _upstream_edges(extractor) == {
        _urn("public.tgt"): {
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/orders,PROD)"
        }
    }


def test_copy_command_skips_non_s3_missing_target_and_unknown_target() -> None:
    extractor = _extractor(known_tables=["public.tgt"])
    extractor._process_copy_command(
        _row(target_schema="public", target_table="tgt", filename="file:///x.csv")
    )
    extractor._process_copy_command(
        _row(target_schema="public", target_table=None, filename="s3://b/x/f.csv")
    )
    extractor._process_copy_command(
        _row(target_schema="public", target_table="temp", filename="s3://b/x/f.csv")
    )
    assert _upstream_edges(extractor) == {}


def test_unload_command_links_table_to_s3_path() -> None:
    extractor = _extractor(known_tables=["public.src"])
    extractor._process_unload_command(
        _row(
            source_schema="public",
            source_table="src",
            filename="s3://bucket/exports/part-0",
        )
    )
    assert _upstream_edges(extractor) == {
        "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/exports,PROD)": {
            _urn("public.src")
        }
    }


def test_unload_command_skips_missing_source_or_unknown_source() -> None:
    extractor = _extractor(known_tables=["public.src"])
    extractor._process_unload_command(_row(filename="s3://bucket/exports/part-0"))
    extractor._process_unload_command(
        _row(
            source_schema="public",
            source_table="temp",
            filename="s3://bucket/exports/part-0",
        )
    )
    extractor._process_unload_command(
        _row(source_schema="public", source_table="src", filename="file:///out")
    )
    assert _upstream_edges(extractor) == {}


# --------------------------------------------------------------------------- #
# External (spectrum / datashare) tables
# --------------------------------------------------------------------------- #


def _external_table(name: str) -> RedshiftTable:
    return RedshiftTable(
        name=name, comment=None, created=None, type="EXTERNAL_TABLE", schema="ext"
    )


def test_external_tables_get_upstream_in_external_platform() -> None:
    extractor = _extractor(
        _config(platform_instance_map={"glue": "glue_prod"}),
        known_tables=["ext.events"],
    )
    all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]] = {
        DB: {
            "ext": [_external_table("events")],
            "missing": [_external_table("ignored")],
        }
    }
    db_schemas = {
        DB: {
            "ext": RedshiftSchema(
                name="ext",
                database=DB,
                type="external",
                external_platform="GLUE",
                external_database="lake",
            )
        }
    }
    extractor._process_external_tables(all_tables, db_schemas)
    assert _upstream_edges(extractor) == {
        _urn("ext.events"): {
            "urn:li:dataset:(urn:li:dataPlatform:glue,glue_prod.lake.events,PROD)"
        }
    }


def test_external_redshift_schema_points_at_upstream_cluster_schema() -> None:
    extractor = _extractor(known_tables=["ext.events"])
    all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]] = {
        DB: {"ext": [_external_table("events")]}
    }
    db_schemas = {
        DB: {
            "ext": RedshiftSchema(
                name="ext",
                database=DB,
                type="external",
                external_platform="redshift",
                external_database="producer_db",
                option='{"SCHEMA":"sales"}',
            )
        }
    }
    extractor._process_external_tables(all_tables, db_schemas)
    assert _upstream_edges(extractor) == {
        _urn("ext.events"): {make_dataset_urn("redshift", "producer_db.sales.events")}
    }


# --------------------------------------------------------------------------- #
# build() orchestration and generate()
# --------------------------------------------------------------------------- #


def test_build_runs_unified_feed_when_column_usage_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(
        email_domain="example.com",
        include_usage_statistics=True,
        include_column_usage_stats=True,
        table_lineage_mode=LineageMode.SQL_BASED,
        include_table_rename_lineage=False,
        include_views=False,
        include_copy_lineage=False,
        include_unload_lineage=False,
        resolve_temp_table_in_lineage=False,
        skip_external_tables=True,
    )
    extractor = _extractor(config)
    executed: List[str] = []

    def fake_get_query_result(conn, query, parameters=None):
        executed.append(query)
        cursor = MagicMock()
        cursor.description = [
            ("query_text",),
            ("starttime",),
            ("username",),
            ("session_id",),
        ]
        cursor.fetchmany.side_effect = [[], []]
        return cursor

    monkeypatch.setattr(
        RedshiftDataDictionary,
        "get_query_result",
        staticmethod(fake_get_query_result),
    )
    extractor.build(MagicMock(), {DB: {"public": []}}, {DB: {}})

    # The unified all-queries feed replaced the narrower insert/create feed.
    assert len(executed) == 1
    assert "ALL_QUERIES" in extractor.report.lineage_phases_timer


def test_generate_warns_when_queries_failed_to_parse() -> None:
    extractor = _extractor(known_tables=["public.tgt"])
    extractor._process_sql_parser_lineage(_row(ddl="insert into public.tgt ((( junk"))
    list(extractor.generate())
    assert any(
        w.title == "Failed to extract some SQL lineage"
        for w in extractor.report.warnings
    )


def test_process_table_renames_counts_unparseable_statements(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    extractor = _extractor(_config(default_schema=None))
    rows = [
        MagicMock(query_text="alter table foo rename to bar", start_time=START),
    ]
    monkeypatch.setattr(
        RedshiftDataDictionary,
        "get_alter_table_commands",
        staticmethod(lambda conn, query: iter(rows)),
    )
    renames, _ = extractor._process_table_renames(
        DB, cast(MagicMock, MagicMock()), {DB: {}}
    )
    assert renames == {}
    assert extractor.report.num_alter_table_parse_errors == 1
