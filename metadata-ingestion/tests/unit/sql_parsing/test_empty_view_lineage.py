from typing import List
from unittest.mock import patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.incremental_lineage_helper import (
    IncrementalLineageConfigMixin,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit_processor import WorkunitProcessorContext
from datahub.ingestion.workunit_processors.auto_incremental_lineage import (
    AutoIncrementalLineageProcessor,
)
from datahub.metadata.schema_classes import ChangeTypeClass, UpstreamLineageClass
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)
from datahub.sql_parsing.sqlglot_lineage import SqlParsingDebugInfo, SqlParsingResult

VIEW = DatasetUrn("postgres", "db.public.report")
UPSTREAM = DatasetUrn("postgres", "db.public.source")


def _aggregator() -> SqlParsingAggregator:
    return SqlParsingAggregator(
        platform="postgres",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )


def _view(aggregator: SqlParsingAggregator, sql: str) -> None:
    aggregator.add_view_definition(
        view_urn=VIEW,
        view_definition=sql,
        default_db="db",
        default_schema="public",
    )


def _lineages(aggregator: SqlParsingAggregator) -> List[MetadataChangeProposalWrapper]:
    return [
        mcp
        for mcp in aggregator.gen_metadata()
        if mcp.entityUrn == str(VIEW) and isinstance(mcp.aspect, UpstreamLineageClass)
    ]


@pytest.mark.parametrize(
    "sql", ["select 1 as id", "create view report as select 1 as id"]
)
def test_empty_view_emits_full_lineage_replacement(sql: str) -> None:
    with _aggregator() as aggregator:
        _view(aggregator, sql)
        results = _lineages(aggregator)
        assert len(results) == 1
        mcp = results[0]
        assert mcp.changeType == ChangeTypeClass.UPSERT
        assert isinstance(mcp.aspect, UpstreamLineageClass)
        assert mcp.aspect.upstreams == []
        assert not mcp.aspect.fineGrainedLineages


def test_remove_last_dependency_then_restore_it() -> None:
    with _aggregator() as aggregator:
        aggregator._schema_resolver.add_raw_schema_info(str(UPSTREAM), {"id": "int"})
        for sql, expected in [
            ("select id from source", [str(UPSTREAM)]),
            ("select 1 as id", []),
            ("select 1 as id", []),
            ("select id from source", [str(UPSTREAM)]),
        ]:
            _view(aggregator, sql)
            results = _lineages(aggregator)
            assert len(results) == 1
            aspect = results[0].aspect
            assert isinstance(aspect, UpstreamLineageClass)
            assert [u.dataset for u in aspect.upstreams] == expected
            assert bool(aspect.fineGrainedLineages) == bool(expected)


def test_failed_parse_does_not_clear_lineage() -> None:
    with _aggregator() as aggregator:
        _view(aggregator, "unparseable definition")
        with patch.object(
            aggregator,
            "_run_sql_parser",
            return_value=SqlParsingResult.make_from_error(ValueError("invalid SQL")),
        ):
            assert _lineages(aggregator) == []


def test_self_reference_does_not_become_authoritative_empty_lineage() -> None:
    with _aggregator() as aggregator:
        _view(aggregator, "select * from report")
        assert _lineages(aggregator) == []


def test_query_log_constant_does_not_clear_lineage() -> None:
    with _aggregator() as aggregator:
        aggregator.add_observed_query(
            ObservedQuery(
                query="create table report as select 1 as id",
                default_db="db",
                default_schema="public",
            )
        )
        assert _lineages(aggregator) == []


def test_fallback_remains_authoritative_when_present() -> None:
    with _aggregator() as aggregator:
        aggregator.add_view_definition(
            view_urn=VIEW,
            view_definition="select 1 as id",
            default_db="db",
            default_schema="public",
            table_level_fallback_upstreams=[str(UPSTREAM)],
        )
        results = _lineages(aggregator)
        assert len(results) == 1
        assert isinstance(results[0].aspect, UpstreamLineageClass)
        assert [u.dataset for u in results[0].aspect.upstreams] == [str(UPSTREAM)]


@pytest.mark.parametrize("incremental", [False, True])
def test_empty_replacement_respects_incremental_lineage_setting(
    incremental: bool,
) -> None:
    with _aggregator() as aggregator:
        _view(aggregator, "select 1 as id")
        results = _lineages(aggregator)
        assert len(results) == 1
        stream = [mcp.as_workunit() for mcp in results]
        ctx = WorkunitProcessorContext(
            source_report=SourceReport(),
            pipeline_context=PipelineContext(run_id="empty-view"),
            source_config=IncrementalLineageConfigMixin(
                incremental_lineage=incremental
            ),
            platform="postgres",
        )
        if AutoIncrementalLineageProcessor.should_enable(ctx):
            stream = list(AutoIncrementalLineageProcessor.create(ctx).process(stream))
        if incremental:
            # Incremental mode deliberately does not remove existing edges.
            assert stream == []
        else:
            assert len(stream) == 1
            assert stream[0].metadata == results[0]


def test_new_parse_error_does_not_reuse_previous_empty_authority() -> None:
    with _aggregator() as aggregator:
        _view(aggregator, "select 1 as id")
        assert len(_lineages(aggregator)) == 1
        _view(aggregator, "invalid definition")
        with patch.object(
            aggregator,
            "_run_sql_parser",
            return_value=SqlParsingResult.make_from_error(ValueError("invalid SQL")),
        ):
            assert _lineages(aggregator) == []


def test_filtered_view_is_not_emitted() -> None:
    with _aggregator() as aggregator:
        _view(aggregator, "select 1 as id")
        with patch.object(aggregator, "is_allowed_table", return_value=False):
            assert _lineages(aggregator) == []


def test_empty_view_does_not_override_other_query_lineage() -> None:
    with _aggregator() as aggregator:
        aggregator.add_observed_query(
            ObservedQuery(
                query="create table report as select id from source",
                default_db="db",
                default_schema="public",
            )
        )
        _view(aggregator, "select 1 as id")
        results = _lineages(aggregator)
        assert len(results) == 1
        assert isinstance(results[0].aspect, UpstreamLineageClass)
        assert [u.dataset for u in results[0].aspect.upstreams] == [str(UPSTREAM)]


def test_column_parse_error_does_not_authorize_empty_view() -> None:
    with _aggregator() as aggregator:
        _view(aggregator, "select 1 as id")
        with patch.object(
            aggregator,
            "_run_sql_parser",
            return_value=SqlParsingResult(
                in_tables=[],
                out_tables=[],
                debug_info=SqlParsingDebugInfo(
                    column_error=ValueError("incomplete columns")
                ),
            ),
        ):
            assert _lineages(aggregator) == []
