"""BigqueryV2Source orchestration: construction wiring, get_workunits_internal flow
for the legacy and queries-v2 paths, and config deprecation warnings."""

from contextlib import contextmanager
from typing import Iterable, Iterator, List
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.bigquery_v2.bigquery import BigqueryV2Source
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import (
    BigQueryQueriesExtractorReport,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryProject
from datahub.ingestion.source.bigquery_v2.bigquery_sharing import (
    BigQuerySharingHandler,
)
from datahub.ingestion.source.bigquery_v2.lineage import BigqueryLineageExtractor
from datahub.ingestion.source.bigquery_v2.queries_extractor import (
    BigQueryQueriesExtractor,
)
from datahub.ingestion.source.bigquery_v2.usage import BigQueryUsageExtractor
from datahub.metadata.schema_classes import StatusClass
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from tests.unit.bigquery.conftest import PROJECT, as_mock, make_source, make_table

BIGQUERY_MODULE = "datahub.ingestion.source.bigquery_v2.bigquery"


def wu(name: str) -> MetadataWorkUnit:
    return MetadataChangeProposalWrapper(
        entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:bigquery,p.d.{name},PROD)",
        aspect=StatusClass(removed=False),
    ).as_workunit()


def graph_ctx() -> PipelineContext:
    ctx = PipelineContext(run_id="test", pipeline_name="bq_pipeline")
    graph = create_autospec(DataHubGraph, instance=True)
    # No prior checkpoint stored in DataHub.
    graph.get_latest_timeseries_value.return_value = None
    ctx.graph = graph
    return ctx


def names(wus: Iterable[MetadataWorkUnit]) -> List[str]:
    out: List[str] = []
    for w in wus:
        assert isinstance(w.metadata, MetadataChangeProposalWrapper)
        assert w.metadata.entityUrn
        out.append(w.metadata.entityUrn.rsplit(".", 1)[1].split(",")[0])
    return out


class TestConstruction:
    def test_create_builds_config_and_delegates_test_connection(self) -> None:
        with (
            patch.object(BigQueryV2Config, "get_bigquery_client"),
            patch.object(BigQueryV2Config, "get_projects_client"),
        ):
            source = BigqueryV2Source.create(
                {"project_id": PROJECT}, PipelineContext(run_id="test")
            )
        assert source.config.project_ids == [PROJECT]

        with patch(
            f"{BIGQUERY_MODULE}.BigQueryTestConnection.test_connection", autospec=True
        ) as tc:
            BigqueryV2Source.test_connection({"project_id": PROJECT})
        tc.assert_called_once_with({"project_id": PROJECT})

    def test_optional_collaborators_are_wired_from_config(self) -> None:
        with patch.object(BigQueryV2Config, "get_policy_tag_manager_client") as ptm:
            source = make_source(
                ctx=graph_ctx(),
                domain={"urn:li:domain:finance": {"allow": [".*"]}},
                extract_policy_tags_from_catalog=True,
                stateful_ingestion={"enabled": True},
                use_queries_v2=False,
                enable_stateful_lineage_ingestion=True,
                enable_stateful_usage_ingestion=True,
                enable_stateful_profiling=True,
                profiling={"enabled": True},
            )

        assert source.domain_registry is not None
        assert (
            source.bq_schema_extractor.schema_api.datacatalog_client is ptm.return_value
        )
        assert source.lineage_extractor.redundant_run_skip_handler is not None
        assert source.usage_extractor.redundant_run_skip_handler is not None
        assert source.profiling_state_handler is not None

    def test_schema_resolver_is_bulk_loaded_from_graph_when_schema_ingestion_is_partial(
        self,
    ) -> None:
        """When not every table's schema will be ingested this run, the SQL
        parser's resolver is pre-filled from DataHub so lineage still resolves
        columns of tables outside the run."""
        resolver = SchemaResolver(platform="bigquery")
        with patch(
            f"{BIGQUERY_MODULE}.provide_schema_resolver", return_value=resolver
        ) as provide:
            source = make_source(ctx=graph_ctx(), include_views=False)

        assert source.sql_parser_schema_resolver is resolver
        assert provide.call_args.kwargs["platform"] == "bigquery"

    def test_schema_resolver_bulk_load_failure_falls_back_with_warning(self) -> None:
        with patch(
            f"{BIGQUERY_MODULE}.provide_schema_resolver",
            side_effect=RuntimeError("503"),
        ):
            source = make_source(ctx=graph_ctx(), include_views=False)

        assert isinstance(source.sql_parser_schema_resolver, SchemaResolver)
        assert any(
            "Failed to bulk-load schemas" in w.message for w in source.report.warnings
        )

    @pytest.mark.xfail(
        strict=True,
        raises=AssertionError,
        reason="BigQueryIdentifierBuilder.__init__ writes the sharded-table suffix "
        "to the class attribute BigqueryTableIdentifier._BQ_SHARDED_TABLE_SUFFIX, "
        "so the most recently constructed source decides the naming of every "
        "source in the process.",
    )
    def test_sharded_table_naming_follows_each_sources_own_config(self) -> None:
        legacy = make_source(enable_legacy_sharded_table_support=True)
        make_source(enable_legacy_sharded_table_support=False)

        assert legacy.identifiers.gen_dataset_urn(PROJECT, "d", "events_20230101") == (
            f"urn:li:dataset:(urn:li:dataPlatform:bigquery,{PROJECT}.d.events,PROD)"
        )


class TestDeprecationWarnings:
    def test_legacy_schema_pattern_and_read_operational_stats_are_flagged(self) -> None:
        source = make_source(
            schema_pattern={"allow": ["only_this"]},
            match_fully_qualified_names=False,
            usage={"include_read_operational_stats": True},
        )

        source._warn_deprecated_configs()

        deprecations = [
            w
            for w in source.report.warnings
            if w.title == "Config option deprecation warning"
        ]
        assert any("schema_pattern" in w.message for w in deprecations)
        assert any("include_read_operational_stats" in w.message for w in deprecations)


class TestWorkunitFlow:
    @contextmanager
    def _wired(self, source: BigqueryV2Source) -> Iterator[MagicMock]:
        """Replace the extractors with autospecced fakes that each yield one
        named workunit; the schema generator stays real (the flow reads its
        discovered state) with only get_project_workunits stubbed."""
        lineage = create_autospec(BigqueryLineageExtractor, instance=True)
        lineage.aggregator = create_autospec(SqlParsingAggregator, instance=True)
        lineage.get_lineage_workunits_for_views_and_snapshots.return_value = [
            wu("view_lineage")
        ]
        lineage.get_lineage_workunits.return_value = [wu("lineage")]
        lineage.gen_lineage_workunits_for_external_table.return_value = [
            wu("gcs_lineage")
        ]
        source.lineage_extractor = lineage

        usage = create_autospec(BigQueryUsageExtractor, instance=True)
        usage.get_usage_workunits.return_value = [wu("usage")]
        source.usage_extractor = usage

        with patch.object(
            source.bq_schema_extractor,
            "get_project_workunits",
            autospec=True,
            return_value=[wu("schema")],
        ) as get_project_workunits:
            yield get_project_workunits

    def test_no_projects_yields_nothing(self) -> None:
        source = make_source()
        with (
            self._wired(source) as get_project_workunits,
            patch(f"{BIGQUERY_MODULE}.get_projects", return_value=[]),
        ):
            assert list(source.get_workunits_internal()) == []
        get_project_workunits.assert_not_called()

    def test_legacy_path_runs_usage_then_lineage_then_external_tables(self) -> None:
        source = make_source(use_queries_v2=False)
        source.bq_schema_extractor.external_tables["urn:ext"] = make_table(
            "ext", ddl="CREATE EXTERNAL TABLE ext"
        )
        with (
            self._wired(source),
            patch(
                f"{BIGQUERY_MODULE}.get_projects",
                return_value=[BigqueryProject(PROJECT, PROJECT)],
            ),
        ):
            emitted = names(source.get_workunits_internal())

        assert emitted == ["schema", "view_lineage", "usage", "lineage", "gcs_lineage"]
        as_mock(
            source.lineage_extractor.gen_lineage_workunits_for_external_table
        ).assert_called_once_with("urn:ext", "CREATE EXTERNAL TABLE ext", graph=None)

    def test_queries_v2_path_uses_queries_extractor_and_registers_shared_lineage(
        self,
    ) -> None:
        """With queries v2 the legacy usage and lineage extractors are bypassed,
        the extractor's report is attached to the source report and known
        linked-dataset lineage is registered with its aggregator."""
        source = make_source(use_queries_v2=True)
        source.sharing_handler = create_autospec(BigQuerySharingHandler, instance=True)
        extractor = create_autospec(BigQueryQueriesExtractor, instance=True)
        extractor.report = BigQueryQueriesExtractorReport()
        extractor.__enter__.return_value = extractor
        extractor.__exit__.return_value = False
        extractor.get_workunits_internal.return_value = [wu("queries")]
        with (
            self._wired(source),
            patch(
                f"{BIGQUERY_MODULE}.get_projects",
                return_value=[BigqueryProject(PROJECT, PROJECT)],
            ),
            patch(
                f"{BIGQUERY_MODULE}.BigQueryQueriesExtractor", return_value=extractor
            ),
            patch.object(BigQueryV2Config, "get_bigquery_client"),
        ):
            emitted = names(source.get_workunits_internal())

        assert emitted == ["schema", "view_lineage", "queries"]
        assert source.report.queries_extractor is extractor.report
        register = as_mock(source.sharing_handler.register_known_lineage)
        register.assert_called_once()
        assert source.lineage_extractor.aggregator in register.call_args.args
        as_mock(source.usage_extractor.get_usage_workunits).assert_not_called()
        as_mock(source.lineage_extractor.get_lineage_workunits).assert_not_called()

    def test_queries_v2_is_skipped_when_both_usage_and_lineage_are_off(self) -> None:
        source = make_source(
            use_queries_v2=True,
            include_usage_statistics=False,
            include_table_lineage=False,
        )
        with (
            self._wired(source),
            patch(
                f"{BIGQUERY_MODULE}.get_projects",
                return_value=[BigqueryProject(PROJECT, PROJECT)],
            ),
            patch(f"{BIGQUERY_MODULE}.BigQueryQueriesExtractor") as extractor_cls,
        ):
            emitted = names(source.get_workunits_internal())

        assert emitted == ["schema", "view_lineage"]
        extractor_cls.assert_not_called()
