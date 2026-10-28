"""BigqueryV2Source orchestration: construction wiring, get_workunits_internal flow
for the legacy and queries-v2 paths, and config deprecation warnings."""

from typing import Any, Iterable, List, Optional
from unittest.mock import MagicMock, patch

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery import BigqueryV2Source
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryProject,
    BigqueryTable,
)
from datahub.metadata.schema_classes import StatusClass
from datahub.sql_parsing.schema_resolver import SchemaResolver

PROJECT = "test-project"
BIGQUERY_MODULE = "datahub.ingestion.source.bigquery_v2.bigquery"


def wu(name: str) -> MetadataWorkUnit:
    return MetadataChangeProposalWrapper(
        entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:bigquery,p.d.{name},PROD)",
        aspect=StatusClass(removed=False),
    ).as_workunit()


def make_source(
    ctx: Optional[PipelineContext] = None, **config_overrides: Any
) -> BigqueryV2Source:
    with (
        patch.object(BigQueryV2Config, "get_bigquery_client"),
        patch.object(BigQueryV2Config, "get_projects_client"),
    ):
        config = BigQueryV2Config.model_validate(
            {"project_id": PROJECT, **config_overrides}
        )
        return BigqueryV2Source(
            config=config, ctx=ctx or PipelineContext(run_id="test")
        )


def graph_ctx() -> PipelineContext:
    ctx = PipelineContext(run_id="test", pipeline_name="bq_pipeline")
    ctx.graph = MagicMock()
    # No prior checkpoint stored in DataHub.
    ctx.graph.get_latest_timeseries_value.return_value = None
    return ctx


def as_mock(obj: object) -> MagicMock:
    assert isinstance(obj, MagicMock)
    return obj


def names(wus: Iterable[MetadataWorkUnit]) -> List[str]:
    return [w.metadata.entityUrn.rsplit(".", 1)[1].split(",")[0] for w in wus]  # type: ignore[union-attr]


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

        with patch(f"{BIGQUERY_MODULE}.BigQueryTestConnection.test_connection") as tc:
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
        assert len(source.report.warnings) == 1


class TestDeprecationWarnings:
    def test_legacy_schema_pattern_and_read_operational_stats_are_flagged(self) -> None:
        source = make_source(
            schema_pattern={"allow": ["only_this"]},
            match_fully_qualified_names=False,
            usage={"include_read_operational_stats": True},
        )

        source._warn_deprecated_configs()

        titles = {w.title for w in source.report.warnings}
        assert "Config option deprecation warning" in titles
        assert len(source.report.warnings) >= 2


class TestWorkunitFlow:
    def _wire(self, source: BigqueryV2Source) -> None:
        source.bq_schema_extractor.get_project_workunits = MagicMock(  # type: ignore[method-assign]
            return_value=[wu("schema")]
        )
        source.lineage_extractor = MagicMock()
        source.lineage_extractor.get_lineage_workunits_for_views_and_snapshots.return_value = [
            wu("view_lineage")
        ]
        source.lineage_extractor.get_lineage_workunits.return_value = [wu("lineage")]
        source.lineage_extractor.gen_lineage_workunits_for_external_table.return_value = [
            wu("gcs_lineage")
        ]
        source.usage_extractor = MagicMock()
        source.usage_extractor.get_usage_workunits.return_value = [wu("usage")]

    def test_no_projects_yields_nothing(self) -> None:
        source = make_source()
        self._wire(source)
        with patch(f"{BIGQUERY_MODULE}.get_projects", return_value=[]):
            assert list(source.get_workunits_internal()) == []
        source.bq_schema_extractor.get_project_workunits.assert_not_called()  # type: ignore[attr-defined]

    def test_legacy_path_runs_usage_then_lineage_then_external_tables(self) -> None:
        source = make_source(use_queries_v2=False)
        self._wire(source)
        source.bq_schema_extractor.external_tables["urn:ext"] = BigqueryTable(
            name="ext",
            comment=None,
            created=None,
            last_altered=None,
            size_in_bytes=None,
            rows_count=None,
            ddl="CREATE EXTERNAL TABLE ext",
        )
        with patch(
            f"{BIGQUERY_MODULE}.get_projects",
            return_value=[BigqueryProject(PROJECT, PROJECT)],
        ):
            emitted = names(source.get_workunits_internal())

        assert emitted == ["schema", "view_lineage", "usage", "lineage", "gcs_lineage"]
        as_mock(
            source.lineage_extractor
        ).gen_lineage_workunits_for_external_table.assert_called_once_with(
            "urn:ext", "CREATE EXTERNAL TABLE ext", graph=None
        )

    def test_queries_v2_path_uses_queries_extractor_and_registers_shared_lineage(
        self,
    ) -> None:
        source = make_source(use_queries_v2=True)
        self._wire(source)
        source.sharing_handler = MagicMock()
        extractor = MagicMock()
        extractor.__enter__.return_value = extractor
        extractor.__exit__.return_value = False
        extractor.get_workunits_internal.return_value = [wu("queries")]
        with (
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
        source.sharing_handler.register_known_lineage.assert_called_once()
        as_mock(source.usage_extractor).get_usage_workunits.assert_not_called()
        as_mock(source.lineage_extractor).get_lineage_workunits.assert_not_called()

    def test_queries_v2_is_skipped_when_both_usage_and_lineage_are_off(self) -> None:
        source = make_source(
            use_queries_v2=True,
            include_usage_statistics=False,
            include_table_lineage=False,
        )
        self._wire(source)
        with (
            patch(
                f"{BIGQUERY_MODULE}.get_projects",
                return_value=[BigqueryProject(PROJECT, PROJECT)],
            ),
            patch(f"{BIGQUERY_MODULE}.BigQueryQueriesExtractor") as extractor_cls,
        ):
            emitted = names(source.get_workunits_internal())

        assert emitted == ["schema", "view_lineage"]
        extractor_cls.assert_not_called()
