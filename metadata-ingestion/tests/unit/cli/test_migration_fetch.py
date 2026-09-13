"""Tests for pipeline_name filtering in migration fetch and CLI layers."""

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from datahub.cli.migrate import (
    _filter_by_pipeline_name,
    dataplatform2instance,
    instance2instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    SystemMetadataClass,
)
from datahub.migration.fetch import (
    _matches_pipeline_name,
    fetch_instance_urns,
    fetch_platform_urns,
)

DATASET_URN_A = "urn:li:dataset:(urn:li:dataPlatform:dbt,project_a.model,PROD)"
DATASET_URN_B = "urn:li:dataset:(urn:li:dataPlatform:dbt,project_b.model,PROD)"


def _make_mcpw(
    urn: str, pipeline_name: str = "my-pipeline"
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=DatasetPropertiesClass(description="test"),
        systemMetadata=SystemMetadataClass(pipelineName=pipeline_name),
    )


def _make_mcpw_no_pipeline(urn: str) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=DatasetPropertiesClass(description="test"),
        systemMetadata=SystemMetadataClass(),
    )


class TestMatchesPipelineName:
    def test_matches_when_pipeline_present(self) -> None:
        graph = MagicMock()
        graph.get_entity_as_mcps.return_value = [
            _make_mcpw(DATASET_URN_A, "my-dbt-pipeline")
        ]
        assert _matches_pipeline_name(graph, DATASET_URN_A, "my-dbt-pipeline") is True

    def test_no_match_when_different_pipeline(self) -> None:
        graph = MagicMock()
        graph.get_entity_as_mcps.return_value = [
            _make_mcpw(DATASET_URN_A, "other-pipeline")
        ]
        assert _matches_pipeline_name(graph, DATASET_URN_A, "my-dbt-pipeline") is False

    def test_no_match_when_no_system_metadata(self) -> None:
        graph = MagicMock()
        mcpw = MetadataChangeProposalWrapper(
            entityUrn=DATASET_URN_A,
            aspect=DatasetPropertiesClass(description="test"),
        )
        graph.get_entity_as_mcps.return_value = [mcpw]
        assert _matches_pipeline_name(graph, DATASET_URN_A, "my-dbt-pipeline") is False

    def test_no_match_when_pipeline_name_is_none(self) -> None:
        graph = MagicMock()
        graph.get_entity_as_mcps.return_value = [_make_mcpw_no_pipeline(DATASET_URN_A)]
        assert _matches_pipeline_name(graph, DATASET_URN_A, "my-dbt-pipeline") is False

    def test_matches_if_any_aspect_has_pipeline(self) -> None:
        graph = MagicMock()
        graph.get_entity_as_mcps.return_value = [
            _make_mcpw_no_pipeline(DATASET_URN_A),
            _make_mcpw(DATASET_URN_A, "my-dbt-pipeline"),
        ]
        assert _matches_pipeline_name(graph, DATASET_URN_A, "my-dbt-pipeline") is True


class TestFetchPlatformUrns:
    def test_returns_all_without_instance(self) -> None:
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = [DATASET_URN_A, DATASET_URN_B]
        graph.get_aspect.return_value = None

        result = fetch_platform_urns(
            graph, platform="dbt", env="PROD", entity_type="dataset"
        )
        assert result == [DATASET_URN_A, DATASET_URN_B]

    def test_skips_entities_with_instance(self) -> None:
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = [DATASET_URN_A]
        graph.get_aspect.return_value = DataPlatformInstanceClass(
            platform="urn:li:dataPlatform:dbt",
            instance="urn:li:dataPlatformInstance:(urn:li:dataPlatform:dbt,existing)",
        )

        result = fetch_platform_urns(
            graph, platform="dbt", env="PROD", entity_type="dataset"
        )
        assert result == []


class TestFetchInstanceUrns:
    def test_returns_all(self) -> None:
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = [DATASET_URN_A, DATASET_URN_B]

        result = fetch_instance_urns(
            graph,
            platform="dbt",
            old_instance="old",
            env="PROD",
            entity_type="dataset",
        )
        assert result == [DATASET_URN_A, DATASET_URN_B]


class TestFilterByPipelineName:
    def test_filters_matching_urns(self) -> None:
        graph = MagicMock()

        def mock_get_entity_as_mcps(urn: str) -> list:
            if urn == DATASET_URN_A:
                return [_make_mcpw(urn, "target-pipeline")]
            return [_make_mcpw(urn, "other-pipeline")]

        graph.get_entity_as_mcps.side_effect = mock_get_entity_as_mcps

        result = _filter_by_pipeline_name(
            graph, [DATASET_URN_A, DATASET_URN_B], "target-pipeline"
        )
        assert result == [DATASET_URN_A]

    def test_returns_empty_when_no_match(self) -> None:
        graph = MagicMock()
        graph.get_entity_as_mcps.return_value = [
            _make_mcpw(DATASET_URN_A, "other-pipeline")
        ]

        result = _filter_by_pipeline_name(
            graph, [DATASET_URN_A, DATASET_URN_B], "target-pipeline"
        )
        assert result == []

    def test_returns_all_when_all_match(self) -> None:
        graph = MagicMock()
        graph.get_entity_as_mcps.return_value = [
            _make_mcpw(DATASET_URN_A, "target-pipeline")
        ]

        result = _filter_by_pipeline_name(
            graph, [DATASET_URN_A, DATASET_URN_B], "target-pipeline"
        )
        assert result == [DATASET_URN_A, DATASET_URN_B]

    def test_empty_input_returns_empty(self) -> None:
        graph = MagicMock()

        result = _filter_by_pipeline_name(graph, [], "target-pipeline")
        assert result == []
        graph.get_entity_as_mcps.assert_not_called()


class TestDataplatform2instancePipelineNameCli:
    """CLI-level tests proving --pipeline-name excludes non-matching entities."""

    @patch("datahub.cli.migrate._migrate_containers")
    @patch("datahub.cli.migrate._run_entity_migration")
    @patch("datahub.cli.migrate._filter_by_pipeline_name")
    @patch("datahub.cli.migrate.fetch_platform_urns")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_pipeline_name_filters_before_migration(
        self,
        mock_graph: MagicMock,
        mock_fetch: MagicMock,
        mock_filter: MagicMock,
        mock_run: MagicMock,
        mock_containers: MagicMock,
    ) -> None:
        mock_graph.return_value = MagicMock()
        mock_fetch.return_value = [DATASET_URN_A, DATASET_URN_B]
        mock_filter.return_value = [DATASET_URN_A]
        mock_run.return_value = MagicMock(__str__=lambda _: "report")

        runner = CliRunner()
        result = runner.invoke(
            dataplatform2instance,
            [
                "--platform",
                "dbt",
                "--instance",
                "my_inst",
                "--pipeline-name",
                "my-pipeline",
                "--dry-run",
                "--force",
                "--entity-types",
                "dataset",
            ],
        )
        assert result.exit_code == 0
        mock_filter.assert_called_once()
        # Only the filtered URN should reach migration
        args = mock_run.call_args
        assert args.kwargs["urns"] == [DATASET_URN_A]

    @patch("datahub.cli.migrate._migrate_containers")
    @patch("datahub.cli.migrate._run_entity_migration")
    @patch("datahub.cli.migrate._filter_by_pipeline_name")
    @patch("datahub.cli.migrate.fetch_platform_urns")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_no_pipeline_name_skips_filter(
        self,
        mock_graph: MagicMock,
        mock_fetch: MagicMock,
        mock_filter: MagicMock,
        mock_run: MagicMock,
        mock_containers: MagicMock,
    ) -> None:
        mock_graph.return_value = MagicMock()
        mock_fetch.return_value = [DATASET_URN_A, DATASET_URN_B]
        mock_run.return_value = MagicMock(__str__=lambda _: "report")

        runner = CliRunner()
        result = runner.invoke(
            dataplatform2instance,
            [
                "--platform",
                "dbt",
                "--instance",
                "my_inst",
                "--dry-run",
                "--force",
                "--entity-types",
                "dataset",
            ],
        )
        assert result.exit_code == 0
        mock_filter.assert_not_called()


class TestInstance2instancePipelineNameCli:
    """CLI-level tests proving --pipeline-name wiring in instance2instance."""

    @patch("datahub.cli.migrate._migrate_containers")
    @patch("datahub.cli.migrate._run_entity_migration")
    @patch("datahub.cli.migrate._filter_by_pipeline_name")
    @patch("datahub.cli.migrate.fetch_instance_urns")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_pipeline_name_filters_before_migration(
        self,
        mock_graph: MagicMock,
        mock_fetch: MagicMock,
        mock_filter: MagicMock,
        mock_run: MagicMock,
        mock_containers: MagicMock,
    ) -> None:
        mock_graph.return_value = MagicMock()
        mock_fetch.return_value = [DATASET_URN_A, DATASET_URN_B]
        mock_filter.return_value = [DATASET_URN_B]
        mock_run.return_value = MagicMock(__str__=lambda _: "report")

        runner = CliRunner()
        result = runner.invoke(
            instance2instance,
            [
                "--platform",
                "dbt",
                "--old-instance",
                "old",
                "--new-instance",
                "new",
                "--pipeline-name",
                "my-pipeline",
                "--dry-run",
                "--force",
                "--entity-types",
                "dataset",
            ],
        )
        assert result.exit_code == 0
        mock_filter.assert_called_once()
        args = mock_run.call_args
        assert args.kwargs["urns"] == [DATASET_URN_B]


class TestMigrateContainersPipelineName:
    """Container migration respects --pipeline-name when set."""

    _SAMPLE_CONTAINER = {
        "urn": "urn:li:container:guid1",
        "aspects": {
            "subTypes": {"value": {"typeNames": ["Database"]}},
            "containerProperties": {
                "value": {
                    "customProperties": {
                        "platform": "snowflake",
                        "env": "PROD",
                        "database": "db1",
                    }
                }
            },
        },
    }

    @patch("datahub.cli.migrate._process_container_relationships")
    @patch("datahub.cli.migration_utils.clone_aspect", return_value=[])
    @patch("datahub.cli.migrate._matches_pipeline_name")
    @patch("datahub.cli.migrate._get_containers_for_migration")
    def test_skips_container_not_matching_pipeline(
        self,
        mock_get: MagicMock,
        mock_matches: MagicMock,
        _mock_clone: MagicMock,
        _mock_rels: MagicMock,
    ) -> None:
        from datahub.cli.migrate import _migrate_containers

        mock_get.return_value = [self._SAMPLE_CONTAINER]
        mock_matches.return_value = False
        emitter = MagicMock()

        _migrate_containers(
            env="PROD",
            platform="snowflake",
            target_instance="newinst",
            should_migrate=lambda props: True,
            dry_run=False,
            hard=False,
            keep=True,
            rest_emitter=emitter,
            pipeline_name="my-pipeline",
        )
        mock_matches.assert_called_once()
        emitter.emit_mcp.assert_not_called()

    @patch("datahub.cli.migrate._process_container_relationships")
    @patch("datahub.cli.migration_utils.clone_aspect", return_value=[])
    @patch("datahub.cli.migrate._matches_pipeline_name")
    @patch("datahub.cli.migrate._get_containers_for_migration")
    def test_migrates_container_matching_pipeline(
        self,
        mock_get: MagicMock,
        mock_matches: MagicMock,
        _mock_clone: MagicMock,
        _mock_rels: MagicMock,
    ) -> None:
        from datahub.cli.migrate import _migrate_containers

        mock_get.return_value = [self._SAMPLE_CONTAINER]
        mock_matches.return_value = True
        emitter = MagicMock()

        _migrate_containers(
            env="PROD",
            platform="snowflake",
            target_instance="newinst",
            should_migrate=lambda props: True,
            dry_run=False,
            hard=False,
            keep=True,
            rest_emitter=emitter,
            pipeline_name="my-pipeline",
        )
        mock_matches.assert_called_once()
        emitter.emit_mcp.assert_called()

    @patch("datahub.cli.migrate._process_container_relationships")
    @patch("datahub.cli.migration_utils.clone_aspect", return_value=[])
    @patch("datahub.cli.migrate._get_containers_for_migration")
    def test_no_pipeline_name_skips_check(
        self,
        mock_get: MagicMock,
        _mock_clone: MagicMock,
        _mock_rels: MagicMock,
    ) -> None:
        from datahub.cli.migrate import _migrate_containers

        mock_get.return_value = [self._SAMPLE_CONTAINER]
        emitter = MagicMock()

        _migrate_containers(
            env="PROD",
            platform="snowflake",
            target_instance="newinst",
            should_migrate=lambda props: True,
            dry_run=False,
            hard=False,
            keep=True,
            rest_emitter=emitter,
        )
        emitter.emit_mcp.assert_called()
