from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.airbyte.config import (
    AirbyteDeploymentType,
    AirbyteSourceConfig,
)
from datahub.ingestion.source.airbyte.models import (
    AirbyteConnectionPartial,
    AirbyteDestinationPartial,
    AirbytePipelineInfo,
    AirbyteSourcePartial,
    AirbyteSyncCatalog,
    AirbyteTagInfo,
    AirbyteWorkspacePartial,
)
from datahub.ingestion.source.airbyte.source import AirbyteSource


@pytest.fixture
def mock_ctx():
    ctx = MagicMock(spec=PipelineContext)
    ctx.graph = MagicMock()
    ctx.pipeline_name = "airbyte_test"
    ctx.pipeline_config = MagicMock()
    ctx.run_id = "test-run-id"
    return ctx


class TestFetchTagsForWorkspace:
    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_fetch_tags_success(self, mock_create_client, mock_ctx):
        mock_client = MagicMock()
        mock_client.list_tags.return_value = [
            {
                "id": "tag-1",
                "name": "production",
                "resourceId": "conn-123",
                "resourceType": "connection",
            },
            {
                "id": "tag-2",
                "name": "critical",
                "resourceId": "conn-123",
                "resourceType": "connection",
            },
        ]
        mock_create_client.return_value = mock_client

        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        source = AirbyteSource(config, mock_ctx)

        tags = source._fetch_tags_for_workspace("workspace-123")

        assert len(tags) == 2
        assert tags[0].name == "production"
        assert tags[1].name == "critical"

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_fetch_tags_exception(self, mock_create_client, mock_ctx):
        mock_client = MagicMock()
        mock_client.list_tags.side_effect = Exception("API error")
        mock_create_client.return_value = mock_client

        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        source = AirbyteSource(config, mock_ctx)

        tags = source._fetch_tags_for_workspace("workspace-123")

        assert tags == []

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_fetch_tags_cached_per_workspace(self, mock_create_client, mock_ctx):
        # Tags are workspace-scoped; calling `_fetch_tags_for_workspace` N
        # times for the same workspace must hit the Airbyte API exactly once.
        # Without caching, a workspace with N connections issues N identical
        # `/tags?workspaceIds=...` requests.
        mock_client = MagicMock()
        mock_client.list_tags.return_value = [
            {"id": "tag-1", "name": "production"},
        ]
        mock_create_client.return_value = mock_client

        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        source = AirbyteSource(config, mock_ctx)

        for _ in range(3):
            tags = source._fetch_tags_for_workspace("workspace-123")
            assert len(tags) == 1
            assert tags[0].name == "production"

        assert mock_client.list_tags.call_count == 1

        # A different workspace must still trigger a fresh fetch.
        source._fetch_tags_for_workspace("workspace-456")
        assert mock_client.list_tags.call_count == 2

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_fetch_tags_failure_is_cached(self, mock_create_client, mock_ctx):
        # A transient failure on the first call should not cause repeat
        # failing calls for the remaining connections in the same workspace.
        mock_client = MagicMock()
        mock_client.list_tags.side_effect = Exception("API error")
        mock_create_client.return_value = mock_client

        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        source = AirbyteSource(config, mock_ctx)

        for _ in range(3):
            assert source._fetch_tags_for_workspace("workspace-123") == []

        assert mock_client.list_tags.call_count == 1


class TestExtractConnectionTags:
    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_extract_connection_tags(self, mock_create_client, mock_ctx):
        mock_create_client.return_value = MagicMock()

        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        source = AirbyteSource(config, mock_ctx)

        pipeline_info = AirbytePipelineInfo(
            workspace=AirbyteWorkspacePartial(
                workspace_id="workspace-123", name="Test Workspace"
            ),
            connection=AirbyteConnectionPartial(
                connection_id="conn-123",
                source_id="source-123",
                destination_id="dest-123",
                name="Test Connection",
            ),
            source=AirbyteSourcePartial(source_id="source-123", name="Test Source"),
            destination=AirbyteDestinationPartial(
                destination_id="dest-123", name="Test Destination"
            ),
        )

        airbyte_tags = [
            AirbyteTagInfo(
                name="production",
                tag_id="tag-1",
                resource_id="conn-123",
                resource_type="connection",
            ),
            AirbyteTagInfo(
                name="other",
                tag_id="tag-2",
                resource_id="conn-456",
                resource_type="connection",
            ),
        ]

        tags = source._extract_connection_tags(pipeline_info, airbyte_tags)

        assert len(tags) == 1
        assert "urn:li:tag:production" in tags[0]


class TestFetchStreamsForSource:
    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_fetch_streams_no_sync_catalog(self, mock_create_client, mock_ctx):
        mock_create_client.return_value = MagicMock()

        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        source = AirbyteSource(config, mock_ctx)

        pipeline_info = AirbytePipelineInfo(
            workspace=AirbyteWorkspacePartial(
                workspace_id="workspace-123", name="Test Workspace"
            ),
            connection=AirbyteConnectionPartial(
                connection_id="conn-123",
                source_id="source-123",
                destination_id="dest-123",
                name="Test Connection",
                sync_catalog=None,
            ),
            source=AirbyteSourcePartial(source_id="source-123", name="Test Source"),
            destination=AirbyteDestinationPartial(
                destination_id="dest-123", name="Test Destination"
            ),
        )

        streams = source._fetch_streams_for_source(pipeline_info)

        assert streams == []

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_fetch_streams_empty_streams(self, mock_create_client, mock_ctx):
        mock_create_client.return_value = MagicMock()

        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        source = AirbyteSource(config, mock_ctx)

        pipeline_info = AirbytePipelineInfo(
            workspace=AirbyteWorkspacePartial(
                workspace_id="workspace-123", name="Test Workspace"
            ),
            connection=AirbyteConnectionPartial(
                connection_id="conn-123",
                source_id="source-123",
                destination_id="dest-123",
                name="Test Connection",
                sync_catalog=AirbyteSyncCatalog(streams=[]),
            ),
            source=AirbyteSourcePartial(source_id="source-123", name="Test Source"),
            destination=AirbyteDestinationPartial(
                destination_id="dest-123", name="Test Destination"
            ),
        )

        streams = source._fetch_streams_for_source(pipeline_info)

        assert streams == []

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_fetch_streams_missing_source_id(self, mock_create_client, mock_ctx):
        mock_create_client.return_value = MagicMock()

        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        source = AirbyteSource(config, mock_ctx)

        pipeline_info = AirbytePipelineInfo(
            workspace=AirbyteWorkspacePartial(
                workspace_id="workspace-123", name="Test Workspace"
            ),
            connection=AirbyteConnectionPartial(
                connection_id="conn-123",
                source_id="",
                destination_id="dest-123",
                name="Test Connection",
            ),
            source=AirbyteSourcePartial(source_id="", name="Test Source"),
            destination=AirbyteDestinationPartial(
                destination_id="dest-123", name="Test Destination"
            ),
        )

        streams = source._fetch_streams_for_source(pipeline_info)

        assert streams == []


class TestGetWorkunitsInternal:
    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_invalid_workspace_skipped_silently(self, mock_create_client, mock_ctx):
        # Workspaces missing IDs should warn and skip, not crash the run.
        mock_client = MagicMock()
        mock_client.list_workspaces.return_value = [
            AirbyteWorkspacePartial(workspace_id="", name="Invalid Workspace")
        ]
        mock_create_client.return_value = mock_client

        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        source = AirbyteSource(config, mock_ctx)

        workunits = list(source.get_workunits_internal())
        assert workunits == []

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_invalid_connection_recorded_as_warning(self, mock_create_client, mock_ctx):
        mock_client = MagicMock()
        mock_client.list_workspaces.return_value = [
            AirbyteWorkspacePartial(workspace_id="workspace-123", name="Test Workspace")
        ]
        mock_client.list_connections.return_value = [
            AirbyteConnectionPartial(
                connection_id="",
                source_id="source-123",
                destination_id="dest-123",
            )
        ]
        mock_create_client.return_value = mock_client

        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        source = AirbyteSource(config, mock_ctx)

        list(source.get_workunits_internal())
        assert source.report.warnings

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_api_exception_reported_as_failure(self, mock_create_client, mock_ctx):
        mock_client = MagicMock()
        mock_client.list_workspaces.return_value = [
            AirbyteWorkspacePartial(workspace_id="workspace-123", name="Test Workspace")
        ]
        mock_client.list_connections.side_effect = Exception("API error")
        mock_create_client.return_value = mock_client

        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        source = AirbyteSource(config, mock_ctx)

        list(source.get_workunits_internal())
        assert source.report.failures


class TestGetWorkunitProcessors:
    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_get_workunit_processors_with_incremental_lineage(
        self, mock_create_client, mock_ctx
    ):
        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            incremental_lineage=True,
        )
        mock_create_client.return_value = MagicMock()
        source = AirbyteSource(config, mock_ctx)

        processors = source.get_workunit_processors()

        assert processors is not None
        # incremental_lineage + stale_entity_removal at minimum.
        assert len(processors) >= 2

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_get_workunit_processors_without_incremental_lineage(
        self, mock_create_client, mock_ctx
    ):
        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            incremental_lineage=False,
        )
        mock_create_client.return_value = MagicMock()
        source = AirbyteSource(config, mock_ctx)

        processors = source.get_workunit_processors()

        # Should still have processors
        assert processors is not None
