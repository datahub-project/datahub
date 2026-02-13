"""Unit tests for Airbyte source coverage improvements."""

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
    AirbyteWorkspacePartial,
)
from datahub.ingestion.source.airbyte.source import AirbyteSource


@pytest.fixture
def mock_ctx():
    """Create a mock pipeline context."""
    ctx = MagicMock(spec=PipelineContext)
    ctx.graph = MagicMock()
    ctx.pipeline_name = "airbyte_test"
    ctx.pipeline_config = MagicMock()
    ctx.run_id = "test-run-id"
    return ctx


class TestValidatePipelineIds:
    """Tests for _validate_pipeline_ids method."""

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_validate_pipeline_ids_valid(self, mock_create_client, mock_ctx):
        """Test validating pipeline IDs with valid data."""
        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        mock_create_client.return_value = MagicMock()
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

        result = source._validate_pipeline_ids(pipeline_info, "test operation")

        assert result is not None
        assert result.workspace_id == "workspace-123"
        assert result.connection_id == "conn-123"

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_validate_pipeline_ids_missing_workspace(
        self, mock_create_client, mock_ctx
    ):
        """Test validation fails when workspace_id is missing."""
        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        mock_create_client.return_value = MagicMock()
        source = AirbyteSource(config, mock_ctx)

        pipeline_info = AirbytePipelineInfo(
            workspace=AirbyteWorkspacePartial(workspace_id="", name="Test Workspace"),
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

        result = source._validate_pipeline_ids(pipeline_info, "test operation")

        assert result is None


class TestFetchTagsForWorkspace:
    """Tests for _fetch_tags_for_workspace method."""

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_fetch_tags_success(self, mock_create_client, mock_ctx):
        """Test successfully fetching tags."""
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
        """Test handling exception when fetching tags."""
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


class TestExtractConnectionTags:
    """Tests for _extract_connection_tags method."""

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_extract_connection_tags(self, mock_create_client, mock_ctx):
        """Test extracting tags for a connection."""
        from datahub.ingestion.source.airbyte.models import AirbyteTagInfo

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
    """Tests for _fetch_streams_for_source method."""

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_fetch_streams_no_sync_catalog(self, mock_create_client, mock_ctx):
        """Test fetching streams when connection has no sync_catalog."""
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
        """Test fetching streams when sync_catalog has no streams."""
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
        """Test fetching streams when source_id is None."""
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


class TestGetWorkunits:
    """Tests for get_workunits method and error handling."""

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_get_workunits_with_invalid_workspace(self, mock_create_client, mock_ctx):
        """Test get_workunits handles invalid workspace gracefully."""
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

        workunits = list(source.get_workunits())

        assert len(workunits) == 0

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_get_workunits_with_invalid_connection(self, mock_create_client, mock_ctx):
        """Test get_workunits handles invalid connection gracefully."""
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

        workunits = list(source.get_workunits())

        assert isinstance(workunits, list)
        assert source.report.warnings

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_get_workunits_with_processing_exception(
        self, mock_create_client, mock_ctx
    ):
        """Test get_workunits handles processing exceptions."""
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

        workunits = list(source.get_workunits())

        assert isinstance(workunits, list)


class TestGetWorkunitProcessors:
    """Tests for get_workunit_processors method."""

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_get_workunit_processors_with_incremental_lineage(
        self, mock_create_client, mock_ctx
    ):
        """Test workunit processors includes incremental lineage when enabled."""
        config = AirbyteSourceConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            incremental_lineage=True,
        )
        mock_create_client.return_value = MagicMock()
        source = AirbyteSource(config, mock_ctx)

        processors = source.get_workunit_processors()

        # Should have at least 2 processors: incremental_lineage + stale_entity_removal
        assert processors is not None
        assert len(processors) >= 2

    @patch("datahub.ingestion.source.airbyte.source.create_airbyte_client")
    def test_get_workunit_processors_without_incremental_lineage(
        self, mock_create_client, mock_ctx
    ):
        """Test workunit processors without incremental lineage."""
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
