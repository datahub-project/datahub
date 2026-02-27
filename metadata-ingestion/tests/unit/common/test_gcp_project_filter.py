"""Unit tests for GCP project filter functionality."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

from google.api_core.exceptions import GoogleAPICallError, PermissionDenied

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.common.gcp_project_filter import (
    GcpProject,
    GcpProjectFilterConfig,
    is_project_allowed,
    resolve_gcp_projects,
)


class TestGcpProjectFilterConfig:
    """Test GcpProjectFilterConfig validation and defaults."""

    def test_default_config(self):
        """Test default configuration values."""
        config = GcpProjectFilterConfig()
        assert config.project_ids == []
        assert config.project_labels == []
        assert config.project_id_pattern.allow == [".*"]

    def test_explicit_project_ids(self):
        """Test with explicit project IDs."""
        config = GcpProjectFilterConfig(
            project_ids=["proj-1", "proj-2"],
        )
        assert config.project_ids == ["proj-1", "proj-2"]

    def test_project_labels_format(self):
        """Test project labels configuration."""
        config = GcpProjectFilterConfig(
            project_labels=["env:prod", "team:ml"],
        )
        assert config.project_labels == ["env:prod", "team:ml"]

    def test_project_id_pattern(self):
        """Test project ID pattern configuration."""
        config = GcpProjectFilterConfig(
            project_id_pattern=AllowDenyPattern(
                allow=["^prod-.*"],
                deny=[".*-test$"],
            )
        )
        assert config.project_id_pattern.allowed("prod-ml")
        assert not config.project_id_pattern.allowed("prod-ml-test")


class TestIsProjectAllowed:
    """Test is_project_allowed function."""

    def test_is_project_allowed_with_explicit_ids(self):
        """Test project filtering with explicit IDs."""
        config = GcpProjectFilterConfig(project_ids=["proj-1", "proj-2"])

        assert is_project_allowed(config, "proj-1")
        assert is_project_allowed(config, "proj-2")
        assert not is_project_allowed(config, "proj-3")

    def test_is_project_allowed_with_pattern(self):
        """Test project filtering with pattern."""
        config = GcpProjectFilterConfig(
            project_id_pattern=AllowDenyPattern(
                allow=["^prod-.*"],
                deny=[".*-test$"],
            )
        )

        assert is_project_allowed(config, "prod-ml")
        assert not is_project_allowed(config, "dev-ml")
        assert not is_project_allowed(config, "prod-ml-test")

    def test_is_project_allowed_empty_list_uses_pattern(self):
        """Test that empty project_ids list falls back to pattern."""
        config = GcpProjectFilterConfig(
            project_ids=[],
            project_id_pattern=AllowDenyPattern(allow=["^prod-.*"]),
        )

        assert is_project_allowed(config, "prod-ml")
        assert not is_project_allowed(config, "dev-ml")

    def test_is_project_allowed_explicit_ids_override_pattern(self):
        """Test that explicit project_ids override pattern."""
        config = GcpProjectFilterConfig(
            project_ids=["dev-specific"],
            project_id_pattern=AllowDenyPattern(allow=["^prod-.*"]),
        )

        # Pattern would deny dev-specific, but explicit ID allows it
        assert is_project_allowed(config, "dev-specific")
        # Pattern would allow prod-ml, but not in explicit list
        assert not is_project_allowed(config, "prod-ml")


class TestResolveGcpProjects:
    """Test resolve_gcp_projects function with various scenarios."""

    def test_resolve_explicit_project_ids(self):
        """Test resolution with explicit project IDs."""
        config = GcpProjectFilterConfig(project_ids=["proj-1", "proj-2"])
        projects = resolve_gcp_projects(config, SourceReport())

        assert len(projects) == 2
        assert all(isinstance(p, GcpProject) for p in projects)
        assert projects[0].id == "proj-1"
        assert projects[0].name == "proj-1"
        assert projects[1].id == "proj-2"
        assert projects[1].name == "proj-2"

    @patch("datahub.ingestion.source.common.gcp_project_filter.ProjectsClient")
    def test_resolve_with_labels(self, mock_client_class):
        """Test resolution with project labels."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_client.search_projects.return_value = [
            SimpleNamespace(project_id="prod-1", display_name="Production 1"),
            SimpleNamespace(project_id="prod-2", display_name="Production 2"),
        ]

        config = GcpProjectFilterConfig(project_labels=["env:prod"])
        projects = resolve_gcp_projects(
            config, SourceReport(), projects_client=mock_client
        )

        assert len(projects) == 2
        assert projects[0].id == "prod-1"
        assert projects[0].name == "Production 1"
        mock_client.search_projects.assert_called_once()

    @patch("datahub.ingestion.source.common.gcp_project_filter.ProjectsClient")
    def test_resolve_with_labels_and_pattern(self, mock_client_class):
        """Test resolution with both labels and pattern filtering."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_client.search_projects.return_value = [
            SimpleNamespace(project_id="prod-ml", display_name="ML Production"),
            SimpleNamespace(project_id="prod-test", display_name="Test Environment"),
        ]

        config = GcpProjectFilterConfig(
            project_labels=["env:prod"],
            project_id_pattern=AllowDenyPattern(deny=[".*-test$"]),
        )
        projects = resolve_gcp_projects(
            config, SourceReport(), projects_client=mock_client
        )

        assert len(projects) == 1
        assert projects[0].id == "prod-ml"

    @patch("datahub.ingestion.source.common.gcp_project_filter.ProjectsClient")
    def test_resolve_all_projects(self, mock_client_class):
        """Test resolution listing all projects."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_client.list_projects.return_value = [
            SimpleNamespace(project_id="proj-1", display_name="Project 1"),
            SimpleNamespace(project_id="proj-2", display_name="Project 2"),
        ]

        config = GcpProjectFilterConfig()
        projects = resolve_gcp_projects(
            config, SourceReport(), projects_client=mock_client
        )

        assert len(projects) == 2
        assert projects[0].id == "proj-1"
        mock_client.list_projects.assert_called_once()

    @patch("datahub.ingestion.source.common.gcp_project_filter.ProjectsClient")
    def test_auth_failure_in_search(self, mock_client_class):
        """Test handling of authentication failures during project search."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_client.search_projects.side_effect = PermissionDenied("Access denied")

        config = GcpProjectFilterConfig(project_labels=["env:prod"])
        report = SourceReport()
        projects = resolve_gcp_projects(config, report, projects_client=mock_client)

        assert len(projects) == 0
        assert len(report.failures) > 0

    @patch("datahub.ingestion.source.common.gcp_project_filter.ProjectsClient")
    def test_auth_failure_in_list(self, mock_client_class):
        """Test handling of authentication failures during project listing."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_client.list_projects.side_effect = GoogleAPICallError("Permission denied")

        config = GcpProjectFilterConfig()
        report = SourceReport()
        projects = resolve_gcp_projects(config, report, projects_client=mock_client)

        assert len(projects) == 0
        assert len(report.failures) > 0

    @patch("datahub.ingestion.source.common.gcp_project_filter.ProjectsClient")
    def test_empty_project_list(self, mock_client_class):
        """Test handling when no projects are found."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_client.list_projects.return_value = []

        config = GcpProjectFilterConfig()
        report = SourceReport()
        projects = resolve_gcp_projects(config, report, projects_client=mock_client)

        assert len(projects) == 0
        assert len(report.failures) > 0  # Should report no projects found

    @patch("datahub.ingestion.source.common.gcp_project_filter.ProjectsClient")
    def test_missing_project_id_field(self, mock_client_class):
        """Test handling of malformed project objects missing project_id."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Project object without project_id attribute
        mock_client.list_projects.return_value = [
            SimpleNamespace(display_name="Project 1"),  # Missing project_id
            SimpleNamespace(project_id="proj-2", display_name="Project 2"),
        ]

        config = GcpProjectFilterConfig()
        projects = resolve_gcp_projects(
            config, SourceReport(), projects_client=mock_client
        )

        # Should skip malformed project and only return valid one
        assert len(projects) == 1
        assert projects[0].id == "proj-2"

    @patch("datahub.ingestion.source.common.gcp_project_filter.ProjectsClient")
    def test_none_display_name(self, mock_client_class):
        """Test handling when display_name is None."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_client.list_projects.return_value = [
            SimpleNamespace(project_id="proj-1", display_name=None),
        ]

        config = GcpProjectFilterConfig()
        projects = resolve_gcp_projects(
            config, SourceReport(), projects_client=mock_client
        )

        assert len(projects) == 1
        # Should fallback to project_id when display_name is None
        assert projects[0].id == "proj-1"
        assert projects[0].name == "proj-1"

    @patch("datahub.ingestion.source.common.gcp_project_filter.ProjectsClient")
    def test_labels_with_no_matching_projects(self, mock_client_class):
        """Test when labels are specified but no projects match."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_client.search_projects.return_value = []

        config = GcpProjectFilterConfig(project_labels=["env:nonexistent"])
        report = SourceReport()
        projects = resolve_gcp_projects(config, report, projects_client=mock_client)

        assert len(projects) == 0

    @patch("datahub.ingestion.source.common.gcp_project_filter.ProjectsClient")
    def test_pattern_filters_all_projects(self, mock_client_class):
        """Test when pattern filters out all projects."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_client.list_projects.return_value = [
            SimpleNamespace(project_id="dev-1", display_name="Dev 1"),
            SimpleNamespace(project_id="dev-2", display_name="Dev 2"),
        ]

        config = GcpProjectFilterConfig(
            project_id_pattern=AllowDenyPattern(allow=["^prod-.*"])
        )
        report = SourceReport()
        projects = resolve_gcp_projects(config, report, projects_client=mock_client)

        assert len(projects) == 0
        assert len(report.failures) > 0  # Should report no projects after filtering

    @patch("datahub.ingestion.source.common.gcp_project_filter.ProjectsClient")
    def test_explicit_empty_project_ids_list(self, mock_client_class):
        """Test with explicitly empty project_ids list."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_client.list_projects.return_value = []

        config = GcpProjectFilterConfig(project_ids=[])
        projects = resolve_gcp_projects(
            config, SourceReport(), projects_client=mock_client
        )

        # Empty list should fall through to list_all_projects
        assert isinstance(projects, list)
        assert len(projects) == 0

    @patch("datahub.ingestion.source.common.gcp_project_filter.ProjectsClient")
    def test_special_characters_in_project_id(self, mock_client_class):
        """Test handling of special characters in project IDs."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_client.list_projects.return_value = [
            SimpleNamespace(project_id="my-project-123", display_name="My Project 123"),
        ]

        config = GcpProjectFilterConfig()
        projects = resolve_gcp_projects(
            config, SourceReport(), projects_client=mock_client
        )

        assert len(projects) == 1
        assert projects[0].id == "my-project-123"
