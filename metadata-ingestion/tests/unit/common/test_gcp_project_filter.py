"""Unit tests for GCP project filter functionality."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from google.api_core.exceptions import GoogleAPICallError, PermissionDenied
from google.auth.exceptions import GoogleAuthError

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.common.gcp_project_filter import (
    GCP_LABEL_PATTERN,
    GCP_PROJECT_ID_PATTERN,
    GcpProject,
    GcpProjectFilterConfig,
    GCPValidationError,
    is_project_allowed,
    resolve_gcp_projects,
    validate_project_id_list,
    validate_project_label_list,
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

    @patch("datahub.ingestion.source.common.gcp_project_filter.ProjectsClient")
    def test_auth_error_in_label_search(self, mock_client_class):
        """Test GoogleAuthError is caught during label-based project search."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.search_projects.side_effect = GoogleAuthError("Invalid credentials")

        config = GcpProjectFilterConfig(project_labels=["env:prod"])
        report = SourceReport()
        projects = resolve_gcp_projects(config, report, projects_client=mock_client)

        assert len(projects) == 0
        assert len(report.failures) > 0


class TestGcpProjectIdPattern:
    """Test GCP_PROJECT_ID_PATTERN regex."""

    @pytest.mark.parametrize(
        "project_id",
        ["my-project", "a12345", "abcdef", "a-b-c-d-e-f", "project-123-abc"],
    )
    def test_valid_project_ids(self, project_id):
        assert GCP_PROJECT_ID_PATTERN.match(project_id)

    @pytest.mark.parametrize(
        "project_id",
        [
            "UPPER",  # uppercase
            "1start",  # starts with digit
            "ab",  # too short (< 6)
            "a" * 31,  # too long (> 30)
            "has space",  # contains space
            "end-",  # ends with hyphen
        ],
    )
    def test_invalid_project_ids(self, project_id):
        assert not GCP_PROJECT_ID_PATTERN.match(project_id)


class TestGcpLabelPattern:
    """Test GCP_LABEL_PATTERN regex."""

    @pytest.mark.parametrize(
        "label",
        ["env:prod", "team:ml-ops", "cost_center:12345"],
    )
    def test_valid_labels(self, label):
        assert GCP_LABEL_PATTERN.match(label)

    @pytest.mark.parametrize(
        "label",
        [
            "no-colon",  # missing colon separator
            "Env:prod",  # uppercase key
            ":value",  # empty key
            "key:",  # empty value
            "env=prod",  # wrong separator
        ],
    )
    def test_invalid_labels(self, label):
        assert not GCP_LABEL_PATTERN.match(label)


class TestValidateProjectIdList:
    """Test validate_project_id_list function."""

    def test_valid_list(self):
        validate_project_id_list(["my-project", "other-proj-1"])

    def test_empty_list_allowed(self):
        validate_project_id_list([])

    def test_empty_list_disallowed(self):
        with pytest.raises(GCPValidationError, match="cannot be an empty list"):
            validate_project_id_list([], allow_empty=False)

    def test_empty_string_rejected(self):
        with pytest.raises(GCPValidationError, match="empty values"):
            validate_project_id_list(["my-project", ""])

    def test_duplicates_rejected(self):
        with pytest.raises(GCPValidationError, match="duplicates"):
            validate_project_id_list(["my-project", "my-project"])

    def test_invalid_format_rejected(self):
        with pytest.raises(GCPValidationError, match="Invalid project_ids format"):
            validate_project_id_list(["UPPERCASE"])


class TestValidateProjectLabelList:
    """Test validate_project_label_list function."""

    def test_valid_list(self):
        validate_project_label_list(["env:prod", "team:ml"])

    def test_empty_list_allowed(self):
        validate_project_label_list([])

    def test_empty_string_rejected(self):
        with pytest.raises(GCPValidationError, match="empty values"):
            validate_project_label_list(["env:prod", ""])

    def test_duplicates_rejected(self):
        with pytest.raises(GCPValidationError, match="duplicates"):
            validate_project_label_list(["env:prod", "env:prod"])

    def test_invalid_format_rejected(self):
        with pytest.raises(GCPValidationError, match="Invalid project_labels format"):
            validate_project_label_list(["env=prod"])
