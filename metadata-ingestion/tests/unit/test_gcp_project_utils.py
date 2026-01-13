import json
import logging
import os
from typing import List
from unittest.mock import MagicMock, Mock, patch

import pytest
from google.api_core.exceptions import (
    DeadlineExceeded,
    FailedPrecondition,
    GoogleAPICallError,
    InvalidArgument,
    NotFound,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
)
from google.api_core.retry import Retry
from google.cloud import resourcemanager_v3

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.common.gcp_project_utils import (
    GCPProject,
    GCPProjectDiscoveryError,
    GCPValidationError,
    _filter_and_validate,
    _filter_projects_by_pattern,
    _handle_discovery_error,
    _is_transient_error_predicate,
    _search_projects_with_retry,
    _validate_pattern_against_explicit_list,
    _validate_pattern_before_discovery,
    call_with_retry,
    gcp_api_retry,
    gcp_credentials_context,
    get_gcp_error_type,
    get_projects,
    get_projects_by_labels,
    get_projects_from_explicit_list,
    is_gcp_transient_error,
    temporary_credentials_file,
    validate_project_id_format,
    validate_project_id_list,
    validate_project_label_format,
    validate_project_label_list,
    with_temporary_credentials,
)


class TestProjectIDValidation:
    """Tests for project ID validation"""

    def test_validate_project_id_format_valid(self) -> None:
        """Valid project IDs should pass"""
        valid_ids = [
            "my-project",
            "project-123",
            "test-project-456",
            "a" * 30,
            "a-b-c-d-e",
        ]
        for project_id in valid_ids:
            validate_project_id_format(project_id)

    def test_validate_project_id_format_invalid(self) -> None:
        """Invalid project IDs should raise GCPValidationError"""
        invalid_ids = [
            "",
            "a",
            "a" * 31,
            "Project-123",
            "-my-project",
            "my-project-",
            "my_project",
            "my project",
            "123-project",
        ]
        for project_id in invalid_ids:
            with pytest.raises(GCPValidationError):
                validate_project_id_format(project_id)

    def test_validate_project_id_list_valid(self) -> None:
        """Valid project ID lists should pass"""
        validate_project_id_list([])
        validate_project_id_list(["project-123", "project-456"])

    def test_validate_project_id_list_empty_not_allowed(self) -> None:
        """Empty list should raise when allow_empty=False"""
        with pytest.raises(GCPValidationError, match="cannot be an empty list"):
            validate_project_id_list([], allow_empty=False)

    def test_validate_project_id_list_duplicates(self) -> None:
        """Duplicate project IDs should raise"""
        with pytest.raises(GCPValidationError, match="[Dd]uplicate"):
            validate_project_id_list(["project-123", "project-456", "project-123"])

    def test_validate_project_id_list_invalid_format(self) -> None:
        """Invalid project ID in list should raise"""
        with pytest.raises(GCPValidationError):
            validate_project_id_list(["valid-project", "Invalid-Project"])


class TestLabelValidation:
    """Tests for label validation"""

    def test_validate_project_label_format_valid(self) -> None:
        """Valid labels should pass"""
        valid_labels = [
            "env:prod",
            "team:data",
            "cost-center:eng-123",
            "a:b",
            "key123:value456",
        ]
        for label in valid_labels:
            validate_project_label_format(label)

    def test_validate_project_label_format_invalid(self) -> None:
        """Invalid labels should raise GCPValidationError"""
        invalid_labels = [
            ":value",
            "",
            "Key:value",
            "key:Value",
            "123key:value",
            "key:value:extra",
        ]
        for label in invalid_labels:
            with pytest.raises(GCPValidationError):
                validate_project_label_format(label)

    def test_validate_project_label_list_valid(self) -> None:
        """Valid label lists should pass"""
        validate_project_label_list([])
        validate_project_label_list(["env:prod", "team:data"])

    def test_validate_project_label_list_duplicates(self) -> None:
        """Duplicate labels should raise"""
        with pytest.raises(GCPValidationError, match="[Dd]uplicate"):
            validate_project_label_list(["env:prod", "team:data", "env:prod"])

    def test_validate_project_label_list_invalid_format(self) -> None:
        """Invalid label in list should raise"""
        with pytest.raises(GCPValidationError):
            validate_project_label_list(["valid:label", "Invalid:label"])


class TestExplicitProjectList:
    def test_returns_all_projects_without_pattern(self) -> None:
        projects = get_projects_from_explicit_list(["p1", "p2", "p3"])
        assert [p.id for p in projects] == ["p1", "p2", "p3"]

    def test_filters_with_allow_pattern(self) -> None:
        projects = get_projects_from_explicit_list(
            ["app-prod", "app-dev", "api-prod"],
            AllowDenyPattern(allow=[".*-prod$"]),
        )
        assert [p.id for p in projects] == ["app-prod", "api-prod"]

    def test_get_projects_uses_explicit_list(self) -> None:
        projects = get_projects(project_ids=["explicit-1", "explicit-2"])
        assert [p.id for p in projects] == ["explicit-1", "explicit-2"]


class TestAutoDiscovery:
    @pytest.mark.parametrize(
        "exception,error_match",
        [
            (PermissionDenied("No access"), "organization-level permissions"),
            (GoogleAPICallError("API error"), "API error"),
            (DeadlineExceeded("Timeout"), "API timeout"),
            (ServiceUnavailable("Down"), "Service unavailable"),
        ],
    )
    def test_error_handling(self, exception: Exception, error_match: str) -> None:
        mock_client = MagicMock()
        mock_client.search_projects.side_effect = exception
        with patch(
            "datahub.ingestion.source.common.gcp_project_utils.gcp_api_retry"
        ) as mock_retry:
            mock_retry.return_value = lambda f: f
            with pytest.raises(GCPProjectDiscoveryError, match=error_match):
                get_projects(client=mock_client)

    def test_empty_result_raises_error(self) -> None:
        mock_client = MagicMock()
        mock_client.search_projects.return_value = iter([])
        with pytest.raises(GCPProjectDiscoveryError, match="No projects discovered"):
            get_projects(client=mock_client)

    def test_all_filtered_out_raises_error(self) -> None:
        mock_client = MagicMock()
        mock_project = MagicMock()
        mock_project.project_id = "dev-project"
        mock_project.display_name = "Dev Project"
        mock_client.search_projects.return_value = iter([mock_project])

        with pytest.raises(
            GCPProjectDiscoveryError, match="excluded by project_id_pattern"
        ):
            get_projects(
                client=mock_client,
                project_id_pattern=AllowDenyPattern(allow=["prod-.*"]),
            )


class TestLabelBasedDiscovery:
    def test_success(self) -> None:
        mock_client = MagicMock()
        mock_project = MagicMock()
        mock_project.project_id = "prod-project"
        mock_project.display_name = "Prod Project"
        mock_client.search_projects.return_value = iter([mock_project])

        projects = get_projects_by_labels(["env:prod"], mock_client)
        assert len(projects) == 1
        assert projects[0].id == "prod-project"

    def test_empty_labels_raises_error(self) -> None:
        mock_client = MagicMock()
        with pytest.raises(GCPProjectDiscoveryError, match="cannot be empty"):
            get_projects_by_labels([], mock_client)

    def test_no_matches_raises_error(self) -> None:
        mock_client = MagicMock()
        mock_client.search_projects.return_value = iter([])
        with pytest.raises(GCPProjectDiscoveryError, match="No projects found"):
            get_projects_by_labels(["env:prod"], mock_client)

    @pytest.mark.parametrize(
        "exception,error_match",
        [
            (PermissionDenied("No access"), "organization-level permissions"),
            (DeadlineExceeded("Timeout"), "API timeout"),
            (ServiceUnavailable("Down"), "Service unavailable"),
        ],
    )
    def test_error_handling(self, exception: Exception, error_match: str) -> None:
        mock_client = MagicMock()
        mock_client.search_projects.side_effect = exception
        with patch(
            "datahub.ingestion.source.common.gcp_project_utils.gcp_api_retry"
        ) as mock_retry:
            mock_retry.return_value = lambda f: f
            with pytest.raises(GCPProjectDiscoveryError, match=error_match):
                get_projects_by_labels(["env:prod"], mock_client)

    def test_permission_denied_provides_targeted_error_message(self) -> None:
        """Permission denied during label discovery should explain org-level permission requirement."""
        mock_client = MagicMock()
        mock_client.search_projects.side_effect = PermissionDenied("Permission denied")
        with patch(
            "datahub.ingestion.source.common.gcp_project_utils.gcp_api_retry"
        ) as mock_retry:
            mock_retry.return_value = lambda f: f
            with pytest.raises(GCPProjectDiscoveryError) as exc_info:
                get_projects_by_labels(["env:prod"], mock_client)

            error_msg = str(exc_info.value)
            assert "organization-level permissions" in error_msg
            assert "resourcemanager.projects.list" in error_msg
            assert "Browser" in error_msg or "roles/browser" in error_msg
            assert "explicit project_ids" in error_msg or "project_ids" in error_msg


class TestTransientErrorDetection:
    @pytest.mark.parametrize(
        "exception,expected",
        [
            (ResourceExhausted("Quota exceeded"), True),
            (DeadlineExceeded("Timeout"), True),
            (ServiceUnavailable("Service down"), True),
            (GoogleAPICallError("Server error"), False),
            (PermissionDenied("Access denied"), False),
            (ValueError("Other error"), False),
        ],
    )
    def test_detection(self, exception: Exception, expected: bool) -> None:
        assert _is_transient_error_predicate(exception) == expected

    def test_5xx_error_code_is_transient(self) -> None:
        exc = GoogleAPICallError("Internal server error")
        exc.code = 500
        assert is_gcp_transient_error(exc) is True

    def test_4xx_error_code_is_not_transient(self) -> None:
        exc = GoogleAPICallError("Bad request")
        exc.code = 400
        assert is_gcp_transient_error(exc) is False

    def test_is_gcp_transient_error_detects_transient_errors(self) -> None:
        """Transient GCP errors should be detected"""
        transient_errors = [
            ServiceUnavailable("Service unavailable"),
            DeadlineExceeded("Timeout"),
            ResourceExhausted("Quota exceeded"),
        ]
        exc_500 = GoogleAPICallError("Internal server error")
        exc_500.code = 500
        transient_errors.append(exc_500)

        for error in transient_errors:
            assert is_gcp_transient_error(error) is True

    def test_is_gcp_transient_error_rejects_permanent_errors(self) -> None:
        """Permanent errors should not be treated as transient"""
        permanent_errors = [
            NotFound("Not found"),
            PermissionDenied("Permission denied"),
            ValueError("Generic error"),
        ]
        for error in permanent_errors:
            assert is_gcp_transient_error(error) is False

    def test_gcp_api_retry_returns_retry_object(self) -> None:
        """gcp_api_retry should return configured Retry object"""
        retry_obj = gcp_api_retry(timeout=60.0)
        assert isinstance(retry_obj, Retry)
        assert retry_obj._predicate is not None

    def test_call_with_retry_succeeds_on_first_try(self) -> None:
        """call_with_retry should return result on first success"""
        mock_func = Mock(return_value="success")
        result = call_with_retry(mock_func, "arg1", timeout=10.0, kwarg1="value1")
        assert result == "success"
        mock_func.assert_called_once_with("arg1", kwarg1="value1")

    def test_call_with_retry_retries_on_transient_error(self) -> None:
        """call_with_retry should retry on transient errors"""
        mock_func = Mock(
            side_effect=[
                ServiceUnavailable("Unavailable"),
                ServiceUnavailable("Still unavailable"),
                "success",
            ]
        )
        with patch("time.sleep"):  # Speed up test
            result = call_with_retry(mock_func, timeout=10.0)
        assert result == "success"
        assert mock_func.call_count == 3

    def test_call_with_retry_raises_on_permanent_error(self) -> None:
        """call_with_retry should not retry permanent errors"""
        mock_func = Mock(side_effect=PermissionDenied("Denied"))
        with pytest.raises(PermissionDenied):
            call_with_retry(mock_func, timeout=10.0)
        mock_func.assert_called_once()

    def test_get_gcp_error_type(self) -> None:
        """get_gcp_error_type should return error category"""
        assert get_gcp_error_type(NotFound("")) == "Not found"
        assert get_gcp_error_type(PermissionDenied("")) == "Permission denied"
        assert get_gcp_error_type(ValueError("")) == "API error"
        assert get_gcp_error_type(InvalidArgument("")) == "Configuration error"
        assert get_gcp_error_type(FailedPrecondition("")) == "Configuration error"

    def test_handle_discovery_error_creates_useful_message(self) -> None:
        """_handle_discovery_error should create actionable error messages"""
        exc = PermissionDenied("Access denied")
        error = _handle_discovery_error(exc, "gcloud projects list", "label-based")
        assert isinstance(error, GCPProjectDiscoveryError)
        assert "PERMISSION_DENIED" in str(error) or "Permission denied" in str(error)
        assert "gcloud projects list" in str(error)
        assert "label-based" in str(error)


class TestPatternFiltering:
    @pytest.mark.parametrize(
        "projects,pattern,expected_ids",
        [
            ([], AllowDenyPattern.allow_all(), []),
            (
                [GCPProject(id="p1", name="P1"), GCPProject(id="p2", name="P2")],
                AllowDenyPattern.allow_all(),
                ["p1", "p2"],
            ),
            (
                [
                    GCPProject(id="prod-app", name="Prod"),
                    GCPProject(id="dev", name="Dev"),
                ],
                AllowDenyPattern(allow=["prod-.*"]),
                ["prod-app"],
            ),
        ],
    )
    def test_filter_projects(
        self,
        projects: List[GCPProject],
        pattern: AllowDenyPattern,
        expected_ids: List[str],
    ) -> None:
        result = _filter_projects_by_pattern(projects, pattern)
        assert [p.id for p in result] == expected_ids

    def test_validate_all_filtered_raises_error(self) -> None:
        projects = [GCPProject(id="dev-project", name="Dev Project")]
        pattern = AllowDenyPattern(allow=["prod-.*"])
        with pytest.raises(GCPProjectDiscoveryError, match="all were excluded"):
            _filter_and_validate(projects, pattern, "test source")


class TestProjectDiscovery:
    """Tests for GCP project discovery functions"""

    def test_filter_projects_by_pattern_no_pattern(self) -> None:
        """Default allow-all pattern should return all projects"""
        projects = [
            GCPProject(id="project-123", name="Project 123"),
            GCPProject(id="project-456", name="Project 456"),
        ]
        pattern = AllowDenyPattern.allow_all()
        result = _filter_projects_by_pattern(projects, pattern)
        assert result == projects

    def test_filter_projects_by_pattern_allow(self) -> None:
        """Allow pattern should filter projects"""
        projects = [
            GCPProject(id="prod-project-1", name="Prod 1"),
            GCPProject(id="dev-project-1", name="Dev 1"),
            GCPProject(id="prod-project-2", name="Prod 2"),
        ]
        pattern = AllowDenyPattern(allow=["^prod-.*"])
        result = _filter_projects_by_pattern(projects, pattern)
        assert len(result) == 2
        assert all(p.id.startswith("prod-") for p in result)

    def test_filter_projects_by_pattern_deny(self) -> None:
        """Deny pattern should exclude projects"""
        projects = [
            GCPProject(id="prod-project-1", name="Prod 1"),
            GCPProject(id="dev-project-1", name="Dev 1"),
            GCPProject(id="test-project-1", name="Test 1"),
        ]
        pattern = AllowDenyPattern(deny=["^test-.*"])
        result = _filter_projects_by_pattern(projects, pattern)
        assert len(result) == 2
        assert all(not p.id.startswith("test-") for p in result)

    def test_filter_and_validate_empty_result_raises(self) -> None:
        """Empty result after filtering should raise"""
        projects: List[GCPProject] = []
        pattern = AllowDenyPattern(allow=["^prod-.*"])
        with pytest.raises(GCPProjectDiscoveryError, match="all were excluded"):
            _filter_and_validate(projects, pattern, "explicit list")

    def test_filter_and_validate_returns_projects(self) -> None:
        """Valid filtered projects should be returned"""
        projects = [GCPProject(id="project-123", name="Project 123")]
        pattern = AllowDenyPattern.allow_all()
        result = _filter_and_validate(projects, pattern, "explicit list")
        assert result == projects

    @patch("datahub.ingestion.source.common.gcp_project_utils.get_projects_client")
    def test_get_projects_from_explicit_list(self, mock_client: MagicMock) -> None:
        """get_projects_from_explicit_list should validate and return projects"""
        project_ids = ["project-123", "project-456"]
        result = get_projects_from_explicit_list(project_ids)
        assert len(result) == 2
        assert result[0].id == "project-123"
        assert result[1].id == "project-456"

    @patch(
        "datahub.ingestion.source.common.gcp_project_utils._search_projects_with_retry"
    )
    def test_get_projects_by_labels(self, mock_search: MagicMock) -> None:
        """get_projects_by_labels should search and filter projects"""
        mock_client = MagicMock(spec=resourcemanager_v3.ProjectsClient)
        labels = ["env:prod", "team:data"]
        mock_search.return_value = [
            GCPProject(id="project-123", name="Project 123"),
            GCPProject(id="project-456", name="Project 456"),
        ]
        result = get_projects_by_labels(labels, mock_client)
        assert len(result) == 2
        assert mock_search.call_count == 1

    @patch("datahub.ingestion.source.common.gcp_project_utils.get_projects_client")
    @patch(
        "datahub.ingestion.source.common.gcp_project_utils.get_projects_from_explicit_list"
    )
    def test_get_projects_with_explicit_ids(
        self, mock_explicit: MagicMock, mock_client: MagicMock
    ) -> None:
        """get_projects should use explicit list when provided"""
        project_ids = ["project-123"]
        mock_explicit.return_value = [GCPProject(id="project-123", name="Project 123")]
        result = get_projects(project_ids=project_ids)
        mock_explicit.assert_called_once()
        assert len(result) == 1

    @patch("datahub.ingestion.source.common.gcp_project_utils.get_projects_client")
    @patch("datahub.ingestion.source.common.gcp_project_utils.get_projects_by_labels")
    def test_get_projects_with_labels(
        self, mock_labels: MagicMock, mock_client: MagicMock
    ) -> None:
        """get_projects should use label-based discovery when provided"""
        labels = ["env:prod"]
        mock_labels.return_value = [GCPProject(id="project-123", name="Project 123")]
        result = get_projects(project_labels=labels)
        mock_labels.assert_called_once()
        assert len(result) == 1

    def test_get_projects_no_input_uses_auto_discovery(self) -> None:
        """get_projects with no input should use auto-discovery"""
        mock_client = MagicMock()
        mock_project = MagicMock()
        mock_project.project_id = "auto-project"
        mock_project.display_name = "Auto Project"
        mock_client.search_projects.return_value = iter([mock_project])
        with patch(
            "datahub.ingestion.source.common.gcp_project_utils.gcp_api_retry"
        ) as mock_retry:
            mock_retry.return_value = lambda f: f
            result = get_projects(client=mock_client)
            assert len(result) == 1
            assert result[0].id == "auto-project"


class TestIntegration:
    """Integration tests with mocked GCP services"""

    @patch(
        "datahub.ingestion.source.common.gcp_project_utils.resourcemanager_v3.ProjectsClient"
    )
    def test_end_to_end_explicit_list_with_pattern(
        self, mock_client_class: MagicMock
    ) -> None:
        """End-to-end test: explicit list + pattern filtering"""
        project_ids = ["prod-project-1", "prod-project-2", "dev-project-1"]
        pattern = AllowDenyPattern(allow=["^prod-.*"])
        result = get_projects_from_explicit_list(project_ids, pattern)
        assert len(result) == 2
        assert all(p.id.startswith("prod-") for p in result)

    @patch(
        "datahub.ingestion.source.common.gcp_project_utils._search_projects_with_retry"
    )
    def test_end_to_end_label_discovery_with_deduplication(
        self, mock_search: MagicMock
    ) -> None:
        """End-to-end test: label discovery returns all projects from combined query"""
        mock_client = MagicMock(spec=resourcemanager_v3.ProjectsClient)
        mock_search.return_value = [
            GCPProject(id="project-123", name="Project 123"),
            GCPProject(id="project-456", name="Project 456"),
        ]
        result = get_projects_by_labels(["env:prod", "team:data"], mock_client)
        assert len(result) == 2
        project_ids = [p.id for p in result]
        assert "project-123" in project_ids
        assert "project-456" in project_ids

    def test_credential_isolation_between_projects(self) -> None:
        """Verify credentials don't leak between project iterations"""
        creds1 = {"type": "service_account", "project_id": "project-1"}
        creds2 = {"type": "service_account", "project_id": "project-2"}
        with gcp_credentials_context(creds1):
            env_var_1 = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        with gcp_credentials_context(creds2):
            env_var_2 = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        assert env_var_1 is not None
        assert env_var_2 is not None
        assert env_var_1 != env_var_2
        assert not os.path.exists(env_var_1)
        assert not os.path.exists(env_var_2)


class TestEdgeCases:
    def test_raises_on_project_with_missing_id(self) -> None:
        mock_client = MagicMock()
        mock_valid = MagicMock()
        mock_valid.project_id = "valid-project"
        mock_valid.display_name = "Valid"
        mock_invalid = MagicMock()
        mock_invalid.project_id = None
        mock_invalid.display_name = "Invalid"
        mock_client.search_projects.return_value = iter([mock_valid, mock_invalid])

        with pytest.raises(GCPProjectDiscoveryError, match="without project_id"):
            list(_search_projects_with_retry(mock_client, "state:ACTIVE"))

    def test_empty_explicit_list_returns_empty(self) -> None:
        projects = get_projects_from_explicit_list([])
        assert projects == []

    def test_explicit_list_all_filtered_raises_error(self) -> None:
        with pytest.raises(
            GCPProjectDiscoveryError, match="excludes ALL 2 explicitly configured"
        ):
            get_projects_from_explicit_list(
                ["dev-1", "dev-2"],
                AllowDenyPattern(allow=["prod-.*"]),
            )

    def test_project_uses_id_as_name_fallback(self) -> None:
        mock_client = MagicMock()
        mock_project = MagicMock()
        mock_project.project_id = "my-project"
        mock_project.display_name = None
        mock_client.search_projects.return_value = iter([mock_project])

        projects = list(_search_projects_with_retry(mock_client, "state:ACTIVE"))
        assert projects[0].name == "my-project"

    def test_empty_credentials_dict_accepted(self) -> None:
        """Empty credentials dict should be handled gracefully"""
        with gcp_credentials_context({}):
            pass

    def test_very_large_project_list(self) -> None:
        """Large project lists should be handled efficiently"""
        large_list = [f"project-{i:04d}" for i in range(1000)]
        validate_project_id_list(large_list)

    def test_special_characters_in_labels(self) -> None:
        """Labels with hyphens, underscores should work"""
        valid_labels = [
            "cost-center:eng-123",
            "team_name:data-platform",
            "environment:prod-us-west-2",
        ]
        validate_project_label_list(valid_labels)

    def test_pattern_with_no_matches(self) -> None:
        """Pattern that matches nothing should raise clear error"""
        projects = [
            GCPProject(id="prod-project-1", name="Prod 1"),
            GCPProject(id="prod-project-2", name="Prod 2"),
        ]
        pattern = AllowDenyPattern(allow=["^dev-.*"])
        with pytest.raises(GCPProjectDiscoveryError, match="all were excluded"):
            _filter_and_validate(projects, pattern, "test")

    @patch("datahub.ingestion.source.common.gcp_project_utils.get_projects_by_labels")
    @patch(
        "datahub.ingestion.source.common.gcp_project_utils.get_projects_from_explicit_list"
    )
    def test_both_ids_and_labels_provided(
        self, mock_explicit: MagicMock, mock_labels: MagicMock
    ) -> None:
        """Providing both IDs and labels should prioritize project_ids"""
        mock_explicit.return_value = [GCPProject(id="project-123", name="P1")]
        mock_labels.return_value = [GCPProject(id="project-456", name="P2")]
        result = get_projects(project_ids=["project-123"], project_labels=["env:prod"])
        assert len(result) == 1
        assert result[0].id == "project-123"
        mock_explicit.assert_called_once()
        mock_labels.assert_not_called()


class TestTemporaryCredentialsFile:
    def test_creates_and_cleans_up_file(self) -> None:
        creds = {"type": "service_account", "project_id": "test-project"}

        with temporary_credentials_file(creds) as cred_path:
            assert os.path.exists(cred_path)
            assert cred_path.endswith(".json")
            with open(cred_path) as f:
                saved_creds = json.load(f)
            assert saved_creds == creds

        assert not os.path.exists(cred_path)

    def test_temporary_credentials_file_empty_dict(self) -> None:
        """Empty credentials dict should still create file"""
        with temporary_credentials_file({}) as filepath:
            assert os.path.exists(filepath)

    def test_cleans_up_on_exception(self) -> None:
        creds = {"type": "service_account"}
        cred_path = None

        with pytest.raises(ValueError), temporary_credentials_file(creds) as path:
            cred_path = path
            assert os.path.exists(cred_path)
            raise ValueError("Simulated error")

        assert cred_path is not None
        assert not os.path.exists(cred_path)

    def test_sets_environment_variable(self) -> None:
        creds = {"type": "service_account", "client_email": "test@example.com"}
        original_env = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        try:
            with temporary_credentials_file(creds) as cred_path:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path
                assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == cred_path
        finally:
            if original_env:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = original_env
            elif "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
                del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    def test_handles_already_deleted_file(self) -> None:
        creds = {"type": "service_account"}

        with temporary_credentials_file(creds) as cred_path:
            os.unlink(cred_path)

    def test_cleanup_failure_logs_error(self, caplog: pytest.LogCaptureFixture) -> None:
        """Cleanup should log error when OSError occurs (e.g., permission denied)."""
        creds = {"type": "service_account", "project_id": "test"}

        with (
            patch(
                "datahub.ingestion.source.common.gcp_project_utils.os.unlink"
            ) as mock_unlink,
            caplog.at_level(logging.ERROR),
        ):
            mock_unlink.side_effect = OSError("Permission denied")
            with temporary_credentials_file(creds):
                pass

        assert any(
            "SECURITY: Failed to cleanup credentials file" in record.message
            for record in caplog.records
        )

    def test_restricts_file_permissions(self) -> None:
        """Temporary credentials file should have restrictive permissions (0600)."""
        creds = {"type": "service_account", "project_id": "test"}
        cred_path = None

        with temporary_credentials_file(creds) as path:
            cred_path = path
            stat_info = os.stat(cred_path)
            file_mode = stat_info.st_mode & 0o777
            assert file_mode == 0o600, (
                f"Expected 0600 permissions, got {oct(file_mode)}"
            )


class TestPatternValidation:
    """Tests for project pattern matching validation"""

    def test_validate_pattern_before_discovery_allows_default(self) -> None:
        """Default allow-all pattern should be allowed"""
        pattern = AllowDenyPattern.allow_all()
        _validate_pattern_before_discovery(pattern, "label-based")

    def test_validate_pattern_before_discovery_rejects_deny_only(self) -> None:
        """Deny-only patterns should raise for discovery methods"""
        pattern = AllowDenyPattern(deny=[".*"])
        with pytest.raises(GCPProjectDiscoveryError, match="[Dd]eny"):
            _validate_pattern_before_discovery(pattern, "label-based")

    def test_validate_pattern_against_explicit_list_allows_default(self) -> None:
        """Default allow-all pattern should be allowed with explicit list"""
        pattern = AllowDenyPattern.allow_all()
        _validate_pattern_against_explicit_list(pattern, ["project-123"])

    def test_validate_pattern_against_explicit_list_warns_on_allow(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Allow pattern with explicit list should issue warning"""
        pattern = AllowDenyPattern(allow=["project-.*"])
        projects = ["project-123", "project-456"]
        with caplog.at_level(logging.WARNING):
            _validate_pattern_against_explicit_list(pattern, projects)


class TestFailFastPatternValidation:
    def test_deny_all_pattern_fails_fast(self) -> None:
        pattern = AllowDenyPattern(deny=[".*"])
        with pytest.raises(GCPProjectDiscoveryError, match="blocks ALL projects"):
            _validate_pattern_before_discovery(pattern, "auto-discovery")

    def test_deny_all_with_explicit_allow_passes(self) -> None:
        pattern = AllowDenyPattern(allow=["prod-.*"], deny=[".*"])
        _validate_pattern_before_discovery(pattern, "auto-discovery")

    def test_glob_syntax_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        pattern = AllowDenyPattern(allow=["prod-*"])
        with caplog.at_level(logging.WARNING):
            _validate_pattern_before_discovery(pattern, "auto-discovery")
        assert "looks like glob syntax" in caplog.text
        assert "Did you mean" in caplog.text

    def test_explicit_list_all_filtered_fails_fast(self) -> None:
        pattern = AllowDenyPattern(allow=["prod-.*"])
        with pytest.raises(GCPProjectDiscoveryError, match="excludes ALL"):
            _validate_pattern_against_explicit_list(pattern, ["dev-1", "dev-2"])

    def test_explicit_list_partial_filter_warns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        pattern = AllowDenyPattern(allow=["prod-.*"])
        with caplog.at_level(logging.WARNING):
            _validate_pattern_against_explicit_list(
                pattern, ["prod-app", "dev-1", "dev-2"]
            )
        assert "will exclude 2 of 3" in caplog.text

    def test_explicit_list_no_filter_passes_silently(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        pattern = AllowDenyPattern.allow_all()
        with caplog.at_level(logging.WARNING):
            _validate_pattern_against_explicit_list(
                pattern, ["prod-app", "dev-1", "dev-2"]
            )
        assert "will exclude" not in caplog.text


class TestGcpCredentialsContext:
    def test_with_credentials_sets_env_var(self) -> None:
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
        creds = {"type": "service_account", "project_id": "test"}
        with gcp_credentials_context(creds):
            assert "GOOGLE_APPLICATION_CREDENTIALS" in os.environ
            assert os.path.exists(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
        assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ

    def test_without_credentials_uses_adc(self) -> None:
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
        with gcp_credentials_context(None):
            assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ

    def test_cleans_up_temp_file_on_exit(self) -> None:
        """Verify temporary credential file is deleted after context exits."""
        creds = {"type": "service_account", "project_id": "test"}
        temp_path = None
        with gcp_credentials_context(creds):
            temp_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
            assert os.path.exists(temp_path)
        assert not os.path.exists(temp_path)

    def test_cleans_up_on_exception(self) -> None:
        """Verify cleanup happens even when exception is raised inside context."""
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
        creds = {"type": "service_account", "project_id": "test"}
        temp_path = None

        with pytest.raises(RuntimeError), gcp_credentials_context(creds):
            temp_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
            raise RuntimeError("simulated failure")

        assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ
        assert temp_path is not None
        assert not os.path.exists(temp_path)

    def test_restores_original_env_var_on_exception(self) -> None:
        """Verify original GOOGLE_APPLICATION_CREDENTIALS is restored after exception."""
        original_path = "/original/credentials.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = original_path
        creds = {"type": "service_account", "project_id": "test"}

        try:
            with pytest.raises(RuntimeError), gcp_credentials_context(creds):
                raise RuntimeError("simulated failure")
        finally:
            assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == original_path
            os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)

    def test_handles_externally_deleted_file(self) -> None:
        """Verify graceful handling when temp file is deleted externally (container env)."""
        creds = {"type": "service_account", "project_id": "test"}
        with gcp_credentials_context(creds):
            temp_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
            os.unlink(temp_path)


class TestWithTemporaryCredentials:
    def test_sets_and_restores_env_var(self) -> None:
        original = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        with with_temporary_credentials("/tmp/test_creds.json"):
            assert (
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == "/tmp/test_creds.json"
            )
        assert os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") == original

    def test_restores_none_when_not_set(self) -> None:
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
        with with_temporary_credentials("/tmp/test_creds.json"):
            assert (
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == "/tmp/test_creds.json"
            )
        assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ

    def test_restores_on_exception(self) -> None:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/original/path.json"
        try:
            with (
                pytest.raises(ValueError),
                with_temporary_credentials("/tmp/test_creds.json"),
            ):
                raise ValueError("test error")
        finally:
            assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == "/original/path.json"
            os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
