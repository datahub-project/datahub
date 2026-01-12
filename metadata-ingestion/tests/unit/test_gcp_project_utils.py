import json
import logging
import os
from typing import List
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import (
    DeadlineExceeded,
    GoogleAPICallError,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
)

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.common.gcp_project_utils import (
    GCPProject,
    GCPProjectDiscoveryError,
    _filter_and_validate,
    _filter_projects_by_pattern,
    _is_rate_limit_error,
    _search_projects_with_retry,
    _validate_pattern_against_explicit_list,
    _validate_pattern_before_discovery,
    get_projects,
    get_projects_by_labels,
    get_projects_from_explicit_list,
    temporary_credentials_file,
    with_temporary_credentials,
)


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
            (PermissionDenied("No access"), "Permission denied"),
            (GoogleAPICallError("API error"), "GCP API error"),
            (DeadlineExceeded("Timeout"), "timeout"),
            (ServiceUnavailable("Down"), "unavailable"),
        ],
    )
    def test_error_handling(self, exception: Exception, error_match: str) -> None:
        mock_client = MagicMock()
        mock_client.search_projects.side_effect = exception
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
            (PermissionDenied("No access"), "Permission denied"),
            (DeadlineExceeded("Timeout"), "timeout"),
            (ServiceUnavailable("Down"), "unavailable"),
        ],
    )
    def test_error_handling(self, exception: Exception, error_match: str) -> None:
        mock_client = MagicMock()
        mock_client.search_projects.side_effect = exception
        with pytest.raises(GCPProjectDiscoveryError, match=error_match):
            get_projects_by_labels(["env:prod"], mock_client)


class TestRateLimitDetection:
    @pytest.mark.parametrize(
        "exception,expected",
        [
            (ResourceExhausted("Quota exceeded"), True),
            (GoogleAPICallError("quota limits"), True),
            (GoogleAPICallError("rate limit exceeded"), True),
            (GoogleAPICallError("Invalid argument"), False),
            (PermissionDenied("Access denied"), False),
            (ValueError("Other error"), False),
        ],
    )
    def test_detection(self, exception: Exception, expected: bool) -> None:
        assert _is_rate_limit_error(exception) == expected


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

    def test_cleans_up_on_exception(self) -> None:
        creds = {"type": "service_account"}
        cred_path = None

        with (
            pytest.raises(ValueError),
            temporary_credentials_file(creds) as path,
        ):
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
        with (
            pytest.raises(ValueError),
            with_temporary_credentials("/tmp/test_creds.json"),
        ):
            raise ValueError("test error")
        assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == "/original/path.json"
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
