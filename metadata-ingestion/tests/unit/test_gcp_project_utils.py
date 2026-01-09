import logging
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
    _filter_projects_by_pattern,
    _is_rate_limit_error,
    _search_projects_with_retry,
    _validate_and_filter_projects,
    get_projects,
    get_projects_by_labels,
    get_projects_from_explicit_list,
    list_all_accessible_projects,
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
            (DeadlineExceeded("Timeout"), "timed out"),
            (ServiceUnavailable("Down"), "unavailable"),
        ],
    )
    def test_error_handling(self, exception: Exception, error_match: str) -> None:
        mock_client = MagicMock()
        mock_client.search_projects.side_effect = exception
        with pytest.raises(GCPProjectDiscoveryError, match=error_match):
            list(list_all_accessible_projects(mock_client))

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
        with pytest.raises(GCPProjectDiscoveryError, match="No projects match labels"):
            get_projects_by_labels(["env:prod"], mock_client)

    @pytest.mark.parametrize(
        "exception,error_match",
        [
            (PermissionDenied("No access"), "Permission denied"),
            (DeadlineExceeded("Timeout"), "timed out"),
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
        with pytest.raises(
            GCPProjectDiscoveryError, match="excluded by project_id_pattern"
        ):
            _validate_and_filter_projects(projects, pattern, "test source")


class TestEdgeCases:
    def test_skips_project_with_missing_id(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_client = MagicMock()
        mock_valid = MagicMock()
        mock_valid.project_id = "valid-project"
        mock_valid.display_name = "Valid"
        mock_invalid = MagicMock()
        mock_invalid.project_id = None
        mock_invalid.display_name = "Invalid"
        mock_client.search_projects.return_value = iter([mock_valid, mock_invalid])

        with caplog.at_level(logging.ERROR):
            projects = list(_search_projects_with_retry(mock_client, "state:ACTIVE"))

        assert len(projects) == 1
        assert projects[0].id == "valid-project"
        assert "GCP returned project without project_id" in caplog.text
