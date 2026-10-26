from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple
from unittest import mock

import pytest
from tableauserverclient.models import ProjectItem, UserItem

from tests.integration.tableau.tableau_mocks import (
    mock_data,
    mock_sdk_client,
    side_effect_datasource_data,
    side_effect_project_data,
)
from tests.test_helpers.probe_parity import (
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

# The parity harness masks its reports against the process-global registry, as
# the CLI does; a secret an earlier test registered would otherwise redact any
# of this fixture's identifiers that contain it.
pytestmark = [
    pytest.mark.integration,
    pytest.mark.usefixtures("_isolate_secret_registry"),
]

# The fixture's project tree, with a third level added under DenyProject:
#   default / DenyProject / Nested
#   Project 2
#   Samples
_NESTED_ID = "5b9f2a8e-0c1d-4e7f-9a3b-6c2d8e1f4a70"
_DENY_PROJECT_ID = "79d02655-88e5-45a6-9f9b-eeaf5fe54903"


def _projects_with_a_third_level(*args: Any, **kwargs: Any) -> Tuple[List, Any]:
    projects, pagination = side_effect_project_data()
    nested = ProjectItem(name="Nested")
    nested._id = _NESTED_ID
    nested.parent_id = _DENY_PROJECT_ID
    return [*projects, nested], pagination


@pytest.fixture
def _tableau_server() -> Iterator[None]:
    # One mocked server for both sides: ingestion's sign-in and the probe's
    # (make_tableau_client) both build it here.
    with mock.patch("datahub.ingestion.source.tableau.tableau.Server") as server:
        client = mock_sdk_client(
            side_effect_query_metadata_response=mock_data(),
            datasources_side_effect=side_effect_datasource_data,
            sign_out_side_effect=lambda: None,
        )
        client.projects.get.side_effect = _projects_with_a_third_level
        # The role complete ingestion needs, so neither side warns about it.
        client.users.get_by_id.return_value = UserItem(
            name="probe-user", site_role="SiteAdministratorExplorer"
        )
        server.return_value = client
        yield


def _recipe(**overrides: Any) -> Dict[str, Any]:
    return {
        "username": "username",
        "password": "test_password",
        "connect_uri": "https://do-not-connect",
        "site": "acryl",
        "page_size": 1000,
        **overrides,
    }


def _leaf(path: str) -> str:
    return path.rsplit("/", 1)[-1]


# Ingestion names a project container by its bare name, and the fixture's
# names are unique, so the last path segment identifies a project.
_PROJECTS = ParityListing(
    "projects",
    "projects",
    lambda index: index.container_names("Project"),
    identity=lambda record: _leaf(record.name),
)
_SITES = ParityListing("sites", "sites", lambda index: index.container_names("Site"))


@pytest.mark.usefixtures("_tableau_server")
def test_hierarchy_readmission_of_nested_projects_matches_ingestion(
    tmp_path: Path,
) -> None:
    report = assert_probe_parity(
        "tableau",
        _recipe(
            project_pattern={"allow": ["^default$", "^Samples$"]},
            extract_project_hierarchy=True,
        ),
        pipeline_ingestion("tableau", tmp_path),
        [_PROJECTS],
    )
    assert report.excluded_by("projects") == {"Project 2": "project_pattern"}
    # Nested is re-admitted under a parent that is itself only re-admitted,
    # which ingestion's one pass decides in listing order; the probe says so.
    assert any("listing order" in w for w in report.kinds["projects"].warnings)


@pytest.mark.usefixtures("_tableau_server")
def test_a_denied_child_and_everything_below_it_match_ingestion(
    tmp_path: Path,
) -> None:
    report = assert_probe_parity(
        "tableau",
        _recipe(
            project_pattern={
                "allow": ["^default$", "^Project 2$", "^Samples$"],
                "deny": ["^DenyProject$"],
            },
            extract_project_hierarchy=True,
        ),
        pipeline_ingestion("tableau", tmp_path),
        [_PROJECTS],
    )
    # Keyed by identity, the bare name. Nested is not denied, but its parent
    # is no longer included to re-admit it.
    assert report.excluded_by("projects") == {
        "DenyProject": "project_pattern",
        "Nested": "project_pattern",
    }


@pytest.mark.usefixtures("_tableau_server")
def test_project_path_pattern_on_nested_paths_matches_ingestion(
    tmp_path: Path,
) -> None:
    report = assert_probe_parity(
        "tableau",
        _recipe(project_path_pattern={"allow": ["^default"]}),
        pipeline_ingestion("tableau", tmp_path),
        [_PROJECTS],
    )
    assert set(report.excluded_by("projects")) == {"Project 2", "Samples"}


@pytest.mark.usefixtures("_tableau_server")
def test_a_suspended_site_is_excluded_as_ingestion_skips_it(tmp_path: Path) -> None:
    # Site 3 is Suspended and matches the pattern; Acryl does not match.
    report = assert_probe_parity(
        "tableau",
        _recipe(
            ingest_multiple_sites=True,
            add_site_container=True,
            site_name_pattern={"allow": ["^Site.*$"]},
        ),
        pipeline_ingestion("tableau", tmp_path),
        [_SITES],
    )
    assert report.excluded_by("sites") == {
        "Acryl": "site_name_pattern",
        "Site 3": "site_state",
    }
