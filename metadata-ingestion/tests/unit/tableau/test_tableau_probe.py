import json
from typing import Any, Dict, List, Optional, Sequence, Tuple
from unittest import mock

import pytest
from tableauserverclient import ProjectItem, SiteItem, UserItem
from tableauserverclient.server.endpoint.exceptions import ServerResponseError

from datahub.ingestion.agent.probe_methods import ProbeMethodResult, run_probe_method
from datahub.ingestion.source.tableau.tableau import TableauConfig
from datahub.ingestion.source.tableau.tableau_probe import TableauMetadataProbe

_RECIPE: Dict[str, object] = {
    "connect_uri": "https://tableau.example.com",
    "site": "my-site",
    "token_name": "probe",
    "token_value": "secret-token",
}


def _page(items: Sequence[Any]) -> Tuple[List[Any], Any]:
    # Shape copied from tests/integration/tableau/test_tableau_ingest.py.
    pagination = mock.MagicMock()
    pagination.total_available = None
    return list(items), pagination


def _project(pid: str, name: str, parent: Optional[str] = None) -> ProjectItem:
    item = ProjectItem(name=name)
    item._id = pid
    item.parent_id = parent
    return item


def _server(projects: Sequence[ProjectItem] = ()) -> mock.MagicMock:
    server = mock.MagicMock()
    server.site_id = "site-luid"
    server.user_id = "user-luid"
    server.version = "3.21"
    user = UserItem(name="probe-user", site_role="SiteAdministratorExplorer")
    server.users.get_by_id.return_value = user
    server.projects.get.side_effect = lambda *a, **k: _page(projects)
    return server


def _probe(server: mock.MagicMock, **config: object) -> TableauMetadataProbe:
    return TableauMetadataProbe(
        TableauConfig.model_validate({**_RECIPE, **config}), server
    )


def _run(server: mock.MagicMock, command: str, **kwargs: object) -> ProbeMethodResult:
    with mock.patch.object(TableauConfig, "make_tableau_client", return_value=server):
        return run_probe_method("tableau", dict(_RECIPE), command, dict(kwargs))


def test_projects_are_paths_including_ones_the_recipe_excludes() -> None:
    server = _server([_project("1", "Sales"), _project("2", "EMEA", "1")])
    probe = _probe(server, project_path_pattern={"deny": ["^Sales"]})
    assert probe.projects(limit=10) == ["Sales", "Sales/EMEA"]


def test_projects_use_the_recipe_separator() -> None:
    server = _server([_project("1", "Sales"), _project("2", "EMEA", "1")])
    assert _probe(server, project_path_separator="|").projects(limit=10) == [
        "Sales",
        "Sales|EMEA",
    ]


def test_a_project_whose_parent_is_hidden_is_reported_at_the_root() -> None:
    server = _server([_project("2", "EMEA", "not-visible")])
    result = _run(server, "projects")
    assert result.result == ["EMEA"]
    assert any("Incomplete project hierarchy" in w for w in result.warnings)


def test_a_forbidden_sites_listing_degrades_with_a_warning() -> None:
    server = _server()
    server.sites.get.side_effect = ServerResponseError("403069", "Forbidden", "no")
    probe = _probe(server)
    assert probe.sites(limit=10) == []
    assert probe.warnings and "403069" in probe.warnings[0]


def test_a_server_error_on_sites_is_raised() -> None:
    server = _server()
    server.sites.get.side_effect = ServerResponseError("500000", "Internal", "boom")
    with pytest.raises(ServerResponseError):
        _probe(server).sites(limit=10)


def test_sites_report_name_content_url_and_state() -> None:
    server = _server()
    site = SiteItem(name="Finance", content_url="finance")
    site.state = "Suspended"
    server.sites.get.side_effect = lambda *a, **k: _page([site])
    assert _probe(server).sites(limit=10) == [
        {"name": "Finance", "content_url": "finance", "state": "Suspended"}
    ]


def test_site_reports_the_role_but_not_the_user_name() -> None:
    server = _server()
    server.sites.get_by_id.return_value = SiteItem(
        name="My Site", content_url="my-site"
    )
    detail = _probe(server).site()
    assert detail["site_role"] == "SiteAdministratorExplorer"
    assert detail["site_administrator_explorer"] is True
    assert "probe-user" not in json.dumps(detail)


def test_leaving_the_probe_signs_out() -> None:
    server = _server()
    with _probe(server):
        pass
    server.auth.sign_out.assert_called_once()
