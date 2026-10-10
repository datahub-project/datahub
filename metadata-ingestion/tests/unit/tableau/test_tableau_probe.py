import json
import logging
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple
from unittest import mock

import pytest
from tableauserverclient import ProjectItem, Server, SiteItem, UserItem, WorkbookItem
from tableauserverclient.server.endpoint.exceptions import (
    InternalServerError,
    ServerResponseError,
)

from datahub.ingestion.agent.probe_methods import ProbeMethodResult, run_probe_method
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeSoftError,
)
from datahub.ingestion.source.tableau.tableau import (
    TableauConfig,
    parse_database_server_hostname,
)
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


class _Endpoint:
    """A TSC endpoint whose `get` is visible to a static attribute lookup.

    TSC.Pager tells an endpoint from a callable with runtime-checkable
    Protocols. From Python 3.12 those use inspect.getattr_static, which does
    not see a MagicMock's lazily created `get`, so a bare MagicMock is paged
    as a callable and the pager unpacks the mock itself.
    """

    def __init__(self) -> None:
        self._other = mock.MagicMock()
        self.get = mock.MagicMock()

    def __getattr__(self, name: str) -> Any:
        # Everything but `get` (get_by_id, ...) stays an ordinary mock.
        return getattr(self._other, name)


def _server(projects: Sequence[ProjectItem] = ()) -> mock.MagicMock:
    server = mock.MagicMock()
    for endpoint in ("sites", "projects", "workbooks"):
        setattr(server, endpoint, _Endpoint())
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


def _workbook(wid: str, name: str, project_id: str) -> WorkbookItem:
    item = WorkbookItem(project_id=project_id, name=name)
    item._id = wid
    return item


def _server_with_workbooks(
    projects: Sequence[ProjectItem], workbooks: Sequence[WorkbookItem]
) -> mock.MagicMock:
    server = _server(projects)
    server.workbooks.get.side_effect = lambda *a, **k: _page(workbooks)
    return server


def test_workbooks_resolve_the_project_by_luid_not_name() -> None:
    projects = [
        _project("f", "Finance"),
        _project("fr", "Reports", "f"),
        _project("s", "Sales"),
        _project("sr", "Reports", "s"),
    ]
    workbooks = [_workbook("w1", "Budget", "fr"), _workbook("w2", "Pipeline", "sr")]
    probe = _probe(_server_with_workbooks(projects, workbooks))
    assert probe.workbooks("Finance/Reports", limit=10) == ["Budget"]


def test_an_unknown_project_path_is_a_bad_argument() -> None:
    probe = _probe(_server_with_workbooks([_project("1", "Sales")], []))
    with pytest.raises(ValueError) as excinfo:
        probe.workbooks("Nope", limit=10)
    assert not isinstance(excinfo.value, ProbeSoftError)


def test_a_forbidden_projects_listing_degrades_the_workbooks_listing() -> None:
    server = _server_with_workbooks([], [])
    server.projects.get.side_effect = ServerResponseError("403004", "Forbidden", "no")
    probe = _probe(server)
    assert probe.workbooks("Sales", limit=10) == []
    assert probe.warnings and "403004" in probe.warnings[0]


def test_workbooks_carry_their_project_as_the_parent() -> None:
    server = _server_with_workbooks(
        [_project("1", "Sales")], [_workbook("w1", "Revenue", "1")]
    )
    result = _run(server, "workbooks", project_path="Sales")
    assert (result.kind, result.parent_path, result.result) == (
        "Workbook",
        ["Sales"],
        ["Revenue"],
    )


def test_a_project_name_with_a_comma_is_not_sent_as_a_server_filter() -> None:
    server = _server_with_workbooks(
        [_project("1", "Sales, EMEA")], [_workbook("w1", "Revenue", "1")]
    )
    assert _probe(server).workbooks("Sales, EMEA", limit=10) == ["Revenue"]
    options = server.workbooks.get.call_args[0][0]
    assert list(options.filter) == []


def test_a_url_connection_is_reduced_to_its_host_as_ingestion_does() -> None:
    assert (
        parse_database_server_hostname("https://db.example.com:5432/x")
        == "db.example.com"
    )
    assert parse_database_server_hostname("db.example.com") == "db.example.com"
    assert parse_database_server_hostname(None) is None


def test_database_servers_report_what_the_routing_maps_key_on() -> None:
    probe = _probe(_server())
    rows: List[Dict[str, object]] = [
        {
            "id": "ds1",
            "name": "warehouse",
            "hostName": "https://db.example.com",
            "connectionType": "snowflake",
        },
        {
            "id": "ds2",
            "name": "mart",
            "hostName": "mart.example.com",
            "connectionType": "postgres",
        },
    ]
    consumed: List[str] = []

    def objects(**_: object) -> Iterator[Dict[str, object]]:
        for row in rows:
            consumed.append(str(row["id"]))
            yield row

    with mock.patch.object(probe._site, "get_connection_objects", side_effect=objects):
        assert probe.database_servers(limit=1) == [
            {
                "id": "ds1",
                "name": "warehouse",
                "host_name": "db.example.com",
                "connection_type": "snowflake",
            }
        ]
    # Stops at the limit rather than paging everything.
    assert consumed == ["ds1"]


def test_an_insufficient_role_never_reports_the_user_name() -> None:
    server = _server()
    server.users.get_by_id.return_value = UserItem(
        name="probe-user", site_role="Explorer"
    )
    server.sites.get_by_id.return_value = SiteItem(
        name="My Site", content_url="my-site"
    )
    result = _run(server, "site")
    assert isinstance(result.result, dict)
    assert result.result["site_administrator_explorer"] is False
    assert any("Insufficient Permissions" in w for w in result.warnings)
    assert "probe-user" not in json.dumps(result.result)
    assert not any("probe-user" in w for w in result.warnings)


def test_a_forbidden_listing_warning_carries_no_server_text() -> None:
    # A warning is text the provider builds, which the framework only scrubs
    # by shape; the server's summary and detail must not be in it at all.
    server = _server()
    server.sites.get.side_effect = ServerResponseError(
        "403069", "PLANTED-summary-text", "PLANTED-detail-text"
    )
    probe = _probe(server)
    assert probe.sites(limit=10) == []
    assert probe.warnings and "403069" in probe.warnings[0]
    assert not any("PLANTED" in w for w in probe.warnings)


def test_a_failed_sign_out_logs_the_class_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = _server()
    server.auth.sign_out.side_effect = RuntimeError("PLANTED-sign-out-text")
    caplog.set_level(logging.WARNING)
    _probe(server).__exit__(None, None, None)
    assert "RuntimeError" in caplog.text
    assert "PLANTED" not in caplog.text


def test_a_server_error_is_labelled_with_its_tableau_code_not_its_text() -> None:
    server = _server()
    server.sites.get.side_effect = ServerResponseError(
        "500000", "PLANTED-summary-text", "PLANTED-detail-text"
    )
    with pytest.raises(ProbeConnectionError) as excinfo:
        _run(server, "sites")
    assert "(ServerResponseError; Tableau 500000)" in str(excinfo.value)
    assert "PLANTED" not in str(excinfo.value)


def test_a_failed_sign_in_is_labelled_with_the_code_ingestion_wrapped() -> None:
    def sign_in(site: str) -> Server:
        # make_tableau_client re-raises a sign-in failure as a ValueError
        # carrying the server's text, from the TSC error.
        cause = ServerResponseError("401002", "PLANTED-summary", "PLANTED-detail")
        raise ValueError(f"Unable to login: {cause}") from cause

    with (
        mock.patch.object(TableauConfig, "make_tableau_client", side_effect=sign_in),
        pytest.raises(ProbeConnectionError) as excinfo,
    ):
        run_probe_method("tableau", dict(_RECIPE), "site", {})
    assert "(ValueError; Tableau 401002)" in str(excinfo.value)
    assert "PLANTED" not in str(excinfo.value)


def test_only_a_tsc_code_of_the_documented_shape_is_read() -> None:
    response = mock.MagicMock(status_code=503, content=b"PLANTED")
    read = TableauMetadataProbe.probe_error_code
    assert read(InternalServerError(response, "https://x")) == "HTTP 503"
    assert read(ServerResponseError("403069", "s", "d")) == "Tableau 403069"
    assert read(ServerResponseError("403 denied", "s", "d")) is None
    assert read(OSError("not tableau")) is None


def test_an_unknown_project_path_reaches_the_caller_with_its_message() -> None:
    server = _server_with_workbooks([_project("1", "Sales")], [])
    with pytest.raises(ProbeArgumentError, match="no project with path 'Nope'"):
        _run(server, "workbooks", project_path="Nope")
