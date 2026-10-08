import json
import pathlib
import re
from typing import Dict, Iterator, List, Optional, Sequence, Set

import pytest
import requests
import requests_mock
from click.testing import CliRunner, Result

import datahub.cli.recipe_cli as rc
from datahub.cli.recipe_cli import recipe as recipe_cli
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import ProbeMethodResult, run_probe_method
from datahub.metadata.urns import DashboardUrn
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    JudgedRecord,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

pytestmark = pytest.mark.usefixtures("_isolate_secret_registry")

_URL = "http://grafana.example.com"
_TOKEN = "glsa_not_a_real_token"
_RECIPE: Dict[str, object] = {"url": _URL, "service_account_token": _TOKEN}
_FOLDER = "Folder"
_DASHBOARD = "Dashboard"
_Request = requests_mock.request._RequestObjectProxy
_Context = requests_mock.response._Context


class _FakeGrafana:
    """The endpoints Grafana ingestion and the probe read, from one fixture,
    so both see the same instance."""

    def __init__(
        self,
        folders: Sequence[Dict[str, object]],
        dashboards: Sequence[Dict[str, object]],
    ) -> None:
        self.folders = list(folders)
        self.dashboards = list(dashboards)

    def install(self, mocker: requests_mock.Mocker) -> None:
        mocker.get(f"{_URL}/api/folders", json=self._folders)
        mocker.get(f"{_URL}/api/search", json=self._search)
        mocker.get(
            re.compile(re.escape(f"{_URL}/api/dashboards/uid/") + ".+"),
            json=self._dashboard,
        )

    @staticmethod
    def _page(items: List[Dict[str, object]], qs: Dict[str, List[str]]) -> object:
        page = int(qs.get("page", ["1"])[0])
        limit = int(qs.get("limit", ["1000"])[0])
        return items[(page - 1) * limit : page * limit]

    def _folders(self, request: _Request, context: _Context) -> object:
        return self._page(self.folders, request.qs)

    def _search(self, request: _Request, context: _Context) -> object:
        qs = request.qs
        hits = [
            {
                "id": index,
                "uid": d["uid"],
                "title": d["title"],
                "url": f"/d/{d['uid']}",
                "uri": f"db/{d['uid']}",
                "type": "dash-db",
                **({"folderTitle": d["folder"]} if d.get("folder") else {}),
            }
            for index, d in enumerate(self.dashboards, start=1)
        ]
        if qs.get("type") == ["dash-db"]:
            return self._page(hits, qs)
        # Untyped, as basic mode asks: Grafana returns folders as well.
        folder_hits = [
            {
                "id": 100 + index,
                "uid": f["uid"],
                "title": f["title"],
                "url": f"/dashboards/f/{f['uid']}",
                "uri": f"db/{f['uid']}",
                "type": "dash-folder",
            }
            for index, f in enumerate(self.folders)
        ]
        return self._page(folder_hits + hits, qs)

    def _dashboard(self, request: _Request, context: _Context) -> object:
        uid = request.path_url.rsplit("/", 1)[-1]
        found = next(d for d in self.dashboards if str(d["uid"]).lower() == uid)
        folder = next(
            (f for f in self.folders if f["title"] == found.get("folder")), None
        )
        return {
            "dashboard": {"uid": found["uid"], "title": found["title"], "panels": []},
            "meta": {"folderId": folder["id"] if folder else None},
        }


def _folder(fid: int, title: str) -> Dict[str, object]:
    return {"id": fid, "uid": f"f{fid}", "title": title}


def _dash(uid: str, title: str, folder: Optional[str] = None) -> Dict[str, object]:
    return {"uid": uid, "title": title, "folder": folder}


_FIXTURE = _FakeGrafana(
    folders=[_folder(1, "Operations"), _folder(2, "Sandbox"), _folder(3, "Finance")],
    dashboards=[
        _dash("ops-1", "Ops Overview", "Operations"),
        _dash("sbx-1", "Sandbox Metrics", "Sandbox"),
        _dash("fin-1", "Revenue", "Finance"),
        _dash("gen-1", "Home"),
    ],
)


@pytest.fixture
def grafana() -> Iterator[_FakeGrafana]:
    with requests_mock.Mocker(case_sensitive=True) as mocker:
        _FIXTURE.install(mocker)
        yield _FIXTURE


def _run(command: str, **config: object) -> ProbeMethodResult:
    return run_probe_method("grafana", {**_RECIPE, **config}, command, {})


def _records(result: ProbeMethodResult) -> List[Dict[str, object]]:
    assert isinstance(result.result, list)
    return result.result


def test_folders_list_every_top_level_folder_by_title(grafana: _FakeGrafana) -> None:
    result = _run("folders", folder_pattern={"deny": ["Sandbox"]})
    assert result.kind == _FOLDER
    assert [r["name"] for r in _records(result)] == [
        "Operations",
        "Sandbox",
        "Finance",
    ]


def test_listings_page_as_ingestion_pages(grafana: _FakeGrafana) -> None:
    result = _run("dashboards", page_size=1)
    assert [r["uid"] for r in _records(result)] == [
        "ops-1",
        "sbx-1",
        "fin-1",
        "gen-1",
    ]


def test_dashboards_carry_their_uid_and_folder(grafana: _FakeGrafana) -> None:
    result = _run("dashboards")
    assert result.kind == _DASHBOARD
    assert _records(result)[0] == {
        "name": "Ops Overview",
        "uid": "ops-1",
        "type": "dash-db",
        "folder": "Operations",
    }


def test_basic_mode_lists_what_its_untyped_search_returns(
    grafana: _FakeGrafana,
) -> None:
    result = _run("dashboards", basic_mode=True)
    types = {r["type"] for r in _records(result)}
    assert types == {"dash-db", "dash-folder"}


def test_a_listing_stops_at_its_limit(grafana: _FakeGrafana) -> None:
    result = run_probe_method("grafana", dict(_RECIPE), "dashboards", {"limit": 2})
    assert len(_records(result)) == 2
    assert result.truncated


def test_the_token_is_sent_as_ingestion_sends_it(grafana: _FakeGrafana) -> None:
    with requests_mock.Mocker() as mocker:
        mocker.get(f"{_URL}/api/folders", json=[])
        _run("folders")
        assert mocker.last_request is not None
        assert mocker.last_request.headers["Authorization"] == f"Bearer {_TOKEN}"
        assert mocker.last_request.verify is True


def test_a_forbidden_listing_degrades_to_empty_with_a_warning() -> None:
    with requests_mock.Mocker() as mocker:
        mocker.get(f"{_URL}/api/folders", status_code=403, text="forbidden body")
        result = _run("folders")
    assert result.result == []
    assert result.warnings == [
        "folders listing returned HTTP 403; treating it as empty."
    ]


def test_basic_mode_says_its_folders_are_not_ingested(grafana: _FakeGrafana) -> None:
    assert any("basic_mode" in w for w in _run("folders", basic_mode=True).warnings)


def test_verify_ssl_off_is_reported_as_ingestion_reports_it(
    grafana: _FakeGrafana,
) -> None:
    result = _run("folders", verify_ssl=False)
    assert any("SSL" in w for w in result.warnings)


def _recipe_file(tmp_path: pathlib.Path) -> str:
    path = tmp_path / "r.yml"
    path.write_text("source:\n  type: grafana\n  config: {}\n")
    return str(path)


def _cli(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, command: str
) -> Result:
    monkeypatch.setattr(
        rc, "_resolve_for_probe", lambda _r: ("grafana", dict(_RECIPE), {_TOKEN})
    )
    monkeypatch.setattr(rc, "_ping_probe", lambda *a, **k: None)
    return CliRunner().invoke(
        recipe_cli, ["probe", "run", command, "--recipe", _recipe_file(tmp_path)]
    )


def test_a_good_listing_exits_zero(
    grafana: _FakeGrafana, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    res = _cli(monkeypatch, tmp_path, "dashboards")
    assert res.exit_code == rc.EXIT_OK, res.output
    assert "Ops Overview" in res.output
    assert _TOKEN not in res.output


@pytest.mark.parametrize("status", [401, 500])
def test_auth_failure_and_server_error_exit_on_the_source_code(
    status: int, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    with requests_mock.Mocker() as mocker:
        mocker.get(f"{_URL}/api/search", status_code=status, text="server text")
        res = _cli(monkeypatch, tmp_path, "dashboards")
    assert res.exit_code == rc.EXIT_CONNECTION, res.output
    assert f"HTTP {status}" in res.output
    assert "server text" not in res.output


def test_an_unreachable_server_exits_on_the_source_code(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    with requests_mock.Mocker() as mocker:
        mocker.get(f"{_URL}/api/folders", exc=requests.exceptions.ConnectionError)
        res = _cli(monkeypatch, tmp_path, "folders")
    assert res.exit_code == rc.EXIT_CONNECTION, res.output


def test_a_payload_that_is_not_a_listing_is_a_recorded_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    with requests_mock.Mocker() as mocker:
        mocker.get(f"{_URL}/api/folders", json={"message": "unexpected"})
        res = _cli(monkeypatch, tmp_path, "folders")
    assert res.exit_code == rc.EXIT_CONNECTION, res.output
    assert "not a list" in res.output


def test_an_unknown_parameter_is_the_callers_mistake(
    grafana: _FakeGrafana, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    monkeypatch.setattr(
        rc, "_resolve_for_probe", lambda _r: ("grafana", dict(_RECIPE), {_TOKEN})
    )
    monkeypatch.setattr(rc, "_ping_probe", lambda *a, **k: None)
    res = CliRunner().invoke(
        recipe_cli,
        [
            "probe",
            "run",
            "folders",
            "--recipe",
            _recipe_file(tmp_path),
            "--folder",
            "x",
        ],
    )
    assert res.exit_code == rc.EXIT_USER, res.output


def _verdicts(
    kind: str, names: List[str], parents: Sequence[str] = (), **config: object
) -> Dict[str, Optional[str]]:
    result = check_filters(
        source_type="grafana",
        config_dict={**_RECIPE, **config},
        kind=kind,
        parent_path=parents,
        names=names,
    )
    return {v.name: v.excluded_by for v in result.results}


def test_a_folder_is_excluded_by_basic_mode_whatever_the_pattern() -> None:
    assert _verdicts(_FOLDER, ["Operations"], basic_mode=True) == {
        "Operations": "basic_mode"
    }


def test_a_dashboard_in_an_excluded_folder_is_still_ingested() -> None:
    verdicts = _verdicts(
        _DASHBOARD,
        ["Sandbox Metrics"],
        parents=["Sandbox"],
        folder_pattern={"deny": ["Sandbox"]},
    )
    assert verdicts == {"Sandbox Metrics": None}


def _emitted_dashboards(index: EmittedIndex) -> Set[str]:
    return {DashboardUrn.from_string(u).dashboard_id for u in index.urns("dashboard")}


def _by_uid(record: JudgedRecord) -> str:
    return record.attributes["uid"]


@pytest.mark.parametrize(
    "case",
    [
        pytest.param({}, id="allow-all"),
        pytest.param(
            {
                "folder_pattern": {"deny": ["^Sandbox$"]},
                "dashboard_pattern": {"allow": ["^Ops", "^Revenue$"]},
            },
            id="both-patterns",
        ),
    ],
)
def test_probe_verdicts_match_enhanced_ingestion(
    grafana: _FakeGrafana, tmp_path: pathlib.Path, case: Dict[str, object]
) -> None:
    recipe = {**_RECIPE, "include_lineage": False, **case}
    report = assert_probe_parity(
        "grafana",
        recipe,
        pipeline_ingestion("grafana", tmp_path),
        [
            ParityListing(
                label="folders",
                command="folders",
                emitted=lambda index: index.container_names(_FOLDER),
            ),
            ParityListing(
                label="dashboards",
                command="dashboards",
                emitted=_emitted_dashboards,
                identity=_by_uid,
            ),
        ],
    )
    if case:
        assert report.excluded_by("folders") == {"Sandbox": "folder_pattern"}
        # The dashboard in the excluded folder is excluded by its own title.
        assert report.excluded_by("dashboards") == {
            "sbx-1": "dashboard_pattern",
            "gen-1": "dashboard_pattern",
        }


def test_probe_verdicts_match_basic_mode_ingestion(
    grafana: _FakeGrafana, tmp_path: pathlib.Path
) -> None:
    recipe = {
        **_RECIPE,
        "basic_mode": True,
        "dashboard_pattern": {"deny": ["^Revenue$"]},
    }
    report = assert_probe_parity(
        "grafana",
        recipe,
        pipeline_ingestion("grafana", tmp_path),
        [
            ParityListing(
                label="folders",
                command="folders",
                emitted=lambda index: index.container_names(_FOLDER),
                expect_empty=True,
                accept_warnings=(re.compile("basic_mode is set.*"),),
            ),
            ParityListing(
                label="dashboards",
                command="dashboards",
                emitted=_emitted_dashboards,
                identity=_by_uid,
            ),
        ],
    )
    assert set(report.excluded_by("folders").values()) == {"basic_mode"}
    assert report.excluded_by("dashboards") == {"fin-1": "dashboard_pattern"}
    # Basic mode's untyped search emits folders as dashboards, and the probe
    # lists them: three folders plus the three dashboards kept.
    assert len(report.kinds["dashboards"].included) == 6


def test_the_report_names_no_secret(grafana: _FakeGrafana) -> None:
    assert _TOKEN not in json.dumps(_run("dashboards").to_dict())
