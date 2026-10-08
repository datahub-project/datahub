import base64
import json
import pathlib
import re
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Sequence, Set, Tuple

import pytest
import requests
import requests_mock
from click.testing import CliRunner, Result

import datahub.cli.recipe_cli as rc
from datahub.cli.recipe_cli import recipe as recipe_cli
from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters
from datahub.ingestion.agent.probe_methods import ProbeMethodResult, run_probe_method
from datahub.ingestion.source.nifi import NifiProcessorType
from tests.test_helpers.probe_parity import (
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

pytestmark = pytest.mark.usefixtures("_isolate_secret_registry")

_SITE = "http://nifi.example.com:8080/nifi/"
_API = "http://nifi.example.com:8080/nifi-api/"
_PASSWORD = "test_password"
_RECIPE: Dict[str, object] = {"site_url": _SITE}
_KIND = "Process Group"
_Request = requests_mock.request._RequestObjectProxy
_Context = requests_mock.response._Context


@dataclass
class _Group:
    id: str
    name: str
    children: List["_Group"] = field(default_factory=list)
    # A ListS3 makes the group's components lineage-relevant, so ingestion
    # emits it as a container.
    ingress: bool = True
    readable: bool = True


@dataclass
class _FakeNifi:
    """The endpoints NiFi ingestion and the probe read, from one tree, so
    both see the same instance. `forbidden` group ids answer 403."""

    root: _Group
    forbidden: Tuple[str, ...] = ()
    failing: Tuple[str, ...] = ()

    def _groups(self) -> Iterator[Tuple[_Group, Optional[str]]]:
        stack: List[Tuple[_Group, Optional[str]]] = [(self.root, None)]
        while stack:
            group, parent = stack.pop()
            yield group, parent
            stack.extend((child, group.id) for child in group.children)

    def install(self, mocker: requests_mock.Mocker) -> None:
        mocker.get(f"{_API}flow/about", json={"about": {"version": "1.22.0"}})
        mocker.get(
            f"{_API}flow/cluster/summary", json={"clusterSummary": {"clustered": False}}
        )
        mocker.get(
            re.compile(re.escape(f"{_API}flow/process-groups/") + ".+"),
            json=self._flow,
        )
        query = f"{_API}provenance/query-1"
        mocker.post(f"{_API}provenance", json={"provenance": {"uri": query}})
        mocker.get(
            query,
            json={
                "provenance": {
                    "finished": True,
                    "results": {"provenanceEvents": [], "total": "0", "totalCount": 0},
                }
            },
        )
        mocker.delete(query, status_code=200)

    def _flow(self, request: _Request, context: _Context) -> object:
        group_id = request.path_url.rsplit("/", 1)[-1]
        if group_id in self.forbidden:
            context.status_code = 403
            return {"message": "forbidden"}
        if group_id in self.failing:
            context.status_code = 500
            return {"message": "server error"}
        for group, parent in self._groups():
            if group.id == group_id or (group_id == "root" and parent is None):
                return {"processGroupFlow": _flow_body(group, parent)}
        context.status_code = 404
        return {}


def _flow_body(group: _Group, parent: Optional[str]) -> Dict[str, object]:
    processors = (
        [
            {
                "component": {
                    "id": f"{group.id}-list",
                    "name": "ListS3",
                    "type": NifiProcessorType.ListS3,
                    "parentGroupId": group.id,
                    "config": {"schedulingPeriod": "1 min", "properties": {}},
                }
            }
        ]
        if group.ingress
        else []
    )
    return {
        "id": group.id,
        "parentGroupId": parent,
        "breadcrumb": {"breadcrumb": {"id": group.id, "name": group.name}},
        "flow": {
            "processors": processors,
            "processGroups": [
                {
                    "id": child.id,
                    **(
                        {
                            "component": {
                                "id": child.id,
                                "name": child.name,
                                "parentGroupId": group.id,
                            }
                        }
                        if child.readable
                        else {}
                    ),
                }
                for child in group.children
            ],
        },
    }


def _tree() -> _Group:
    return _Group(
        "root-id",
        "Main Flow",
        ingress=False,
        children=[
            _Group("g-ingest", "Ingest", [_Group("g-nested", "Nested Ingest")]),
            _Group("g-wip", "WIP Area", [_Group("g-inner", "Inner Keep")]),
        ],
    )


@pytest.fixture
def nifi() -> Iterator[requests_mock.Mocker]:
    with requests_mock.Mocker() as mocker:
        _FakeNifi(_tree()).install(mocker)
        yield mocker


def _run(command: str, **config: object) -> ProbeMethodResult:
    return run_probe_method("nifi", {**_RECIPE, **config}, command, {})


def _records(result: ProbeMethodResult) -> List[Dict[str, object]]:
    assert isinstance(result.result, list)
    return result.result


def test_process_groups_walk_the_whole_tree_with_each_ones_ancestors(
    nifi: requests_mock.Mocker,
) -> None:
    result = _run("process_groups", process_group_pattern={"deny": ["^WIP"]})
    assert result.kind == _KIND
    assert [(r["name"], json.loads(str(r["ancestors"]))) for r in _records(result)] == [
        ("Ingest", ["Main Flow"]),
        # Listed although the recipe excludes it, and walked into.
        ("WIP Area", ["Main Flow"]),
        ("Nested Ingest", ["Main Flow", "Ingest"]),
        ("Inner Keep", ["Main Flow", "WIP Area"]),
    ]
    assert _records(result)[2]["parent_id"] == "g-ingest"


def test_flow_reports_the_root_group(nifi: requests_mock.Mocker) -> None:
    assert _run("flow").result == {"name": "Main Flow", "id": "root-id"}


def test_a_listing_stops_at_its_limit(nifi: requests_mock.Mocker) -> None:
    result = run_probe_method("nifi", dict(_RECIPE), "process_groups", {"limit": 2})
    assert len(_records(result)) == 2
    assert result.truncated


def test_a_forbidden_root_degrades_to_empty_with_a_warning() -> None:
    with requests_mock.Mocker() as mocker:
        _FakeNifi(_tree(), forbidden=("root",)).install(mocker)
        result = _run("process_groups")
    assert result.result == []
    assert result.warnings == [
        "root process group returned HTTP 403; treating it as empty."
    ]


def test_a_forbidden_group_is_listed_but_not_walked_into() -> None:
    with requests_mock.Mocker() as mocker:
        _FakeNifi(_tree(), forbidden=("g-wip",)).install(mocker)
        result = _run("process_groups")
    assert [r["name"] for r in _records(result)] == [
        "Ingest",
        "WIP Area",
        "Nested Ingest",
    ]
    assert result.warnings == [
        "process group 'Main Flow/WIP Area' returned HTTP 403; treating it as empty."
    ]


def test_a_group_the_credential_cannot_read_is_counted() -> None:
    tree = _tree()
    tree.children.append(_Group("g-secret", "Hidden", readable=False))
    with requests_mock.Mocker() as mocker:
        _FakeNifi(tree).install(mocker)
        result = _run("process_groups")
    assert "Hidden" not in [r["name"] for r in _records(result)]
    assert any(w.startswith("1 process groups") for w in result.warnings)


def test_single_user_signs_in_and_sends_the_token(
    nifi: requests_mock.Mocker,
) -> None:
    nifi.post(f"{_API}access/token", text="issued-token")
    _run("flow", auth="SINGLE_USER", username="probe", password=_PASSWORD)
    assert nifi.last_request is not None
    assert nifi.last_request.headers["Authorization"] == "Bearer issued-token"


def test_kerberos_signs_in_at_the_kerberos_endpoint(
    nifi: requests_mock.Mocker,
) -> None:
    nifi.post(f"{_API}access/kerberos", text="kerberos-token")
    _run("flow", auth="KERBEROS", username="probe", password=_PASSWORD)
    assert nifi.last_request is not None
    assert nifi.last_request.headers["Authorization"] == "Bearer kerberos-token"


def test_basic_auth_is_sent_as_ingestion_sends_it(
    nifi: requests_mock.Mocker,
) -> None:
    _run("flow", auth="BASIC_AUTH", username="probe", password=_PASSWORD)
    expected = base64.b64encode(f"probe:{_PASSWORD}".encode()).decode()
    assert nifi.last_request is not None
    assert nifi.last_request.headers["Authorization"] == f"Basic {expected}"


def test_ca_file_is_the_sessions_verify(nifi: requests_mock.Mocker) -> None:
    _run("flow", ca_file=False)
    assert nifi.last_request is not None
    assert nifi.last_request.verify is False


def _recipe_file(tmp_path: pathlib.Path) -> str:
    path = tmp_path / "r.yml"
    path.write_text("source:\n  type: nifi\n  config: {}\n")
    return str(path)


def _cli(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    command: str,
    *params: str,
    **config: object,
) -> Result:
    monkeypatch.setattr(
        rc,
        "_resolve_for_probe",
        lambda _r: ("nifi", {**_RECIPE, **config}, {_PASSWORD}),
    )
    monkeypatch.setattr(rc, "_ping_probe", lambda *a, **k: None)
    return CliRunner().invoke(
        recipe_cli,
        ["probe", "run", command, "--recipe", _recipe_file(tmp_path), *params],
    )


def test_a_good_listing_exits_zero(
    nifi: requests_mock.Mocker,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    res = _cli(
        monkeypatch,
        tmp_path,
        "process_groups",
        auth="BASIC_AUTH",
        username="probe",
        password=_PASSWORD,
    )
    assert res.exit_code == rc.EXIT_OK, res.output
    assert "Nested Ingest" in res.output
    assert _PASSWORD not in res.output


def test_a_refused_sign_in_exits_on_the_source_code(
    nifi: requests_mock.Mocker,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    nifi.post(f"{_API}access/token", status_code=401, text="bad credentials")
    res = _cli(
        monkeypatch,
        tmp_path,
        "process_groups",
        auth="SINGLE_USER",
        username="probe",
        password=_PASSWORD,
    )
    assert res.exit_code == rc.EXIT_CONNECTION, res.output
    assert "HTTP 401" in res.output
    assert "bad credentials" not in res.output


def test_a_missing_client_certificate_exits_on_the_source_code(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    res = _cli(
        monkeypatch,
        tmp_path,
        "flow",
        auth="CLIENT_CERT",
        client_cert_file=str(tmp_path / "absent.pem"),
    )
    assert res.exit_code == rc.EXIT_CONNECTION, res.output


def test_a_server_error_inside_the_tree_fails_the_command(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    with requests_mock.Mocker() as mocker:
        _FakeNifi(_tree(), failing=("g-nested",)).install(mocker)
        res = _cli(monkeypatch, tmp_path, "process_groups")
    assert res.exit_code == rc.EXIT_CONNECTION, res.output
    assert "HTTP 500" in res.output


def test_an_unreachable_server_exits_on_the_source_code(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    with requests_mock.Mocker() as mocker:
        mocker.get(
            re.compile(re.escape(_API) + ".*"),
            exc=requests.exceptions.ConnectionError,
        )
        res = _cli(monkeypatch, tmp_path, "flow")
    assert res.exit_code == rc.EXIT_CONNECTION, res.output


def test_an_unknown_parameter_is_the_callers_mistake(
    nifi: requests_mock.Mocker,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    res = _cli(monkeypatch, tmp_path, "process_groups", "--group", "x")
    assert res.exit_code == rc.EXIT_USER, res.output


def _judge(
    names: Sequence[str],
    parents: Sequence[str] = (),
    attributes: Optional[Sequence[Dict[str, str]]] = None,
    **config: object,
) -> FilterCheckResult:
    return check_filters(
        source_type="nifi",
        config_dict={**_RECIPE, **config},
        kind=_KIND,
        parent_path=parents,
        names=names,
        attributes=attributes,
    )


def _excluded_by(result: FilterCheckResult) -> Dict[str, Optional[str]]:
    return {v.name: v.excluded_by for v in result.results}


def test_a_group_under_an_excluded_parent_is_excluded() -> None:
    result = _judge(
        ["Inner Keep"],
        parents=["Main Flow", "WIP Area"],
        process_group_pattern={"deny": ["^WIP"]},
    )
    assert _excluded_by(result) == {"Inner Keep": "process_group_pattern"}


def test_the_root_is_judged_as_an_ancestor_too() -> None:
    result = _judge(
        ["Ingest"],
        parents=["Main Flow"],
        process_group_pattern={"allow": ["^Ingest$"]},
    )
    assert _excluded_by(result) == {"Ingest": "process_group_pattern"}


def test_listed_ancestors_are_judged_without_parents() -> None:
    result = _judge(
        ["Inner Keep", "Ingest"],
        attributes=[
            {"ancestors": json.dumps(["Main Flow", "WIP Area"])},
            {"ancestors": json.dumps(["Main Flow"])},
        ],
        process_group_pattern={"deny": ["^WIP"]},
    )
    assert _excluded_by(result) == {
        "Inner Keep": "process_group_pattern",
        "Ingest": None,
    }
    assert any("'WIP Area'" in w for w in result.warnings)


def test_malformed_ancestors_fall_back_to_the_name_with_a_warning() -> None:
    result = _judge(
        ["Inner Keep"],
        attributes=[{"ancestors": "Main Flow/WIP Area"}],
        process_group_pattern={"deny": ["^WIP"]},
    )
    assert _excluded_by(result) == {"Inner Keep": None}
    assert any("JSON list" in w for w in result.warnings)


@pytest.mark.parametrize(
    "pattern, excluded",
    [
        pytest.param({}, set(), id="allow-all"),
        pytest.param(
            {"deny": ["^WIP"]},
            {"WIP Area", "Inner Keep"},
            id="deny-a-subtree",
        ),
        pytest.param(
            # Inner Keep's own name is allowed; its parent's is not.
            {"allow": ["^Main Flow$", "^Ingest$", "^Inner"]},
            {"Nested Ingest", "WIP Area", "Inner Keep"},
            id="allow-list-and-inheritance",
        ),
    ],
)
def test_probe_verdicts_match_ingestion(
    nifi: requests_mock.Mocker,
    tmp_path: pathlib.Path,
    pattern: Dict[str, object],
    excluded: Set[str],
) -> None:
    recipe = {
        **_RECIPE,
        "emit_process_group_as_container": True,
        "process_group_pattern": pattern,
    }
    report = assert_probe_parity(
        "nifi",
        recipe,
        pipeline_ingestion("nifi", tmp_path),
        [
            ParityListing(
                label="process_groups",
                command="process_groups",
                emitted=lambda index: index.container_names(_KIND),
            )
        ],
    )
    assert set(report.excluded_by("process_groups")) == excluded


def test_a_denied_root_leaves_nothing_to_ingest(
    nifi: requests_mock.Mocker, tmp_path: pathlib.Path
) -> None:
    recipe = {
        **_RECIPE,
        "emit_process_group_as_container": True,
        "process_group_pattern": {"deny": ["^Main Flow$"]},
    }
    report = assert_probe_parity(
        "nifi",
        recipe,
        pipeline_ingestion("nifi", tmp_path),
        [
            ParityListing(
                label="process_groups",
                command="process_groups",
                emitted=lambda index: index.container_names(_KIND),
                expect_empty=True,
            )
        ],
    )
    assert len(report.excluded_by("process_groups")) == 4
