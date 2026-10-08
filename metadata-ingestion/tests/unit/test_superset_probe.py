"""Superset's and Preset's probe: listings over a mocked REST API, the exit code
each failure gets, and verdicts proven against a mocked ingestion run."""

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Mapping, Optional, Set

import pytest
import requests
from click.testing import CliRunner, Result
from requests_mock import Mocker
from requests_mock.request import _RequestObjectProxy
from requests_mock.response import _Context

import datahub.cli.recipe_cli as rc
from datahub.cli.recipe_cli import recipe as recipe_group
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import (
    ProbeMethodResult,
    _provider_class,
    run_probe_method,
)
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.preset_probe import PresetMetadataProbe
from datahub.ingestion.source.superset_probe import SupersetMetadataProbe
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    DashboardInfoClass,
    DatasetPropertiesClass,
)
from datahub.metadata.urns import ChartUrn, DashboardUrn, DatasetUrn
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    JudgedRecord,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

BASE = "http://superset.example.com"
MANAGER = "http://manager.example.com"
OWNER_EMAIL = "someone@example.com"
# Must not equal any identifier in the fixture, or redaction masks it.
FIXTURE_SECRET = "fixture-secret"


@dataclass
class FakeSuperset:
    """A Superset REST API serving databases, datasets, charts and dashboards,
    paged as the real one pages them. Every list record carries an owner and a
    changed_by with a person's name and email, which no listing may return."""

    databases: List[Dict[str, object]] = field(
        default_factory=lambda: [
            {"id": 1, "database_name": "analytics", "backend": "postgresql"},
            {"id": 2, "database_name": "sandbox", "backend": "postgresql"},
        ]
    )
    datasets: List[Dict[str, object]] = field(
        default_factory=lambda: [
            {"id": 1, "table_name": "orders", "database_id": 1, "schema": "public"},
            {
                "id": 2,
                "table_name": "staging_orders",
                "database_id": 1,
                "schema": "public",
            },
            {"id": 3, "table_name": "events", "database_id": 2, "schema": "raw"},
        ]
    )
    charts: List[Dict[str, object]] = field(
        default_factory=lambda: [
            {"id": 10, "slice_name": "Revenue", "datasource_id": 1},
            {"id": 11, "slice_name": "tmp_check", "datasource_id": 2},
            {"id": 12, "slice_name": "Events by day", "datasource_id": 3},
        ]
    )
    dashboards: List[Dict[str, object]] = field(
        default_factory=lambda: [
            {"id": 100, "dashboard_title": "Sales", "chart_ids": [10, 11]},
            {"id": 101, "dashboard_title": "Scratch pad", "chart_ids": [12]},
        ]
    )
    # Statuses to answer a list endpoint with instead of its records.
    list_status: Dict[str, int] = field(default_factory=dict)
    # Leave `database` off the dataset list records, as an older server might.
    dataset_list_without_database: bool = False
    requested: List[str] = field(default_factory=list)

    def register(self, mock: Mocker, *, preset: bool = False) -> None:
        if preset:
            mock.post(f"{MANAGER}/v1/auth/", json={"payload": {"access_token": "tok"}})
            mock.get(f"{BASE}/version", json={})
        else:
            mock.post(f"{BASE}/api/v1/security/login", json={"access_token": "tok"})
        for entity in ("database", "dataset", "chart", "dashboard"):
            mock.get(f"{BASE}/api/v1/{entity}/", json=self._lister(entity))
            mock.get(
                f"{BASE}/api/v1/{entity}/related/owners",
                json={"count": 0, "result": []},
            )
        mock.get(re.compile(rf"{BASE}/api/v1/dataset/\d+$"), json=self._dataset)
        mock.get(re.compile(rf"{BASE}/api/v1/dashboard/\d+$"), json=self._dashboard)
        mock.get(
            re.compile(rf"{BASE}/api/v1/dashboard/\d+/charts$"),
            json={"result": []},
        )

    def _person(self) -> Dict[str, object]:
        return {"id": 7, "first_name": "Some", "last_name": "One", "email": OWNER_EMAIL}

    def _database(self, database_id: object) -> Dict[str, object]:
        return next(d for d in self.databases if d["id"] == database_id)

    def _list_record(self, entity: str, item: Dict[str, object]) -> Dict[str, object]:
        record = {k: v for k, v in item.items() if k not in ("chart_ids",)}
        record["owners"] = [self._person()]
        record["changed_by"] = self._person()
        record["changed_on_utc"] = "2024-01-01T00:00:00.000000+0000"
        if entity == "dataset":
            database = self._database(record.pop("database_id"))
            if not self.dataset_list_without_database:
                record["database"] = {
                    "id": database["id"],
                    "database_name": database["database_name"],
                }
        if entity == "chart":
            record.update(viz_type="table", url=f"/explore/{item['id']}", params="{}")
        if entity == "dashboard":
            record.update(url=f"/dashboard/{item['id']}", status="published")
        return record

    def _lister(
        self, entity: str
    ) -> Callable[[_RequestObjectProxy, _Context], Dict[str, object]]:
        source = {
            "database": self.databases,
            "dataset": self.datasets,
            "chart": self.charts,
            "dashboard": self.dashboards,
        }[entity]

        def respond(
            request: _RequestObjectProxy, context: _Context
        ) -> Dict[str, object]:
            self.requested.append(request.url)
            status = self.list_status.get(entity)
            if status is not None:
                context.status_code = status
                return {"message": "refused"}
            # q=(page:N,page_size:M), Superset's rison query. login() checks its
            # token with one bare GET of the dashboard listing.
            query = request.qs.get("q", ["(page:0,page_size:100)"])[0]
            match = re.search(r"page:(\d+),page_size:(\d+)", query)
            assert match is not None
            page, size = int(match.group(1)), int(match.group(2))
            chunk = source[page * size : (page + 1) * size]
            return {
                "count": len(source),
                "result": [self._list_record(entity, item) for item in chunk],
            }

        return respond

    def _dataset(
        self, request: _RequestObjectProxy, context: _Context
    ) -> Dict[str, object]:
        self.requested.append(request.url)
        dataset_id = int(request.path.rsplit("/", 1)[-1])
        item = next(d for d in self.datasets if d["id"] == dataset_id)
        database = self._database(item["database_id"])
        return {
            "id": dataset_id,
            "result": {
                "id": dataset_id,
                "table_name": item["table_name"],
                "schema": item["schema"],
                "database": dict(database),
                "columns": [],
                "metrics": [],
                "sql": None,
                "changed_on_utc": "2024-01-01T00:00:00.000000+0000",
            },
        }

    def _dashboard(
        self, request: _RequestObjectProxy, context: _Context
    ) -> Dict[str, object]:
        dashboard_id = int(request.path.rsplit("/", 1)[-1])
        item = next(d for d in self.dashboards if d["id"] == dashboard_id)
        chart_ids = item["chart_ids"]
        assert isinstance(chart_ids, list)
        position = {f"CHART-{c}": {"meta": {"chartId": c}} for c in chart_ids}
        return {
            "id": dashboard_id,
            "result": {
                "id": dashboard_id,
                "dashboard_title": item["dashboard_title"],
                "position_json": json.dumps(position),
            },
        }


def _recipe(**overrides: object) -> Dict[str, object]:
    return {
        "connect_uri": BASE,
        "username": "probe-user",
        "password": FIXTURE_SECRET,
        **overrides,
    }


def _preset_recipe(**overrides: object) -> Dict[str, object]:
    return {
        "connect_uri": BASE,
        "manager_uri": MANAGER,
        "api_key": "fixture-key",
        "api_secret": FIXTURE_SECRET,
        **overrides,
    }


def _run(
    command: str,
    recipe: Optional[Dict[str, object]] = None,
    source_type: str = "superset",
    **kwargs: object,
) -> ProbeMethodResult:
    return run_probe_method(source_type, dict(recipe or _recipe()), command, kwargs)


def _records(result: ProbeMethodResult) -> List[Dict[str, object]]:
    assert isinstance(result.result, list)
    return result.result


def _names(result: ProbeMethodResult) -> List[str]:
    return [str(r["name"]) for r in _records(result)]


# --- listings ---------------------------------------------------------------


def test_each_listing_names_what_its_pattern_is_matched_against(
    requests_mock: Mocker,
) -> None:
    FakeSuperset().register(requests_mock)
    # The recipe denies one of each: a denied object is listed, not hidden.
    recipe = _recipe(
        dashboard_pattern={"deny": ["^Scratch"]},
        chart_pattern={"deny": ["^tmp_"]},
        dataset_pattern={"deny": ["^staging_"]},
        database_pattern={"deny": ["^sandbox$"]},
    )
    assert _names(_run("dashboards", recipe)) == ["Sales", "Scratch pad"]
    assert _names(_run("charts", recipe)) == ["Revenue", "tmp_check", "Events by day"]
    assert _names(_run("datasets", recipe)) == ["orders", "staging_orders", "events"]
    assert _names(_run("databases", recipe)) == ["analytics", "sandbox"]


def test_each_listing_declares_the_kind_its_pattern_filters(
    requests_mock: Mocker,
) -> None:
    FakeSuperset().register(requests_mock)
    assert {
        c: _run(c).kind for c in ("dashboards", "charts", "datasets", "databases")
    } == {
        "dashboards": "Dashboard",
        "charts": "Chart",
        "datasets": "Dataset",
        "databases": "Database",
    }


def test_records_carry_ids_and_databases_but_never_people(
    requests_mock: Mocker,
) -> None:
    FakeSuperset().register(requests_mock)
    datasets = _run("datasets")
    assert datasets.result == [
        {"name": "orders", "database": "analytics", "schema": "public", "id": 1},
        {
            "name": "staging_orders",
            "database": "analytics",
            "schema": "public",
            "id": 2,
        },
        {"name": "events", "database": "sandbox", "schema": "raw", "id": 3},
    ]
    for command in ("dashboards", "charts", "datasets", "databases"):
        dumped = json.dumps(_run(command).result)
        assert OWNER_EMAIL not in dumped
        assert "owners" not in dumped and "changed_by" not in dumped


def test_a_listing_walks_every_page_and_stops_at_the_limit(
    requests_mock: Mocker,
) -> None:
    fake = FakeSuperset(
        dashboards=[
            {"id": i, "dashboard_title": f"d{i:02d}", "chart_ids": []}
            for i in range(30)
        ]
    )
    fake.register(requests_mock)
    whole = _run("dashboards", limit=100)
    assert len(_names(whole)) == 30 and not whole.truncated
    # login() also GETs the dashboard listing once, without a page query.
    pages = [u for u in fake.requested if "/dashboard/?q=" in u]
    assert len(pages) == 2

    fake.requested.clear()
    bounded = _run("dashboards", limit=3)
    assert _names(bounded) == ["d00", "d01", "d02"] and bounded.truncated
    # The limit is reached on page one, so page two is never requested.
    assert len([u for u in fake.requested if "/dashboard/?q=" in u]) == 1


def test_a_dataset_record_without_its_database_reads_the_detail_ingestion_reads(
    requests_mock: Mocker,
) -> None:
    fake = FakeSuperset(dataset_list_without_database=True)
    fake.register(requests_mock)
    result = _run("datasets")
    assert [r["database"] for r in _records(result)] == [
        "analytics",
        "analytics",
        "sandbox",
    ]
    assert any(re.search(r"/api/v1/dataset/1$", u) for u in fake.requested)


def test_an_unnamed_record_is_left_out_with_a_count(requests_mock: Mocker) -> None:
    FakeSuperset(
        charts=[
            {"id": 10, "slice_name": "Revenue", "datasource_id": 1},
            {"id": 11, "slice_name": None, "datasource_id": 1},
        ]
    ).register(requests_mock)
    result = _run("charts")
    assert _names(result) == ["Revenue"]
    assert result.warnings == [
        "1 chart record(s) had no name and were left out; ingestion drops them too"
    ]


@pytest.mark.parametrize("status", [403, 404])
def test_a_listing_the_role_cannot_read_is_empty_with_a_warning(
    requests_mock: Mocker, status: int
) -> None:
    FakeSuperset(list_status={"database": status}).register(requests_mock)
    result = _run("databases")
    assert result.result == []
    assert result.warnings == [
        f"the database listing returned HTTP {status}; treating it as empty."
    ]
    assert not result.failures


def test_preset_logs_in_at_the_manager_with_its_api_key(
    requests_mock: Mocker,
) -> None:
    FakeSuperset().register(requests_mock, preset=True)
    result = _run("dashboards", _preset_recipe(), source_type="preset")
    assert _names(result) == ["Sales", "Scratch pad"]
    login = next(r for r in requests_mock.request_history if r.method == "POST")
    assert login.url == f"{MANAGER}/v1/auth/"
    assert login.json() == {"name": "fixture-key", "secret": FIXTURE_SECRET}


def test_each_connector_names_its_own_provider() -> None:
    assert _provider_class("superset") is SupersetMetadataProbe
    assert _provider_class("preset") is PresetMetadataProbe


def test_the_provider_closes_the_session_it_opened(requests_mock: Mocker) -> None:
    FakeSuperset().register(requests_mock)
    from datahub.ingestion.source.superset import SupersetConfig

    closed: List[bool] = []
    with SupersetMetadataProbe.for_config(
        SupersetConfig.model_validate(_recipe())
    ) as probe:
        probe.dashboards()
        session = probe._source().session
        original = session.close

        def close() -> None:
            closed.append(True)
            original()

        session.close = close  # type: ignore[method-assign]
    assert closed == [True]


def test_a_refused_login_is_a_connection_error_not_a_defect(
    requests_mock: Mocker,
) -> None:
    requests_mock.post(
        f"{BASE}/api/v1/security/login",
        status_code=401,
        json={"message": "Not authorized"},
    )
    with pytest.raises(ProbeConnectionError, match="no access token"):
        _run("dashboards")


# --- exit codes, through the CLI ---------------------------------------------


def _cli(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    args: List[str],
    config: Optional[Dict[str, object]] = None,
) -> Result:
    monkeypatch.setattr(
        rc,
        "_resolve_for_probe",
        lambda _r: ("superset", dict(config or _recipe()), {FIXTURE_SECRET}),
    )
    monkeypatch.setattr(rc, "_ping_probe", lambda *a, **k: None)
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text("source:\n  type: superset\n  config: {}\n")
    return CliRunner().invoke(
        recipe_group, ["probe", "run", *args, "--recipe", str(recipe_file)]
    )


def test_cli_a_listing_exits_0(
    requests_mock: Mocker, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    FakeSuperset().register(requests_mock)
    res = _cli(monkeypatch, tmp_path, ["dashboards"])
    assert res.exit_code == 0, res.output
    assert "Sales" in res.output


def test_cli_an_unknown_command_exits_2(
    requests_mock: Mocker, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    FakeSuperset().register(requests_mock)
    res = _cli(monkeypatch, tmp_path, ["workbooks"])
    assert res.exit_code == 2, res.output


def test_cli_a_refused_login_exits_3(
    requests_mock: Mocker, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    requests_mock.post(
        f"{BASE}/api/v1/security/login",
        status_code=401,
        json={"message": "Not authorized"},
    )
    res = _cli(monkeypatch, tmp_path, ["dashboards"])
    assert res.exit_code == 3, res.output
    assert "no access token" in res.stderr
    assert FIXTURE_SECRET not in res.output


@pytest.mark.parametrize("status", [401, 500])
def test_cli_an_unauthorised_or_failing_listing_exits_3(
    requests_mock: Mocker,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    status: int,
) -> None:
    # 401 and 5xx are not degraded to a warning: the token or the server is
    # broken, and an empty listing would hide it.
    FakeSuperset(list_status={"chart": status}).register(requests_mock)
    res = _cli(monkeypatch, tmp_path, ["charts"])
    assert res.exit_code == 3, res.output
    assert f"HTTP {status}" in json.loads(res.stderr)["error"]


def test_cli_an_unreachable_host_exits_3(
    requests_mock: Mocker, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    requests_mock.post(
        f"{BASE}/api/v1/security/login", exc=requests.exceptions.ConnectionError
    )
    res = _cli(monkeypatch, tmp_path, ["databases"])
    assert res.exit_code == 3, res.output


def test_cli_a_listing_answer_without_results_exits_3(
    requests_mock: Mocker, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    FakeSuperset().register(requests_mock)
    requests_mock.get(f"{BASE}/api/v1/chart/", json={"message": "maintenance"})
    res = _cli(monkeypatch, tmp_path, ["charts"])
    assert res.exit_code == 3, res.output
    assert "without a result list" in json.loads(res.stderr)["error"]


def test_cli_a_non_json_login_answer_exits_3(
    requests_mock: Mocker, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # A proxy's HTML login page in front of Superset.
    requests_mock.post(f"{BASE}/api/v1/security/login", text="<html>sign in</html>")
    res = _cli(monkeypatch, tmp_path, ["databases"])
    assert res.exit_code == 3, res.output


# --- verdicts ----------------------------------------------------------------


def test_a_dataset_is_judged_by_its_database_from_the_listing() -> None:
    result = check_filters(
        source_type="superset",
        config_dict=_recipe(
            ingest_datasets=True, database_pattern={"deny": ["^sandbox$"]}
        ),
        kind="dataset",
        parent_path=[],
        names=["orders", "events"],
        attributes=[{"database": "analytics"}, {"database": "sandbox"}],
    )
    assert [(v.name, v.included, v.excluded_by) for v in result.results] == [
        ("orders", True, None),
        ("events", False, "database_pattern"),
    ]
    assert not result.warnings


def test_a_bare_dataset_name_says_its_database_was_not_judged() -> None:
    result = check_filters(
        source_type="superset",
        config_dict=_recipe(
            ingest_datasets=True, database_pattern={"deny": ["^sandbox$"]}
        ),
        kind="Dataset",
        parent_path=[],
        names=["events"],
    )
    assert result.results[0].included
    assert any("database_pattern" in w for w in result.warnings)


def test_a_dataset_under_a_denied_database_parent_is_excluded() -> None:
    result = check_filters(
        source_type="superset",
        config_dict=_recipe(
            ingest_datasets=True, database_pattern={"deny": ["^sandbox$"]}
        ),
        kind="Dataset",
        parent_path=["sandbox"],
        names=["events"],
    )
    assert not result.results[0].included


def test_datasets_are_off_unless_ingest_datasets_is_set() -> None:
    result = check_filters(
        source_type="superset",
        config_dict=_recipe(),
        kind="Dataset",
        parent_path=[],
        names=["orders"],
        attributes=[{"database": "analytics"}],
    )
    assert result.results[0].excluded_by == "ingest_datasets"


# --- parity with ingestion -----------------------------------------------------


def _by_id(record: JudgedRecord) -> str:
    return record.attributes["id"]


def _dataset_identity(record: JudgedRecord) -> str:
    return ".".join(
        (record.attributes["database"], record.attributes["schema"], record.name)
    )


def _emitted_charts(index: EmittedIndex) -> Set[str]:
    return {
        ChartUrn.from_string(u).chart_id
        for u in index.urns("chart", with_aspect=ChartInfoClass)
    }


def _emitted_dashboards(index: EmittedIndex) -> Set[str]:
    return {
        DashboardUrn.from_string(u).dashboard_id
        for u in index.urns("dashboard", with_aspect=DashboardInfoClass)
    }


def _emitted_datasets(index: EmittedIndex) -> Set[str]:
    # with_aspect: a chart's lineage names its dataset's URN on the warehouse
    # platform too, and that one is not Superset's own.
    return {
        DatasetUrn.from_string(u).name
        for u in index.urns("dataset", with_aspect=DatasetPropertiesClass)
    }


def _listings(
    *, charts_off: bool = False, datasets_off: bool = False
) -> List[ParityListing]:
    return [
        ParityListing(
            "dashboards",
            "dashboards",
            emitted=_emitted_dashboards,
            identity=_by_id,
        ),
        ParityListing(
            "charts",
            "charts",
            emitted=_emitted_charts,
            identity=_by_id,
            expect_empty=charts_off,
        ),
        ParityListing(
            "datasets",
            "datasets",
            emitted=_emitted_datasets,
            identity=_dataset_identity,
            expect_empty=datasets_off,
        ),
    ]


_FILTERED: Mapping[str, object] = {
    "ingest_datasets": True,
    "dashboard_pattern": {"deny": ["^Scratch"]},
    "chart_pattern": {"deny": ["^tmp_"]},
    "dataset_pattern": {"deny": ["^staging_"]},
    "database_pattern": {"deny": ["^sandbox$"]},
}


@pytest.mark.parametrize("source_type", ["superset", "preset"])
def test_probe_verdicts_match_ingestion(
    requests_mock: Mocker, tmp_path: Path, source_type: str
) -> None:
    preset = source_type == "preset"
    FakeSuperset().register(requests_mock, preset=preset)
    recipe = (_preset_recipe if preset else _recipe)(**_FILTERED)

    report = assert_probe_parity(
        source_type, recipe, pipeline_ingestion(source_type, tmp_path), _listings()
    )

    assert report.excluded_by("dashboards") == {"101": "dashboard_pattern"}
    # A chart reading a dataset in a denied database is still ingested.
    assert report.excluded_by("charts") == {"11": "chart_pattern"}
    assert report.excluded_by("datasets") == {
        "analytics.public.staging_orders": "dataset_pattern",
        "sandbox.raw.events": "database_pattern",
    }


def test_switched_off_kinds_match_ingestion(
    requests_mock: Mocker, tmp_path: Path
) -> None:
    FakeSuperset().register(requests_mock)
    recipe = _recipe(ingest_charts=False)  # ingest_datasets defaults to False

    report = assert_probe_parity(
        "superset",
        recipe,
        pipeline_ingestion("superset", tmp_path),
        _listings(charts_off=True, datasets_off=True),
    )

    assert set(report.excluded_by("charts").values()) == {"ingest_charts"}
    assert set(report.excluded_by("datasets").values()) == {"ingest_datasets"}
