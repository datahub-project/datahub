import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
import yaml
from click.testing import CliRunner, Result

from datahub.cli.recipe_cli import recipe as recipe_cli
from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import ProbeMethodResult, run_probe_method
from datahub.ingestion.agent.verdicts import Verdict
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_selection import (
    DatasetFacts,
    dataset_verdict,
    folder_verdict,
    sql_schema_filter_allows,
    sql_schema_filter_value,
)
from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries

pytestmark = pytest.mark.usefixtures("_isolate_secret_registry")

_HOST = "http://dremio.example.com:9047"
_API = f"{_HOST}/api/v3"
_TOKEN = "fake-personal-access-token"
_EXIT_USER = 2
_EXIT_SOURCE = 3


def _recipe(**overrides: object) -> Dict[str, object]:
    return {
        "hostname": "dremio.example.com",
        "port": 9047,
        "tls": False,
        "authentication_method": "PAT",
        "password": _TOKEN,
        **overrides,
    }


def _config(**overrides: object) -> DremioSourceConfig:
    return DremioSourceConfig.model_validate(_recipe(**overrides))


def _records(result: ProbeMethodResult) -> List[Dict[str, Any]]:
    assert isinstance(result.result, list)
    return result.result


def _names(result: ProbeMethodResult) -> List[str]:
    return [record["name"] for record in _records(result)]


# --- dremio_selection --------------------------------------------------------


def test_the_dataset_query_condition_is_a_case_blind_search() -> None:
    # REGEXP_LIKE finds the pattern anywhere, unlike AllowDenyPattern.
    pattern = AllowDenyPattern(allow=["sales"])
    assert sql_schema_filter_allows(pattern, "archive.sales_2020")
    assert not AllowDenyPattern(allow=["sales"]).allowed("archive.sales_2020")
    assert not sql_schema_filter_allows(pattern, "marketing")
    assert sql_schema_filter_allows(AllowDenyPattern(deny=["^tmp"]), "space.tmp"), (
        "a deny is anchored only where the pattern anchors it"
    )
    assert not sql_schema_filter_allows(AllowDenyPattern(deny=["^tmp"]), "tmp.x")


def test_the_condition_is_the_one_the_query_is_built_with() -> None:
    patterns = ["Sales\\..*", ".*"]
    # ".*" among the allow entries drops the whole condition.
    assert DremioSQLQueries.pushed_patterns(patterns, allow=True) == []
    assert DremioSQLQueries.pattern_condition(patterns, "F") == ""
    assert DremioSQLQueries.pushed_patterns(["Sales"], allow=False) == ["SALES"]
    assert DremioSQLQueries.pattern_condition(["Sales"], "F", allow=False) == (
        "AND NOT REGEXP_LIKE(F, '(SALES)')"
    )


def test_the_filtered_value_depends_on_the_edition() -> None:
    assert sql_schema_filter_value("COMMUNITY", "space.f1", "orders") == "space.f1"
    assert (
        sql_schema_filter_value("ENTERPRISE", "space.f1", "orders") == "space.f1.orders"
    )


def test_a_dataset_meets_the_query_then_the_reflection_root_then_its_pattern() -> None:
    config = _config(
        schema_pattern={"deny": ["archive"]}, dataset_pattern={"deny": [".*\\.tmp_.*"]}
    )
    assert dataset_verdict(
        config, DatasetFacts(["archive"], "tmp_x", "archive")
    ) == Verdict.exclude("schema_pattern")
    assert dataset_verdict(
        config, DatasetFacts(["_accelerator_", "r1"], "x", "_accelerator_.r1")
    ) == Verdict.exclude("_accelerator_")
    assert dataset_verdict(
        config, DatasetFacts(["space"], "tmp_x", "space")
    ) == Verdict.exclude("dataset_pattern")
    assert dataset_verdict(
        config, DatasetFacts(["lake"], "unread", "lake", has_columns=False)
    ) == Verdict.exclude("no_column_metadata")
    # Ingestion passes no query value: the query already applied it.
    assert dataset_verdict(config, DatasetFacts(["archive"], "orders")) == (
        Verdict.include()
    )


def test_a_folder_needs_its_root_to_pass() -> None:
    config = _config(schema_pattern={"deny": ["^other$"]})
    assert folder_verdict(config, ["other", "f1"]) == Verdict.exclude("schema_pattern")
    assert folder_verdict(config, ["space", "f1"]) == Verdict.include()


# --- probe filter ------------------------------------------------------------


def _judge(
    kind: str,
    names: List[str],
    attributes: Optional[List[Dict[str, str]]] = None,
    **config: object,
) -> List[Optional[str]]:
    result = check_filters(
        source_type="dremio",
        config_dict=_recipe(**config),
        kind=kind,
        parent_path=[],
        names=names,
        attributes=attributes,
    )
    return [r.excluded_by for r in result.results]


def test_a_saved_dataset_record_is_judged_for_its_edition() -> None:
    attributes = {"schema": "sales"}
    names = ["sales.orders"]
    # The Community query filters "SALES", Enterprise's "SALES.ORDERS".
    assert _judge(
        "Table",
        names,
        [{**attributes, "edition": "COMMUNITY"}],
        schema_pattern={"allow": ["^sales$"]},
    ) == [None]
    assert _judge(
        "Table",
        names,
        [{**attributes, "edition": "ENTERPRISE"}],
        schema_pattern={"allow": ["^sales$"]},
    ) == ["schema_pattern"]


def test_a_bare_dataset_name_skips_the_query_condition_and_says_so() -> None:
    result = check_filters(
        source_type="dremio",
        config_dict=_recipe(
            schema_pattern={"allow": ["^nothing$"]},
            dataset_pattern={"deny": [".*\\.secret$"]},
        ),
        kind="View",
        parent_path=[],
        names=["space.f1.secret", "space.f1.open"],
    )
    assert [r.excluded_by for r in result.results] == ["dataset_pattern", None]
    assert any("no Dremio edition" in w for w in result.warnings)


def test_containers_are_judged_on_their_full_path() -> None:
    # A root passes as the first segment of a dotted allow entry, read as
    # text: an escaped dot does not count.
    assert _judge(
        "Dremio Space",
        ["space", "other"],
        schema_pattern={"allow": ["space.f1"]},
    ) == [None, "schema_pattern"]
    assert _judge(
        "Dremio Space", ["space"], schema_pattern={"allow": ["space\\.f1"]}
    ) == ["schema_pattern"]
    assert _judge(
        "Dremio Folder",
        ["other.f1", "space.f1"],
        [{"root": "other"}, {"root": "space"}],
        schema_pattern={"deny": ["^other$"]},
    ) == ["schema_pattern", None]


# --- the provider, over a fake Dremio ----------------------------------------


def _catalog_entry(name: str, container_type: str, entry_id: str) -> Dict[str, Any]:
    return {
        "id": entry_id,
        "path": [name],
        "type": "CONTAINER",
        "containerType": container_type,
    }


def _child(path: List[str], child_id: str) -> Dict[str, Any]:
    return {"id": child_id, "path": path, "type": "CONTAINER"}


def _register(
    requests_mock: Any, rows: Optional[List[Dict[str, object]]] = None
) -> None:
    requests_mock.get(f"{_API}/catalog/privileges", status_code=404, json={})
    requests_mock.get(
        f"{_API}/catalog",
        json={
            "data": [
                _catalog_entry("lake", "SOURCE", "id-lake"),
                _catalog_entry("space", "SPACE", "id-space"),
                _catalog_entry("@someone", "HOME", "id-home"),
            ]
        },
    )
    requests_mock.get(
        f"{_API}/catalog/id-lake",
        json={"entityType": "source", "children": [_child(["lake", "raw"], "id-raw")]},
    )
    requests_mock.get(f"{_API}/catalog/id-raw", json={"entityType": "folder"})
    requests_mock.get(
        f"{_API}/catalog/id-space",
        json={"entityType": "space", "children": [_child(["space", "f1"], "id-f1")]},
    )
    requests_mock.get(
        f"{_API}/catalog/id-f1",
        json={
            "entityType": "folder",
            "children": [_child(["space", "f1", "f2"], "id-f2")],
        },
    )
    requests_mock.get(f"{_API}/catalog/id-f2", json={"entityType": "folder"})
    requests_mock.get(
        f"{_API}/catalog/id-home",
        json={"entityType": "home", "children": [_child(["@someone", "mine"], "id-m")]},
    )
    requests_mock.get(f"{_API}/catalog/id-m", json={"entityType": "folder"})
    requests_mock.post(f"{_API}/sql", json={"id": "job-1"})
    requests_mock.get(f"{_API}/job/job-1/", json={"jobState": "COMPLETED"})
    requests_mock.get(
        f"{_API}/job/job-1/results",
        json={"rows": rows if rows is not None else []},
    )


def test_sources_and_spaces_are_listed_with_home_spaces_withheld_when_dropped(
    requests_mock: Any,
) -> None:
    _register(requests_mock)
    assert _names(run_probe_method("dremio", _recipe(), "sources", {})) == ["lake"]
    spaces = run_probe_method("dremio", _recipe(), "spaces", {})
    assert _names(spaces) == ["space", "@someone"]

    dropped = run_probe_method(
        "dremio", _recipe(schema_pattern={"deny": ["^@"]}), "spaces", {}
    )
    assert _names(dropped) == ["space"]
    assert any("1 home space(s)" in w for w in dropped.warnings)


def test_folders_are_walked_under_every_root_by_full_path(
    requests_mock: Any,
) -> None:
    _register(requests_mock)
    records = _records(run_probe_method("dremio", _recipe(), "folders", {}))
    assert [(r["name"], r["root"]) for r in records] == [
        ("lake.raw", "lake"),
        ("space.f1", "space"),
        ("space.f1.f2", "space"),
        ("@someone.mine", "@someone"),
    ]
    in_space = run_probe_method("dremio", _recipe(), "folders", {"container": "space"})
    assert _names(in_space) == ["space.f1", "space.f1.f2"]


def test_a_refused_catalog_entry_degrades_with_a_warning(requests_mock: Any) -> None:
    _register(requests_mock)
    requests_mock.get(f"{_API}/catalog/id-f1", status_code=403, json={})
    result = run_probe_method("dremio", _recipe(), "folders", {"container": "space"})
    assert _names(result) == []
    assert any("HTTP 403" in w for w in result.warnings)


def test_a_container_dremio_will_not_look_up_is_counted(requests_mock: Any) -> None:
    _register(requests_mock)
    requests_mock.get(f"{_API}/catalog/id-raw", status_code=404, json={})
    result = run_probe_method("dremio", _recipe(), "folders", {"container": "lake"})
    assert _names(result) == []
    assert any("1 catalog container(s) answered 404" in w for w in result.warnings)


def test_datasets_carry_their_schema_and_edition(requests_mock: Any) -> None:
    _register(
        requests_mock,
        rows=[
            {"TABLE_SCHEMA": "lake.db", "TABLE_NAME": "unread", "COLUMN_COUNT": 0},
            {"TABLE_SCHEMA": "space.f1", "TABLE_NAME": "orders", "COLUMN_COUNT": 3},
            {"TABLE_SCHEMA": "@someone", "TABLE_NAME": "scratch", "COLUMN_COUNT": 1},
        ],
    )
    records = _records(run_probe_method("dremio", _recipe(), "views", {}))
    community = {"edition": "COMMUNITY"}
    assert records == [
        {
            "name": "lake.db.unread",
            "schema": "lake.db",
            **community,
            "has_columns": False,
        },
        {
            "name": "space.f1.orders",
            "schema": "space.f1",
            **community,
            "has_columns": True,
        },
        {
            "name": "@someone.scratch",
            "schema": "@someone",
            **community,
            "has_columns": True,
        },
    ]
    sent = requests_mock.request_history[-3].json()["sql"]
    assert "TABLE_TYPE = 'VIEW'" in sent and "LIMIT 201" in sent

    dropped = run_probe_method(
        "dremio", _recipe(schema_pattern={"deny": ["^@"]}), "views", {}
    )
    assert _names(dropped) == ["lake.db.unread", "space.f1.orders"]
    assert any("1 dataset(s) in home spaces" in w for w in dropped.warnings)


def _cli(tmp_path: Path, config: Dict[str, object], *args: str) -> Result:
    recipe_file = tmp_path / "recipe.yml"
    recipe_file.write_text(
        yaml.safe_dump({"source": {"type": "dremio", "config": config}})
    )
    return CliRunner().invoke(
        recipe_cli, ["probe", "run", args[0], "--recipe", str(recipe_file), *args[1:]]
    )


def test_an_unknown_container_is_the_callers_mistake(
    requests_mock: Any, tmp_path: Path
) -> None:
    _register(requests_mock)
    result = _cli(tmp_path, _recipe(), "folders", "--container", "Space")
    assert result.exit_code == _EXIT_USER, result.output
    assert "'space'" in json.loads(result.stderr)["error"]


def test_a_rejected_token_is_the_sources(requests_mock: Any, tmp_path: Path) -> None:
    _register(requests_mock)
    requests_mock.get(f"{_API}/catalog", status_code=401, json={})
    result = _cli(tmp_path, _recipe(), "sources")
    assert result.exit_code == _EXIT_SOURCE, result.output
    assert _TOKEN not in result.output


def test_a_failed_listing_query_is_the_sources(
    requests_mock: Any, tmp_path: Path
) -> None:
    _register(requests_mock)
    requests_mock.post(f"{_API}/sql", json={"errorMessage": "denied"})
    result = _cli(tmp_path, _recipe(), "tables")
    assert result.exit_code == _EXIT_SOURCE, result.output
