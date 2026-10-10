import json
from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml
from click.testing import CliRunner, Result

from datahub.cli.recipe_cli import recipe as recipe_cli
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import ProbeMethodResult, run_probe_method
from datahub.ingestion.agent.verdicts import Verdict
from datahub.ingestion.source.common.subtypes import BIContainerSubTypes
from datahub.ingestion.source.sigma.config import SigmaSourceConfig
from datahub.ingestion.source.sigma.sigma_selection import (
    DataModelFacts,
    WorkbookFacts,
    data_model_verdict,
    placement_verdict,
    workbook_verdict,
)
from tests.unit.sigma.probe_tenant import (
    API,
    FINANCE_WS,
    PERSONAL_WS,
    SALES_WS,
    recipe,
    register_tenant,
)

pytestmark = pytest.mark.usefixtures("_isolate_secret_registry")

_EXIT_USER = 2
_EXIT_SOURCE = 3
_WORKBOOK = str(BIContainerSubTypes.SIGMA_WORKBOOK)
_DATA_MODEL = str(BIContainerSubTypes.SIGMA_DATA_MODEL)


def _config(**overrides: Any) -> SigmaSourceConfig:
    return SigmaSourceConfig.model_validate(recipe(**overrides))


def _records(result: ProbeMethodResult) -> List[Dict[str, Any]]:
    assert isinstance(result.result, list)
    return result.result


def _names(result: ProbeMethodResult) -> List[str]:
    return [record["name"] for record in _records(result)]


def _run(command: str, **kwargs: object) -> List[Dict[str, Any]]:
    return _records(run_probe_method("sigma", recipe(), command, dict(kwargs)))


def _cli(tmp_path: Path, config: Dict[str, object], *args: str) -> Result:
    recipe_file = tmp_path / "recipe.yml"
    recipe_file.write_text(
        yaml.safe_dump({"source": {"type": "sigma", "config": config}})
    )
    return CliRunner().invoke(
        recipe_cli, ["probe", "run", *args[:1], "--recipe", str(recipe_file), *args[1:]]
    )


def test_placement_reads_the_workspace_name_else_the_shared_switch() -> None:
    config = _config(workspace_pattern={"deny": ["Finance"]})
    assert placement_verdict(config, "Sales") == Verdict.include()
    assert placement_verdict(config, "Finance") == Verdict.exclude("workspace_pattern")
    assert placement_verdict(config, None) == Verdict.exclude("ingest_shared_entities")
    assert (
        placement_verdict(_config(ingest_shared_entities=True), None)
        == Verdict.include()
    )


def test_a_workbooks_name_is_judged_before_its_placement() -> None:
    config = _config(
        workbook_pattern={"deny": ["Draft.*"]}, workspace_pattern={"deny": ["Finance"]}
    )
    facts = WorkbookFacts("Draft One", in_files=False, workspace_name="Finance")
    assert workbook_verdict(config, facts).excluded_by == "workbook_pattern"
    assert (
        workbook_verdict(config, WorkbookFacts("Plan", in_files=False)).excluded_by
        == "missing_file_metadata"
    )
    # Facts a bare --name lacks are skipped, not guessed.
    assert workbook_verdict(config, WorkbookFacts("Plan")) == Verdict.include()
    assert (
        data_model_verdict(
            config, DataModelFacts("M", workspace_name="Finance")
        ).excluded_by
        == "workspace_pattern"
    )


def test_workspaces_are_listed_by_the_name_ingestion_matches(
    requests_mock: Any,
) -> None:
    register_tenant(requests_mock)
    # The API's "User Folder" is matched, and so listed, as "My documents".
    assert [w["name"] for w in _run("workspaces")] == [
        "Sales",
        "Finance",
        "My documents",
    ]


def test_workbooks_carry_the_facts_ingestion_reads(requests_mock: Any) -> None:
    register_tenant(requests_mock)
    records = {r["name"]: r for r in _run("workbooks")}
    assert records["Revenue Overview"]["workspace"] == "Sales"
    assert records["Shared Report"]["has_workspace"] is False
    assert "workspace" not in records["Shared Report"]
    assert records["Unfiled Report"]["in_files"] is False


def test_workbooks_in_one_workspace(requests_mock: Any) -> None:
    register_tenant(requests_mock)
    result = run_probe_method("sigma", recipe(), "workbooks", {"workspace": "Finance"})
    assert _names(result) == ["Budget Plan"]
    assert result.parent_path == ["Finance"]


def test_data_models_need_no_files_row(requests_mock: Any) -> None:
    register_tenant(requests_mock)
    records = {r["name"]: r for r in _run("data_models")}
    assert records["Finance Model"]["workspace"] == "Finance"
    assert "in_files" not in records["Finance Model"]
    assert records["Shared Model"]["has_workspace"] is False


def test_a_personal_space_the_recipe_drops_is_withheld_with_a_count(
    requests_mock: Any,
) -> None:
    register_tenant(requests_mock)
    config = recipe(workspace_pattern={"deny": ["My documents"]})
    workspaces = run_probe_method("sigma", config, "workspaces", {})
    assert _names(workspaces) == ["Sales", "Finance"]
    assert any("1 personal space(s)" in w for w in workspaces.warnings)

    workbooks = run_probe_method("sigma", config, "workbooks", {})
    assert "Scratch Notes" not in _names(workbooks)
    assert any("1 workbook(s) in a personal space" in w for w in workbooks.warnings)


def test_an_ingested_personal_space_is_listed(requests_mock: Any) -> None:
    register_tenant(requests_mock)
    workbooks = run_probe_method("sigma", recipe(), "workbooks", {})
    assert "Scratch Notes" in _names(workbooks)
    assert workbooks.warnings == []


def test_a_saved_workbook_record_is_judged_on_its_placement() -> None:
    result = check_filters(
        source_type="sigma",
        config_dict=recipe(workspace_pattern={"deny": ["Finance"]}),
        kind=_WORKBOOK,
        parent_path=[],
        names=["Budget Plan", "Shared Report", "Unfiled Report"],
        attributes=[
            {"workspace": "Finance", "has_workspace": "true", "in_files": "true"},
            {"has_workspace": "false", "in_files": "true"},
            {"has_workspace": "false", "in_files": "false"},
        ],
    )
    assert [r.excluded_by for r in result.results] == [
        "workspace_pattern",
        "ingest_shared_entities",
        "missing_file_metadata",
    ]
    assert result.warnings == []


def test_a_bare_name_is_judged_on_its_pattern_and_says_what_it_skipped() -> None:
    result = check_filters(
        source_type="sigma",
        config_dict=recipe(data_model_pattern={"deny": ["Old.*"]}),
        kind=_DATA_MODEL,
        parent_path=[],
        names=["Old Model", "New Model"],
    )
    assert [r.included for r in result.results] == [False, True]
    assert any("no workspace for 'New Model'" in w for w in result.warnings)


def test_a_parent_workspace_excludes_what_is_in_it() -> None:
    result = check_filters(
        source_type="sigma",
        config_dict=recipe(workspace_pattern={"deny": ["Finance"]}),
        kind=_WORKBOOK,
        parent_path=["Finance"],
        names=["Budget Plan"],
    )
    assert result.results[0].excluded_by == "workspace_pattern"


def test_the_data_model_switch_excludes_every_data_model() -> None:
    result = check_filters(
        source_type="sigma",
        config_dict=recipe(ingest_data_models=False),
        kind=_DATA_MODEL,
        parent_path=[],
        names=["Sales Model"],
    )
    assert result.results[0].excluded_by == "ingest_data_models"


def test_an_unknown_workspace_is_the_callers_mistake(
    requests_mock: Any, tmp_path: Path
) -> None:
    register_tenant(requests_mock)
    result = _cli(tmp_path, recipe(), "workbooks", "--workspace", "sales")
    assert result.exit_code == _EXIT_USER, result.output
    # A case-only miss names the listed spelling.
    assert "'Sales'" in json.loads(result.stderr)["error"]


def test_a_withheld_personal_space_cannot_be_named(
    requests_mock: Any, tmp_path: Path
) -> None:
    register_tenant(requests_mock)
    result = _cli(
        tmp_path,
        recipe(workspace_pattern={"deny": ["My documents"]}),
        "workbooks",
        "--workspace",
        "My documents",
    )
    assert result.exit_code == _EXIT_USER, result.output


def test_a_rejected_credential_is_the_sources(
    requests_mock: Any, tmp_path: Path
) -> None:
    register_tenant(requests_mock)
    requests_mock.post(f"{API}/auth/token", status_code=401, json={})
    result = _cli(tmp_path, recipe(), "workspaces")
    assert result.exit_code == _EXIT_SOURCE, result.output
    assert "fake-client-secret" not in result.output


def test_a_refused_listing_is_not_an_empty_tenant(
    requests_mock: Any, tmp_path: Path
) -> None:
    register_tenant(
        requests_mock,
        overrides={f"{API}/workspaces?limit=50": {"status_code": 403, "json": {}}},
    )
    result = _cli(tmp_path, recipe(), "workspaces")
    assert result.exit_code == _EXIT_SOURCE, result.output


def test_a_refused_workspace_lookup_leaves_its_workbooks_shared(
    requests_mock: Any,
) -> None:
    register_tenant(
        requests_mock,
        overrides={
            f"{API}/workspaces?limit=50": {"json": {"entries": [], "nextPage": None}},
            f"{API}/workspaces/{SALES_WS}": {"status_code": 403, "json": {}},
            f"{API}/workspaces/{FINANCE_WS}": {"status_code": 403, "json": {}},
            f"{API}/workspaces/{PERSONAL_WS}": {"status_code": 403, "json": {}},
        },
    )
    records = {r["name"]: r for r in _run("workbooks")}
    # As ingestion: a workspace that refuses is no workspace at all.
    assert records["Budget Plan"]["has_workspace"] is False
