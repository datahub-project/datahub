import json
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple
from unittest import mock

import pytest

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.filter_input import listing_from_run
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.run.pipeline import Pipeline
from datahub.metadata.urns import ChartUrn, DashboardUrn, DatasetUrn
from tests.unit.looker.looker_probe_fixtures import fake_looker, recipe

_STATE_GRAPH = (
    "datahub.ingestion.source.state_provider."
    "datahub_ingestion_checkpointing_provider.DataHubGraph"
)


def _ingest(tmp_path: Path, config: Dict[str, Any], **pipeline: Any) -> List[dict]:
    out = tmp_path / "out.json"
    with fake_looker():
        run = Pipeline.create(
            {
                "run_id": "probe-parity",
                **pipeline,
                "source": {"type": "looker", "config": config},
                "sink": {"type": "file", "config": {"filename": str(out)}},
            }
        )
        run.run()
        run.raise_from_status()
    return json.loads(out.read_text())


def _urns(records: List[dict], entity_type: str) -> Set[str]:
    return {
        r["entityUrn"]
        for r in records
        if r.get("entityType") == entity_type and "entityUrn" in r
    }


def _dashboard_ids(records: List[dict]) -> Set[str]:
    # urn:li:dashboard:(looker,dashboards.<id>)
    return {
        DashboardUrn.from_string(u).dashboard_id.removeprefix("dashboards.")
        for u in _urns(records, "dashboard")
    }


def _chart_ids(records: List[dict]) -> Set[str]:
    # urn:li:chart:(looker,dashboard_elements.<element id | looks_<look id>>)
    return {
        ChartUrn.from_string(u).chart_id.removeprefix("dashboard_elements.")
        for u in _urns(records, "chart")
    }


def _explore_names(records: List[dict], model: str) -> Set[str]:
    prefix = f"{model}.explore."
    names = {DatasetUrn.from_string(u).name for u in _urns(records, "dataset")}
    return {n.removeprefix(prefix) for n in names if n.startswith(prefix)}


def _model_names(records: List[dict]) -> Set[str]:
    named: Dict[str, str] = {}
    models: Set[str] = set()
    for r in records:
        if r.get("entityType") != "container":
            continue
        aspect = (r.get("aspect") or {}).get("json") or {}
        if r.get("aspectName") == "containerProperties":
            named[r["entityUrn"]] = aspect["name"]
        if r.get("aspectName") == "subTypes" and "LookML Model" in aspect.get(
            "typeNames", []
        ):
            models.add(r["entityUrn"])
    return {named[u] for u in models if u in named}


def _probe(
    config: Dict[str, Any], command: str, params: Dict[str, Any]
) -> Tuple[Set[str], Dict[str, Any]]:
    """`probe run <command> --report-to r.json`, then `probe filter
    --from-run r.json`: the JSON round trip is the one the CLI performs."""
    with fake_looker():
        run = run_probe_method("looker", config, command, params)
    envelope = json.loads(json.dumps(run.to_dict()))
    listing = listing_from_run(envelope)
    result = check_filters(
        source_type="looker",
        config_dict=config,
        kind=str(listing.kind),
        parent_path=listing.parent_path,
        names=listing.names,
        attributes=listing.attributes,
    )
    included = {v.name for v in result.results if v.included}
    return included, {v.name: v.excluded_by for v in result.results}


def test_dashboards_and_charts_match_ingestion(tmp_path: Path) -> None:
    config = recipe(
        dashboard_pattern={"deny": ["^2$"]},
        chart_pattern={"deny": ["^14$"]},
        skip_personal_folders=True,
        folder_path_pattern={"deny": ["^Shared/Archive"]},
    )
    emitted = _ingest(tmp_path, config)
    included, reasons = _probe(config, "dashboards", {})
    assert included == _dashboard_ids(emitted) == {"1", "6"}
    assert reasons == {
        "1": None,
        "2": "dashboard_pattern",
        "3": "skip_personal_folders",
        "4": "include_deleted",
        "5": "folder_path_pattern",
        "6": None,
    }
    # Every listed dashboard, kept or not: the charts of one ingestion drops
    # must read excluded on their own listing's facts.
    charts: Set[str] = set()
    for dashboard in sorted(reasons):
        kept, _ = _probe(config, "charts", {"dashboard": dashboard})
        charts |= kept
    assert charts == _chart_ids(emitted) == {"11", "61", "62"}


def test_deleted_dashboards_and_a_withheld_personal_path_match_ingestion(
    tmp_path: Path,
) -> None:
    config = recipe(
        include_deleted=True,
        dashboard_pattern={"deny": ["^2$"]},
        folder_path_pattern={"deny": ["^Users/"]},
    )
    emitted = _ingest(tmp_path, config)
    included, reasons = _probe(config, "dashboards", {})
    assert included == _dashboard_ids(emitted) == {"1", "4", "5", "6"}
    assert reasons["3"] == "folder_path_pattern"


def test_standalone_looks_match_ingestion(
    tmp_path: Path, mock_datahub_graph: mock.MagicMock
) -> None:
    config = recipe(
        extract_independent_looks=True,
        skip_personal_folders=True,
        stateful_ingestion={
            "enabled": True,
            "state_provider": {
                "type": "datahub",
                "config": {"datahub_api": {"server": "http://localhost:8080"}},
            },
        },
    )
    with mock.patch(_STATE_GRAPH, mock_datahub_graph) as graph:
        graph.return_value = mock_datahub_graph
        emitted = _ingest(tmp_path, config, pipeline_name="probe-parity-looks")
    standalone = {
        c.removeprefix("looks_") for c in _chart_ids(emitted) if c.startswith("looks_")
    }
    included, reasons = _probe(config, "looks", {"trace_charts": True})
    assert included == standalone == {"101"}
    assert reasons == {
        "101": None,
        "102": "skip_personal_folders",
        "103": "look_has_no_query",
        "104": "include_deleted",
        "105": "on_a_kept_dashboard",
        "106": "look_has_no_query",
    }
    # Untraced, the look on a dashboard is the one name it cannot judge.
    untraced, _ = _probe(config, "looks", {})
    assert untraced - standalone == {"105"}


def test_explores_and_models_match_ingestion_when_not_used_only(
    tmp_path: Path,
) -> None:
    config = recipe(emit_used_explores_only=False)
    emitted = _ingest(tmp_path, config)
    explores, _ = _probe(config, "explores", {"model": "sales"})
    assert explores == _explore_names(emitted, "sales") == {
        "orders",
        "customers",
        "archived",
        "unused",
    }
    models, reasons = _probe(config, "models", {})
    assert models == _model_names(emitted) == {"sales"}
    assert reasons["empty"] == "model_has_no_explores"


@pytest.mark.parametrize(
    "filters",
    [
        # Ingestion records an explore as used before folder_path_pattern
        # drops the dashboard, so `archived` is still emitted here.
        {
            "dashboard_pattern": {"deny": ["^2$"]},
            "folder_path_pattern": {"deny": ["^Shared/Archive"]},
        },
        {"chart_pattern": {"deny": ["^51$"]}},
        {"dashboard_pattern": {"deny": ["^5$", "^6$"]}},
    ],
)
def test_traced_explores_and_models_match_ingestion_by_default(
    tmp_path: Path, filters: Dict[str, Any]
) -> None:
    config = recipe(**filters)
    emitted = _ingest(tmp_path, config)
    explores, _ = _probe(config, "explores", {"model": "sales", "trace_charts": True})
    assert explores == _explore_names(emitted, "sales")
    models, reasons = _probe(config, "models", {"trace_charts": True})
    assert models == _model_names(emitted) == {"sales"}
    assert reasons["empty"] == "emit_used_explores_only"
