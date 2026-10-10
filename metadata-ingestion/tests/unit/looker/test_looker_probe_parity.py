import dataclasses
import re
from pathlib import Path
from typing import Any, Dict, List, Set
from unittest import mock

import pytest

from datahub.metadata.urns import ChartUrn, DashboardUrn, DatasetUrn
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    ParityListing,
    ParityReport,
    assert_probe_parity,
    by_name,
    pipeline_ingestion,
)
from tests.unit.looker.looker_probe_fixtures import fake_looker, recipe

_STATE_GRAPH = (
    "datahub.ingestion.source.state_provider."
    "datahub_ingestion_checkpointing_provider.DataHubGraph"
)


def _chart_ids(index: EmittedIndex) -> Set[str]:
    # urn:li:chart:(looker,dashboard_elements.<element id | looks_<look id>>)
    return {
        ChartUrn.from_string(u).chart_id.removeprefix("dashboard_elements.")
        for u in index.urns("chart")
    }


def _explore_names(index: EmittedIndex, model: str) -> Set[str]:
    prefix = f"{model}.explore."
    names = {DatasetUrn.from_string(u).name for u in index.urns("dataset")}
    return {n.removeprefix(prefix) for n in names if n.startswith(prefix)}


def _parity(
    tmp_path: Path, config: Dict[str, Any], listings: List[ParityListing]
) -> ParityReport:
    with fake_looker():
        ingest = pipeline_ingestion("looker", tmp_path)
        return assert_probe_parity("looker", config, ingest, listings)


_DASHBOARDS = ParityListing(
    "dashboards",
    "dashboards",
    lambda i: {
        DashboardUrn.from_string(u).dashboard_id.removeprefix("dashboards.")
        for u in i.urns("dashboard")
    },
)
# Element ids are unique across dashboards, and the chart URN carries no
# dashboard, so a chart is matched by its bare id.
_CHARTS = ParityListing(
    "charts",
    "charts",
    lambda i: {c for c in _chart_ids(i) if not c.startswith("looks_")},
    fan_out=FanOut("dashboards", "dashboard"),
    identity=by_name,
)
_LOOKS = ParityListing(
    "looks",
    "looks",
    lambda i: {
        c.removeprefix("looks_") for c in _chart_ids(i) if c.startswith("looks_")
    },
    kwargs={"trace_charts": True},
)
_EXPLORES = ParityListing(
    "explores",
    "explores",
    lambda i: _explore_names(i, "sales"),
    kwargs={"model": "sales"},
)
_TRACED_EXPLORES = dataclasses.replace(
    _EXPLORES, kwargs={"model": "sales", "trace_charts": True}
)
_MODELS = ParityListing("models", "models", lambda i: i.container_names("LookML Model"))
_TRACED_MODELS = dataclasses.replace(_MODELS, kwargs={"trace_charts": True})

# looker_probe.py, `_note_parent_dashboard`: `charts --dashboard <id>` warns,
# under a dashboard one of ingestion's non-id rules drops, that ingestion reads
# none of its charts. That is a note on the parent's verdict, not a degraded
# fetch. fullmatch anchors each pattern at both ends; `\d+` keeps them to the
# dashboard ids the fixture lists.
_PARENT_DELETED = re.compile(
    r"dashboard '\d+' is deleted and include_deleted is false, so ingestion "
    r"reads none of its charts"
)
_PARENT_PERSONAL = re.compile(
    r"dashboard '\d+' is in a personal folder and skip_personal_folders is "
    r"set, so ingestion reads none of its charts"
)
_PARENT_FOLDER_DENIED = re.compile(
    r"dashboard '\d+' is in (?:folder '[^']*'|a personal folder), which "
    r"folder_path_pattern denies, so ingestion reads none of its charts"
)


def test_dashboards_and_charts_match_ingestion(tmp_path: Path) -> None:
    config = recipe(
        dashboard_pattern={"deny": ["^2$"]},
        chart_pattern={"deny": ["^14$"]},
        skip_personal_folders=True,
        folder_path_pattern={"deny": ["^Shared/Archive"]},
    )
    charts = dataclasses.replace(
        _CHARTS,
        accept_warnings=(_PARENT_DELETED, _PARENT_PERSONAL, _PARENT_FOLDER_DENIED),
    )
    report = _parity(tmp_path, config, [_DASHBOARDS, charts])
    assert report.kinds["dashboards"].included == {"1", "6"}
    assert report.excluded_by("dashboards") == {
        "2": "dashboard_pattern",
        "3": "skip_personal_folders",
        "4": "include_deleted",
        "5": "folder_path_pattern",
    }
    assert report.kinds["charts"].included == {"11", "61", "62"}
    noted = {w.split("'")[1]: w for w in report.kinds["charts"].accepted_warnings}
    assert set(noted) == {"3", "4", "5"}
    assert _PARENT_PERSONAL.fullmatch(noted["3"])
    assert _PARENT_DELETED.fullmatch(noted["4"])
    assert _PARENT_FOLDER_DENIED.fullmatch(noted["5"])


def test_deleted_dashboards_and_a_withheld_personal_path_match_ingestion(
    tmp_path: Path,
) -> None:
    config = recipe(
        include_deleted=True,
        dashboard_pattern={"deny": ["^2$"]},
        folder_path_pattern={"deny": ["^Users/"]},
    )
    # Dashboard 3 sits under the denied Users/ path; 2 is dropped by id,
    # which gives no note.
    charts = dataclasses.replace(_CHARTS, accept_warnings=(_PARENT_FOLDER_DENIED,))
    report = _parity(tmp_path, config, [_DASHBOARDS, charts])
    assert report.kinds["dashboards"].included == {"1", "4", "5", "6"}
    assert report.excluded_by("dashboards")["3"] == "folder_path_pattern"


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
    ingest = pipeline_ingestion("looker", tmp_path, pipeline_name="probe-parity-looks")
    with fake_looker(), mock.patch(_STATE_GRAPH, mock_datahub_graph) as graph:
        graph.return_value = mock_datahub_graph
        report = assert_probe_parity("looker", config, ingest, [_LOOKS])
        # Untraced, the look on a dashboard is the one name it cannot judge.
        with pytest.raises(
            AssertionError,
            match=r"disagree:\n  looks: probe filter includes '105', but ingestion did not emit it\Z",
        ):
            assert_probe_parity(
                "looker", config, ingest, [dataclasses.replace(_LOOKS, kwargs={})]
            )
    assert report.kinds["looks"].included == {"101"}
    assert report.excluded_by("looks") == {
        "102": "skip_personal_folders",
        "103": "look_has_no_query",
        "104": "include_deleted",
        "105": "on_a_kept_dashboard",
        "106": "look_has_no_query",
    }


def test_explores_and_models_match_ingestion_when_not_used_only(
    tmp_path: Path,
) -> None:
    config = recipe(emit_used_explores_only=False)
    report = _parity(tmp_path, config, [_EXPLORES, _MODELS])
    assert report.kinds["explores"].included == {
        "orders",
        "customers",
        "archived",
        "unused",
    }
    assert report.kinds["models"].included == {"sales"}
    assert report.excluded_by("models")["empty"] == "model_has_no_explores"


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
    report = _parity(tmp_path, recipe(**filters), [_TRACED_EXPLORES, _TRACED_MODELS])
    assert report.kinds["models"].included == {"sales"}
    assert report.excluded_by("models")["empty"] == "emit_used_explores_only"
