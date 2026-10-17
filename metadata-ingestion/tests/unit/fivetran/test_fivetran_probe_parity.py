"""Fivetran's `probe filter` verdicts, checked against what ingestion emits."""

import dataclasses
from pathlib import Path
from typing import Dict, Set

import pytest

from datahub.ingestion.source.fivetran.config import REST_CONNECTOR_MATCH_NOTE
from datahub.metadata.urns import DataFlowUrn
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    JudgedRecord,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)
from tests.unit.fivetran.fivetran_probe_fixtures import (
    API_CONFIG,
    db_recipe,
    mocked_log_db,
    mocked_rest_api,
)


def _connector_ids(index: EmittedIndex) -> Set[str]:
    # urn:li:dataFlow:(fivetran,<connector_id>,<env>)
    return {DataFlowUrn.from_string(u).flow_id for u in index.urns("dataFlow")}


def _connector_id(record: JudgedRecord) -> str:
    return record.attributes["connector_id"]


# A listing taken without --destination carries destination_id per record;
# one taken under each destination is judged through --parent. Both must agree.
_LISTINGS = [
    ParityListing("connectors", "connectors", _connector_ids, identity=_connector_id),
    ParityListing(
        "by-destination",
        "connectors",
        _connector_ids,
        identity=_connector_id,
        fan_out=FanOut("destinations", "destination"),
    ),
]
# In REST mode every `connectors` listing warns REST_CONNECTOR_MATCH_NOTE
# (fivetran_probe.py, `connectors`: `if self._uses_rest: self._warn(...)`).
# It is a note on the rule, not a degraded fetch, so the REST cases accept
# it by name; the DB cases must not, or the entry would fail as stale.
_REST_LISTINGS = [
    dataclasses.replace(listing, accept_warnings=(REST_CONNECTOR_MATCH_NOTE,))
    for listing in _LISTINGS
]

_DENY_HR = {"connector_patterns": {"deny": ["^hr_pg$"]}}
_DENY_DEST_B = {"destination_patterns": {"deny": ["^dest_b$"]}}
# conn_a1 is sales_pg's id: REST keeps it through the id, DB (name only) drops it.
_BY_ID_OR_NAME = {"connector_patterns": {"allow": ["^conn_a1$", "^sheets$"]}}


@pytest.mark.parametrize("filters", [_DENY_HR, _DENY_DEST_B, _BY_ID_OR_NAME])
def test_log_database_mode_agrees_with_ingestion(
    tmp_path: Path, filters: Dict[str, object]
) -> None:
    with mocked_log_db():
        assert_probe_parity(
            "fivetran",
            db_recipe(**filters),
            pipeline_ingestion("fivetran", tmp_path),
            _LISTINGS,
        )


@pytest.mark.parametrize("filters", [_DENY_HR, _DENY_DEST_B, _BY_ID_OR_NAME])
def test_rest_mode_agrees_with_ingestion(
    tmp_path: Path, filters: Dict[str, object]
) -> None:
    with mocked_rest_api():
        report = assert_probe_parity(
            "fivetran",
            {"api_config": API_CONFIG, **filters},
            pipeline_ingestion("fivetran", tmp_path),
            _REST_LISTINGS,
        )
    # The listing carried every fact, so no half-judged warning from
    # probe filter, and the run's only warning was the accepted note.
    assert report.kinds["connectors"].warnings == ()
    assert report.kinds["connectors"].accepted_warnings == (REST_CONNECTOR_MATCH_NOTE,)


def test_each_reader_reports_its_own_first_exclusion(tmp_path: Path) -> None:
    both = {"connector_patterns": {"deny": ["^hr_pg$"]}, **_DENY_DEST_B}
    with mocked_log_db():
        db = assert_probe_parity(
            "fivetran",
            db_recipe(**both),
            pipeline_ingestion("fivetran", tmp_path),
            _LISTINGS[:1],
        )
    with mocked_rest_api():
        rest = assert_probe_parity(
            "fivetran",
            {"api_config": API_CONFIG, **both},
            pipeline_ingestion("fivetran", tmp_path),
            _REST_LISTINGS[:1],
        )
    assert db.excluded_by("connectors") == {"conn_b1": "connector_patterns"}
    assert rest.excluded_by("connectors") == {"conn_b1": "destination_patterns"}
