from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Union

import pytest
import requests_mock
import time_machine
from pydantic import SecretStr

from datahub.api.entities.dataprocess.dataprocess_instance import InstanceRunResult
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.boomi_data_integration.client import (
    BoomiDataIntegrationClient,
)
from datahub.ingestion.source.boomi_data_integration.config import (
    BoomiDataIntegrationSourceConfig,
)
from datahub.ingestion.source.boomi_data_integration.models import RiverRun
from datahub.ingestion.source.boomi_data_integration.report import (
    BoomiDataIntegrationSourceReport,
)
from datahub.ingestion.source.boomi_data_integration.run_history import (
    RunGroup,
    select_recent_runs,
)
from datahub.ingestion.source.boomi_data_integration.source import (
    BoomiDataIntegrationSource,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRelationshipsClass,
    DataProcessInstanceRunEventClass,
    DataProcessTypeClass,
)
from datahub.sdk.entity import Entity

ACCOUNT_ID = "acct-0001"
ENV = "env-a"
BASE_URL = f"https://api.rivery.io/v1/accounts/{ACCOUNT_ID}"
ENV_URL = f"{BASE_URL}/environments/{ENV}"
FROZEN_TIME = datetime(2026, 11, 3, 12, tzinfo=timezone.utc)
DAY_MILLIS = 86_400_000
NOW_MILLIS = int(datetime(2026, 11, 3, 12, tzinfo=timezone.utc).timestamp() * 1000)


def _run(
    run_id: str,
    status: str = "succeeded",
    start: int = NOW_MILLIS - DAY_MILLIS,
    end: Optional[int] = None,
    river_id: str = "river-load",
    **fields: object,
) -> Dict[str, object]:
    return {
        "run_id": run_id,
        "river_cross_id": river_id,
        "status": status,
        "datasource_id": "postgres",
        "start_date_in_milliseconds": start,
        "end_date_in_milliseconds": end if end is not None else start + 60_000,
        "run_group_id": run_id,
        "units": 0.5,
        "rpu": 0.5,
        "trigger_name": "api",
        "is_sub_river_run": False,
        **fields,
    }


def _rows(*rows: Dict[str, object]) -> List[RiverRun]:
    return [RiverRun.model_validate(row) for row in rows]


def _group(*rows: Dict[str, object]) -> RunGroup:
    groups = select_recent_runs(_rows(*rows), max_runs=10).groups
    assert len(groups) == 1
    return groups[0]


def test_rows_sharing_a_run_group_fold_into_one_run() -> None:
    group = _group(
        _run("r1", start=1_000, end=5_000, run_group_id="g", target_name="A"),
        _run(
            "r2",
            "failed",
            start=500,
            end=9_000,
            run_group_id="g",
            target_name="B",
            error_description="B broke",
        ),
        _run("r3", start=2_000, end=3_000, run_group_id="g", target_name="C"),
    )

    assert group.group_id == "g"
    assert (group.start_millis, group.end_millis) == (500, 9_000)
    assert group.run_ids == {"r1", "r2", "r3"}
    assert group.result == InstanceRunResult.FAILURE
    assert group.properties() == {
        "status": "failed, succeeded",
        "trigger_name": "api",
        "units": "1.5",
        "rpu": "1.5",
        "error_description": "B broke",
        "table_count": "3",
        "run_group_id": "g",
    }


def test_large_units_keep_full_precision() -> None:
    group = _group(_run("r1", units=1_234_567.25, rpu=None))
    assert group.properties()["units"] == "1234567.25"


def test_row_without_run_group_uses_its_run_id() -> None:
    group = _group(_run("r1", run_group_id=None, target_name="ORDERS"))
    assert group.group_id == "r1"
    assert group.properties()["target_name"] == "ORDERS"
    assert "run_group_id" not in group.properties()


@pytest.mark.parametrize(
    "statuses, expected",
    [
        (["succeeded"], InstanceRunResult.SUCCESS),
        (["SUCCEEDED"], InstanceRunResult.SUCCESS),
        (["failed"], InstanceRunResult.FAILURE),
        (["canceled"], InstanceRunResult.SKIPPED),
        (["canceled", "skipped"], InstanceRunResult.SKIPPED),
        (["succeeded", "canceled"], InstanceRunResult.SUCCESS),
        (["pending"], None),
        (["running", "failed"], None),
        (["queued"], None),
        (["queued", "failed"], InstanceRunResult.FAILURE),
    ],
)
def test_status_mapping(
    statuses: List[str], expected: Optional[InstanceRunResult]
) -> None:
    group = _group(
        *[_run(f"r{i}", status, run_group_id="g") for i, status in enumerate(statuses)]
    )
    assert group.result == expected


def test_error_description_is_truncated() -> None:
    group = _group(_run("r1", "failed", error_description="x" * 2_000))
    assert len(group.properties()["error_description"]) == 500


@pytest.mark.parametrize(
    "trigger, chain, expected",
    [
        ("scheduled", None, DataProcessTypeClass.BATCH_SCHEDULED),
        ("rescheduled", None, DataProcessTypeClass.BATCH_SCHEDULED),
        ("ui", None, DataProcessTypeClass.BATCH_AD_HOC),
        ("api", None, DataProcessTypeClass.BATCH_AD_HOC),
        (None, None, DataProcessTypeClass.BATCH_AD_HOC),
        (
            "logic",
            [{"trigger_name": "scheduled"}, {"trigger_name": "logic", "run_id": "p"}],
            DataProcessTypeClass.BATCH_SCHEDULED,
        ),
        (
            "logic",
            [{"trigger_name": "ui"}, {"trigger_name": "logic", "run_id": "p"}],
            DataProcessTypeClass.BATCH_AD_HOC,
        ),
    ],
)
def test_run_type(
    trigger: Optional[str], chain: Optional[List[Dict[str, str]]], expected: str
) -> None:
    group = _group(_run("r1", trigger_name=trigger, trigger_chain=chain))
    assert group.run_type == expected


def test_cap_keeps_most_recent_runs_first() -> None:
    rows = [_run(f"r{i}", start=i * 1_000) for i in range(5)]
    # Two rows of one older group arrive on either side of the others.
    rows.insert(0, _run("old-a", start=10, run_group_id="old"))
    rows.append(_run("old-b", start=20, run_group_id="old"))

    selection = select_recent_runs(_rows(*rows), max_runs=3)

    assert [g.group_id for g in selection.groups] == ["r4", "r3", "r2"]
    assert selection.scanned == 6
    assert selection.over_cap == 3


def test_pruned_run_is_not_rebuilt_from_later_rows() -> None:
    rows = [
        _run("keep-a", start=1_000, run_group_id="keep"),
        _run("drop-a", start=500, run_group_id="drop"),
        _run("other", start=400),
        # Arrives after "drop" was pruned; on its own it would outrank "keep".
        _run("drop-b", start=5_000, run_group_id="drop"),
        _run("keep-b", start=900, run_group_id="keep"),
    ]

    selection = select_recent_runs(_rows(*rows), max_runs=1)

    assert [g.group_id for g in selection.groups] == ["keep"]
    assert selection.groups[0].run_ids == {"keep-a", "keep-b"}
    assert selection.groups[0].start_millis == 900
    assert (selection.scanned, selection.over_cap) == (3, 2)


def _source(**overrides: object) -> BoomiDataIntegrationSource:
    config: Dict[str, object] = {
        "account_id": ACCOUNT_ID,
        "environment_tokens": [{"environment_id": ENV, "api_token": "token-a"}],
        "max_requests_per_second": 50,
    }
    config.update(overrides)
    return BoomiDataIntegrationSource.create(
        config, PipelineContext(run_id="boomi-di-runs-unit")
    )


def _page(items: List[Dict[str, object]], next_page: Optional[str] = "") -> Dict:
    return {"items": items, "next_page": next_page, "current_page_size": len(items)}


def _summary(river_id: str, river_type: str) -> Dict[str, object]:
    return {"river_cross_id": river_id, "name": river_id, "river_type": river_type}


def _activity(river_id: str, **fields: object) -> Dict[str, object]:
    return {"cross_id": river_id, "river_id": river_id, "failed": 1, **fields}


def _register_rivers(mock: requests_mock.Mocker, env_url: str = ENV_URL) -> None:
    mock.get(env_url, json={"cross_id": ENV, "environment_name": "Prod"})
    mock.get(
        f"{env_url}/rivers",
        json=_page(
            [
                _summary("river-load", "source_to_target"),
                _summary("river-logic", "logic"),
                _summary("river-quiet", "actions"),
            ]
        ),
    )
    mock.get(
        f"{env_url}/rivers/river-load",
        json={
            "cross_id": "river-load",
            "name": "Load",
            "type": "source_to_target",
            "properties": {
                "source": {"name": "postgres"},
                "target": {"name": "snowflake"},
            },
        },
    )
    mock.get(
        f"{env_url}/rivers/river-logic",
        json={"cross_id": "river-logic", "name": "Logic", "type": "logic"},
    )
    mock.get(
        f"{env_url}/rivers/river-quiet",
        json={"cross_id": "river-quiet", "name": "Quiet", "type": "actions"},
    )


def _aspects(
    source: BoomiDataIntegrationSource,
) -> Dict[str, List[object]]:
    stream: Iterable[Union[MetadataWorkUnit, Entity]] = source.get_workunits_internal()
    by_urn: Dict[str, List[object]] = {}
    for item in stream:
        if isinstance(item, MetadataWorkUnit) and isinstance(
            item.metadata, MetadataChangeProposalWrapper
        ):
            by_urn.setdefault(str(item.metadata.entityUrn), []).append(
                item.metadata.aspect
            )
    return by_urn


def _of_type(aspects: List[object], cls: type) -> List:
    return [a for a in aspects if isinstance(a, cls)]


@time_machine.travel(FROZEN_TIME, tick=False)
def test_runs_are_emitted_for_rivers_that_ran() -> None:
    source = _source()
    with requests_mock.Mocker() as mock:
        _register_rivers(mock)
        mock.get(
            f"{ENV_URL}/activities",
            json=_page(
                [
                    _activity("river-load"),
                    _activity("river-logic"),
                    _activity("river-gone", succeeded=3, failed=0),
                ]
            ),
        )
        mock.get(
            f"{ENV_URL}/rivers/river-logic/runs",
            json=_page(
                [
                    _run(
                        "logic-1",
                        river_id="river-logic",
                        trigger_name="scheduled",
                        datasource_id="logic",
                        target_name="Logic",
                    )
                ]
            ),
        )
        mock.get(
            f"{ENV_URL}/rivers/river-load/runs",
            json=_page(
                [
                    _run(
                        "load-1",
                        "running",
                        trigger_name="logic",
                        triggered_by_run_id="logic-1",
                    ),
                    _run("load-2", "canceled", start=NOW_MILLIS - 2 * DAY_MILLIS),
                ]
            ),
        )
        by_urn = _aspects(source)
        history = mock.request_history

    report = source.get_report()
    assert not report.failures and not report.warnings
    assert (report.runs_scanned, report.runs_emitted) == (3, 3)
    assert report.runs_river_not_emitted == 3
    # The river with no activity in the window is never asked for runs.
    assert not [r for r in history if "river-quiet/runs" in r.url]
    runs_call = next(r for r in history if r.path.endswith("river-load/runs"))
    assert runs_call.qs["start_time"] == ["2026-10-27t12:00:00"]
    assert runs_call.qs["end_time"] == ["2026-11-03t12:00:00"]
    assert runs_call.qs["items_per_page"] == ["200"]

    urns = {
        _of_type(aspects, DataProcessInstancePropertiesClass)[0].name: urn
        for urn, aspects in by_urn.items()
        if _of_type(aspects, DataProcessInstancePropertiesClass)
    }
    assert set(urns) == {"logic-1", "load-1", "load-2"}
    runs = {name: by_urn[urn] for name, urn in urns.items()}

    logic_rel = _of_type(runs["logic-1"], DataProcessInstanceRelationshipsClass)[0]
    assert logic_rel.parentTemplate == (
        "urn:li:dataFlow:(boomi-data-integration,river-logic,PROD)"
    )
    assert (
        "target_name"
        not in (
            _of_type(runs["logic-1"], DataProcessInstancePropertiesClass)[0]
        ).customProperties
    )

    child_rel = _of_type(runs["load-1"], DataProcessInstanceRelationshipsClass)[0]
    assert child_rel.parentTemplate == (
        "urn:li:dataJob:(urn:li:dataFlow:(boomi-data-integration,river-load,PROD),load)"
    )
    assert child_rel.parentInstance == urns["logic-1"]
    child_props = _of_type(runs["load-1"], DataProcessInstancePropertiesClass)[0]
    assert child_props.type == DataProcessTypeClass.BATCH_AD_HOC
    child_events = _of_type(runs["load-1"], DataProcessInstanceRunEventClass)
    assert [e.status for e in child_events] == ["STARTED"]

    canceled_events = _of_type(runs["load-2"], DataProcessInstanceRunEventClass)
    assert [e.status for e in canceled_events] == ["STARTED", "COMPLETE"]
    assert canceled_events[1].result.type == InstanceRunResult.SKIPPED
    assert canceled_events[1].durationMillis == 60_000
    assert (
        _of_type(runs["load-2"], DataProcessInstanceRelationshipsClass)[
            0
        ].parentInstance
        is None
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_unknown_status_warns_once_and_gets_start_event_only() -> None:
    source = _source()
    with requests_mock.Mocker() as mock:
        _register_rivers(mock)
        mock.get(f"{ENV_URL}/activities", json=_page([_activity("river-load")]))
        mock.get(
            f"{ENV_URL}/rivers/river-load/runs",
            json=_page(
                [
                    _run("r1", "Queued"),
                    _run("r2", "QUEUED", start=NOW_MILLIS - 2 * DAY_MILLIS),
                ]
            ),
        )
        by_urn = _aspects(source)

    report = source.get_report()
    assert [w.title for w in report.warnings] == [
        "Unknown Boomi Data Integration run status"
    ]
    assert report.runs_unknown_status == 2
    events = [
        e.status
        for aspects in by_urn.values()
        for e in _of_type(aspects, DataProcessInstanceRunEventClass)
    ]
    assert events == ["STARTED", "STARTED"]


@time_machine.travel(FROZEN_TIME, tick=False)
def test_platform_instance_is_set_on_runs() -> None:
    source = _source(platform_instance="eu-prod")
    with requests_mock.Mocker() as mock:
        _register_rivers(mock)
        mock.get(f"{ENV_URL}/activities", json=_page([_activity("river-load")]))
        mock.get(f"{ENV_URL}/rivers/river-load/runs", json=_page([_run("r1")]))
        by_urn = _aspects(source)
    instances = [
        aspect.instance
        for urn, aspects in by_urn.items()
        if urn.startswith("urn:li:dataProcessInstance:")
        for aspect in _of_type(aspects, DataPlatformInstanceClass)
    ]
    assert instances == [
        "urn:li:dataPlatformInstance:(urn:li:dataPlatform:boomi-data-integration,eu-prod)"
    ]


@pytest.mark.parametrize(
    "overrides, api_url, expected",
    [
        (
            {},
            "https://api.rivery.io",
            "https://console.rivery.io/activities/acct-0001/env-a/activities/river-load?param=run_group_id&run=g1",
        ),
        ({"region": "au"}, "https://api.ap-southeast-2.rivery.io", None),
        (
            {"region": "au", "console_url": "https://console.example.com"},
            "https://api.ap-southeast-2.rivery.io",
            "https://console.example.com/activities/acct-0001/env-a/activities/river-load?param=run_group_id&run=g1",
        ),
    ],
)
@time_machine.travel(FROZEN_TIME, tick=False)
def test_run_console_url(
    overrides: Dict[str, object], api_url: str, expected: Optional[str]
) -> None:
    source = _source(**overrides)
    env_url = f"{api_url}/v1/accounts/{ACCOUNT_ID}/environments/{ENV}"
    with requests_mock.Mocker() as mock:
        _register_rivers(mock, env_url)
        mock.get(f"{env_url}/activities", json=_page([_activity("river-load")]))
        mock.get(
            f"{env_url}/rivers/river-load/runs",
            json=_page([_run("r1", run_group_id="g1"), _run("r2", run_group_id=None)]),
        )
        by_urn = _aspects(source)
    urls = sorted(
        (
            (aspect.customProperties.get("run_group_id"), aspect.externalUrl)
            for aspects in by_urn.values()
            for aspect in _of_type(aspects, DataProcessInstancePropertiesClass)
        ),
        key=lambda pair: pair[0] or "",
    )
    # A row without a run group has no confirmed console link.
    assert urls == [(None, None), ("g1", expected)]


@pytest.mark.parametrize("endpoint", ["activities", "rivers/river-load/runs"])
@time_machine.travel(FROZEN_TIME, tick=False)
def test_forbidden_run_history_warns_about_scope(endpoint: str) -> None:
    source = _source()
    with requests_mock.Mocker() as mock:
        _register_rivers(mock)
        mock.get(f"{ENV_URL}/activities", json=_page([_activity("river-load")]))
        mock.get(f"{ENV_URL}/rivers/river-load/runs", json=_page([_run("r1")]))
        mock.get(
            f"{ENV_URL}/{endpoint}",
            status_code=403,
            json={"detail": "Insufficient permissions"},
        )
        by_urn = _aspects(source)

    report = source.get_report()
    assert not report.failures
    assert [w.title for w in report.warnings] == [
        "Cannot read Boomi Data Integration run history"
    ]
    assert "activity:list" in next(iter(report.warnings)).message
    assert list(report.runs_unavailable_environments) == [ENV]
    assert not any("dataProcessInstance" in urn for urn in by_urn)


@time_machine.travel(FROZEN_TIME, tick=False)
def test_server_errors_are_failures() -> None:
    source = _source()
    with requests_mock.Mocker() as mock:
        _register_rivers(mock)
        mock.get(
            f"{ENV_URL}/activities",
            json=_page([_activity("river-load"), _activity("river-logic")]),
        )
        mock.get(f"{ENV_URL}/rivers/river-load/runs", status_code=404, json={})
        mock.get(
            f"{ENV_URL}/rivers/river-logic/runs",
            json=_page([_run("l1", river_id="river-logic")]),
        )
        _aspects(source)

    report = source.get_report()
    assert [f.title for f in report.failures] == ["Failed to list river runs"]
    assert report.runs_emitted == 1


@time_machine.travel(FROZEN_TIME, tick=False)
def test_run_history_can_be_turned_off() -> None:
    source = _source(include_run_history=False)
    with requests_mock.Mocker() as mock:
        _register_rivers(mock)
        _aspects(source)
        assert not [r for r in mock.request_history if "activities" in r.url]


@pytest.mark.parametrize("next_page", ["", None])
def test_run_listing_ends_on_blank_next_page(next_page: Optional[str]) -> None:
    config = BoomiDataIntegrationSourceConfig.model_validate(
        {"account_id": ACCOUNT_ID, "api_token": "t"}
    )
    client = BoomiDataIntegrationClient(config, BoomiDataIntegrationSourceReport())
    start = datetime(2026, 10, 27, 12, 30, 15, 999, tzinfo=timezone.utc)
    with requests_mock.Mocker() as mock:
        mock.get(
            f"{ENV_URL}/rivers/river-load/runs",
            [
                {"json": _page([_run("r1")], next_page="https://next")},
                {"json": _page([], next_page=next_page)},
            ],
        )
        runs = list(
            client.list_river_runs(
                ENV, "river-load", SecretStr("t"), start, start + timedelta(days=1)
            )
        )
        assert [r.run_id for r in runs] == ["r1"]
        assert [r.qs["page"] for r in mock.request_history] == [["1"], ["2"]]
        # Sent as UTC with no zone suffix or fraction.
        assert mock.request_history[0].qs["start_time"] == ["2026-10-27t12:30:15"]


def test_window_is_sent_in_utc() -> None:
    config = BoomiDataIntegrationSourceConfig.model_validate(
        {"account_id": ACCOUNT_ID, "api_token": "t"}
    )
    client = BoomiDataIntegrationClient(config, BoomiDataIntegrationSourceReport())
    plus_two = timezone(timedelta(hours=2))
    with requests_mock.Mocker() as mock:
        mock.get(f"{ENV_URL}/activities", json=_page([]))
        list(
            client.list_activity_summaries(
                ENV,
                SecretStr("t"),
                datetime(2026, 11, 1, 2, tzinfo=plus_two),
                datetime(2026, 11, 2, 2, tzinfo=plus_two),
            )
        )
        assert mock.request_history[0].qs["start_time"] == ["2026-11-01t00:00:00"]
        assert mock.request_history[0].qs["end_time"] == ["2026-11-02t00:00:00"]


@pytest.mark.parametrize(
    "days, valid", [(1, True), (14, True), (0, False), (15, False)]
)
def test_run_history_days_is_bounded_by_retention(days: int, valid: bool) -> None:
    config = {"account_id": ACCOUNT_ID, "api_token": "t", "run_history_days": days}
    if valid:
        BoomiDataIntegrationSourceConfig.model_validate(config)
    else:
        with pytest.raises(ValueError):
            BoomiDataIntegrationSourceConfig.model_validate(config)


@time_machine.travel(FROZEN_TIME, tick=False)
def test_activities_server_error_is_a_failure() -> None:
    source = _source()
    with requests_mock.Mocker() as mock:
        _register_rivers(mock)
        mock.get(f"{ENV_URL}/activities", status_code=500, json={})
        _aspects(source)

    report = source.get_report()
    assert [f.title for f in report.failures] == [
        "Failed to list Boomi Data Integration activities"
    ]
    assert list(report.runs_unavailable_environments) == [ENV]


@time_machine.travel(FROZEN_TIME, tick=False)
def test_sub_river_activity_lists_runs_of_its_master_river() -> None:
    source = _source()
    with requests_mock.Mocker() as mock:
        _register_rivers(mock)
        mock.get(
            f"{ENV_URL}/activities",
            json=_page(
                [
                    _activity(
                        "sub-1",
                        is_sub_river=True,
                        master_river_id="river-load",
                    )
                ]
            ),
        )
        mock.get(
            f"{ENV_URL}/rivers/river-load/runs",
            json=_page(
                [
                    _run(
                        "r1",
                        run_group_id="g",
                        is_sub_river_run=True,
                        sub_river_id="sub-1",
                    ),
                    _run(
                        "r2",
                        run_group_id="g",
                        is_sub_river_run=True,
                        sub_river_id="sub-2",
                    ),
                ]
            ),
        )
        by_urn = _aspects(source)

    properties = [
        p
        for aspects in by_urn.values()
        for p in _of_type(aspects, DataProcessInstancePropertiesClass)
    ]
    assert len(properties) == 1
    assert properties[0].customProperties["sub_river_id"] == "sub-1, sub-2"
