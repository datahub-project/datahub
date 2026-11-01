"""PowerBiAPI refresh-history and refresh-schedule handling (`extract_refresh_history`)."""

from typing import Any, Dict, List, Optional, Union
from unittest.mock import MagicMock

import requests

from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    DatasetRefresh,
    DatasetRefreshSchedule,
    PowerBIDataset,
    Workspace,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import PowerBiAPI

WORKSPACE_ID = "64ED5CAD-7C10-4684-8180-826122881108"
DATASET_ID = "05169CD2-E713-41E6-9600-1D8066D95445"

SCHEDULE = DatasetRefreshSchedule(
    days=["Monday"], times=["07:00"], local_time_zone_id="UTC", enabled=True
)


def _http_error(status: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(response=response)


def _refresh(status: str, request_id: str) -> DatasetRefresh:
    return DatasetRefresh(
        request_id=request_id,
        refresh_type="Scheduled",
        status=status,
        start_time="2024-01-02T03:04:05Z",
        end_time="2024-01-02T03:05:05Z",
    )


def _workspace() -> Workspace:
    return Workspace(
        id=WORKSPACE_ID,
        name="demo-workspace",
        type="Workspace",
        webUrl=None,
        datasets={},
        dashboards={},
        reports={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
    )


def _dataset() -> PowerBIDataset:
    return PowerBIDataset(
        id=DATASET_ID,
        name="Insurance Model",
        description="",
        webUrl=None,
        workspace_id=WORKSPACE_ID,
        workspace_name="demo-workspace",
        parameters={},
        tables=[],
        tags=[],
    )


def _api(
    refreshes: Union[List[DatasetRefresh], Exception],
    schedule: Union[DatasetRefreshSchedule, Exception] = SCHEDULE,
    extract_refresh_history: bool = True,
) -> PowerBiAPI:
    base: Dict[str, Any] = {
        "tenant_id": "fake",
        "client_id": "foo",
        "client_secret": "bar",
        "extract_refresh_history": extract_refresh_history,
    }
    api = PowerBiAPI.__new__(PowerBiAPI)
    api._PowerBiAPI__config = PowerBiDashboardSourceConfig.model_validate(base)  # type: ignore[attr-defined]
    api.reporter = PowerBiDashboardSourceReport()
    api.log_http_error = MagicMock()  # type: ignore[method-assign]

    regular = MagicMock()
    if isinstance(refreshes, Exception):
        regular.get_dataset_refreshes.side_effect = refreshes
    else:
        regular.get_dataset_refreshes.return_value = refreshes
    if isinstance(schedule, Exception):
        regular.get_dataset_refresh_schedule.side_effect = schedule
    else:
        regular.get_dataset_refresh_schedule.return_value = schedule
    api._PowerBiAPI__regular_api_resolver = regular  # type: ignore[attr-defined]
    return api


def _warning_titles(api: PowerBiAPI) -> List[Optional[str]]:
    return [w.title for w in api.reporter.warnings]


def test_latest_completed_and_latest_failed_refresh_are_kept() -> None:
    # Newest first: a second, older failure must not replace the newest one,
    # and a Disabled entry matches neither bucket.
    api = _api(
        [
            _refresh("Failed", "r-failed-new"),
            _refresh("Disabled", "r-disabled"),
            _refresh("Completed", "r-ok"),
            _refresh("Failed", "r-failed-old"),
        ]
    )
    dataset = _dataset()

    api._fill_refresh_history(_workspace(), dataset)

    assert dataset.last_refresh is not None
    assert dataset.last_refresh.request_id == "r-ok"
    assert dataset.last_failed_refresh is not None
    assert dataset.last_failed_refresh.request_id == "r-failed-new"
    assert dataset.refresh_schedule == SCHEDULE
    assert api.reporter.refresh_history_fetched == 1
    assert api.reporter.refresh_schedule_fetched == 1
    assert _warning_titles(api) == []


def test_not_refreshable_dataset_is_skipped_without_warning() -> None:
    # System datasets such as the usage-metrics model answer 415; that is not
    # a loss worth an operator's attention, and the schedule is not asked for.
    api = _api(_http_error(415))
    dataset = _dataset()

    api._fill_refresh_history(_workspace(), dataset)

    assert dataset.last_refresh is None
    assert dataset.refresh_schedule is None
    assert api.reporter.datasets_not_refreshable == 1
    assert api.reporter.refresh_history_fetched == 0
    assert _warning_titles(api) == []
    api._PowerBiAPI__regular_api_resolver.get_dataset_refresh_schedule.assert_not_called()  # type: ignore[attr-defined]


def test_other_refresh_history_errors_warn() -> None:
    api = _api(_http_error(500))

    api._fill_refresh_history(_workspace(), _dataset())

    assert _warning_titles(api) == ["Refresh History Not Fetched"]
    assert api.reporter.datasets_not_refreshable == 0


def test_missing_schedule_is_silent() -> None:
    for status in (404, 400):
        api = _api([_refresh("Completed", "r-ok")], schedule=_http_error(status))
        dataset = _dataset()

        api._fill_refresh_history(_workspace(), dataset)

        assert dataset.last_refresh is not None
        assert dataset.refresh_schedule is None
        assert api.reporter.refresh_schedule_not_configured == 1
        assert api.reporter.refresh_schedule_failed == 0
        assert _warning_titles(api) == []


def test_other_schedule_errors_warn_but_keep_the_history() -> None:
    api = _api([_refresh("Completed", "r-ok")], schedule=_http_error(500))
    dataset = _dataset()

    api._fill_refresh_history(_workspace(), dataset)

    assert dataset.last_refresh is not None
    assert dataset.refresh_schedule is None
    assert api.reporter.refresh_schedule_failed == 1
    assert _warning_titles(api) == ["Refresh Schedule Not Fetched"]


def test_disabled_flag_makes_no_calls() -> None:
    api = _api([_refresh("Completed", "r-ok")], extract_refresh_history=False)
    dataset = _dataset()

    api._fill_refresh_history(_workspace(), dataset)

    assert dataset.last_refresh is None
    api._PowerBiAPI__regular_api_resolver.get_dataset_refreshes.assert_not_called()  # type: ignore[attr-defined]
