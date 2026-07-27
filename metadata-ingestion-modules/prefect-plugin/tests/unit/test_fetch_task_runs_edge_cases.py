"""Tests for _fetch_task_runs_page pagination and _fetch_all_task_runs_stable polling."""

import asyncio
import logging
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from prefect.client.schemas import TaskRun
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId
from prefect.client.schemas.sorting import TaskRunSort

from prefect_datahub.datahub_emitter import DatahubEmitter

VALID_FLOW_RUN_ID = "c3b947e5-3fa1-4b46-a2e2-58d50c938f2e"


def _make_task_run(task_run_id: str) -> TaskRun:
    return TaskRun.model_validate(
        {
            "id": task_run_id,
            "name": f"task-{task_run_id[:8]}",
            "flow_run_id": VALID_FLOW_RUN_ID,
            "task_key": f"__main__.task_{task_run_id[:8]}",
            "dynamic_key": "0",
            "state_type": "COMPLETED",
            "state_name": "Completed",
            "run_count": 1,
            "flow_run_run_count": 1,
            "total_run_time": 0.1,
            "estimated_run_time": 0.1,
            "estimated_start_time_delta": 0.0,
        }
    )


def _fake_task_runs(n: int, start: int = 0) -> List[TaskRun]:
    # Unique IDs per call so multi-page tests don't collide after dedup-by-id.
    return [
        _make_task_run(f"00000000-0000-0000-0000-{i:012d}")
        for i in range(start, start + n)
    ]


@pytest.fixture
def emitter():
    with patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True):
        yield DatahubEmitter()


@pytest.fixture
def fast_sleep():
    async def _no_sleep(_seconds):
        return None

    with patch(
        "prefect_datahub.datahub_emitter.asyncio.sleep", side_effect=_no_sleep
    ) as p:
        yield p


@pytest.fixture
def mock_run_logger():
    fake_logger = MagicMock(spec=logging.Logger)
    with patch(
        "prefect_datahub.datahub_emitter.get_run_logger", return_value=fake_logger
    ):
        yield fake_logger


def test_fetch_task_runs_page_converts_str_to_uuid_in_filter(emitter):
    captured = {}

    async def fake_read_task_runs(**kwargs):
        captured.update(kwargs)
        return []

    client = MagicMock()
    client.read_task_runs = AsyncMock(side_effect=fake_read_task_runs)

    with patch("prefect_datahub.datahub_emitter.orchestration") as mock_orch:
        mock_orch.get_client.return_value = client
        asyncio.run(emitter._fetch_task_runs_page(VALID_FLOW_RUN_ID, offset=0))

    flow_run_filter = captured["flow_run_filter"]
    assert isinstance(flow_run_filter, FlowRunFilter)
    assert isinstance(flow_run_filter.id, FlowRunFilterId)
    assert flow_run_filter.id.any_ == [UUID(VALID_FLOW_RUN_ID)]


def test_fetch_task_runs_page_passes_paging_args(emitter):
    captured = {}

    async def fake_read_task_runs(**kwargs):
        captured.update(kwargs)
        return []

    client = MagicMock()
    client.read_task_runs = AsyncMock(side_effect=fake_read_task_runs)

    with patch("prefect_datahub.datahub_emitter.orchestration") as mock_orch:
        mock_orch.get_client.return_value = client
        asyncio.run(emitter._fetch_task_runs_page(VALID_FLOW_RUN_ID, offset=400))

    assert captured["limit"] == 200
    assert captured["offset"] == 400
    assert captured["sort"] == TaskRunSort.ID_DESC


@pytest.mark.parametrize("count", [0, 50])
def test_stable_single_page_stabilises_without_warning(
    emitter, fast_sleep, mock_run_logger, count
):
    page = _fake_task_runs(count)
    with patch.object(
        emitter, "_fetch_task_runs_page", new=AsyncMock(return_value=page)
    ):
        result = asyncio.run(emitter._fetch_all_task_runs_stable(VALID_FLOW_RUN_ID))
    assert len(result) == count
    mock_run_logger.warning.assert_not_called()


def test_stable_exactly_200_then_empty_second_page(emitter, fast_sleep):
    first = _fake_task_runs(200)

    async def side(_flow_run_id, offset):
        return first if offset == 0 else []

    fetch = AsyncMock(side_effect=side)
    with patch.object(emitter, "_fetch_task_runs_page", new=fetch):
        result = asyncio.run(emitter._fetch_all_task_runs_stable(VALID_FLOW_RUN_ID))
    assert len(result) == 200


def test_stable_multi_page_above_limit(emitter, fast_sleep):
    pages = {
        0: _fake_task_runs(200, start=0),
        200: _fake_task_runs(200, start=200),
        400: _fake_task_runs(50, start=400),
    }

    async def side(_flow_run_id, offset):
        return pages.get(offset, [])

    fetch = AsyncMock(side_effect=side)
    with patch.object(emitter, "_fetch_task_runs_page", new=fetch):
        result = asyncio.run(emitter._fetch_all_task_runs_stable(VALID_FLOW_RUN_ID))
    assert len(result) == 450


def test_stable_warns_when_count_never_stabilises(emitter, fast_sleep, mock_run_logger):
    counter = {"n": 0}

    async def side(_flow_run_id, offset):
        if offset == 0:
            counter["n"] += 1
            return _fake_task_runs(counter["n"])
        return []

    with patch.object(
        emitter, "_fetch_task_runs_page", new=AsyncMock(side_effect=side)
    ):
        result = asyncio.run(emitter._fetch_all_task_runs_stable(VALID_FLOW_RUN_ID))

    assert len(result) == 50
    mock_run_logger.warning.assert_called_once()
    msg = mock_run_logger.warning.call_args.args[0]
    assert "did not stabilise" in msg


def test_stable_late_stabilisation_does_not_warn(emitter, fast_sleep, mock_run_logger):
    counts = [1, 2] + [5] * 100

    async def side(_flow_run_id, offset):
        if offset == 0:
            n = counts.pop(0)
            return _fake_task_runs(n)
        return []

    with patch.object(
        emitter, "_fetch_task_runs_page", new=AsyncMock(side_effect=side)
    ):
        result = asyncio.run(emitter._fetch_all_task_runs_stable(VALID_FLOW_RUN_ID))

    assert len(result) == 5
    mock_run_logger.warning.assert_not_called()


def test_stable_dedup_removes_cross_page_duplicates(emitter, fast_sleep):
    """When offset paging returns a task_run ID already seen on a prior page
    (possible if a row is inserted between page fetches), the dedup-by-id step
    must collapse duplicates so each task run appears exactly once."""
    page1 = _fake_task_runs(200, start=0)
    # Simulate a race: page2 includes an ID already returned by page1
    dup = _make_task_run(str(page1[5].id))
    new = _make_task_run("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    page2 = [dup, new]

    async def side(_flow_run_id, offset):
        return page1 if offset == 0 else page2

    with patch.object(
        emitter, "_fetch_task_runs_page", new=AsyncMock(side_effect=side)
    ):
        result = asyncio.run(emitter._fetch_all_task_runs_stable(VALID_FLOW_RUN_ID))

    assert len(result) == 201  # 200 unique from page1 + 1 new (dup collapsed)
    ids = [str(r.id) for r in result]
    assert ids.count(str(page1[5].id)) == 1
