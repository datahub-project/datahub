import time
from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import MagicMock

from datahub.ingestion.source.gc.execution_request_cleanup import (
    CleanupRecord,
    DatahubExecutionRequestCleanup,
    DatahubExecutionRequestCleanupConfig,
    DatahubExecutionRequestCleanupReport,
)

DAY_MS = 24 * 3600 * 1000
NOW_MS = int(time.time()) * 1000


def _record(
    urn: str,
    *,
    days_ago: float,
    ingestion_source: str = "",
    source_type: str = "",
    status: str = "SUCCESS",
) -> CleanupRecord:
    return CleanupRecord(
        urn=urn,
        request_id=urn.split(":")[-1],
        status=status,
        ingestion_source=ingestion_source,
        source_type=source_type,
        requested_at=int(NOW_MS - days_ago * DAY_MS),
    )


def _deleted_urns(
    config: DatahubExecutionRequestCleanupConfig, records: List[CleanupRecord]
) -> List[str]:
    cleanup = DatahubExecutionRequestCleanup(
        graph=MagicMock(),
        report=DatahubExecutionRequestCleanupReport(),
        config=config,
    )

    # Feed the records directly instead of hitting the OpenAPI scroll endpoint.
    def fake_scroll(
        overrides: Optional[Dict[str, Any]] = None,
    ) -> Iterator[CleanupRecord]:
        return iter(records)

    cleanup._scroll_execution_requests = fake_scroll  # type: ignore[method-assign]
    return [entry.urn for entry in cleanup._scroll_garbage_records()]


def test_per_source_type_override_applies_shorter_window() -> None:
    # Observe requests (sourceless, grouped by source.type) keep only 5 days, while
    # ingestion requests for a real source keep the global 90 days — so records of the
    # same age are treated differently based on their bucket's resolved policy.
    config = DatahubExecutionRequestCleanupConfig(
        keep_history_min_count=2,
        keep_history_max_count=1000,
        keep_history_max_days=90,
        source_type_overrides={"OBSERVE_SOURCE": {"keep_history_max_days": 5}},
    )
    records = [
        # newest-first, as the scroll endpoint returns them
        _record(
            "urn:li:dataHubExecutionRequest:obs-0",
            days_ago=0,
            source_type="OBSERVE_SOURCE",
        ),
        _record(
            "urn:li:dataHubExecutionRequest:ing-0",
            days_ago=0,
            ingestion_source="urn:li:dataHubIngestionSource:s1",
            source_type="SCHEDULED_INGESTION_SOURCE",
        ),
        _record(
            "urn:li:dataHubExecutionRequest:obs-1",
            days_ago=1,
            source_type="OBSERVE_SOURCE",
        ),
        _record(
            "urn:li:dataHubExecutionRequest:ing-1",
            days_ago=1,
            ingestion_source="urn:li:dataHubIngestionSource:s1",
            source_type="SCHEDULED_INGESTION_SOURCE",
        ),
        _record(
            "urn:li:dataHubExecutionRequest:obs-6",
            days_ago=6,
            source_type="OBSERVE_SOURCE",
        ),
        _record(
            "urn:li:dataHubExecutionRequest:ing-6",
            days_ago=6,
            ingestion_source="urn:li:dataHubIngestionSource:s1",
            source_type="SCHEDULED_INGESTION_SOURCE",
        ),
    ]

    deleted = _deleted_urns(config, records)

    # obs-6 is beyond the 5-day observe window; its ingestion twin ing-6 is well
    # within the 90-day ingestion window and is preserved.
    assert deleted == ["urn:li:dataHubExecutionRequest:obs-6"]


def test_corrupted_record_deleted_only_when_no_type_and_no_source() -> None:
    config = DatahubExecutionRequestCleanupConfig()
    records = [
        _record(
            "urn:li:dataHubExecutionRequest:has-type",
            days_ago=0,
            source_type="OBSERVE_SOURCE",
        ),
        _record(
            "urn:li:dataHubExecutionRequest:corrupt", days_ago=0
        ),  # no source, no type
    ]

    deleted = _deleted_urns(config, records)

    assert deleted == ["urn:li:dataHubExecutionRequest:corrupt"]


def test_running_guard_still_protects_sourceless_records() -> None:
    # A RUNNING sourceless request within 30 days is preserved even past its retention
    # window; only once it is older than the guard does it get deleted.
    config = DatahubExecutionRequestCleanupConfig(
        keep_history_min_count=1,
        source_type_overrides={"OBSERVE_SOURCE": {"keep_history_max_days": 5}},
    )
    records = [
        _record(
            "urn:li:dataHubExecutionRequest:obs-fresh",
            days_ago=0,
            source_type="OBSERVE_SOURCE",
        ),
        _record(
            "urn:li:dataHubExecutionRequest:obs-running-recent",
            days_ago=10,
            source_type="OBSERVE_SOURCE",
            status="RUNNING",
        ),
        _record(
            "urn:li:dataHubExecutionRequest:obs-running-stale",
            days_ago=40,
            source_type="OBSERVE_SOURCE",
            status="RUNNING",
        ),
    ]

    deleted = _deleted_urns(config, records)

    assert deleted == ["urn:li:dataHubExecutionRequest:obs-running-stale"]


def test_get_retention_partial_override_falls_back_to_global() -> None:
    config = DatahubExecutionRequestCleanupConfig(
        keep_history_min_count=10,
        keep_history_max_count=1000,
        keep_history_max_days=90,
        source_type_overrides={
            "OBSERVE_SOURCE": {
                "keep_history_max_days": 5,
                "keep_history_max_count": 10000,
            }
        },
    )

    observe = config.get_retention("OBSERVE_SOURCE")
    assert observe.min_count == 10  # unset -> global default
    assert observe.max_count == 10000  # overridden
    assert observe.max_milliseconds == 5 * DAY_MS  # overridden

    default = config.get_retention("SCHEDULED_INGESTION_SOURCE")
    assert default.min_count == 10
    assert default.max_count == 1000
    assert default.max_milliseconds == 90 * DAY_MS


def test_ingestion_sources_are_bucketed_independently() -> None:
    # Each ingestion source keeps its own min_count; one source must not evict
    # another's history. Guards against collapsing buckets by source.type instead
    # of by ingestion source URN.
    config = DatahubExecutionRequestCleanupConfig(keep_history_min_count=3)
    records = [
        _record(
            "urn:li:dataHubExecutionRequest:a-0",
            days_ago=0,
            ingestion_source="urn:li:dataHubIngestionSource:a",
            source_type="SCHEDULED_INGESTION_SOURCE",
        ),
        _record(
            "urn:li:dataHubExecutionRequest:b-0",
            days_ago=1,
            ingestion_source="urn:li:dataHubIngestionSource:b",
            source_type="SCHEDULED_INGESTION_SOURCE",
        ),
        _record(
            "urn:li:dataHubExecutionRequest:a-old",
            days_ago=400,
            ingestion_source="urn:li:dataHubIngestionSource:a",
            source_type="SCHEDULED_INGESTION_SOURCE",
        ),
        _record(
            "urn:li:dataHubExecutionRequest:b-old",
            days_ago=400,
            ingestion_source="urn:li:dataHubIngestionSource:b",
            source_type="SCHEDULED_INGESTION_SOURCE",
        ),
    ]

    # Each source has 2 records (< min_count 3), so nothing is deleted despite the
    # 400-day age — the two sources are retained in independent buckets.
    assert _deleted_urns(config, records) == []
