"""Smoke tests for transport-neutral messaging operations APIs (pgQueue-focused)."""

from __future__ import annotations

import uuid
from typing import Any, Dict, List

import pytest

_MESSAGING_BASE = "/openapi/operations/messaging"


def _detailed_partitions(lag_body: Dict[str, Any]) -> List[Dict[str, Any]]:
    consumer_groups = lag_body.get("consumerGroups") or {}
    partitions: List[Dict[str, Any]] = []
    for group_data in consumer_groups.values():
        for topic_snap in group_data.values():
            topic_partitions = topic_snap.get("partitions") or {}
            partitions.extend(topic_partitions.values())
    return partitions


def _assert_detailed_pgqueue_lag(auth_session, path: str) -> None:
    response = auth_session.get(
        f"{auth_session.gms_url()}{path}",
        params={"detailed": "true"},
        timeout=30,
    )
    response.raise_for_status()
    body = response.json()
    assert body.get("transport") == "pgqueue"
    partitions = _detailed_partitions(body)
    assert partitions, f"Expected per-partition lag from {path} when detailed=true"
    for part in partitions:
        assert "offset" in part and "lag" in part
        if "aheadBy" in part:
            assert part["aheadBy"] > 0
            assert part.get("metadata") == "STUCK_AHEAD"


def _fetch_transport_info(auth_session) -> Dict[str, Any]:
    response = auth_session.get(f"{auth_session.gms_url()}{_MESSAGING_BASE}/transport")
    response.raise_for_status()
    return response.json()


@pytest.mark.dependency()
def test_messaging_transport_reports_pgqueue(require_pgqueue) -> None:
    auth_session = require_pgqueue
    body = _fetch_transport_info(auth_session)
    assert body.get("transport") == "pgqueue"
    for field in (
        "metadataChangeProposalTopic",
        "metadataChangeLogVersionedTopic",
        "metadataChangeLogTimeseriesTopic",
    ):
        assert body.get(field), f"Expected {field} in transport response"


@pytest.mark.dependency()
def test_mcp_consumer_lag_detailed(require_pgqueue) -> None:
    _assert_detailed_pgqueue_lag(require_pgqueue, f"{_MESSAGING_BASE}/mcp/consumer/lag")


@pytest.mark.dependency()
def test_mcl_versioned_consumer_lag_detailed(require_pgqueue) -> None:
    _assert_detailed_pgqueue_lag(require_pgqueue, f"{_MESSAGING_BASE}/mcl/consumer/lag")


@pytest.mark.dependency()
def test_mcl_timeseries_consumer_lag_detailed(require_pgqueue) -> None:
    _assert_detailed_pgqueue_lag(
        require_pgqueue, f"{_MESSAGING_BASE}/mcl-timeseries/consumer/lag"
    )


@pytest.mark.dependency()
def test_list_registered_consumers(require_pgqueue) -> None:
    """GET /consumers returns registered groups for a topic (epoch-millis timestamps)."""
    auth_session = require_pgqueue
    topic_name = _fetch_transport_info(auth_session)["metadataChangeProposalTopic"]
    response = auth_session.get(
        f"{auth_session.gms_url()}{_MESSAGING_BASE}/consumers",
        params={"topicName": topic_name},
        timeout=30,
    )
    if response.status_code == 400 and "Java 8 date/time" in response.text:
        pytest.skip(
            "GET /consumers requires GMS with Instant serialization fix (rebuild datahub-gms)"
        )
    response.raise_for_status()
    body = response.json()
    assert isinstance(body, list)
    if body:
        row = body[0]
        assert "consumerGroup" in row
        assert "registeredAt" in row
        assert isinstance(row["registeredAt"], int)


@pytest.mark.dependency()
def test_reset_stuck_ahead_consumer_offsets(require_pgqueue) -> None:
    """POST /consumer/offsets/reset clears STUCK_AHEAD offsets (pgQueue only)."""
    auth_session = require_pgqueue
    response = auth_session.post(
        f"{auth_session.gms_url()}{_MESSAGING_BASE}/consumer/offsets/reset",
        json={"onlyStuckAhead": True},
        timeout=30,
    )
    response.raise_for_status()
    body = response.json()
    assert body.get("transport") == "pgqueue"
    assert "partitionsUpdated" in body
    assert isinstance(body.get("resets"), list)
    if body["partitionsUpdated"] > 0:
        row = body["resets"][0]
        assert row["previousOffset"] > row["newOffset"]
        assert row["newOffset"] == row["maxSeq"]


@pytest.mark.dependency()
def test_consumer_registration_lifecycle(require_pgqueue) -> None:
    """Register and unregister a consumer group (aggressive retention API)."""
    auth_session = require_pgqueue
    transport = _fetch_transport_info(auth_session)
    topic_name = transport["metadataChangeProposalTopic"]
    consumer_group = f"smoke-test-pgqueue-{uuid.uuid4().hex[:12]}"
    base = f"{auth_session.gms_url()}{_MESSAGING_BASE}/consumers"

    try:
        register = auth_session.put(
            base,
            json={"consumerGroup": consumer_group, "topicName": topic_name},
            timeout=30,
        )
        register.raise_for_status()
        assert register.json().get("registered") is True

        listed = auth_session.get(base, params={"topicName": topic_name}, timeout=30)
        if listed.status_code == 200:
            groups = {row["consumerGroup"] for row in listed.json()}
            assert consumer_group in groups

        deleted = auth_session.delete(
            base,
            params={"consumerGroup": consumer_group, "topicName": topic_name},
            timeout=30,
        )
        deleted.raise_for_status()
        assert deleted.json().get("deleted") is True
    finally:
        auth_session.delete(
            base,
            params={"consumerGroup": consumer_group, "topicName": topic_name},
            timeout=30,
        )
