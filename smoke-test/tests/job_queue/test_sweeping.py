import logging
import time
import uuid
from typing import List

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import ExecutionRequestResultClass
from datahub.sdk.patterns.job_queue import Sweeper
from datahub.sdk.patterns.job_queue.discovery import WorkItem
from tests.job_queue.test_claiming import (
    create_execution_request,
    delete_execution_request,
    make_claim,
)

logger = logging.getLogger(__name__)


class _StaticDiscovery:
    """Discovery that returns a fixed list of URNs (no search indexing needed)."""

    def __init__(self, urns: List[str]) -> None:
        self._urns = urns

    def poll(self) -> List[WorkItem]:
        return [WorkItem(urn=urn) for urn in self._urns]


def make_sweeper(graph, urns: List[str]) -> Sweeper:
    """Build a Sweeper targeting specific URNs with RUNNING -> FAILURE semantics."""
    return Sweeper(
        graph=graph,
        aspect_name="dataHubExecutionRequestResult",
        aspect_class=ExecutionRequestResultClass,
        is_stale=lambda aspect: aspect.status == "RUNNING",
        make_swept=lambda aspect: ExecutionRequestResultClass(
            status="FAILURE",
            startTimeMs=int(time.time() * 1000),
        ),
        discovery=_StaticDiscovery(urns),
    )


@pytest.mark.remote_executor
def test_sweeper_force_releases_stale_claim(auth_session, openapi_graph_client):
    """Sweep a single stale claim: claim -> sweep -> verify swept state."""
    graph = openapi_graph_client
    request_id = str(uuid.uuid4())
    urn = create_execution_request(graph, request_id)
    try:
        claim = make_claim(graph)
        assert claim.try_claim(urn, "worker-1") is True

        result = graph.get_aspect(urn, ExecutionRequestResultClass)
        assert result is not None
        assert result.status == "RUNNING"

        sweeper = make_sweeper(graph, [urn])
        sweep_result = sweeper.sweep()
        logger.info("Sweep result: swept=%s", sweep_result.swept)

        assert sweep_result.swept == [urn]
        assert sweep_result.failed == []
        assert sweep_result.skipped == []

        result = graph.get_aspect(urn, ExecutionRequestResultClass)
        assert result is not None
        assert result.status == "FAILURE"
    finally:
        delete_execution_request(graph, urn)


@pytest.mark.remote_executor
def test_sweeper_crash_recovery_full_cycle(auth_session, openapi_graph_client):
    """Full crash-recovery: worker crashes -> sweeper cleans up -> new worker re-claims."""
    graph = openapi_graph_client
    request_id = str(uuid.uuid4())
    urn = create_execution_request(graph, request_id)
    try:
        # Worker-1 claims the entity
        claim_1 = make_claim(graph)
        assert claim_1.try_claim(urn, "worker-1") is True

        result = graph.get_aspect(urn, ExecutionRequestResultClass)
        assert result is not None
        assert result.status == "RUNNING"

        # Simulate crash: discard claim_1 without releasing
        del claim_1

        # Sweeper detects stale claim and force-releases
        sweeper = make_sweeper(graph, [urn])
        sweep_result = sweeper.sweep()
        logger.info("Sweep result: swept=%s", sweep_result.swept)
        assert sweep_result.swept == [urn]

        result = graph.get_aspect(urn, ExecutionRequestResultClass)
        assert result is not None
        assert result.status == "FAILURE"

        # Worker-2 can now re-claim (FAILURE is not "claimed")
        claim_2 = make_claim(graph)
        assert claim_2.try_claim(urn, "worker-2") is True

        result = graph.get_aspect(urn, ExecutionRequestResultClass)
        assert result is not None
        assert result.status == "RUNNING"

        # Worker-2 completes successfully
        assert claim_2.release(urn, "worker-2") is True

        result = graph.get_aspect(urn, ExecutionRequestResultClass)
        assert result is not None
        assert result.status == "SUCCESS"
    finally:
        delete_execution_request(graph, urn)


@pytest.mark.remote_executor
def test_sweeper_skips_non_stale(auth_session, openapi_graph_client):
    """Sweeper selectively sweeps: old claim is stale, recent claim is not.

    Uses time-based staleness (startTimeMs) to distinguish the two entities.
    This mirrors real-world sweeper behavior where claims are considered stale
    only after a timeout.

    Timing safety: Entity A's timestamp is set 1 hour in the past, with a
    30-minute threshold — giving 30 minutes of margin on both sides. This
    can only flake if a single test takes >30 min between claim and sweep,
    or the system clock jumps backwards by 30+ min mid-test. No time-freeze
    library is needed.

    We use startTimeMs rather than executorInstanceId because startTimeMs
    reliably round-trips through all GMS versions. executorInstanceId may
    be dropped by older GMS builds that lack it in the server-side model
    (returns None on read-back even when written).
    """
    graph = openapi_graph_client
    id_a = str(uuid.uuid4())
    id_b = str(uuid.uuid4())
    urn_a = create_execution_request(graph, id_a)
    urn_b = create_execution_request(graph, id_b)
    try:
        # Claim both entities — A gets an artificially old timestamp
        claim_a = make_claim(graph)
        claim_b = make_claim(graph)
        assert claim_a.try_claim(urn_a, "worker-1") is True
        assert claim_b.try_claim(urn_b, "worker-2") is True

        # Overwrite A's result with a very old startTimeMs (1 hour ago)
        # to simulate a stale claim, while B keeps its recent timestamp.
        old_time_ms = int(time.time() * 1000) - 3_600_000
        stale_aspect = ExecutionRequestResultClass(
            status="RUNNING",
            startTimeMs=old_time_ms,
        )
        graph.emit_mcp(
            MetadataChangeProposalWrapper(entityUrn=urn_a, aspect=stale_aspect)
        )

        # Threshold: claims older than 30 minutes are stale
        threshold_ms = 30 * 60 * 1000

        def is_stale(aspect: ExecutionRequestResultClass) -> bool:
            if aspect.status != "RUNNING":
                return False
            ts = aspect.startTimeMs
            if ts is None:
                return True
            return (int(time.time() * 1000) - int(ts)) > threshold_ms

        sweeper = Sweeper(
            graph=graph,
            aspect_name="dataHubExecutionRequestResult",
            aspect_class=ExecutionRequestResultClass,
            is_stale=is_stale,
            make_swept=lambda aspect: ExecutionRequestResultClass(
                status="FAILURE",
                startTimeMs=int(time.time() * 1000),
            ),
            discovery=_StaticDiscovery([urn_a, urn_b]),
        )

        sweep_result = sweeper.sweep()
        logger.info(
            "Sweep result: swept=%s, skipped=%s",
            sweep_result.swept,
            sweep_result.skipped,
        )

        # A was stale (old timestamp) -> swept; B was recent -> skipped
        assert sweep_result.swept == [urn_a]
        assert sweep_result.skipped == [urn_b]

        result_a = graph.get_aspect(urn_a, ExecutionRequestResultClass)
        assert result_a is not None
        assert result_a.status == "FAILURE"

        result_b = graph.get_aspect(urn_b, ExecutionRequestResultClass)
        assert result_b is not None
        assert result_b.status == "RUNNING"

        # Release B normally
        assert claim_b.release(urn_b, "worker-2") is True
    finally:
        delete_execution_request(graph, urn_a)
        delete_execution_request(graph, urn_b)


@pytest.mark.remote_executor
def test_sweeper_handles_concurrent_release(auth_session, openapi_graph_client):
    """Sweeper skips an entity that was released between discovery and sweep."""
    graph = openapi_graph_client
    request_id = str(uuid.uuid4())
    urn = create_execution_request(graph, request_id)
    try:
        claim = make_claim(graph)
        assert claim.try_claim(urn, "worker-1") is True

        # Worker releases before sweeper runs
        assert claim.release(urn, "worker-1") is True

        result = graph.get_aspect(urn, ExecutionRequestResultClass)
        assert result is not None
        assert result.status == "SUCCESS"

        # Sweeper sees the entity but it's no longer stale
        sweeper = make_sweeper(graph, [urn])
        sweep_result = sweeper.sweep()
        logger.info(
            "Sweep result: swept=%s, skipped=%s",
            sweep_result.swept,
            sweep_result.skipped,
        )

        assert sweep_result.swept == []
        assert sweep_result.skipped == [urn]

        # Entity remains in SUCCESS state
        result = graph.get_aspect(urn, ExecutionRequestResultClass)
        assert result is not None
        assert result.status == "SUCCESS"
    finally:
        delete_execution_request(graph, urn)
