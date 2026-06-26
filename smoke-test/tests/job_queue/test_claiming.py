import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    ExecutionRequestInputClass,
    ExecutionRequestKeyClass,
    ExecutionRequestResultClass,
    ExecutionRequestSourceClass,
)
from datahub.sdk.patterns.job_queue import Claim

logger = logging.getLogger(__name__)


def create_execution_request(graph, request_id: str) -> str:
    """Create an ExecutionRequest entity and return its URN."""
    urn = f"urn:li:dataHubExecutionRequest:{request_id}"
    aspect = ExecutionRequestInputClass(
        task="RUN_INGEST",
        args={
            "recipe": '{"source": {"type": "demo-data", "config": {}}}',
            "version": "1.0.0",
        },
        executorId="test-job-queue-sdk",
        requestedAt=int(time.time() * 1000),
        source=ExecutionRequestSourceClass(
            type="MANUAL_INGESTION_SOURCE",
            ingestionSource="urn:li:dataHubIngestionSource:job-queue-test",
        ),
    )
    mcpw = MetadataChangeProposalWrapper(
        entityKeyAspect=ExecutionRequestKeyClass(id=request_id),
        entityUrn=urn,
        entityType="dataHubExecutionRequest",
        aspectName="dataHubExecutionRequestInput",
        aspect=aspect,
        changeType="UPSERT",
    )
    graph.emit_mcp(mcpw)
    logger.info("Created ExecutionRequest %s", urn)
    return urn


def delete_execution_request(graph, urn: str) -> None:
    """Delete an ExecutionRequest entity and its aspects."""
    for aspect_name in (
        "dataHubExecutionRequestResult",
        "dataHubExecutionRequestInput",
    ):
        try:
            graph.emit_mcp(
                MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspectName=aspect_name,
                    changeType=ChangeTypeClass.DELETE,
                )
            )
        except Exception:
            logger.debug("Could not delete aspect %s on %s", aspect_name, urn)
    graph.hard_delete_entity(urn)
    logger.info("Cleaned up %s", urn)


def make_claim(graph) -> Claim:
    """Build a Claim configured for ExecutionRequestResult."""
    return Claim(
        graph=graph,
        aspect_name="dataHubExecutionRequestResult",
        aspect_class=ExecutionRequestResultClass,
        make_claimed=lambda owner, current: ExecutionRequestResultClass(
            status="RUNNING",
            startTimeMs=int(time.time() * 1000),
            executorInstanceId=owner,
        ),
        make_released=lambda owner, current: ExecutionRequestResultClass(
            status="SUCCESS",
            startTimeMs=int(time.time() * 1000),
            executorInstanceId=owner,
        ),
        is_claimed=lambda aspect: aspect.status == "RUNNING",  # type: ignore[attr-defined]
    )


@pytest.mark.remote_executor
def test_single_claim_and_release(auth_session, openapi_graph_client):
    """Validates the basic claim -> release lifecycle works against real GMS."""
    graph_client = openapi_graph_client
    request_id = str(uuid.uuid4())
    urn = create_execution_request(graph_client, request_id)
    try:
        claim = make_claim(graph_client)

        # Claim
        assert claim.try_claim(urn, "worker-1") is True
        assert urn in claim.held_urns

        # Verify claimed state
        result = graph_client.get_aspect(urn, ExecutionRequestResultClass)
        assert result is not None
        assert result.status == "RUNNING"
        if result.executorInstanceId is not None:
            assert result.executorInstanceId == "worker-1"

        # Release
        assert claim.release(urn, "worker-1") is True

        # Verify released state
        result = graph_client.get_aspect(urn, ExecutionRequestResultClass)
        assert result is not None
        assert result.status == "SUCCESS"
    finally:
        delete_execution_request(graph_client, urn)


@pytest.mark.remote_executor
def test_concurrent_claim_exactly_one_wins(auth_session, openapi_graph_client):
    """Validates that when N workers race to claim the same entity, exactly one succeeds."""
    graph_client = openapi_graph_client
    request_id = str(uuid.uuid4())
    urn = create_execution_request(graph_client, request_id)
    try:
        num_workers = 5
        claims = [make_claim(graph_client) for _ in range(num_workers)]

        results = {}
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {
                executor.submit(claims[i].try_claim, urn, f"worker-{i}"): i
                for i in range(num_workers)
            }
            for future in as_completed(futures):
                worker_idx = futures[future]
                results[worker_idx] = future.result()

        winners = [i for i, won in results.items() if won]
        losers = [i for i, won in results.items() if not won]
        logger.info("Winners: %s, Losers: %s", winners, losers)

        assert len(winners) == 1, (
            f"Expected exactly 1 winner, got {len(winners)}: {winners}"
        )
        assert len(losers) == num_workers - 1

        # Verify the aspect was claimed
        winner_id = f"worker-{winners[0]}"
        result = graph_client.get_aspect(urn, ExecutionRequestResultClass)
        assert result is not None
        assert result.status == "RUNNING"
        if result.executorInstanceId is not None:
            assert result.executorInstanceId == winner_id

        # Winner releases
        winner_claim = claims[winners[0]]
        assert winner_claim.release(urn, winner_id) is True
    finally:
        delete_execution_request(graph_client, urn)


@pytest.mark.remote_executor
def test_claim_then_second_claim_fails(auth_session, openapi_graph_client):
    """Validates sequential claim conflict (no threading, deterministic)."""
    graph_client = openapi_graph_client
    request_id = str(uuid.uuid4())
    urn = create_execution_request(graph_client, request_id)
    try:
        claim_a = make_claim(graph_client)
        claim_b = make_claim(graph_client)

        # First claim succeeds
        assert claim_a.try_claim(urn, "worker-a") is True

        # Second claim on same entity fails (already claimed)
        assert claim_b.try_claim(urn, "worker-b") is False

        # Release the first claim
        assert claim_a.release(urn, "worker-a") is True

        # After release (SUCCESS), GMS prevents status reverting to RUNNING
        result = graph_client.get_aspect(urn, ExecutionRequestResultClass)
        assert result is not None
        assert result.status == "SUCCESS"
    finally:
        delete_execution_request(graph_client, urn)


@pytest.mark.remote_executor
def test_release_untracked_returns_false(auth_session, openapi_graph_client):
    """Release of an untracked URN returns False."""
    graph_client = openapi_graph_client
    claim = make_claim(graph_client)
    assert (
        claim.release("urn:li:dataHubExecutionRequest:nonexistent", "worker-1") is False
    )


@pytest.mark.remote_executor
def test_from_fields_shortcut(auth_session, openapi_graph_client):
    """Validates Claim.from_fields() works end-to-end against real GMS."""
    graph_client = openapi_graph_client
    request_id = str(uuid.uuid4())
    urn = create_execution_request(graph_client, request_id)
    try:
        claim = Claim.from_fields(
            graph=graph_client,
            aspect_name="dataHubExecutionRequestResult",
            aspect_class=ExecutionRequestResultClass,
            owner_field="executorInstanceId",
            state_field="status",
            claimed_state="RUNNING",
            released_state="SUCCESS",
            timestamp_field="startTimeMs",
        )

        # Claim
        assert claim.try_claim(urn, "worker-fields") is True

        # Verify fields are set
        result = graph_client.get_aspect(urn, ExecutionRequestResultClass)
        assert result is not None
        assert result.status == "RUNNING"
        if result.executorInstanceId is not None:
            assert result.executorInstanceId == "worker-fields"
        assert result.startTimeMs is not None and result.startTimeMs > 0

        # Release
        assert claim.release(urn, "worker-fields") is True

        result = graph_client.get_aspect(urn, ExecutionRequestResultClass)
        assert result is not None
        assert result.status == "SUCCESS"
    finally:
        delete_execution_request(graph_client, urn)
