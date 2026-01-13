import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from datahub.configuration.env_vars import get_rest_sink_default_max_threads
from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import EmitMode
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass
from tests.utils import delete_urn

logger = logging.getLogger(__name__)

# Test Configuration
RATE_LIMIT_MCPS = 50  # MCPs per interval
RATE_LIMIT_INTERVAL_SECONDS = 10  # Interval duration in seconds
RATE_LIMIT_TOLERANCE = 2  # Allow 200% of rate limit
FAILURE_TOLERANCE = 0.01  # Allow 1% failures
NUM_TEST_MCPS = 300  # Number of MCPs to emit for testing
INITIAL_SLEEP_SECONDS = 2  # Initial sleep to allow rate limiter to turn on


def create_test_mcp() -> MetadataChangeProposalWrapper:
    """Create a simple GlobalTags MCP for throttle testing."""
    urn = make_dataset_urn("test", "throttle_test_dataset")
    tags = GlobalTagsClass(
        tags=[TagAssociationClass(tag=make_tag_urn(f"throttle_test_{random.random()}"))]
    )
    return MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=tags,
    )


def emit_mcp_with_timing(
    graph_client: DataHubGraph,
    mcp: MetadataChangeProposalWrapper,
    emit_mode: EmitMode,
) -> tuple[float, float, bool, Exception | None]:
    """Emit MCP asynchronously in thread and return start, end times, success, and exception."""
    start_time = time.time()
    try:
        time.sleep(random.uniform(0, 0.1))  # Simulate gms lag
        graph_client.emit_mcp(mcp, emit_mode=emit_mode)
        end_time = time.time()
        return start_time, end_time, True, None
    except Exception as e:
        end_time = time.time()
        return start_time, end_time, False, e


def calculate_max_mcps_in_window(
    completion_timestamps: list[float], window_seconds: float
) -> int:
    """
    Calculate the maximum number of MCPs completed in any sliding window.

    Args:
        completion_timestamps: List of timestamps when MCPs completed
        window_seconds: Duration of the sliding window in seconds

    Returns:
        Maximum number of MCPs that completed in any window_seconds interval
    """
    if not completion_timestamps:
        return 0

    # Sort timestamps to process chronologically
    sorted_timestamps = sorted(completion_timestamps)

    max_count = 0

    first_time = sorted_timestamps[0]
    time_of_max = first_time
    # For each timestamp, count how many MCPs completed in [timestamp - window, timestamp]
    for end_time in sorted_timestamps:
        start_time = end_time - window_seconds
        # Count timestamps in the window [start_time, end_time]
        count = sum(1 for t in sorted_timestamps if start_time <= t <= end_time)
        if count > max_count:
            max_count = count
            time_of_max = end_time

    logger.info(
        f"Max MCPs in window of {max_count} found after: {time_of_max - first_time - window_seconds} seconds"
    )
    return max_count


@pytest.fixture(scope="module", autouse=True)
def cleanup_test_data(graph_client):
    """Clean up test datasets after throttle tests complete."""
    yield
    logger.info("Cleaning up throttle test data")
    test_urn = make_dataset_urn("test", "throttle_test_dataset")
    delete_urn(graph_client, test_urn)


@pytest.mark.skip(
    "For local testing only; requires rate limiting to be configured on the server."
)
@pytest.mark.parametrize(
    "emit_settings",
    [
        # Restli ingestion does not work as well because Restli doesn't set Retry-After headers
        # And default Retry logic has no backoff jitter, leading to thundering herd issues
        ("restli", EmitMode.SYNC_PRIMARY, 3),
        ("openapi", EmitMode.SYNC_PRIMARY, 3),
        ("openapi", EmitMode.ASYNC_WAIT, 3),
    ],
    ids=[
        "sync restli",
        "sync openapi",
        "async openapi",
    ],
)
def test_mcp_throttle(
    graph_client, openapi_graph_client, emit_settings: tuple[str, EmitMode, float]
):
    client_name, emit_mode, duration_tolerance = emit_settings
    client = graph_client if client_name == "restli" else openapi_graph_client

    logger.info(f"Starting throttle test - emitting {NUM_TEST_MCPS} MCPs")
    logger.info(
        f"Rate limit: {RATE_LIMIT_MCPS} MCPs per {RATE_LIMIT_INTERVAL_SECONDS}s interval"
    )

    success_count = 0
    failed_count = 0
    completion_timestamps: list[float] = []

    mcp = create_test_mcp()
    start_time = time.time()
    with ThreadPoolExecutor(
        max_workers=get_rest_sink_default_max_threads()
    ) as executor:
        futures = []
        for i in range(NUM_TEST_MCPS):
            futures.append(
                executor.submit(emit_mcp_with_timing, client, mcp, emit_mode)
            )
            time.sleep(0.05)  # Simulate work
            if i == RATE_LIMIT_MCPS:
                time.sleep(INITIAL_SLEEP_SECONDS)  # Allow system to stabilize

        for future in as_completed(futures):
            _start_time, end_time, success, exception = future.result()
            if success:
                success_count += 1
                completion_timestamps.append(end_time)
            else:
                failed_count += 1
                logger.error(f"MCP failed unexpectedly: {exception}")

    elapsed = time.time() - start_time

    # Calculate maximum MCPs in any sliding window
    max_mcps_in_window = calculate_max_mcps_in_window(
        completion_timestamps, RATE_LIMIT_INTERVAL_SECONDS
    )
    max_allowed_mcps = int(RATE_LIMIT_MCPS * RATE_LIMIT_TOLERANCE)

    completion_offsets = [t - start_time for t in sorted(completion_timestamps)]
    logger.info(f"Sync test completed in {elapsed:.2f}s")
    logger.info(f"Success: {success_count}, Failed: {failed_count}")
    logger.info(
        f"Max MCPs in any {RATE_LIMIT_INTERVAL_SECONDS}s window: {max_mcps_in_window} "
        f"(limit: {RATE_LIMIT_MCPS}, max allowed: {max_allowed_mcps})"
    )

    # import matplotlib.pyplot as plt
    # import numpy as np
    #
    # data_min = np.min(completion_offsets)
    # data_max = np.max(completion_offsets)
    # bin_width = RATE_LIMIT_INTERVAL_SECONDS // 3
    # custom_bins = np.arange(data_min, data_max + bin_width, bin_width)
    # plt.hist(completion_offsets, bins=custom_bins, edgecolor="black")
    # plt.xlabel("Duration (s)")
    # plt.ylabel("Frequency")
    # plt.title("Distribution of Durations")
    # plt.show()

    logger.info(completion_offsets)

    # Assertions - all MCPs should succeed via auto-retry, and throughput should be limited
    max_failures_allowed = int(NUM_TEST_MCPS * FAILURE_TOLERANCE)
    assert failed_count <= max_failures_allowed, (
        f"Expected at most {max_failures_allowed} MCPs to fail via auto-retry, but {failed_count} failed"
    )
    assert success_count == NUM_TEST_MCPS, (
        f"Expected {NUM_TEST_MCPS} successful MCPs, got {success_count}"
    )
    assert max_mcps_in_window <= max_allowed_mcps, (
        f"Max MCPs in window {max_mcps_in_window} exceeded max allowed {max_allowed_mcps}"
    )

    max_allowed_time = INITIAL_SLEEP_SECONDS + (
        NUM_TEST_MCPS
        / RATE_LIMIT_MCPS
        * RATE_LIMIT_INTERVAL_SECONDS
        * duration_tolerance
    )
    assert elapsed <= max_allowed_time, (
        f"Total elapsed time {elapsed:.2f}s exceeded expected {max_allowed_time:.2f}s"
    )
