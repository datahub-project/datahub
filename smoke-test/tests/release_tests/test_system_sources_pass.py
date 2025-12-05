# ABOUTME: Release test that verifies all SYSTEM ingestion sources have passing last runs.
# ABOUTME: Queries SYSTEM sources and checks their most recent execution status is SUCCESS.

import logging

import pytest

from tests.utilities.env_vars import get_system_source_execution_timeout_minutes
from tests.utilities.metadata_operations import (
    create_ingestion_execution_request,
    list_ingestion_sources_with_filter,
    poll_execution_requests,
)

logger = logging.getLogger(__name__)

SYSTEM_SOURCES_MIN_COUNT = 7


@pytest.mark.release_tests
def test_system_sources_pass(auth_session):
    """Verify that all SYSTEM ingestion sources pass when executed.

    This test:
    1. Queries all SYSTEM ingestion sources
    2. Submits execution requests for each source
    3. Polls execution status until all complete or timeout
    4. Asserts all executions succeeded
    """
    timeout_minutes = get_system_source_execution_timeout_minutes()

    logger.info("Querying SYSTEM ingestion sources...")
    sources_data = list_ingestion_sources_with_filter(
        auth_session,
        filters=[{"field": "sourceType", "values": ["SYSTEM"], "negated": False}],
        include_executions=False,
    )

    total_sources = sources_data["total"]
    ingestion_sources = sources_data["ingestionSources"]

    logger.info(f"Found {total_sources} SYSTEM ingestion sources")
    assert total_sources > 0, "Expected at least one SYSTEM ingestion source"
    assert total_sources >= SYSTEM_SOURCES_MIN_COUNT, (
        f"Expected at least {SYSTEM_SOURCES_MIN_COUNT} SYSTEM sources, "
        f"but found {total_sources}"
    )

    logger.info(f"Submitting execution requests for {total_sources} sources...")
    execution_tracking = {}

    for source in ingestion_sources:
        source_name = source["name"]
        source_urn = source["urn"]

        try:
            execution_urn = create_ingestion_execution_request(auth_session, source_urn)
            execution_tracking[source_name] = {
                "source_urn": source_urn,
                "execution_urn": execution_urn,
            }
            logger.info(f"Submitted execution for '{source_name}': {execution_urn}")
        except Exception as e:
            logger.error(f"Failed to submit execution for '{source_name}': {e}")
            execution_tracking[source_name] = {
                "source_urn": source_urn,
                "execution_urn": None,
                "status": "SUBMISSION_FAILED",
                "error": str(e),
            }

    logger.info(
        f"Polling {len(execution_tracking)} executions "
        f"(timeout: {timeout_minutes} minutes)..."
    )
    results = poll_execution_requests(
        auth_session,
        execution_tracking,
        timeout_minutes=timeout_minutes,
        poll_interval_seconds=5,
    )

    successful_sources = []
    failed_sources = []
    timed_out_sources = []

    for source_name, result in results.items():
        status = result.get("status")

        if status == "SUCCESS":
            successful_sources.append(source_name)
            logger.info(f"✓ Source '{source_name}' passed")
        elif status == "TIMEOUT":
            timed_out_sources.append(
                {
                    "name": source_name,
                    "urn": result["source_urn"],
                    "execution_urn": result["execution_urn"],
                }
            )
            logger.warning(f"⏱ Source '{source_name}' timed out")
        else:
            failed_sources.append(
                {
                    "name": source_name,
                    "urn": result["source_urn"],
                    "status": status,
                    "execution_urn": result["execution_urn"],
                }
            )
            logger.error(f"✗ Source '{source_name}' failed with status: {status}")

    logger.info("\n=== Execution Summary ===")
    logger.info(f"Total sources: {total_sources}")
    logger.info(f"Successful: {len(successful_sources)}")
    logger.info(f"Failed: {len(failed_sources)}")
    logger.info(f"Timed out: {len(timed_out_sources)}")

    if timed_out_sources:
        logger.error(
            f"The following {len(timed_out_sources)} source(s) timed out "
            f"after {timeout_minutes} minutes: {timed_out_sources}"
        )

    if failed_sources:
        logger.error(
            f"The following {len(failed_sources)} source(s) failed: {failed_sources}"
        )

    assert not timed_out_sources, (
        f"{len(timed_out_sources)} source(s) timed out after {timeout_minutes} minutes: "
        f"{[s['name'] for s in timed_out_sources]}"
    )

    assert not failed_sources, (
        f"{len(failed_sources)} source(s) failed: {[s['name'] for s in failed_sources]}"
    )

    assert len(successful_sources) >= SYSTEM_SOURCES_MIN_COUNT, (
        f"Expected at least {SYSTEM_SOURCES_MIN_COUNT} successful sources, "
        f"but only {len(successful_sources)} succeeded"
    )
