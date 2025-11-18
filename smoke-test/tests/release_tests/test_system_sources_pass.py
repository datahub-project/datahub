# ABOUTME: Release test that verifies all SYSTEM ingestion sources have passing last runs.
# ABOUTME: Queries SYSTEM sources and checks their most recent execution status is SUCCESS.

import logging

import pytest

from tests.utilities.metadata_operations import list_ingestion_sources_with_filter

logger = logging.getLogger(__name__)

SYSTEM_SOURCES_MIN_COUNT = 7


# TODO This requires an additional check that the system sources
# TODO passed a run after the last upgrade was done
@pytest.mark.release_tests
def test_system_sources_pass(auth_session):
    """Verify that all SYSTEM ingestion sources have passing last runs.

    This test queries all ingestion sources with sourceType=SYSTEM and checks that
    their most recent execution has a status of SUCCESS. Sources without any completed
    runs are tracked separately but don't cause test failure.
    """
    # Query for all SYSTEM ingestion sources with their latest execution
    sources_data = list_ingestion_sources_with_filter(
        auth_session,
        filters=[{"field": "sourceType", "values": ["SYSTEM"], "negated": False}],
        include_executions=True,
    )

    total_sources = sources_data["total"]
    ingestion_sources = sources_data["ingestionSources"]

    logger.info(f"Found {total_sources} SYSTEM ingestion sources")
    assert total_sources > 0, "Expected at least one SYSTEM ingestion source"

    failing_sources = []
    sources_without_runs = []
    sources_with_success = []

    for source in ingestion_sources:
        source_name = source["name"]
        source_urn = source["urn"]
        executions = source.get("executions", {}).get("executionRequests", [])

        if not executions:
            logger.debug(f"Source '{source_name}' has no executions")
            sources_without_runs.append(source_name)
            continue

        latest_execution = executions[0]
        result = latest_execution.get("result")

        if result is None:
            # Execution hasn't completed yet
            logger.debug(f"Source '{source_name}' has incomplete execution")
            sources_without_runs.append(source_name)
            continue

        status = result.get("status")
        if status == "SUCCESS":
            logger.info(f"Source '{source_name}' passed with SUCCESS status")
            sources_with_success.append(source_name)
        else:
            logger.warning(
                f"Source '{source_name}' has non-SUCCESS status: {status} "
                f"(execution: {latest_execution['urn']})"
            )
            failing_sources.append(
                {
                    "name": source_name,
                    "urn": source_urn,
                    "status": status,
                    "execution_urn": latest_execution["urn"],
                }
            )

    # Report findings
    if sources_without_runs:
        logger.info(f"Sources without completed runs: {sources_without_runs}")

    if failing_sources:
        logger.error(
            f"Found {len(failing_sources)} SYSTEM source(s) with non-passing last runs: "
            f"{failing_sources}"
        )
    else:
        logger.info(
            f"All {total_sources} SYSTEM sources passed. "
            f"Sources without runs: {len(sources_without_runs)}"
        )

    assert len(sources_with_success) >= SYSTEM_SOURCES_MIN_COUNT

    assert not failing_sources, (
        f"Found {len(failing_sources)} SYSTEM source(s) with non-passing last runs: "
        f"{failing_sources}"
    )
