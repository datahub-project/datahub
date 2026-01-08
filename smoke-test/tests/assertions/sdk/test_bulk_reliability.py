"""
Bulk reliability test for assertion SDK.

Tests sequential assertion syncing across multiple datasets to verify:
- No state leakage between calls
- Reliability under sustained usage
- Proper handling of multiple datasets in sequence

This mirrors real-world usage where users run scripts that sync assertions
across many datasets.
"""

import hashlib
import logging
from typing import List

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import StatusClass
from datahub.sdk.main_client import DataHubClient
from tests.assertions.sdk.helpers import (
    generate_unique_test_id,
    get_assertion_by_urn,
    get_nested_value,
    wait_for_assertion_sync,
)
from tests.utils import delete_urn

logger = logging.getLogger(__name__)

# Number of datasets to create for bulk testing
NUM_BULK_TEST_DATASETS = 5


def deterministic_uuid(dataset_urn: str, assertion_type: str) -> str:
    """Generate a deterministic UUID based on dataset and assertion type.

    This enables idempotent scripts - re-running generates the same URNs.
    """
    input_str = f"{dataset_urn}:{assertion_type}"
    hash_hex = hashlib.sha256(input_str.encode()).hexdigest()
    return hash_hex[:32]


def test_bulk_sync_assertions_across_datasets(
    graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test sequential assertion syncing across multiple datasets.

    Creates multiple test datasets, then sequentially syncs freshness and volume
    assertions across all of them. Verifies all assertions persisted correctly.

    This tests SDK reliability under sustained sequential usage - the pattern
    users actually run when syncing assertions across their data catalog.
    """
    test_id = generate_unique_test_id()
    datasets: List[str] = []
    assertion_urns: List[str] = []

    try:
        # Phase 1: Create test datasets
        logger.info(f"Creating {NUM_BULK_TEST_DATASETS} test datasets...")
        for i in range(NUM_BULK_TEST_DATASETS):
            dataset_urn = make_dataset_urn(
                platform="postgres", name=f"bulk_test_{test_id}_{i}"
            )
            datasets.append(dataset_urn)

            # Emit status to create the dataset
            mcpw = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=StatusClass(removed=False)
            )
            graph_client.emit(mcpw)

        wait_for_assertion_sync()
        logger.info(f"Created {len(datasets)} test datasets")

        # Phase 2: Sequentially sync assertions across all datasets
        logger.info("Syncing assertions across datasets...")
        for i, dataset_urn in enumerate(datasets):
            # Generate deterministic URNs for idempotency
            freshness_urn = (
                f"urn:li:assertion:{deterministic_uuid(dataset_urn, 'freshness')}"
            )
            volume_urn = f"urn:li:assertion:{deterministic_uuid(dataset_urn, 'volume')}"

            # Sync freshness assertion
            freshness_assertion = (
                datahub_client.assertions.sync_smart_freshness_assertion(
                    dataset_urn=dataset_urn,
                    urn=freshness_urn,
                    display_name=f"Bulk Test Freshness {i}",
                    sensitivity="high",
                )
            )
            assertion_urns.append(str(freshness_assertion.urn))
            logger.info(f"Created freshness assertion {i + 1}/{NUM_BULK_TEST_DATASETS}")

            # Verify returned URN matches what we provided
            assert str(freshness_assertion.urn) == freshness_urn
            assert str(freshness_assertion.dataset_urn) == dataset_urn

            # Sync volume assertion
            volume_assertion = datahub_client.assertions.sync_smart_volume_assertion(
                dataset_urn=dataset_urn,
                urn=volume_urn,
                display_name=f"Bulk Test Volume {i}",
                sensitivity="low",
                schedule="0 */2 * * *",  # Every 2 hours instead of hourly/daily
            )
            assertion_urns.append(str(volume_assertion.urn))
            logger.info(f"Created volume assertion {i + 1}/{NUM_BULK_TEST_DATASETS}")

            # Verify returned URN matches what we provided
            assert str(volume_assertion.urn) == volume_urn
            assert str(volume_assertion.dataset_urn) == dataset_urn

        # Phase 3: Wait for all writes to sync
        logger.info("Waiting for writes to sync...")
        wait_for_assertion_sync()

        # Phase 4: Verify all assertions exist on server
        logger.info("Verifying all assertions persisted correctly...")
        for i, dataset_urn in enumerate(datasets):
            freshness_urn = (
                f"urn:li:assertion:{deterministic_uuid(dataset_urn, 'freshness')}"
            )
            volume_urn = f"urn:li:assertion:{deterministic_uuid(dataset_urn, 'volume')}"

            # Verify freshness assertion
            freshness_fetched = get_assertion_by_urn(
                graph_client, dataset_urn, freshness_urn
            )
            assert freshness_fetched is not None, (
                f"Freshness assertion missing for dataset {i}"
            )
            assert freshness_fetched["info"]["type"] == "FRESHNESS"
            assert (
                freshness_fetched["info"]["freshnessAssertion"]["entityUrn"]
                == dataset_urn
            )

            # Verify monitor exists for freshness (using nested data, no double-fetch)
            freshness_monitor = freshness_fetched.get("monitor")
            assert freshness_monitor is not None and freshness_monitor.get("urn"), (
                f"Monitor missing for freshness assertion on dataset {i}"
            )

            # Verify monitor has correct sensitivity setting
            # Sensitivity levels: LOW=1, MEDIUM=5, HIGH=10
            freshness_sensitivity = get_nested_value(
                freshness_fetched,
                "monitor.info.assertionMonitor.settings.inferenceSettings.sensitivity.level",
            )
            assert freshness_sensitivity == 10, (
                f"Expected HIGH sensitivity (10) for freshness on dataset {i}, got {freshness_sensitivity}"
            )

            # Verify volume assertion
            volume_fetched = get_assertion_by_urn(graph_client, dataset_urn, volume_urn)
            assert volume_fetched is not None, (
                f"Volume assertion missing for dataset {i}"
            )
            assert volume_fetched["info"]["type"] == "VOLUME"
            assert volume_fetched["info"]["volumeAssertion"]["entityUrn"] == dataset_urn

            # Verify monitor exists for volume (using nested data, no double-fetch)
            volume_monitor = volume_fetched.get("monitor")
            assert volume_monitor is not None and volume_monitor.get("urn"), (
                f"Monitor missing for volume assertion on dataset {i}"
            )

            # Verify monitor has correct sensitivity setting
            # Sensitivity levels: LOW=1, MEDIUM=5, HIGH=10
            volume_sensitivity = get_nested_value(
                volume_fetched,
                "monitor.info.assertionMonitor.settings.inferenceSettings.sensitivity.level",
            )
            assert volume_sensitivity == 1, (
                f"Expected LOW sensitivity (1) for volume on dataset {i}, got {volume_sensitivity}"
            )

            # Verify smart volume assertion uses ROW_COUNT_TOTAL type (not ROW_COUNT_CHANGE)
            # Note: Smart assertions don't populate rowCountTotal field (which contains
            # explicit thresholds) - they use inferred thresholds instead.
            volume_type = get_nested_value(volume_fetched, "info.volumeAssertion.type")
            assert volume_type == "ROW_COUNT_TOTAL", (
                f"Smart volume assertion should have type ROW_COUNT_TOTAL for dataset {i}, got {volume_type}"
            )

            logger.info(
                f"Verified assertions for dataset {i + 1}/{NUM_BULK_TEST_DATASETS}"
            )

        logger.info(
            f"Successfully synced and verified {len(assertion_urns)} assertions "
            f"across {len(datasets)} datasets"
        )

        # Phase 5: Test idempotency - re-run sync and verify same URNs
        logger.info("Testing idempotency - re-running sync...")
        for i, dataset_urn in enumerate(datasets):
            freshness_urn = (
                f"urn:li:assertion:{deterministic_uuid(dataset_urn, 'freshness')}"
            )

            # Re-sync the same assertion with same display name
            freshness_assertion = (
                datahub_client.assertions.sync_smart_freshness_assertion(
                    dataset_urn=dataset_urn,
                    urn=freshness_urn,
                    display_name=f"Bulk Test Freshness {i}",  # Keep display name same
                    sensitivity="high",
                )
            )

            # Should return the same URN (update, not create)
            assert str(freshness_assertion.urn) == freshness_urn

        wait_for_assertion_sync()

        # Validate assertions remain unchanged after re-sync
        logger.info("Validating assertions after idempotency test...")
        for _i, dataset_urn in enumerate(datasets):
            freshness_urn = (
                f"urn:li:assertion:{deterministic_uuid(dataset_urn, 'freshness')}"
            )

            # Fetch and validate assertion properties remain correct
            freshness_fetched = get_assertion_by_urn(
                graph_client, dataset_urn, freshness_urn
            )
            assert freshness_fetched is not None
            assert freshness_fetched["info"]["type"] == "FRESHNESS"

            # Verify sensitivity is still HIGH after re-sync
            # Sensitivity levels: LOW=1, MEDIUM=5, HIGH=10
            freshness_sensitivity = get_nested_value(
                freshness_fetched,
                "monitor.info.assertionMonitor.settings.inferenceSettings.sensitivity.level",
            )
            assert freshness_sensitivity == 10

        logger.info("Idempotency test passed - re-sync updated existing assertions")

    finally:
        # Cleanup: Delete all assertions (batch deletes with single wait at end)
        logger.info("Cleaning up assertions...")
        for assertion_urn in assertion_urns:
            try:
                graph_client.delete_entity(assertion_urn, hard=True)
            except Exception as e:
                logger.warning(f"Failed to cleanup assertion {assertion_urn}: {e}")

        # Cleanup: Delete all test datasets
        logger.info("Cleaning up datasets...")
        for dataset_urn in datasets:
            try:
                delete_urn(graph_client, dataset_urn)
            except Exception as e:
                logger.warning(f"Failed to cleanup dataset {dataset_urn}: {e}")

        # Single wait after all deletes
        wait_for_assertion_sync()
