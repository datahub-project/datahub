# ABOUTME: Smoke tests for Slack notifications when glossary terms are added/removed from data assets.
# ABOUTME: Tests direct term changes (not proposals) and verifies Slack messages are sent.

import logging
import time

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities.env_vars import (
    get_release_test_notification_channel,
    get_release_test_notification_token,
)
from tests.utilities.metadata_operations import add_term, get_dataset_terms, remove_term
from tests.utilities.slack_helpers import (
    check_slack_notification,
    get_channel_id_by_name,
)
from tests.utils import delete_urns_from_file, ingest_file_via_rest

TEST_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"
TEST_TERM_URN = "urn:li:glossaryTerm:test-term-notification"
TEST_TERM_URN_2 = "urn:li:glossaryTerm:test-term-notification-2"

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    """Ingest test glossary terms for term notification tests."""
    logger.info("Ingesting term notification test data")
    ingest_file_via_rest(
        auth_session, "tests/release_tests/term_notification_data.json"
    )
    yield
    logger.info("Cleaning up term notification test data")
    delete_urns_from_file(
        graph_client, "tests/release_tests/term_notification_data.json"
    )


@pytest.mark.release_tests
def test_add_term_with_slack_notification(auth_session, ingest_cleanup_data):
    """
    Test that adding a glossary term to a dataset triggers a Slack notification.

    Steps:
    1. Ensure term is NOT already on dataset (cleanup)
    2. Generate unique timestamp for Slack message tracking
    3. Add term via addTerm mutation
    4. Wait for eventual consistency
    5. Verify term was added to dataset
    6. Verify Slack notification was sent
    7. Cleanup: Remove term
    """

    # Step 1: Cleanup - ensure term NOT on dataset
    logger.info(f"Cleaning up any existing term {TEST_TERM_URN}")
    try:
        remove_term(auth_session, TEST_DATASET_URN, TEST_TERM_URN)
        wait_for_writes_to_sync()
        logger.info("Removed existing term")
    except Exception as e:
        logger.info(f"Term not present (expected): {e}")

    # Step 2: Generate timestamp for message tracking
    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting term add test at timestamp: {timestamp_seconds}")

    # Step 3: Add term to dataset
    logger.info(f"Adding term {TEST_TERM_URN} to {TEST_DATASET_URN}")
    result = add_term(
        auth_session,
        TEST_DATASET_URN,
        TEST_TERM_URN,
    )
    assert result is True, "Expected addTerm to return True"
    logger.info("Term added successfully")

    # Step 4: Wait for eventual consistency
    wait_for_writes_to_sync()

    # Step 5: Verify term was added
    logger.info("Verifying term was added to dataset")
    terms_data = get_dataset_terms(auth_session, TEST_DATASET_URN)
    assert terms_data is not None, "Expected terms data to be present"
    terms = terms_data.get("terms", [])
    term_urns = [term["term"]["urn"] for term in terms]
    assert TEST_TERM_URN in term_urns, (
        f"Term {TEST_TERM_URN} not found in dataset terms"
    )
    logger.info("Term verified in dataset terms")

    # Step 6: Verify Slack notification
    slack_channel = get_release_test_notification_channel()
    slack_token = get_release_test_notification_token()
    if not slack_channel or not slack_token:
        raise ValueError(
            "RELEASE_TEST_NOTIFICATION_CHANNEL or RELEASE_TEST_NOTIFICATION_TOKEN not set"
        )

    logger.info("Verifying Slack notification was sent")
    channel_id = get_channel_id_by_name(slack_token, slack_channel)
    assert channel_id is not None, f"Failed to resolve channel '{slack_channel}' to ID"

    check_slack_notification(
        slack_token,
        channel_id,
        "added",
        timestamp_seconds,
        expected_text="term",
    )
    logger.info("Slack notification verified")

    # Step 7: Cleanup - remove term
    logger.info("Cleaning up - removing term")
    try:
        remove_term(auth_session, TEST_DATASET_URN, TEST_TERM_URN)
        wait_for_writes_to_sync()
        logger.info("Cleanup successful")
    except Exception as e:
        logger.warning(f"Cleanup failed: {e}")

    logger.info("Test complete")


@pytest.mark.release_tests
def test_remove_term_with_slack_notification(auth_session, ingest_cleanup_data):
    """
    Test that removing a glossary term from a dataset triggers a Slack notification.

    Steps:
    1. Add term to dataset (setup)
    2. Wait for consistency
    3. Generate unique timestamp for Slack message tracking
    4. Remove term via removeTerm mutation
    5. Wait for eventual consistency
    6. Verify term was removed from dataset
    7. Verify Slack notification was sent
    """

    # Step 1: Setup - add term first
    logger.info(f"Setup: Adding term {TEST_TERM_URN} to {TEST_DATASET_URN}")
    result = add_term(
        auth_session,
        TEST_DATASET_URN,
        TEST_TERM_URN,
    )
    assert result is True, "Expected addTerm to return True"

    # Step 2: Wait for consistency
    wait_for_writes_to_sync()
    logger.info("Term added for test setup")

    # Step 3: Generate timestamp for message tracking
    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting term remove test at timestamp: {timestamp_seconds}")

    # Step 4: Remove term from dataset
    logger.info(f"Removing term {TEST_TERM_URN} from {TEST_DATASET_URN}")
    result = remove_term(auth_session, TEST_DATASET_URN, TEST_TERM_URN)
    assert result is True, "Expected removeTerm to return True"
    logger.info("Term removed successfully")

    # Step 5: Wait for eventual consistency
    wait_for_writes_to_sync()

    # Step 6: Verify term was removed
    logger.info("Verifying term was removed from dataset")
    terms_data = get_dataset_terms(auth_session, TEST_DATASET_URN)
    terms = terms_data.get("terms", []) if terms_data else []
    term_urns = [term["term"]["urn"] for term in terms]
    assert TEST_TERM_URN not in term_urns, (
        f"Term {TEST_TERM_URN} should not be in dataset terms"
    )
    logger.info("Term removal verified")

    # Step 7: Verify Slack notification
    slack_channel = get_release_test_notification_channel()
    slack_token = get_release_test_notification_token()
    if not slack_channel or not slack_token:
        raise ValueError(
            "RELEASE_TEST_NOTIFICATION_CHANNEL or RELEASE_TEST_NOTIFICATION_TOKEN not set"
        )

    logger.info("Verifying Slack notification was sent for removal")
    channel_id = get_channel_id_by_name(slack_token, slack_channel)
    assert channel_id is not None, f"Failed to resolve channel '{slack_channel}' to ID"

    check_slack_notification(
        slack_token,
        channel_id,
        "removed",
        timestamp_seconds,
        expected_text="term",
    )
    logger.info("Slack removal notification verified")

    logger.info("Test complete")
