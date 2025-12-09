# ABOUTME: Smoke tests for Slack notifications when datasets are marked as deprecated/undeprecated.
# ABOUTME: Tests direct deprecation changes and verifies Slack messages are sent.

import logging
import time

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities.env_vars import (
    get_release_test_notification_channel,
    get_release_test_notification_token,
)
from tests.utilities.metadata_operations import (
    get_dataset_deprecation,
    update_deprecation,
)
from tests.utilities.slack_helpers import (
    check_slack_notification,
    get_channel_id_by_name,
)
from tests.utils import delete_urns_from_file, ingest_file_via_rest

TEST_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    """Ingest test dataset for deprecation notification tests."""
    logger.info("Ingesting deprecation notification test data")
    ingest_file_via_rest(
        auth_session, "tests/release_tests/deprecation_notification_data.json"
    )
    yield
    logger.info("Cleaning up deprecation notification test data")
    delete_urns_from_file(
        graph_client, "tests/release_tests/deprecation_notification_data.json"
    )


@pytest.mark.release_tests
def test_mark_deprecated_with_slack_notification(auth_session, ingest_cleanup_data):
    """
    Test that marking a dataset as deprecated triggers a Slack notification.

    Steps:
    1. Ensure dataset is NOT already deprecated (cleanup)
    2. Generate unique timestamp for Slack message tracking
    3. Mark dataset as deprecated via updateDeprecation mutation
    4. Wait for eventual consistency
    5. Verify dataset was marked as deprecated
    6. Verify Slack notification was sent
    7. Cleanup: Mark as undeprecated
    """

    # Step 1: Cleanup - ensure dataset NOT deprecated
    logger.info(f"Cleaning up any existing deprecation on {TEST_DATASET_URN}")
    try:
        update_deprecation(auth_session, TEST_DATASET_URN, deprecated=False)
        wait_for_writes_to_sync()
        logger.info("Removed existing deprecation")
    except Exception as e:
        logger.info(f"Dataset not deprecated (expected): {e}")

    # Step 2: Generate timestamp for message tracking
    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting deprecation test at timestamp: {timestamp_seconds}")

    # Step 3: Mark dataset as deprecated
    logger.info(f"Marking {TEST_DATASET_URN} as deprecated")
    result = update_deprecation(
        auth_session,
        TEST_DATASET_URN,
        deprecated=True,
        note="This dataset is being deprecated for testing",
    )
    assert result is True, "Expected updateDeprecation to return True"
    logger.info("Dataset marked as deprecated successfully")

    # Step 4: Wait for eventual consistency
    wait_for_writes_to_sync()

    # Step 5: Verify dataset was marked as deprecated
    logger.info("Verifying dataset was marked as deprecated")
    deprecation_data = get_dataset_deprecation(auth_session, TEST_DATASET_URN)
    assert deprecation_data is not None, "Expected deprecation data to be present"
    assert deprecation_data.get("deprecated") is True, (
        "Dataset should be marked as deprecated"
    )
    logger.info("Deprecation verified on dataset")

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
        "deprecated",
        timestamp_seconds,
        expected_text="marked as deprecated",
    )
    logger.info("Slack notification verified")

    # Step 7: Cleanup - mark as undeprecated
    logger.info("Cleaning up - marking as undeprecated")
    try:
        update_deprecation(auth_session, TEST_DATASET_URN, deprecated=False)
        wait_for_writes_to_sync()
        logger.info("Cleanup successful")
    except Exception as e:
        logger.warning(f"Cleanup failed: {e}")

    logger.info("Test complete")


@pytest.mark.release_tests
def test_mark_undeprecated_with_slack_notification(auth_session, ingest_cleanup_data):
    """
    Test that marking a dataset as undeprecated triggers a Slack notification.

    Steps:
    1. Mark dataset as deprecated (setup)
    2. Wait for consistency
    3. Generate unique timestamp for Slack message tracking
    4. Mark dataset as undeprecated via updateDeprecation mutation
    5. Wait for eventual consistency
    6. Verify dataset was marked as undeprecated
    7. Verify Slack notification was sent
    """

    # Step 1: Setup - mark as deprecated first
    logger.info(f"Setup: Marking {TEST_DATASET_URN} as deprecated")
    result = update_deprecation(
        auth_session,
        TEST_DATASET_URN,
        deprecated=True,
        note="Temporarily deprecated for testing",
    )
    assert result is True, "Expected updateDeprecation to return True"

    # Step 2: Wait for consistency
    wait_for_writes_to_sync()
    logger.info("Dataset marked as deprecated for test setup")

    # Step 3: Generate timestamp for message tracking
    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting undeprecation test at timestamp: {timestamp_seconds}")

    # Step 4: Mark dataset as undeprecated
    logger.info(f"Marking {TEST_DATASET_URN} as undeprecated")
    result = update_deprecation(auth_session, TEST_DATASET_URN, deprecated=False)
    assert result is True, "Expected updateDeprecation to return True"
    logger.info("Dataset marked as undeprecated successfully")

    # Step 5: Wait for eventual consistency
    wait_for_writes_to_sync()

    # Step 6: Verify dataset was marked as undeprecated
    logger.info("Verifying dataset was marked as undeprecated")
    deprecation_data = get_dataset_deprecation(auth_session, TEST_DATASET_URN)
    # Deprecation data may be None or deprecated=False
    if deprecation_data is not None:
        assert deprecation_data.get("deprecated") is False, (
            "Dataset should not be marked as deprecated"
        )
    logger.info("Undeprecation verified on dataset")

    # Step 7: Verify Slack notification
    slack_channel = get_release_test_notification_channel()
    slack_token = get_release_test_notification_token()
    if not slack_channel or not slack_token:
        raise ValueError(
            "RELEASE_TEST_NOTIFICATION_CHANNEL or RELEASE_TEST_NOTIFICATION_TOKEN not set"
        )

    logger.info("Verifying Slack notification was sent for undeprecation")
    channel_id = get_channel_id_by_name(slack_token, slack_channel)
    assert channel_id is not None, f"Failed to resolve channel '{slack_channel}' to ID"

    check_slack_notification(
        slack_token,
        channel_id,
        "un-deprecated",
        timestamp_seconds,
        expected_text="marked as un-deprecated",
    )
    logger.info("Slack undeprecation notification verified")

    logger.info("Test complete")
