# ABOUTME: Smoke tests for Slack notifications when owners are added/removed from data assets.
# ABOUTME: Tests direct owner changes (not proposals) and verifies Slack messages are sent.

import logging
import time

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities.env_vars import (
    get_release_test_notification_channel,
    get_release_test_notification_token,
)
from tests.utilities.metadata_operations import (
    add_owner,
    get_dataset_owners,
    remove_owner,
)
from tests.utilities.slack_helpers import (
    check_slack_notification,
    get_channel_id_by_name,
)
from tests.utils import delete_urns_from_file, ingest_file_via_rest

TEST_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"
TEST_OWNER_URN = "urn:li:corpuser:test-owner-notification"
TEST_OWNER_URN_2 = "urn:li:corpuser:test-owner-notification-2"
TEST_GROUP_URN = "urn:li:corpGroup:test-group-notification"

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    """Ingest test users/groups for owner notification tests."""
    logger.info("Ingesting owner notification test data")
    ingest_file_via_rest(
        auth_session, "tests/release_tests/owner_notification_data.json"
    )
    yield
    logger.info("Cleaning up owner notification test data")
    delete_urns_from_file(
        graph_client, "tests/release_tests/owner_notification_data.json"
    )


@pytest.mark.release_tests
def test_add_owner_with_slack_notification(auth_session, ingest_cleanup_data):
    """
    Test that adding an owner to a dataset triggers a Slack notification.

    Steps:
    1. Ensure owner is NOT already on dataset (cleanup)
    2. Generate unique timestamp for Slack message tracking
    3. Add owner via addOwner mutation
    4. Wait for eventual consistency
    5. Verify owner was added to dataset
    6. Verify Slack notification was sent
    7. Cleanup: Remove owner
    """

    # Step 1: Cleanup - ensure owner NOT on dataset
    logger.info(f"Cleaning up any existing owner {TEST_OWNER_URN}")
    try:
        remove_owner(auth_session, TEST_DATASET_URN, TEST_OWNER_URN)
        wait_for_writes_to_sync()
        logger.info("Removed existing owner")
    except Exception as e:
        logger.info(f"Owner not present (expected): {e}")

    # Step 2: Generate timestamp for message tracking
    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting owner add test at timestamp: {timestamp_seconds}")

    # Step 3: Add owner to dataset
    logger.info(f"Adding owner {TEST_OWNER_URN} to {TEST_DATASET_URN}")
    result = add_owner(
        auth_session,
        TEST_DATASET_URN,
        TEST_OWNER_URN,
        owner_entity_type="CORP_USER",
        ownership_type="TECHNICAL_OWNER",
    )
    assert result is True, "Expected addOwner to return True"
    logger.info("Owner added successfully")

    # Step 4: Wait for eventual consistency
    wait_for_writes_to_sync()

    # Step 5: Verify owner was added
    logger.info("Verifying owner was added to dataset")
    ownership_data = get_dataset_owners(auth_session, TEST_DATASET_URN)
    owners = ownership_data.get("owners", []) if ownership_data else []
    owner_urns = [owner["owner"]["urn"] for owner in owners]
    assert TEST_OWNER_URN in owner_urns, (
        f"Owner {TEST_OWNER_URN} not found in dataset owners"
    )
    logger.info("Owner verified in dataset ownership")

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
        expected_text="owner",
    )
    logger.info("Slack notification verified")

    # Step 7: Cleanup - remove owner
    logger.info("Cleaning up - removing owner")
    try:
        remove_owner(auth_session, TEST_DATASET_URN, TEST_OWNER_URN)
        wait_for_writes_to_sync()
        logger.info("Cleanup successful")
    except Exception as e:
        logger.warning(f"Cleanup failed: {e}")

    logger.info("Test complete")


@pytest.mark.release_tests
def test_remove_owner_with_slack_notification(auth_session, ingest_cleanup_data):
    """
    Test that removing an owner from a dataset triggers a Slack notification.

    Steps:
    1. Add owner to dataset (setup)
    2. Wait for consistency
    3. Generate unique timestamp for Slack message tracking
    4. Remove owner via removeOwner mutation
    5. Wait for eventual consistency
    6. Verify owner was removed from dataset
    7. Verify Slack notification was sent
    """

    # Step 1: Setup - add owner first
    logger.info(f"Setup: Adding owner {TEST_OWNER_URN} to {TEST_DATASET_URN}")
    result = add_owner(
        auth_session,
        TEST_DATASET_URN,
        TEST_OWNER_URN,
        owner_entity_type="CORP_USER",
        ownership_type="TECHNICAL_OWNER",
    )
    assert result is True, "Expected addOwner to return True"

    # Step 2: Wait for consistency
    wait_for_writes_to_sync()
    logger.info("Owner added for test setup")

    # Step 3: Generate timestamp for message tracking
    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting owner remove test at timestamp: {timestamp_seconds}")

    # Step 4: Remove owner from dataset
    logger.info(f"Removing owner {TEST_OWNER_URN} from {TEST_DATASET_URN}")
    result = remove_owner(auth_session, TEST_DATASET_URN, TEST_OWNER_URN)
    assert result is True, "Expected removeOwner to return True"
    logger.info("Owner removed successfully")

    # Step 5: Wait for eventual consistency
    wait_for_writes_to_sync()

    # Step 6: Verify owner was removed
    logger.info("Verifying owner was removed from dataset")
    ownership_data = get_dataset_owners(auth_session, TEST_DATASET_URN)
    owners = ownership_data.get("owners", []) if ownership_data else []
    owner_urns = [owner["owner"]["urn"] for owner in owners]
    assert TEST_OWNER_URN not in owner_urns, (
        f"Owner {TEST_OWNER_URN} should not be in dataset owners"
    )
    logger.info("Owner removal verified")

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
        expected_text="owner",
    )
    logger.info("Slack removal notification verified")

    logger.info("Test complete")


@pytest.mark.release_tests
def test_add_group_owner_with_slack_notification(auth_session, ingest_cleanup_data):
    """
    Test that adding a GROUP owner (not user) triggers Slack notification.

    This verifies the notification system works for both user and group owners.
    """

    # Cleanup
    logger.info(f"Cleaning up any existing group owner {TEST_GROUP_URN}")
    try:
        remove_owner(auth_session, TEST_DATASET_URN, TEST_GROUP_URN)
        wait_for_writes_to_sync()
    except Exception as e:
        logger.info(f"Group owner not present (expected): {e}")

    # Generate timestamp
    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting group owner add test at timestamp: {timestamp_seconds}")

    # Add group owner
    logger.info(f"Adding group owner {TEST_GROUP_URN} to {TEST_DATASET_URN}")
    result = add_owner(
        auth_session,
        TEST_DATASET_URN,
        TEST_GROUP_URN,
        owner_entity_type="CORP_GROUP",
        ownership_type="BUSINESS_OWNER",
    )
    assert result is True, "Expected addOwner to return True for group"

    wait_for_writes_to_sync()

    # Verify group owner added
    ownership_data = get_dataset_owners(auth_session, TEST_DATASET_URN)
    owners = ownership_data.get("owners", []) if ownership_data else []
    owner_urns = [owner["owner"]["urn"] for owner in owners]
    assert TEST_GROUP_URN in owner_urns, (
        f"Group owner {TEST_GROUP_URN} not found in dataset owners"
    )
    logger.info("Group owner verified")

    # Verify Slack notification
    slack_channel = get_release_test_notification_channel()
    slack_token = get_release_test_notification_token()
    if not slack_channel or not slack_token:
        raise ValueError(
            "RELEASE_TEST_NOTIFICATION_CHANNEL or RELEASE_TEST_NOTIFICATION_TOKEN not set"
        )

    channel_id = get_channel_id_by_name(slack_token, slack_channel)
    assert channel_id is not None, f"Failed to resolve channel '{slack_channel}' to ID"

    check_slack_notification(
        slack_token,
        channel_id,
        "added",
        timestamp_seconds,
        expected_text="owner",
    )
    logger.info("Group owner Slack notification verified")

    # Cleanup
    try:
        remove_owner(auth_session, TEST_DATASET_URN, TEST_GROUP_URN)
        wait_for_writes_to_sync()
        logger.info("Cleanup successful")
    except Exception as e:
        logger.warning(f"Cleanup failed: {e}")

    logger.info("Test complete")
