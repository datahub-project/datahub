# ABOUTME: Smoke tests for Slack notifications when tags are added/removed from data assets.
# ABOUTME: Tests direct tag changes (not proposals) and verifies Slack messages are sent.

import logging
import time

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities.env_vars import (
    get_release_test_notification_channel,
    get_release_test_notification_token,
)
from tests.utilities.metadata_operations import add_tag, get_dataset_tags, remove_tag
from tests.utilities.slack_helpers import (
    check_slack_notification,
    get_channel_id_by_name,
)
from tests.utils import delete_urns_from_file, ingest_file_via_rest

TEST_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"
TEST_TAG_URN = "urn:li:tag:test-tag-notification"
TEST_TAG_URN_2 = "urn:li:tag:test-tag-notification-2"

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    """Ingest test tags for tag notification tests."""
    logger.info("Ingesting tag notification test data")
    ingest_file_via_rest(auth_session, "tests/release_tests/tag_notification_data.json")
    yield
    logger.info("Cleaning up tag notification test data")
    delete_urns_from_file(
        graph_client, "tests/release_tests/tag_notification_data.json"
    )


@pytest.mark.release_tests
def test_add_tag_with_slack_notification(auth_session, ingest_cleanup_data):
    """
    Test that adding a tag to a dataset triggers a Slack notification.

    Steps:
    1. Ensure tag is NOT already on dataset (cleanup)
    2. Generate unique timestamp for Slack message tracking
    3. Add tag via addTag mutation
    4. Wait for eventual consistency
    5. Verify tag was added to dataset
    6. Verify Slack notification was sent
    7. Cleanup: Remove tag
    """

    # Step 1: Cleanup - ensure tag NOT on dataset
    logger.info(f"Cleaning up any existing tag {TEST_TAG_URN}")
    try:
        remove_tag(auth_session, TEST_DATASET_URN, TEST_TAG_URN)
        wait_for_writes_to_sync()
        logger.info("Removed existing tag")
    except Exception as e:
        logger.info(f"Tag not present (expected): {e}")

    # Step 2: Generate timestamp for message tracking
    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting tag add test at timestamp: {timestamp_seconds}")

    # Step 3: Add tag to dataset
    logger.info(f"Adding tag {TEST_TAG_URN} to {TEST_DATASET_URN}")
    result = add_tag(auth_session, TEST_DATASET_URN, TEST_TAG_URN)
    assert result is True, "Expected addTag to return True"
    logger.info("Tag added successfully")

    # Step 4: Wait for eventual consistency
    wait_for_writes_to_sync()

    # Step 5: Verify tag was added
    logger.info("Verifying tag was added to dataset")
    tags_data = get_dataset_tags(auth_session, TEST_DATASET_URN)
    assert tags_data is not None, "Expected tags data to be present"
    tags = tags_data.get("tags", [])
    tag_urns = [tag["tag"]["urn"] for tag in tags]
    assert TEST_TAG_URN in tag_urns, f"Tag {TEST_TAG_URN} not found in dataset tags"
    logger.info("Tag verified in dataset tags")

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
        expected_text="tag",
    )
    logger.info("Slack notification verified")

    # Step 7: Cleanup - remove tag
    logger.info("Cleaning up - removing tag")
    try:
        remove_tag(auth_session, TEST_DATASET_URN, TEST_TAG_URN)
        wait_for_writes_to_sync()
        logger.info("Cleanup successful")
    except Exception as e:
        logger.warning(f"Cleanup failed: {e}")

    logger.info("Test complete")


@pytest.mark.release_tests
def test_remove_tag_with_slack_notification(auth_session, ingest_cleanup_data):
    """
    Test that removing a tag from a dataset triggers a Slack notification.

    Steps:
    1. Add tag to dataset (setup)
    2. Wait for consistency
    3. Generate unique timestamp for Slack message tracking
    4. Remove tag via removeTag mutation
    5. Wait for eventual consistency
    6. Verify tag was removed from dataset
    7. Verify Slack notification was sent
    """

    # Step 1: Setup - add tag first
    logger.info(f"Setup: Adding tag {TEST_TAG_URN} to {TEST_DATASET_URN}")
    result = add_tag(auth_session, TEST_DATASET_URN, TEST_TAG_URN)
    assert result is True, "Expected addTag to return True"

    # Step 2: Wait for consistency
    wait_for_writes_to_sync()
    logger.info("Tag added for test setup")

    # Step 3: Generate timestamp for message tracking
    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting tag remove test at timestamp: {timestamp_seconds}")

    # Step 4: Remove tag from dataset
    logger.info(f"Removing tag {TEST_TAG_URN} from {TEST_DATASET_URN}")
    result = remove_tag(auth_session, TEST_DATASET_URN, TEST_TAG_URN)
    assert result is True, "Expected removeTag to return True"
    logger.info("Tag removed successfully")

    # Step 5: Wait for eventual consistency
    wait_for_writes_to_sync()

    # Step 6: Verify tag was removed
    logger.info("Verifying tag was removed from dataset")
    tags_data = get_dataset_tags(auth_session, TEST_DATASET_URN)
    tags = tags_data.get("tags", []) if tags_data else []
    tag_urns = [tag["tag"]["urn"] for tag in tags]
    assert TEST_TAG_URN not in tag_urns, (
        f"Tag {TEST_TAG_URN} should not be in dataset tags"
    )
    logger.info("Tag removal verified")

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
        expected_text="tag",
    )
    logger.info("Slack removal notification verified")

    logger.info("Test complete")


@pytest.mark.release_tests
def test_add_multiple_tags_with_slack_notification(auth_session, ingest_cleanup_data):
    """
    Test that adding multiple tags triggers Slack notifications.

    This verifies the notification system works for multiple tag operations.
    """

    # Cleanup
    logger.info(f"Cleaning up any existing tags {TEST_TAG_URN} and {TEST_TAG_URN_2}")
    try:
        remove_tag(auth_session, TEST_DATASET_URN, TEST_TAG_URN)
    except Exception as e:
        logger.info(f"Tag {TEST_TAG_URN} not present (expected): {e}")
    try:
        remove_tag(auth_session, TEST_DATASET_URN, TEST_TAG_URN_2)
    except Exception as e:
        logger.info(f"Tag {TEST_TAG_URN_2} not present (expected): {e}")
    wait_for_writes_to_sync()

    # Generate timestamp
    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting multiple tag add test at timestamp: {timestamp_seconds}")

    # Add first tag
    logger.info(f"Adding tag {TEST_TAG_URN} to {TEST_DATASET_URN}")
    result = add_tag(auth_session, TEST_DATASET_URN, TEST_TAG_URN)
    assert result is True, "Expected addTag to return True for first tag"

    wait_for_writes_to_sync()

    # Add second tag
    logger.info(f"Adding tag {TEST_TAG_URN_2} to {TEST_DATASET_URN}")
    result = add_tag(auth_session, TEST_DATASET_URN, TEST_TAG_URN_2)
    assert result is True, "Expected addTag to return True for second tag"

    wait_for_writes_to_sync()

    # Verify both tags added
    tags_data = get_dataset_tags(auth_session, TEST_DATASET_URN)
    assert tags_data is not None, "Expected tags data to be present"
    tags = tags_data.get("tags", [])
    tag_urns = [tag["tag"]["urn"] for tag in tags]
    assert TEST_TAG_URN in tag_urns, f"Tag {TEST_TAG_URN} not found in dataset tags"
    assert TEST_TAG_URN_2 in tag_urns, f"Tag {TEST_TAG_URN_2} not found in dataset tags"
    logger.info("Both tags verified")

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
        expected_text="tag",
    )
    logger.info("Tag Slack notification verified")

    # Cleanup
    try:
        remove_tag(auth_session, TEST_DATASET_URN, TEST_TAG_URN)
        logger.info(f"Removed {TEST_TAG_URN}")
    except Exception as e:
        logger.warning(f"Failed to remove {TEST_TAG_URN}: {e}")
    try:
        remove_tag(auth_session, TEST_DATASET_URN, TEST_TAG_URN_2)
        logger.info(f"Removed {TEST_TAG_URN_2}")
    except Exception as e:
        logger.warning(f"Failed to remove {TEST_TAG_URN_2}: {e}")
    wait_for_writes_to_sync()
    logger.info("Cleanup complete")

    logger.info("Test complete")
