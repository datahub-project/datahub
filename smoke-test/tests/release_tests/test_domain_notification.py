# ABOUTME: Smoke tests for Slack notifications when domains are added/removed from data assets.
# ABOUTME: Tests direct domain changes and verifies Slack messages are sent for ENTITY_DOMAIN_CHANGE notifications.

import logging
import time

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities.env_vars import (
    get_release_test_notification_channel,
    get_release_test_notification_token,
)
from tests.utilities.metadata_operations import (
    get_dataset_domain,
    set_domain,
    unset_domain,
)
from tests.utilities.slack_helpers import (
    check_slack_notification,
    get_channel_id_by_name,
)
from tests.utils import delete_urns_from_file, ingest_file_via_rest

TEST_DATASET_SET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDomainDataset_SetTest,PROD)"
)
TEST_DATASET_UNSET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDomainDataset_UnsetTest,PROD)"
)
TEST_DOMAIN_URN = "urn:li:domain:test-domain-notification"

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    """Ingest test domain and dataset for domain notification tests."""
    logger.info("Ingesting domain notification test data")
    ingest_file_via_rest(
        auth_session, "tests/release_tests/domain_notification_data.json"
    )
    yield
    logger.info("Cleaning up domain notification test data")
    delete_urns_from_file(
        graph_client, "tests/release_tests/domain_notification_data.json"
    )


@pytest.mark.release_tests
def test_set_domain_with_slack_notification(auth_session, ingest_cleanup_data):
    """
    Test that setting a domain on a dataset triggers a Slack notification.

    Steps:
    1. Ensure domain is NOT already on dataset (cleanup)
    2. Generate unique timestamp for Slack message tracking
    3. Set domain via setDomain mutation
    4. Wait for eventual consistency
    5. Verify domain was set on dataset
    6. Verify Slack notification was sent
    7. Cleanup: Unset domain
    """

    # Step 1: Cleanup - ensure domain NOT on dataset
    logger.info(f"Cleaning up any existing domain on {TEST_DATASET_SET_URN}")
    try:
        unset_domain(auth_session, TEST_DATASET_SET_URN)
        wait_for_writes_to_sync()
        logger.info("Removed existing domain")
    except Exception as e:
        logger.info(f"Domain not present (expected): {e}")

    # Step 2: Generate timestamp for message tracking
    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting domain set test at timestamp: {timestamp_seconds}")

    # Step 3: Set domain on dataset
    logger.info(f"Setting domain {TEST_DOMAIN_URN} on {TEST_DATASET_SET_URN}")
    result = set_domain(
        auth_session,
        TEST_DATASET_SET_URN,
        TEST_DOMAIN_URN,
    )
    assert result is True, "Expected setDomain to return True"
    logger.info("Domain set successfully")

    # Step 4: Wait for eventual consistency
    wait_for_writes_to_sync()

    # Step 5: Verify domain was set
    logger.info("Verifying domain was set on dataset")
    domain_data = get_dataset_domain(auth_session, TEST_DATASET_SET_URN)
    assert domain_data is not None, "Expected domain data to be present"
    assert domain_data.get("domain") is not None, "Expected domain object to be present"
    assert domain_data["domain"]["urn"] == TEST_DOMAIN_URN, (
        f"Domain {TEST_DOMAIN_URN} not found on dataset"
    )
    logger.info("Domain verified on dataset")

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
        expected_text="domain",
    )
    logger.info("Slack notification verified")

    # Step 7: Cleanup - unset domain
    logger.info("Cleaning up - unsetting domain")
    try:
        unset_domain(auth_session, TEST_DATASET_SET_URN)
        wait_for_writes_to_sync()
        logger.info("Cleanup successful")
    except Exception as e:
        logger.warning(f"Cleanup failed: {e}")

    logger.info("Test complete")


@pytest.mark.release_tests
def test_unset_domain_with_slack_notification(auth_session, ingest_cleanup_data):
    """
    Test that unsetting a domain from a dataset triggers a Slack notification.

    Steps:
    1. Set domain on dataset (setup)
    2. Wait for consistency
    3. Generate unique timestamp for Slack message tracking
    4. Unset domain via unsetDomain mutation
    5. Wait for eventual consistency
    6. Verify domain was removed from dataset
    7. Verify Slack notification was sent
    """

    # Step 1: Setup - set domain first
    logger.info(f"Setup: Setting domain {TEST_DOMAIN_URN} on {TEST_DATASET_UNSET_URN}")
    result = set_domain(
        auth_session,
        TEST_DATASET_UNSET_URN,
        TEST_DOMAIN_URN,
    )
    assert result is True, "Expected setDomain to return True"

    # Step 2: Wait for consistency
    wait_for_writes_to_sync()
    logger.info("Domain set for test setup")

    # Step 3: Generate timestamp for message tracking
    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting domain unset test at timestamp: {timestamp_seconds}")

    # Step 4: Unset domain from dataset
    logger.info(f"Unsetting domain from {TEST_DATASET_UNSET_URN}")
    result = unset_domain(auth_session, TEST_DATASET_UNSET_URN)
    assert result is True, "Expected unsetDomain to return True"
    logger.info("Domain unset successfully")

    # Step 5: Wait for eventual consistency
    wait_for_writes_to_sync()

    # Step 6: Verify domain was removed
    logger.info("Verifying domain was removed from dataset")
    domain_data = get_dataset_domain(auth_session, TEST_DATASET_UNSET_URN)
    assert domain_data is None or domain_data.get("domain") is None, (
        f"Domain {TEST_DOMAIN_URN} should not be on dataset"
    )
    logger.info("Domain removal verified")

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
        expected_text="domain",
    )
    logger.info("Slack removal notification verified")

    logger.info("Test complete")
