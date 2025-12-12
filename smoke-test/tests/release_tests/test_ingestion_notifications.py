# ABOUTME: Release test for ingestion start and failure global notifications.
# ABOUTME: Tests that Slack notifications are sent when ingestion runs start and fail.

import json
import logging
import time

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities.metadata_operations import (
    create_ingestion_execution_request,
    create_ingestion_source,
    find_ingestion_source_by_name,
    poll_until_execution_fails,
    poll_until_execution_starts,
    verify_ingestion_slack_notification,
)

logger = logging.getLogger(__name__)

# Test constants
TEST_SOURCE_NAME_START = "test-ingestion-start-notification"
TEST_SOURCE_NAME_FAILURE = "test-ingestion-failure-notification"
POLL_TIMEOUT_SECONDS = 120


@pytest.mark.release_tests
def test_ingestion_start_notification(auth_session):
    """
    Test that starting an ingestion run triggers a Slack notification.

    Steps:
    1. Check if test source already exists (for idempotency)
    2. Create a valid demo-data ingestion source (if not exists)
    3. Wait for source to be available
    4. Generate unique timestamp for Slack message tracking
    5. Trigger ingestion execution
    6. Poll until execution starts
    7. Verify Slack notification was sent for ingestion start
    """

    # Step 1 & 2: Check if source already exists, create if not
    logger.info(f"Checking if source '{TEST_SOURCE_NAME_START}' already exists...")
    source_urn = find_ingestion_source_by_name(auth_session, TEST_SOURCE_NAME_START)

    if source_urn is None:
        logger.info(f"Creating new ingestion source '{TEST_SOURCE_NAME_START}'...")
        demo_data_recipe = json.dumps(
            {
                "source": {
                    "type": "demo-data",
                    "config": {},
                }
            }
        )

        source_urn = create_ingestion_source(
            auth_session,
            name=TEST_SOURCE_NAME_START,
            source_type="demo-data",
            recipe=demo_data_recipe,
        )
        logger.info(f"Created ingestion source: {source_urn}")

        # Step 3: Wait for source to be available
        wait_for_writes_to_sync()
        logger.info("Source is available")

    # Step 4: Generate timestamp for message tracking
    # Note: Timestamp is generated before action to ensure we capture the notification window
    # check_slack_notification() will search backwards using clock_skew_buffer
    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting ingestion at timestamp: {timestamp_seconds}")

    # Step 5: Trigger ingestion execution
    logger.info(f"Triggering ingestion execution for source: {source_urn}")
    execution_urn = create_ingestion_execution_request(auth_session, source_urn)
    logger.info(f"Execution triggered: {execution_urn}")

    # Step 6: Poll until execution starts
    execution_started = poll_until_execution_starts(
        auth_session, execution_urn, timeout_seconds=POLL_TIMEOUT_SECONDS
    )
    assert execution_started, (
        f"Execution did not start within {POLL_TIMEOUT_SECONDS} seconds"
    )

    # Step 7: Verify Slack notification
    verify_ingestion_slack_notification(
        source_name=TEST_SOURCE_NAME_START,
        timestamp_seconds=timestamp_seconds,
        expected_text="has started",
    )

    logger.info("Test complete")


@pytest.mark.release_tests
def test_ingestion_failure_notification(auth_session):
    """
    Test that a failed ingestion run triggers a Slack notification.

    Steps:
    1. Check if test source already exists (for idempotency)
    2. Create a mysql source with invalid credentials (if not exists)
    3. Wait for source to be available
    4. Generate unique timestamp for Slack message tracking
    5. Trigger ingestion execution
    6. Poll until execution fails
    7. Verify Slack notification was sent for ingestion failure
    """

    # Step 1 & 2: Check if source already exists, create if not
    logger.info(f"Checking if source '{TEST_SOURCE_NAME_FAILURE}' already exists...")
    source_urn = find_ingestion_source_by_name(auth_session, TEST_SOURCE_NAME_FAILURE)

    if source_urn is None:
        logger.info(f"Creating new ingestion source '{TEST_SOURCE_NAME_FAILURE}'...")
        mysql_recipe = json.dumps(
            {
                "source": {
                    "type": "mysql",
                    "config": {
                        "username": "test_user",
                        "password": "PASSWORD",
                        "host_port": "localhost:3306",
                        "database": "test_db",
                    },
                }
            }
        )

        source_urn = create_ingestion_source(
            auth_session,
            name=TEST_SOURCE_NAME_FAILURE,
            source_type="mysql",
            recipe=mysql_recipe,
        )
        logger.info(f"Created ingestion source with invalid creds: {source_urn}")

        # Step 3: Wait for source to be available
        wait_for_writes_to_sync()
        logger.info("Source is available")

    # Step 4: Generate timestamp for message tracking
    timestamp_seconds = str(int(time.time()))
    logger.info(f"Starting ingestion at timestamp: {timestamp_seconds}")

    # Step 5: Trigger ingestion execution
    logger.info(f"Triggering ingestion execution for source: {source_urn}")
    execution_urn = create_ingestion_execution_request(auth_session, source_urn)
    logger.info(f"Execution triggered: {execution_urn}")

    # Step 6: Poll until execution fails
    execution_failed = poll_until_execution_fails(
        auth_session, execution_urn, timeout_seconds=POLL_TIMEOUT_SECONDS
    )
    assert execution_failed, (
        f"Execution did not fail within {POLL_TIMEOUT_SECONDS} seconds"
    )

    # Step 7: Verify Slack notification
    verify_ingestion_slack_notification(
        source_name=TEST_SOURCE_NAME_FAILURE,
        timestamp_seconds=timestamp_seconds,
        expected_text="has failed",
    )

    logger.info("Test complete")
