import logging
import time

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities.env_vars import (
    get_release_test_notification_channel,
    get_release_test_notification_token,
)
from tests.utilities.metadata_operations import (
    list_incidents,
    raise_incident,
    update_incident_status,
)
from tests.utilities.slack_helpers import (
    check_slack_notification,
    get_channel_id_by_name,
)

TEST_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"

logger = logging.getLogger(__name__)


@pytest.mark.release_tests
def test_raise_incident_and_resolve_via_slack(auth_session):
    incidents_data = list_incidents(auth_session, TEST_DATASET_URN)
    existing_incidents = incidents_data["incidents"]

    if existing_incidents:
        logger.info(
            f"Found {len(existing_incidents)} existing incidents, cleaning up before test"
        )
        for incident in existing_incidents:
            logger.info(f"Resolving incident: {incident['urn']}")
            incident_urn = incident["urn"]
            update_incident_status(
                auth_session, incident_urn, "RESOLVED", "Cleanup before test"
            )
            logger.info(f"Incident resolved: {incident_urn}")

    incidents_data = list_incidents(auth_session, TEST_DATASET_URN)
    assert incidents_data["total"] == 0, "Expected zero active incidents after cleanup"

    timestamp = int(time.time() * 1000)
    timestamp_seconds = str(int(time.time()))
    incident_title = f"Test Incident {timestamp}"
    incident_description = f"Test incident raised at {timestamp} for automated testing"

    new_incident_urn = raise_incident(
        auth_session,
        TEST_DATASET_URN,
        "OPERATIONAL",
        incident_title,
        incident_description,
        "CRITICAL",
    )

    assert new_incident_urn is not None, "Expected incident URN to be returned"
    wait_for_writes_to_sync()

    incidents_data = list_incidents(auth_session, TEST_DATASET_URN)
    assert incidents_data["total"] == 1, "Expected one active incident after raising"

    found_incident = None
    for incident in incidents_data["incidents"]:
        if incident["urn"] == new_incident_urn:
            found_incident = incident
            break

    assert found_incident is not None, "Raised incident not found in list"
    assert found_incident["title"] == incident_title
    assert found_incident["description"] == incident_description
    assert found_incident["incidentType"] == "OPERATIONAL"
    assert found_incident["priority"] == "CRITICAL"
    assert found_incident["incidentStatus"]["state"] == "ACTIVE"

    slack_channel = get_release_test_notification_channel()
    slack_token = get_release_test_notification_token()
    if not slack_channel or not slack_token:
        raise ValueError(
            "RELEASE_TEST_NOTIFICATION_CHANNEL or RELEASE_TEST_NOTIFICATION_TOKEN not set"
        )

    logger.info("Verifying Slack notification was sent")

    channel_id = get_channel_id_by_name(slack_token, slack_channel)
    assert channel_id is not None, f"Failed to resolve channel '{slack_channel}' to ID"

    initial_message = check_slack_notification(
        slack_token,
        channel_id,
        incident_title,
        timestamp_seconds,
    )
    logger.info("Slack notification verification passed")

    initial_message_ts = initial_message.get("ts")
    assert initial_message_ts is not None, "Message timestamp not found"
    logger.info(f"Initial message timestamp: {initial_message_ts}")

    logger.info(f"Resolving incident: {new_incident_urn}")
    update_incident_status(
        auth_session, new_incident_urn, "RESOLVED", "Test resolution"
    )
    wait_for_writes_to_sync()
    logger.info(f"Incident resolved: {new_incident_urn}")

    incidents_data = list_incidents(auth_session, TEST_DATASET_URN, state="ALL")
    assert incidents_data["total"] >= 1, (
        "Expected at least one incident after resolution"
    )

    resolved_incident = None
    for incident in incidents_data["incidents"]:
        if incident["urn"] == new_incident_urn:
            resolved_incident = incident
            break

    assert resolved_incident is not None, "Resolved incident not found in list"
    assert resolved_incident["incidentStatus"]["state"] == "RESOLVED", (
        "Incident status should be RESOLVED"
    )

    logger.info("Verifying Slack message was updated with 'Reopen Incident'")
    check_slack_notification(
        slack_token,
        channel_id,
        incident_title,
        timestamp_seconds,
        expected_text="Reopen Incident",
    )
    logger.info("Slack message update verification passed")
