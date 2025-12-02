import logging
import time

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities.env_vars import (
    get_release_test_notification_channel,
    get_release_test_notification_token,
)
from tests.utilities.metadata_operations import (
    accept_proposal,
    create_tag_proposal,
    list_proposals,
    reject_proposal,
    remove_tag,
)
from tests.utilities.slack_helpers import (
    check_slack_notification,
    get_channel_id_by_name,
)

TEST_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"
TEST_TAG_URN = "urn:li:tag:Legacy"

logger = logging.getLogger(__name__)


@pytest.mark.release_tests
def test_create_proposal_and_accept_reject_via_slack(auth_session):
    # Step 1: Clean up any pending proposals for this dataset
    proposals_data = list_proposals(auth_session, TEST_DATASET_URN, status="PENDING")
    existing_proposals = proposals_data["actionRequests"]

    if existing_proposals:
        logger.info(
            f"Found {len(existing_proposals)} existing proposals, cleaning up before test"
        )
        for proposal in existing_proposals:
            logger.info(f"Rejecting proposal: {proposal['urn']}")
            reject_proposal(auth_session, proposal["urn"], "Cleanup before test")
            logger.info(f"Proposal rejected: {proposal['urn']}")
        wait_for_writes_to_sync()

    proposals_data = list_proposals(auth_session, TEST_DATASET_URN, status="PENDING")
    assert proposals_data["total"] == 0, "Expected zero pending proposals after cleanup"

    # Step 2: CRITICAL - Ensure tag is not already applied
    # If tag is already on the dataset, proposal functionality will not work
    try:
        remove_tag(auth_session, TEST_DATASET_URN, TEST_TAG_URN)
        logger.info(f"Removed existing tag {TEST_TAG_URN} from dataset")
        wait_for_writes_to_sync()
    except Exception as e:
        # Tag wasn't applied, which is expected
        logger.info(f"Tag not present on dataset (expected): {e}")

    # ===== ACCEPTANCE FLOW =====

    # Step 3: Create first tag proposal with unique timestamp-based identifier
    timestamp = int(time.time() * 1000)
    timestamp_seconds = str(int(time.time()))
    proposal_description = f"Test Proposal {timestamp}"

    new_proposal_urn = create_tag_proposal(
        auth_session,
        TEST_DATASET_URN,
        [TEST_TAG_URN],
        proposal_description,
    )

    assert new_proposal_urn is not None, "Expected proposal URN to be returned"
    wait_for_writes_to_sync()

    # Step 4: Verify proposal was created
    proposals_data = list_proposals(auth_session, TEST_DATASET_URN, status="PENDING")
    assert proposals_data["total"] >= 1, "Expected at least one pending proposal"

    found_proposal = None
    for proposal in proposals_data["actionRequests"]:
        if proposal["urn"] == new_proposal_urn:
            found_proposal = proposal
            break

    assert found_proposal is not None, "Created proposal not found in list"
    assert found_proposal["description"] == proposal_description
    assert found_proposal["type"] == "TAG_ASSOCIATION"
    assert found_proposal["status"] == "PENDING"

    # Step 5: Verify Slack notification was sent
    slack_channel = get_release_test_notification_channel()
    slack_token = get_release_test_notification_token()
    if not slack_channel or not slack_token:
        raise ValueError(
            "RELEASE_TEST_NOTIFICATION_CHANNEL or RELEASE_TEST_NOTIFICATION_TOKEN not set"
        )

    logger.info("Verifying Slack notification was sent for proposal")

    channel_id = get_channel_id_by_name(slack_token, slack_channel)
    assert channel_id is not None, f"Failed to resolve channel '{slack_channel}' to ID"

    # Search for "New Proposal Raised" in Slack message
    # The message format is: "Acryl Support has proposed to add Tag(s) Legacy for SampleHdfsDataset"
    check_slack_notification(
        slack_token,
        channel_id,
        "New Proposal Raised",  # Search for notification title
        timestamp_seconds,
    )
    logger.info("Slack notification verification passed")

    # Step 6: Accept the proposal
    logger.info(f"Accepting proposal: {new_proposal_urn}")
    accept_result = accept_proposal(auth_session, new_proposal_urn, "Test acceptance")
    assert accept_result is True, "Expected acceptance to succeed"
    wait_for_writes_to_sync()
    logger.info(f"Proposal accepted: {new_proposal_urn}")

    # Step 7: Verify proposal status is now COMPLETED/ACCEPTED
    proposals_data = list_proposals(auth_session, TEST_DATASET_URN, status="COMPLETED")
    assert proposals_data["total"] >= 1, "Expected at least one completed proposal"

    accepted_proposal = None
    for proposal in proposals_data["actionRequests"]:
        if proposal["urn"] == new_proposal_urn:
            accepted_proposal = proposal
            break

    assert accepted_proposal is not None, "Accepted proposal not found in list"
    assert accepted_proposal["status"] == "COMPLETED"
    assert accepted_proposal["result"] == "ACCEPTED"

    # Step 8: Verify Slack message was updated with acceptance
    # After acceptance, the notification changes to "Proposal Status Changed"
    # with message "has accepted the proposal to add Tag(s) Legacy for SampleHdfsDataset"
    logger.info("Verifying Slack message was updated with acceptance status")
    check_slack_notification(
        slack_token,
        channel_id,
        "Proposal Status Changed",
        timestamp_seconds,
        expected_text="has accepted the proposal",
    )
    logger.info("Slack message update verification passed")

    # Step 9: Remove the tag to prepare for rejection flow test
    logger.info("Removing tag from dataset to prepare for rejection test")
    remove_result = remove_tag(auth_session, TEST_DATASET_URN, TEST_TAG_URN)
    assert remove_result is True, "Expected tag removal to succeed"
    wait_for_writes_to_sync()
    logger.info("Tag removed successfully")

    # ===== REJECTION FLOW =====

    # Step 10: Create second tag proposal for rejection test
    timestamp2 = int(time.time() * 1000)
    timestamp_seconds2 = str(int(time.time()))
    proposal_description2 = f"Test Proposal Rejection {timestamp2}"

    second_proposal_urn = create_tag_proposal(
        auth_session,
        TEST_DATASET_URN,
        [TEST_TAG_URN],
        proposal_description2,
    )

    assert second_proposal_urn is not None, (
        "Expected second proposal URN to be returned"
    )
    wait_for_writes_to_sync()

    # Step 11: Verify second proposal was created
    proposals_data = list_proposals(auth_session, TEST_DATASET_URN, status="PENDING")
    assert proposals_data["total"] >= 1, "Expected at least one pending proposal"

    found_second_proposal = None
    for proposal in proposals_data["actionRequests"]:
        if proposal["urn"] == second_proposal_urn:
            found_second_proposal = proposal
            break

    assert found_second_proposal is not None, "Second proposal not found in list"
    assert found_second_proposal["description"] == proposal_description2
    assert found_second_proposal["status"] == "PENDING"

    # Step 12: Verify Slack notification for second proposal
    logger.info("Verifying Slack notification was sent for second proposal")

    check_slack_notification(
        slack_token,
        channel_id,
        "New Proposal Raised",
        timestamp_seconds2,
    )
    logger.info("Second proposal Slack notification verification passed")

    # Step 13: Reject the second proposal
    logger.info(f"Rejecting proposal: {second_proposal_urn}")
    reject_result = reject_proposal(auth_session, second_proposal_urn, "Test rejection")
    assert reject_result is True, "Expected rejection to succeed"
    wait_for_writes_to_sync()
    logger.info(f"Proposal rejected: {second_proposal_urn}")

    # Step 14: Verify proposal status is now COMPLETED/REJECTED
    proposals_data = list_proposals(auth_session, TEST_DATASET_URN, status="COMPLETED")

    rejected_proposal = None
    for proposal in proposals_data["actionRequests"]:
        if proposal["urn"] == second_proposal_urn:
            rejected_proposal = proposal
            break

    assert rejected_proposal is not None, "Rejected proposal not found in list"
    assert rejected_proposal["status"] == "COMPLETED"
    assert rejected_proposal["result"] == "REJECTED"

    # Step 15: Verify Slack message was updated with rejection status
    logger.info("Verifying Slack message was updated with rejection status")
    check_slack_notification(
        slack_token,
        channel_id,
        "Proposal Status Changed",
        timestamp_seconds2,
        expected_text="has rejected the proposal",
    )
    logger.info("Slack rejection notification verification passed")

    # Step 16: Final cleanup - ensure tag is not on dataset
    logger.info("Final cleanup - ensuring tag is not on dataset")
    try:
        remove_tag(auth_session, TEST_DATASET_URN, TEST_TAG_URN)
        logger.info("Tag removed in final cleanup")
    except Exception:
        # Tag already not present, which is expected after rejection
        logger.info("Tag not present (expected after rejection)")

    logger.info("Test complete")
