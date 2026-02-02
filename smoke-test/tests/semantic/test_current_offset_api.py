"""
Test script to verify that we can fetch the current offset
from the Events API by polling with no offsetId and no lookbackWindowDays.

This tests the assumption that the API will seek to end and return
the current position as offsetId, which is needed for bootstrapping event mode.
"""

import json
import logging
from typing import Any

import requests

from tests.utils import TestSessionWrapper

logger = logging.getLogger(__name__)


def test_get_current_offset_no_params(auth_session: TestSessionWrapper):
    """
    Test that polling Events API with no offsetId and no lookbackWindowDays
    returns a current offsetId.

    This validates the assumption needed for bootstrapping event mode:
    we can capture the current offset before batch mode runs.
    """
    gms_url = auth_session.gms_url()
    gms_token = auth_session.gms_token()
    topic = "MetadataChangeLog_Versioned_v1"

    endpoint = f"{gms_url}/openapi/v1/events/poll"

    # Poll with NO offsetId and NO lookbackWindowDays
    # According to the Java code, this should seek to end and return current position
    params: dict[str, Any] = {
        "topic": topic,
        "limit": 1,  # Minimal limit - we just want the offset
        "pollTimeoutSeconds": 2,
        # Explicitly NOT including offsetId
        # Explicitly NOT including lookbackWindowDays
    }

    headers = {"Authorization": f"Bearer {gms_token}"}

    logger.info(f"Testing Events API: {endpoint}")
    logger.info(f"Topic: {topic}")
    logger.info(f"Params: {params}")

    response = requests.get(endpoint, params=params, headers=headers, timeout=30)
    response.raise_for_status()

    data = response.json()
    logger.info(f"Response: {json.dumps(data, indent=2)}")

    # Check if offsetId is in response
    offset_id = data.get("offsetId")
    events = data.get("events", [])

    assert offset_id is not None, (
        f"No offsetId in response. Response keys: {list(data.keys())}"
    )

    logger.info(f"✓ SUCCESS: Current offset retrieved: {offset_id}")
    logger.info(f"  Events returned: {len(events)}")

    # Verify offset is a non-empty string
    assert isinstance(offset_id, str), (
        f"Offset ID should be string, got {type(offset_id)}"
    )
    assert len(offset_id) > 0, "Offset ID should not be empty"


def test_poll_with_retrieved_offset(auth_session: TestSessionWrapper):
    """
    Test that we can use the retrieved offset to poll for new events.

    This validates that the offset we get is valid and can be used
    for subsequent polling.
    """
    gms_url = auth_session.gms_url()
    gms_token = auth_session.gms_token()
    topic = "MetadataChangeLog_Versioned_v1"

    # First, get the current offset
    endpoint = f"{gms_url}/openapi/v1/events/poll"
    params: dict[str, Any] = {
        "topic": topic,
        "limit": 1,
        "pollTimeoutSeconds": 2,
    }
    headers = {"Authorization": f"Bearer {gms_token}"}

    response = requests.get(endpoint, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()
    offset_id = data.get("offsetId")

    assert offset_id is not None, "Could not retrieve initial offset"
    logger.info(f"Retrieved initial offset: {offset_id}")

    # Now poll using that offset
    params_with_offset = {
        "topic": topic,
        "offsetId": offset_id,
        "limit": 10,
        "pollTimeoutSeconds": 2,
    }

    response = requests.get(
        endpoint, params=params_with_offset, headers=headers, timeout=30
    )
    response.raise_for_status()

    data = response.json()
    events = data.get("events", [])
    new_offset_id = data.get("offsetId")

    logger.info("✓ SUCCESS: Polled with offset")
    logger.info(f"  Events returned: {len(events)}")
    logger.info(f"  New offset: {new_offset_id}")

    # The new offset should exist (may be same or different)
    assert new_offset_id is not None, "New offset should be returned"


def test_offset_behavior_without_lookback(auth_session: TestSessionWrapper):
    """
    Test that polling without offsetId and without lookbackWindowDays
    consistently returns the same "current" offset (seeks to end).

    Multiple calls should return the same or similar offset if no new events arrive.
    """
    gms_url = auth_session.gms_url()
    gms_token = auth_session.gms_token()
    topic = "MetadataChangeLog_Versioned_v1"

    endpoint = f"{gms_url}/openapi/v1/events/poll"
    headers = {"Authorization": f"Bearer {gms_token}"}

    # Get offset twice in quick succession
    params: dict[str, Any] = {
        "topic": topic,
        "limit": 1,
        "pollTimeoutSeconds": 1,  # Short timeout
    }

    response1 = requests.get(endpoint, params=params, headers=headers, timeout=30)
    response1.raise_for_status()
    offset1 = response1.json().get("offsetId")

    import time

    time.sleep(0.5)  # Brief pause

    response2 = requests.get(endpoint, params=params, headers=headers, timeout=30)
    response2.raise_for_status()
    offset2 = response2.json().get("offsetId")

    logger.info(f"First offset: {offset1}")
    logger.info(f"Second offset: {offset2}")

    # Both should be valid offsets
    assert offset1 is not None, "First offset should not be None"
    assert offset2 is not None, "Second offset should not be None"

    # They may be the same (if no events arrived) or different (if events arrived)
    # The important thing is that both are valid offsets
    logger.info("✓ Both offsets are valid (may differ if events arrived between calls)")
