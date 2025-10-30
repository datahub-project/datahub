import logging

import pytest

from tests.test_result_msg import set_default_channel_name
from tests.utilities.metadata_operations import get_global_settings

logger = logging.getLogger(__name__)


def _get_default_channel_name(auth_session) -> str | None:
    """Extract defaultChannelName from global Slack settings."""
    global_settings = get_global_settings(auth_session)
    if global_settings is None:
        return None

    integration_settings = global_settings.get("integrationSettings")
    if integration_settings is None:
        return None

    slack_settings = integration_settings.get("slackSettings")
    if slack_settings is None:
        return None

    return slack_settings.get("defaultChannelName")


@pytest.mark.read_only
def test_getting_slack_settings_no_exception(auth_session):
    """Test that we can retrieve global settings without exception."""
    default_channel = _get_default_channel_name(auth_session)
    logger.info(f"Default Slack channel: {default_channel}")

    if default_channel is not None:
        set_default_channel_name(default_channel)

    # This test verifies the settings can be retrieved without errors
    # The channel may or may not be configured, both are valid
    # We are testing that no exception is raised
