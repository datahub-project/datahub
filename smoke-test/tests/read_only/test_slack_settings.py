import logging

import pytest

from tests.utilities.metadata_operations import get_default_channel_name

logger = logging.getLogger(__name__)


@pytest.mark.read_only
def test_getting_slack_settings_no_exception(auth_session):
    """Test that we can retrieve global settings without exception."""
    default_channel = get_default_channel_name(auth_session)
    logger.info(f"Default Slack channel: {default_channel}")

    # This test verifies the settings can be retrieved without errors
    # The channel may or may not be configured, both are valid
    # We are testing that no exception is raised
