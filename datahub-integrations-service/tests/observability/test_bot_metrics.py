"""Unit tests for bot observability metrics."""

from datahub_integrations.observability.bot_metrics import (
    BotCommand,
    BotPlatform,
    OAuthFlow,
    datahub_query_tracker,
    record_datahub_query,
    record_oauth_flow,
    record_slack_api_call,
    record_teams_api_call,
)


def test_bot_platform_enum():
    """Test BotPlatform enum values."""
    assert BotPlatform.SLACK.value == "slack"
    assert BotPlatform.TEAMS.value == "teams"


def test_bot_command_enum():
    """Test BotCommand enum values."""
    assert BotCommand.SEARCH.value == "search"
    assert BotCommand.GET.value == "get"
    assert BotCommand.ASK.value == "ask"
    assert BotCommand.HELP.value == "help"
    assert BotCommand.MENTION.value == "mention"


def test_oauth_flow_enum():
    """Test OAuthFlow enum values."""
    assert OAuthFlow.INSTALL.value == "install"
    assert OAuthFlow.CALLBACK.value == "callback"
    assert OAuthFlow.REFRESH.value == "refresh"


def test_record_datahub_query():
    """Test recording DataHub query metrics."""
    # Should execute without error
    record_datahub_query(
        platform=BotPlatform.SLACK,
        query_type="search",
        duration_seconds=0.5,
        success=True,
    )


def test_record_slack_api_call():
    """Test recording Slack API call metrics."""
    # Should execute without error
    record_slack_api_call(
        api_method="chat.postMessage",
        duration_seconds=0.2,
        success=True,
    )


def test_record_teams_api_call():
    """Test recording Teams API call metrics."""
    # Should execute without error
    record_teams_api_call(
        api_method="bot.send",
        duration_seconds=0.3,
        success=True,
    )


def test_record_oauth_flow():
    """Test recording OAuth flow metrics."""
    # Should execute without error
    record_oauth_flow(
        platform=BotPlatform.TEAMS,
        flow=OAuthFlow.CALLBACK,
        duration_seconds=1.5,
        success=True,
    )


def test_datahub_query_tracker_success():
    """Test DataHub query tracker context manager for successful query."""
    # Should execute without error
    with datahub_query_tracker("entity", BotPlatform.SLACK):
        pass  # Simulate successful query


def test_datahub_query_tracker_with_error():
    """Test DataHub query tracker context manager handles errors."""
    # Should record error metric but not suppress exception
    try:
        with datahub_query_tracker("search", BotPlatform.TEAMS):
            raise ValueError("Query failed")
    except ValueError:
        pass  # Expected - tracker should not suppress exception
