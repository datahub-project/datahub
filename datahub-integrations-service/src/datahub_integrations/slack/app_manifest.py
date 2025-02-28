import json
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import slack_sdk
from loguru import logger

from datahub_integrations.app import DATAHUB_FRONTEND_URL
from datahub_integrations.slack.config import (
    SLACK_PROXY,
    SlackAppConfigCredentials,
    SlackAppDetails,
    SlackConnection,
    slack_config,
)

# When in local development, routes to localhost won't work. Instead, we will enable Slack's
# socket mode so that our application can receive events from Slack via a websocket.
USE_SOCKET_MODE = DATAHUB_FRONTEND_URL.startswith("http://localhost:")
if USE_SOCKET_MODE:
    logger.info(
        "Slack socket mode is enabled. To receive events from Slack, you must also run `python scripts/slack_socket_mode.py`"
    )


minimal_slack_bot_scopes = [
    # Required for slash commands / shortcuts.
    "commands",
    # Required to get @DataHub messages and send messages as @DataHub.
    "app_mentions:read",
    "chat:write",
    "chat:write.public",
    # When sending messages, we want to use a custom icon_url so that we can display the DataHub Cloud logo.
    "chat:write.customize",
    # Required to get workspace ID and create links to user profiles
    "team:read",
    # Allows the bot to join a public channel when someone configures notifications to be sent to one
    "channels:join",
    # Required to unfurl links.
    "links:read",
    "links:write",
    # Required to resolve user IDs to names/emails + enable lookup by email address.
    "users:read",
    "users:read.email",
]
slack_bot_scopes = minimal_slack_bot_scopes + [
    # Required to see conversation details + read messages.
    "channels:history",
    "channels:read",
    "groups:history",
    "groups:read",
    "im:history",
    "im:read",
    "mpim:read",
    "mpim:history",
    "metadata.message:read",
    # Future-proofing.
    "reactions:read",
    "reactions:write",
]


def get_slack_app_manifest(is_minimal_permissions: Optional[bool]) -> str:
    manifest = {
        "_metadata": {
            "major_version": 1,
            "minor_version": 1,
        },
        "display_information": {
            "name": "DataHub Cloud",
            "background_color": "#142f39",
            # The short tagline shows up in the app hover cards.
            "description": "A modern approach to data discovery and metadata management",
            # The long description appears on the app install page and the about app details page in Slack.
            "long_description": (
                "The DataHub Cloud integration for Slack allows you to receive real-time "
                "notifications about changes to your data, unfurl links in both Slack "
                "and DataHub, and to search across your data from within Slack."
            ),
        },
        "features": {
            "bot_user": {"display_name": "DataHub", "always_online": True},
            "shortcuts": [
                {
                    "name": "Add as documentation",
                    "type": "message",
                    "callback_id": "attach_to_asset",
                    "description": "Add a given message/thread as documentation for an asset in DataHub",
                }
            ],
            "slash_commands": [
                {
                    "command": "/acryl" if not USE_SOCKET_MODE else "/acryl-dev",
                    "url": f"{DATAHUB_FRONTEND_URL}/integrations/slack/commands",
                    "description": "Search across your DataHub instance",
                    "usage_hint": "search [query] | get [entity-urn]",
                    "should_escape": False,
                },
                {
                    "command": "/datahub" if not USE_SOCKET_MODE else "/datahub-dev",
                    "url": f"{DATAHUB_FRONTEND_URL}/integrations/slack/commands",
                    "description": "Search across your DataHub instance",
                    "usage_hint": "search [query] | get [entity-urn]",
                    "should_escape": False,
                },
            ],
            "unfurl_domains": [
                (
                    urlparse(DATAHUB_FRONTEND_URL).hostname
                    if not USE_SOCKET_MODE
                    else f"slacktest.{urlparse(DATAHUB_FRONTEND_URL).hostname}"
                )
            ],
        },
        "oauth_config": {
            "redirect_urls": list(
                {f"{DATAHUB_FRONTEND_URL}/integrations/slack/oauth_callback"}
            ),
            "scopes": {
                "bot": (
                    slack_bot_scopes
                    if not is_minimal_permissions
                    else minimal_slack_bot_scopes
                ),
            },
        },
        "settings": {
            "event_subscriptions": {
                "request_url": f"{DATAHUB_FRONTEND_URL}/integrations/slack/events",
                "bot_events": (
                    [
                        "app_mention",
                        "link_shared",
                    ]
                    + (
                        [
                            "message.channels",
                            "message.groups",
                            "message.im",
                            "message.mpim",
                        ]
                        if not is_minimal_permissions
                        else []
                    )
                ),
            },
            "interactivity": {
                "is_enabled": True,
                "request_url": f"{DATAHUB_FRONTEND_URL}/integrations/slack/actions",
            },
            "org_deploy_enabled": False,
            "socket_mode_enabled": USE_SOCKET_MODE,
            "token_rotation_enabled": False,
        },
    }
    return json.dumps(manifest)


def get_slack_client(slack_config: SlackConnection) -> slack_sdk.web.WebClient:
    slack_config = slack_config.copy(deep=True)
    assert slack_config.app_config_tokens, "App config tokens must be set"

    # Refresh the token if required.
    if slack_config.app_config_tokens.is_expired():
        logger.info("Refreshing slack token")
        slack_client = slack_sdk.web.WebClient(proxy=SLACK_PROXY)  # no token required

        res = slack_client.api_call(
            "tooling.tokens.rotate",
            params={
                "refresh_token": slack_config.app_config_tokens.refresh_token,
            },
        ).validate()

        slack_config = slack_config.copy(
            update=dict(
                app_config_tokens=SlackAppConfigCredentials(
                    access_token=res["token"],
                    refresh_token=res["refresh_token"],
                    exp=datetime.fromtimestamp(res["exp"], tz=timezone.utc),
                )
            )
        )

    assert slack_config.app_config_tokens
    return slack_sdk.web.WebClient(
        proxy=SLACK_PROXY, token=slack_config.app_config_tokens.access_token
    )


def create_app_with_manifest(
    slack_config: SlackConnection,
    manifest: str,
) -> SlackConnection:
    slack_client = get_slack_client(slack_config)
    # Create a new app.
    logger.info("Creating a new slack app")

    # These API were merged into a feature branch, but never made it to master.
    # See https://github.com/slackapi/python-slack-sdk/issues/1119 and
    # https://github.com/slackapi/python-slack-sdk/pull/1123 and
    # https://github.com/slackapi/python-slack-sdk/blob/bdb555170e29ca5395623adf0e0b31210abce492/tests/slack_sdk_async/web/test_web_client_coverage.py#L44.
    res = slack_client.api_call(
        "apps.manifest.create",
        params={
            "manifest": manifest,
        },
    ).validate()

    slack_config = slack_config.copy(
        update=dict(
            app_details=SlackAppDetails(
                app_id=res["app_id"],
                client_id=res["credentials"]["client_id"],
                client_secret=res["credentials"]["client_secret"],
                signing_secret=res["credentials"]["signing_secret"],
                verification_token=res["credentials"]["verification_token"],
            )
        )
    )

    return slack_config


def update_app_with_manifest(
    slack_config: SlackConnection,
    manifest: str,
) -> SlackConnection:
    slack_client = get_slack_client(slack_config)

    # Update an existing app.
    if slack_config.app_details is None or slack_config.app_details.app_id is None:
        raise Exception("Slack config missing app_details.app_id")

    logger.info(f"Updating manifest for app {slack_config.app_details.app_id}")

    slack_client.api_call(
        "apps.manifest.update",
        params={
            "app_id": slack_config.app_details.app_id,
            "manifest": manifest,
        },
    ).validate()

    return slack_config


if __name__ == "__main__":
    config = slack_config.get_config()
    logger.debug(f"Config: {config.json(indent=2)}")

    manifest = get_slack_app_manifest(is_minimal_permissions=None)
    # only called during testing
    config2 = update_app_with_manifest(config, manifest)
    # breakpoint()
    slack_config.save_config(config2)
