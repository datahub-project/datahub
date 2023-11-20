import json
from datetime import datetime, timezone

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
USE_SOCKET_MODE = DATAHUB_FRONTEND_URL.startswith("http://localhost:9002")
if USE_SOCKET_MODE:
    logger.info(
        "Slack socket mode is enabled. To receive events from Slack, you must also run `python scripts/slack_socket_mode.py`"
    )

slack_bot_scopes = [
    # Required for slash commands / shortcuts.
    "commands",
    # Required to get @Acryl messages and send messages as @Acryl.
    "app_mentions:read",
    "chat:write",
    "chat:write.public",
    # When sending messages, we want to use a custom icon_url so that we can display the Acryl logo.
    "chat:write.customize",
    # Required to see conversation details + read messages.
    "channels:history",
    "channels:read",
    "groups:history",
    "groups:read",
    "im:history",
    "im:read",
    "mpim:history",
    "mpim:read",
    "metadata.message:read",
    # TODO: Should we add channels:join?
    # If we add that, then we can set the app to automatically join all public channels.
    # However, that would send a message to the channel when the app joins, which is not ideal.
    # Required to unfurl links.
    "links:read",
    "links:write",
    # Required to resolve user IDs to names/emails + enable lookup by email address.
    "users:read",
    "users:read.email",
    # Future-proofing.
    "reactions:read",
    "reactions:write",
]


def get_slack_app_manifest() -> str:
    manifest = {
        "_metadata": {
            "major_version": 1,
            "minor_version": 1,
        },
        "display_information": {
            "name": "Acryl Data",
            "background_color": "#142f39",
            # The short tagline shows up in the app hover cards.
            "description": "A modern approach to data discovery and metadata management",
            # The long description appears on the app install page and the about app details page in Slack.
            "long_description": "The Acryl Data integration for Slack allows you to receive real-time notifications about changes to your data, unfurl links in both Slack and Acryl, and to search across your data from within Slack.",
        },
        "features": {
            "bot_user": {"display_name": "Acryl", "always_online": True},
            "shortcuts": [
                {
                    "name": "Add as documentation",
                    "type": "message",
                    "callback_id": "attach_to_asset",
                    "description": "Add a given message/thread as documentation for an asset in Acryl",
                }
            ],
            "slash_commands": [
                {
                    "command": "/acryl",
                    "url": f"{DATAHUB_FRONTEND_URL}/integrations/slack/commands",
                    "description": "Search across your Acryl instance",
                    "usage_hint": "keywords",
                    "should_escape": False,
                }
            ],
            "unfurl_domains": [
                "acryl.io",
            ],
        },
        "oauth_config": {
            "redirect_urls": list(
                set(
                    [
                        f"{DATAHUB_FRONTEND_URL}/integrations/slack/oauth_callback",
                        # For testing only: allows using a local frontend server or integrations service.
                        "http://localhost:9002/integrations/slack/oauth_callback",
                        "http://localhost:9003/public/slack/oauth_callback",
                    ]
                )
            ),
            "scopes": {
                "bot": slack_bot_scopes,
            },
        },
        "settings": {
            "event_subscriptions": {
                "request_url": f"{DATAHUB_FRONTEND_URL}/integrations/slack/events",
                "bot_events": [
                    "app_mention",
                    "link_shared",
                    "message.channels",
                    "message.groups",
                    "message.im",
                    "message.mpim",
                ],
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


def upsert_app_with_manifest(
    slack_config: SlackConnection, manifest: str
) -> SlackConnection:
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
    slack_client = slack_sdk.web.WebClient(
        proxy=SLACK_PROXY, token=slack_config.app_config_tokens.access_token
    )

    if not slack_config.app_details:
        # We need to create a new app.
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
    else:
        # We need to update an existing app.
        logger.info(f"Updating manifest for app {slack_config.app_details.app_id}")

        res = slack_client.api_call(
            "apps.manifest.update",
            params={
                "app_id": slack_config.app_details.app_id,
                "manifest": manifest,
            },
        ).validate()

        # TODO: Add a "requires reinstall" flag?

    return slack_config


if __name__ == "__main__":
    config = slack_config.get_config()
    logger.debug(f"Config: {config.json(indent=2)}")

    manifest = get_slack_app_manifest()
    config2 = upsert_app_with_manifest(config, manifest)
    # breakpoint()
    slack_config.save_config(config2)
