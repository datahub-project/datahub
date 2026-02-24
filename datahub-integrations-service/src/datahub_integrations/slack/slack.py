import base64
import dataclasses
import functools
import json
import re
import secrets
import time
from typing import Annotated, Any, Dict, Optional, Tuple, Union
from urllib.parse import quote, urlencode

import fastapi
import slack_bolt
import slack_sdk.errors
import slack_sdk.web
from datahub.configuration.common import GraphError
from datahub.utilities.time import datetime_to_ts_millis
from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from loguru import logger
from pydantic import BaseModel
from slack_bolt import Ack, Respond, Say
from slack_bolt.adapter.fastapi import SlackRequestHandler
from slack_sdk.oauth import AuthorizeUrlGenerator

from datahub_integrations.app import DATAHUB_FRONTEND_URL, graph
from datahub_integrations.graphql.incidents import (
    UPDATE_INCIDENT_PRIORITY_MUTATION,
    UPDATE_INCIDENT_STATUS_MUTATION,
)
from datahub_integrations.graphql.slack import SLACK_GET_ENTITY_QUERY
from datahub_integrations.graphql.subscription import (
    CREATE_SUBSCRIPTION,
    DELETE_SUBSCRIPTION,
)
from datahub_integrations.notifications.notification_tracking import (
    NotificationChannel,
    NotificationType,
)
from datahub_integrations.oauth import (
    get_authenticated_user,
    get_state_store,
)
from datahub_integrations.observability import BotPlatform, OAuthFlow, otel_duration
from datahub_integrations.slack.app_manifest import (
    create_app_with_manifest,
    get_slack_app_manifest,
    slack_bot_scopes,
    update_app_with_manifest,
)
from datahub_integrations.slack.command.mention import (
    DATAHUB_MENTION_FEEDBACK_BUTTON_ID,
    DATAHUB_MENTION_FOLLOWUP_QUESTION_BUTTON_ID,
    FeedbackPayload,
    FollowupQuestionPayload,
    SlackMentionEvent,
    handle_app_mention,
    handle_feedback,
    handle_followup_question,
)
from datahub_integrations.slack.command.router import handle_command
from datahub_integrations.slack.command.search import search
from datahub_integrations.slack.config import (
    SLACK_PROXY,
    SlackConnection,
    SlackGlobalSettings,
    slack_config,
)
from datahub_integrations.slack.constants import DATAHUB_SLACK_ICON_URL
from datahub_integrations.slack.context import (
    IncidentContext,
    IncidentSelectOption,
    SearchContext,
)
from datahub_integrations.slack.feature_flags import get_require_slack_oauth_binding
from datahub_integrations.slack.oauth_state_store import InMemoryStateStore
from datahub_integrations.slack.render.constants import ACRYL_COLOR
from datahub_integrations.slack.render.render_entity import (
    render_entity_modal,
    render_entity_preview,
)
from datahub_integrations.slack.render.render_resolve_incident import (
    render_resolve_incident,
)
from datahub_integrations.slack.render.render_subscription import (
    EntityChangeTypeGroup,
    render_subscription_modal,
)
from datahub_integrations.slack.utils.datahub_user import (
    build_authorization_error_blocks,
    build_connect_account_blocks,
    get_datahub_user,
    get_user_information,
    graph_as_system,
    graph_as_user,
    resolve_slack_user_to_datahub,
)
from datahub_integrations.slack.utils.entity_extract import (
    ExtractedEntity,
    get_type_url,
)
from datahub_integrations.slack.utils.time import slack_ts_to_datetime
from datahub_integrations.slack.utils.urls import extract_urn_from_url
from datahub_integrations.telemetry.notification_events import (
    NotificationSlackAction,
    NotificationSlackActionEvent,
)
from datahub_integrations.telemetry.telemetry import track_saas_event

# 7 days because admins may take some time to approve
_state_store = InMemoryStateStore(expiration_seconds=60 * 60 * 24 * 7)


# Slack user OAuth flows use the shared InMemoryOAuthStateStore (same as MCP plugins).
# This ensures all user-facing OAuth flows share a single state management pattern.
SLACK_USER_OAUTH_FLOW_ID = "slack-user-oauth"

private_router = fastapi.APIRouter()
public_router = fastapi.APIRouter()


def _track_notification_slack_action(
    *,
    user_urn: str | None,
    notification_id: str,
    notification_type: NotificationType,
    notification_channel: NotificationChannel = NotificationChannel.SLACK,
    action: NotificationSlackAction,
    success: bool,
) -> None:
    track_saas_event(
        NotificationSlackActionEvent(
            user_urn=user_urn,
            action=action,
            success=success,
            notificationType=notification_type,
            notificationChannel=notification_channel,
            notificationId=notification_id,
        )
    )


def get_oauth_url_generator(config: SlackConnection) -> AuthorizeUrlGenerator:
    assert config.app_details
    assert config.app_details.client_id

    return AuthorizeUrlGenerator(
        client_id=config.app_details.client_id,
        scopes=slack_bot_scopes,
        redirect_uri=f"{DATAHUB_FRONTEND_URL}/integrations/slack/oauth_callback",
    )


class SlackConnectResponse(BaseModel):
    """Response from the Slack connect endpoint."""

    authorization_url: str


@public_router.post("/slack/connect", response_model=SlackConnectResponse)
async def slack_connect(
    user_urn: Annotated[str, Depends(get_authenticated_user)],
) -> SlackConnectResponse:
    """
    Initiate a Slack user account-linking (OIDC) flow.

    Follows the same pattern as the MCP plugin OAuth connect endpoint:
    1. Authenticates the user via JWT / PLAY_SESSION cookie
    2. Stores state in the shared InMemoryOAuthStateStore (single-use, TTL)
    3. Returns the full authorization URL for the frontend to redirect to

    The callback (/slack/oauth_callback) consumes the nonce to retrieve
    the trusted user_urn — preventing tampering and replay attacks.
    """
    logger.info(f"Initiating Slack OAuth connect for user: {user_urn}")

    config = slack_config.get_connection()
    if not config.app_details or not config.app_details.client_id:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Slack app credentials not configured",
        )

    redirect_uri = f"{DATAHUB_FRONTEND_URL}/integrations/slack/oauth_callback"
    client_id = config.app_details.client_id

    # Build OIDC authorization URL WITHOUT the state parameter.
    # The shared state store's create_state() will append &state=<nonce>.
    oidc_nonce = secrets.token_urlsafe(12)
    base_auth_url = "https://slack.com/openid/connect/authorize"
    params = urlencode(
        {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "nonce": oidc_nonce,
            "scope": "openid email profile",
        }
    )
    authorization_url_without_state = f"{base_auth_url}?{params}"

    # Store state in the shared OAuth state store (single-use, TTL-limited).
    # We repurpose redirect_uri for the frontend redirect path, since the
    # actual OAuth redirect_uri is already baked into the URL above.
    state_store = get_state_store()
    result = state_store.create_state(
        user_urn=user_urn,
        plugin_id=SLACK_USER_OAUTH_FLOW_ID,
        redirect_uri="/settings/personal-notifications",
        authorization_url=authorization_url_without_state,
        code_verifier="",  # Slack OIDC doesn't use PKCE
    )

    return SlackConnectResponse(authorization_url=result.authorization_url)


@private_router.post("/slack/reload_credentials")
def reload_slack_credentials() -> None:
    """Reload Slack credentials from GMS and refreshes existing services appropriately."""

    slack_config.reload()


@public_router.get("/slack/install")
@otel_duration(
    metric_name="bot_oauth_duration",
    labels={"platform": BotPlatform.SLACK.value, "flow": OAuthFlow.INSTALL.value},
)
def install_slack_app(request: Request) -> RedirectResponse:
    config = slack_config.reload()

    # Get all query parameters as a dict
    query_params = (
        dict(request.query_params) if request.query_params is not None else None
    )
    is_minimal_slack_permissions = (
        (query_params.get("requestMinimalSlackPermissions", "false") == "true")
        if query_params is not None
        else False
    )

    # Create the Slack app manifest before attempting to install.
    manifest = get_slack_app_manifest(
        is_minimal_permissions=is_minimal_slack_permissions
    )
    try:
        config = create_app_with_manifest(config, manifest)
    except slack_sdk.errors.SlackApiError as e:
        error_message = e.response["error"]
        logger.exception(f"Error during Slack app creation: {error_message}")
        raise fastapi.HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to create Slack app: {error_message}",
        ) from e
    slack_config.save_config(config)
    assert config.app_details, "App details should be present after provisioning."

    # Generate the OAuth URL and redirect to it.
    authorize_url_generator = get_oauth_url_generator(config)
    state = _state_store.issue()

    url = authorize_url_generator.generate(state=state)
    logger.debug(f"Redirecting to {url}")

    return RedirectResponse(url=url)


@public_router.get("/slack/refresh-installation")
@otel_duration(
    metric_name="bot_oauth_duration",
    labels={"platform": BotPlatform.SLACK.value, "flow": OAuthFlow.REFRESH.value},
)
def refresh_slack_app(request: Request) -> RedirectResponse:
    config = slack_config.reload()

    # Get all query parameters as a dict
    query_params = (
        dict(request.query_params) if request.query_params is not None else None
    )
    is_minimal_slack_permissions = (
        (query_params.get("requestMinimalSlackPermissions", "false") == "true")
        if query_params is not None
        else False
    )

    # Update the Slack app manifest before attempting to install.
    manifest = get_slack_app_manifest(
        is_minimal_permissions=is_minimal_slack_permissions
    )
    try:
        config = update_app_with_manifest(config, manifest)
    except slack_sdk.errors.SlackApiError as e:
        error_message = e.response["error"]
        logger.exception(
            f"Error during refreshing existing Slack app tokens: {error_message}"
        )
        raise fastapi.HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to refresh existing Slack app tokens: {error_message}. Instead, try providing a fresh set of tokens and clicking the 'create a new installation' link in the footer.",
        ) from e
    slack_config.save_config(config)
    assert config.app_details, "App details should be present after provisioning."

    # Generate the OAuth URL and redirect to it.
    authorize_url_generator = get_oauth_url_generator(config)
    state = _state_store.issue()

    url = authorize_url_generator.generate(state=state)
    logger.debug(f"Redirecting to {url}")

    return RedirectResponse(url=url)


def _parse_oauth_state(state: str) -> Dict[str, Any]:
    """
    Parse the OAuth state parameter to determine flow type.

    State formats:
    1. **Nonce** in the shared ``InMemoryOAuthStateStore`` with
       ``plugin_id == SLACK_USER_OAUTH_FLOW_ID`` — user account-linking
       flow.  The nonce is consumed atomically (single-use) and the stored
       ``OAuthState`` provides the trusted ``user_urn``.
    2. **Opaque string** managed by ``_state_store`` — admin install flow.
    """
    state_store = get_state_store()

    oauth_state = state_store.get_and_consume_state(state)
    if oauth_state is not None and oauth_state.plugin_id == SLACK_USER_OAUTH_FLOW_ID:
        return {
            "flow": "user",
            "user_urn": oauth_state.user_urn,
            "redirect_path": oauth_state.redirect_uri,
        }

    # ── Opaque string → install flow ──
    return {"flow": "install", "raw_state": state}


@public_router.get("/slack/oauth_callback", response_model=None)
@otel_duration(
    metric_name="bot_oauth_duration",
    labels={"platform": BotPlatform.SLACK.value, "flow": OAuthFlow.CALLBACK.value},
)
def oauth_callback(
    state: str,
    code: Optional[str] = None,
    error: Optional[str] = None,
    error_description: Optional[str] = None,
) -> Union[RedirectResponse, Dict[str, Any]]:
    """
    Unified OAuth callback for all Slack OAuth flows.

    Routes to appropriate handler based on 'flow' in state parameter:
    - 'install': Admin app installation (OAuth 2.0) - saves bot token
    - 'user': User account linking (OIDC) - links Slack to DataHub user

    Backward compatible: if state doesn't specify flow, defaults to 'install'.
    """
    # Parse state to determine flow type
    state_data = _parse_oauth_state(state)
    flow = state_data.get("flow", "install")

    logger.info(f"Slack OAuth callback received: flow={flow}")

    # Route to appropriate handler based on flow
    if flow == "user":
        # User account linking flow (OIDC)
        return _handle_user_oauth_flow(
            code=code,
            state=state,
            state_data=state_data,
            error=error,
            error_description=error_description,
        )
    else:
        # Install flow (OAuth 2.0) - default for backward compatibility
        return _handle_install_flow(
            code=code,
            state=state,
            error=error,
            error_description=error_description,
        )


def _handle_install_flow(
    code: Optional[str],
    state: str,
    error: Optional[str],
    error_description: Optional[str],
) -> RedirectResponse:
    """Handle admin app installation flow (OAuth 2.0)."""
    config = slack_config.get_connection()
    assert config.app_details, "App details should be present after provisioning."
    assert config.app_details.client_id, (
        "Client id should be present after provisioning"
    )
    assert config.app_details.client_secret, (
        "Client secret should be present after provisioning"
    )

    if not _state_store.consume(state):
        raise fastapi.HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not find the installation state. If you were waiting for an Admin approval, our system may have timed out. In this case, we recommend you try a Manual Installation Refresh, as outlined on the bottom of the troubleshooting page: https://docs.datahub.com/docs/managed-datahub/slack/saas-slack-troubleshoot",
        )

    if error:
        raise fastapi.HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error: {error}. Description: {error_description}",
        )
    assert code

    # Logic based on https://slack.dev/python-slack-sdk/oauth/.
    slack_client = slack_sdk.web.WebClient(proxy=SLACK_PROXY)  # no token required

    authorize_url_generator = get_oauth_url_generator(config)
    try:
        oauth_response = slack_client.oauth_v2_access(
            client_id=config.app_details.client_id,
            client_secret=config.app_details.client_secret,
            redirect_uri=authorize_url_generator.redirect_uri,
            code=code,
        ).validate()
    except slack_sdk.errors.SlackApiError as e:
        error_message = e.response["error"]
        logger.exception(f"Error during Slack OAuth access: {error_message}")
        raise fastapi.HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to complete Slack OAuth: {error_message}",
        ) from e

    authed_user = oauth_response["authed_user"]
    logger.info(
        f"Completed app install for team {oauth_response.get('team')}, approved by {authed_user}"
    )

    # Save the new bot token.
    bot_token = oauth_response["access_token"]
    new_config = config.model_copy(
        update=dict(
            bot_token=bot_token,
        )
    )
    slack_config.save_config(new_config)

    # Send a welcome message to the user who just installed us.
    # TODO: Add more context to this message + a link back to the integrations page.
    app = get_slack_app(new_config, slack_config.get_global_settings())
    app.client.chat_postMessage(
        channel=authed_user["id"],
        text="DataHub has been connected to Slack!",
        icon_url=DATAHUB_SLACK_ICON_URL,
    )

    return RedirectResponse(url="/settings/integrations/slack")


def _handle_user_oauth_flow(
    code: Optional[str],
    state: str,
    state_data: Dict[str, Any],
    error: Optional[str],
    error_description: Optional[str],
) -> Union[RedirectResponse, Dict[str, Any]]:
    """Handle user account linking flow (OIDC).

    ``state_data`` has already been validated by ``_parse_oauth_state``
    (single-use nonce consumed atomically from the state store).
    ``user_urn`` is extracted from the server-side state and is trustworthy.
    """
    redirect_path = state_data.get("redirect_path", "/settings/personal-notifications")

    # Validate redirect_path is a safe relative path (prevent open redirects).
    # Must start with "/" and not contain protocol markers, encoded slashes,
    # backslashes, or path-traversal sequences.
    if not _is_safe_redirect_path(redirect_path):
        logger.error(f"Rejected unsafe redirect_path: {redirect_path}")
        redirect_path = "/settings/personal-notifications"

    if error:
        logger.error(f"OAuth error: {error} - {error_description}")
        return _build_oauth_error_redirect(redirect_path, error)

    if not code:
        logger.error("No authorization code in OAuth callback")
        return _build_oauth_error_redirect(redirect_path, "No authorization code")

    user_urn = state_data.get("user_urn")

    return handle_personal_notifications_oauth(
        code=code,
        user_urn=user_urn,
        redirect_url=redirect_path,
        state=state,
    )


@functools.lru_cache(maxsize=1)
def get_slack_app(
    connection: SlackConnection,
    settings: SlackGlobalSettings,
) -> slack_bolt.App:
    if not connection.app_details:
        raise fastapi.HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Slack app manifest has not been provisioned yet.",
        )
    if connection.bot_token is None:
        raise fastapi.HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Slack app has not been installed into a workspace yet.",
        )

    logger.info("Initializing Slack bolt sdk app.")

    # Initializes your app with your bot token and signing secret
    app = slack_bolt.App(
        client=slack_sdk.web.WebClient(proxy=SLACK_PROXY, token=connection.bot_token),
        signing_secret=connection.app_details.signing_secret,
        # As per the docs:
        # > One secret is the new Signing Secret, and one is the deprecated verification token.
        # > We strongly recommend you only use the Signing Secret from now on.
        # verification_token=config.app_details.verification_token,
    )

    # @app.middleware  # or app.use(log_request)
    # def log_request(
    #     body: dict, next: Callable[[], slack_bolt.BoltResponse]
    # ) -> slack_bolt.BoltResponse:
    #     logger.debug(body)
    #     return next()

    # Listen for unfurl events
    @app.event("link_shared")
    def handle_link_shared(ack: Ack, body: dict) -> None:
        ack()

        logger.info(f"Got link unfurl request: {body}")
        event = body["event"]

        # TODO: unfurl multiple links

        # Get the link URL from the event body
        link_url: str = event["links"][0]["url"]

        urn = extract_urn_from_url(link_url)
        if not urn:
            logger.debug(f"No urn found in link, skipping: {link_url}")
            return
        logger.debug(f"URN: {urn}")

        variables = {"urn": urn}
        data = graph.execute_graphql(SLACK_GET_ENTITY_QUERY, variables=variables)
        raw_entity = data["entity"]

        if not raw_entity or not raw_entity.get("properties"):
            # If the entity doesn't exist, GMS may "mint" the entity at read time.
            # If that happens, we can expect properties to be None.
            logger.debug(f"Entity details not found, skipping: {link_url}")
            return

        # Call the Slack API method to unfurl the link.
        # See https://api.slack.com/docs/message-link-unfurling#link_unfurling_with_api
        app.client.chat_unfurl(
            channel=event["channel"],
            ts=event["message_ts"],
            unfurls={
                link_url: {**render_entity_preview(raw_entity), "color": ACRYL_COLOR}
            },
            # user_auth_url='...',
        )

    @app.message("test-acryl-bot")
    def handle_test_message(message: dict, say: Say) -> None:
        # If you send a message containing "test-acryl-bot" to a channel
        # containing the bot, it will respond with a message. This is mostly
        # useful for testing and debugging.
        logger.info(message)
        say(
            f"Hey <@{message['user']}>, DataHub is available in this channel!",
            icon_url=DATAHUB_SLACK_ICON_URL,
        )

    @app.event("message")
    def handle_message_events(body: dict) -> None:
        # logger.info(f"message handler: {body}")
        pass

    @app.event("app_mention")
    def handle_app_mention_event(event: dict, say: Say) -> None:
        logger.info(event)

        thread_ts = event.get("thread_ts")
        message_ts = event["ts"]

        if not settings.datahub_at_mention_enabled:
            say(
                text="The AI-powered @DataHub bot is currently disabled. "
                "Contact your administrator to enable it or use /datahub commands instead.",
                icon_url=DATAHUB_SLACK_ICON_URL,
                thread_ts=thread_ts or message_ts,  # Reply in the thread
            )
            return

        parsed_event = SlackMentionEvent(
            channel_id=event["channel"],
            message_ts=message_ts,
            original_thread_ts=thread_ts,
            user_id=event["user"],
            message_text=event["text"],
        )
        handle_app_mention(app, parsed_event)

    @app.action(re.compile(f"{DATAHUB_MENTION_FOLLOWUP_QUESTION_BUTTON_ID}.*"))
    def handle_followup_question_event(ack: Ack, action: dict, body: dict) -> None:
        logger.debug(f"followup question handler - action: {action}; body: {body}")
        ack()

        # Extract the question info from the button value.
        channel_id = body["channel"]["id"]
        user_id = body["user"]["id"]
        payload = FollowupQuestionPayload.model_validate_json(action["value"])

        handle_followup_question(app, channel_id, user_id, payload)

    @app.action(re.compile(f"{DATAHUB_MENTION_FEEDBACK_BUTTON_ID}.*"))
    def handle_feedback_event(ack: Ack, action: dict, body: dict) -> None:
        # Acknowledge the button click
        ack()

        logger.debug(f"Feedback handler - action: {action}; body: {body}")

        # Extract feedback data from the button value
        channel_id = body["channel"]["id"]
        bot_message_ts = body["message"]["ts"]
        user_id = body["user"]["id"]
        user_name = body["user"]["name"]
        payload = FeedbackPayload.model_validate_json(action["value"])

        handle_feedback(app, channel_id, bot_message_ts, user_id, user_name, payload)

    @app.command(re.compile(r"^/acryl.*"))
    def handle_command_acryl(ack: Ack, respond: Respond, command: dict) -> None:
        handle_command(app, graph, ack, respond, command)

    @app.command(re.compile(r"^/datahub.*"))
    def handle_command_datahub(ack: Ack, respond: Respond, command: dict) -> None:
        handle_command(app, graph, ack, respond, command)

    @app.action("view_details")
    def handle_view_details(
        ack: Ack, respond: Respond, action: dict, body: dict
    ) -> None:
        logger.debug(f"action: {action}")
        logger.debug(f"body: {body}")
        ack()

        variables = {"urn": action["value"]}
        data = graph.execute_graphql(SLACK_GET_ENTITY_QUERY, variables=variables)
        logger.debug(f"get entity: {data}")
        app.client.views_open(
            trigger_id=body["trigger_id"],
            view=render_entity_modal(ExtractedEntity(data["entity"])),
        )

    @app.action(re.compile(r"^search.*"))
    def handle_search(ack: Ack, respond: Respond, action: dict, body: dict) -> None:
        logger.debug(f"action: {action}")
        logger.debug(f"body: {body}")
        ack()

        user_urn = get_datahub_user(
            app,
            body["user"]["id"],
            require_oauth_binding=get_require_slack_oauth_binding(),
        )
        # Parse the JSON value
        return search(
            graph,
            ack,
            respond,
            body["channel"]["name"],
            user_urn,
            context=SearchContext(**json.loads(action["value"])),
        )

    @app.action("post_search_result")
    def handle_post_search_result(
        ack: Ack, respond: Respond, action: dict, body: dict
    ) -> None:
        logger.debug(f"body: {body}")
        ack()

        value = json.loads(action["value"])
        variables = {"urn": value["urn"]}
        data = None
        try:
            data = graph.execute_graphql(SLACK_GET_ENTITY_QUERY, variables=variables)
        except Exception as e:
            logger.error(f"Error fetching entity due to error {e}")
            raise e

        try:
            result = respond(
                blocks=[
                    {
                        "type": "rich_text",
                        "elements": [
                            {
                                "type": "rich_text_section",
                                "elements": [
                                    {
                                        "type": "user",
                                        "user_id": body["user"]["id"],
                                    },
                                    {
                                        "type": "text",
                                        "text": " shared ",
                                    },
                                    {
                                        "type": "link",
                                        "url": get_type_url(
                                            value["entity_type"], value["urn"]
                                        ),
                                        "text": value["name"],
                                    },
                                    {"type": "text", "text": " on DataHub Cloud:"},
                                ],
                            }
                        ],
                    },
                ],
                attachments=[
                    {**render_entity_preview(data["entity"]), "color": ACRYL_COLOR}
                ],
                replace_original=False,
                response_type="in_channel",
            )
            if result.status_code != 200:
                logger.error(
                    f"Respond hook came back with {result.body} status={result.status_code}"
                )
        except Exception as e:
            logger.error(f"Respond faild with {e}")

    @app.action("subscribe")
    def handle_subscribe(ack: Ack, respond: Respond, action: dict, body: dict) -> None:
        logger.debug(f"body: {body}")
        logger.debug(f"view: {body}")
        ack()

        variables = {"urn": action["value"]}
        data = graph.execute_graphql(SLACK_GET_ENTITY_QUERY, variables=variables)
        logger.debug(f"get entity: {data}")
        app.client.views_open(
            trigger_id=body["trigger_id"],
            view=render_subscription_modal(
                ExtractedEntity(data["entity"]), body["response_url"]
            ),
        )

    @app.action("unsubscribe")
    def handle_unsubscribe(
        ack: Ack, respond: Respond, action: dict, body: dict
    ) -> None:
        logger.debug(f"body: {body}")
        logger.debug(f"view: {body}")
        ack()

        variables = {"input": {"subscriptionUrn": action["value"]}}
        require_oauth = get_require_slack_oauth_binding()
        slack_user_id = body["user"]["id"]

        resolution = resolve_slack_user_to_datahub(
            slack_user_id=slack_user_id,
            require_oauth_binding=require_oauth,
        )

        if resolution.should_prompt_connection:
            text, blocks = build_connect_account_blocks(
                slack_user_id, action_description="manage subscriptions"
            )
            respond(text=text, blocks=blocks, replace_original=False)
            return

        user_urn = resolution.user_urn
        if not user_urn and not require_oauth:
            email, user_urn, _ = get_user_information(
                app, slack_user_id, require_oauth_binding=False
            )
            if not user_urn:
                respond(
                    f"❗ Unsubscribe failed: could not find corresponding DataHub user with email {email}"
                )
                return

        if not user_urn:
            respond("❗ Unsubscribe failed: could not resolve your DataHub account.")
            return

        impersonation_graph = graph_as_user(user_urn)

        data = impersonation_graph.execute_graphql(
            DELETE_SUBSCRIPTION,
            variables=variables,
        )

        logger.debug(f"delete subscription: {data}")
        respond(text="✅ Subscription deleted 🔕!", replace_original=False)

    @app.view_submission("subscribe")
    def handle_view_submission(ack: Ack, body: dict, view: dict) -> None:
        logger.debug(f"body: {body}")
        logger.debug(f"view: {body}")
        private_metadata = json.loads(view["private_metadata"])
        entity_urn = private_metadata["urn"]
        response_url = private_metadata.get("response_url")
        if not response_url:
            logger.warning(
                f"No response_url in subscribe modal private_metadata for entity {entity_urn}"
            )
            ack()
            return
        respond = Respond(
            response_url=response_url,
            proxy=app.client.proxy,
            ssl=app.client.ssl,
        )
        ack()

        require_oauth = get_require_slack_oauth_binding()
        slack_user_id = body["user"]["id"]

        resolution = resolve_slack_user_to_datahub(
            slack_user_id=slack_user_id,
            require_oauth_binding=require_oauth,
        )

        if resolution.should_prompt_connection:
            text, blocks = build_connect_account_blocks(
                slack_user_id, action_description="manage subscriptions"
            )
            respond(text=text, blocks=blocks, replace_original=False)
            return

        user_urn = resolution.user_urn
        if not user_urn and not require_oauth:
            email, user_urn, user_urns = get_user_information(
                app, slack_user_id, require_oauth_binding=False
            )
            logger.debug(f"matched user urns: {user_urns}")
            if not user_urn:
                respond(
                    f"❗ Subscription failed: could not find corresponding DataHub user with email {email}"
                )
                return
            if len(user_urns) > 1:
                respond(
                    f"❗ Subscription failed: found multiple corresponding DataHub users with email {email}"
                )
                return

        if not user_urn:
            respond("❗ Subscription failed: could not resolve your DataHub account.")
            return

        impersonation_graph = graph_as_user(user_urn)

        options = view["state"]["values"]["ect_block"]["ect_action"]["selected_options"]
        entity_change_types = [
            {"entityChangeType": change_type.value}
            for option in options
            for change_type in EntityChangeTypeGroup(option["value"]).get_change_types()
        ]
        input = {
            "entityUrn": entity_urn,
            "subscriptionTypes": ["ENTITY_CHANGE"],
            "entityChangeTypes": entity_change_types,
            "notificationConfig": {
                "notificationSettings": {
                    "sinkTypes": ["SLACK"],
                    "slackSettings": {"userHandle": body["user"]["id"]},
                }
            },
        }
        data = impersonation_graph.execute_graphql(
            CREATE_SUBSCRIPTION,
            variables={"input": input},
        )

        logger.debug(f"create subscription: {data}")
        respond(text="✅ Subscription created 🔔!", replace_original=False)

    @app.action("resolve_incident")
    def handle_resolve_incident(
        ack: Ack, respond: Respond, action: dict, body: dict
    ) -> None:
        logger.debug(f"body: {body}")
        logger.debug(f"view: {body}")
        ack()

        context = IncidentContext(**json.loads(action["value"]))

        app.client.views_open(
            trigger_id=body["trigger_id"],
            view=render_resolve_incident(
                context.urn, body["response_url"], context.stage, context
            ),
        )

    @app.view_submission("resolve_incident")
    def handle_resolve_incident_submit(ack: Ack, body: dict, view: dict) -> None:
        logger.debug(f"body: {body}")
        logger.debug(f"view: {body}")
        ack()

        private_metadata = json.loads(view["private_metadata"])
        incident_urn = private_metadata["urn"]
        response_url = private_metadata.get("response_url")

        require_oauth = get_require_slack_oauth_binding()
        slack_user_id = body["user"]["id"]

        # Resolve user with shared logic
        resolution = resolve_slack_user_to_datahub(
            slack_user_id=slack_user_id,
            require_oauth_binding=require_oauth,
        )

        # If OAuth is required but user hasn't connected, prompt them
        if resolution.should_prompt_connection:
            logger.info(
                f"User {slack_user_id} needs to connect their account to resolve incidents"
            )
            text, blocks = build_connect_account_blocks(
                slack_user_id, action_description="manage incidents"
            )
            if response_url:
                respond = Respond(
                    response_url=response_url,
                    proxy=app.client.proxy,
                    ssl=app.client.ssl,
                )
                respond(text=text, blocks=blocks, replace_original=False)
            else:
                logger.warning(
                    f"No response_url to send connect-account prompt for user {slack_user_id}"
                )
            return

        # Fall back to email-based lookup if OAuth not required
        user_urn = resolution.user_urn
        if not user_urn and not require_oauth:
            email, user_urn, user_urns = get_user_information(
                app,
                slack_user_id,
                require_oauth_binding=False,
            )
            logger.debug(f"matched user urns: {user_urns}")
            if not user_urn:
                logger.warning(
                    f"Could not find corresponding DataHub user with email {email}. Resolving incident as system."
                )
            if len(user_urns) > 1:
                logger.warning(
                    f"Found multiple corresponding DataHub users with email {email}. Resolving incident as system."
                )

        impersonation_graph = graph_as_user(user_urn) if user_urn else graph_as_system()

        note = view["state"]["values"]["incident_message_input"][
            "incident_message_input_action"
        ]["value"]

        stage = None
        if "incident_stage_select" in view["state"]["values"]:
            select_action = view["state"]["values"]["incident_stage_select"][
                "select_incident_stage"
            ]
            selected_option = IncidentSelectOption(
                **json.loads(select_action["selected_option"]["value"])
            )
            stage = selected_option.value

        input = {"state": "RESOLVED", "message": note, "stage": stage or None}

        try:
            data = impersonation_graph.execute_graphql(
                UPDATE_INCIDENT_STATUS_MUTATION,
                variables={"urn": incident_urn, "input": input},
            )
            logger.debug(f"resolved incident!: {data}")
            _track_notification_slack_action(
                user_urn=user_urn,
                notification_id=incident_urn,
                notification_type=NotificationType.INCIDENT,
                action=NotificationSlackAction.RESOLVE,
                success=bool(data),
            )
        except GraphError as e:
            error_str = str(e)
            if "UNAUTHORIZED" in error_str or "403" in error_str:
                logger.warning(
                    f"User {slack_user_id} (urn={user_urn}) is not authorized to resolve incident {incident_urn}"
                )
                text, blocks = build_authorization_error_blocks(
                    slack_user_id, action_description="resolve this incident"
                )
                if response_url:
                    error_respond = Respond(
                        response_url=response_url,
                        proxy=app.client.proxy,
                        ssl=app.client.ssl,
                    )
                    error_respond(text=text, blocks=blocks, replace_original=False)
                else:
                    logger.warning(
                        f"No response_url to send auth error for user {slack_user_id} on incident {incident_urn}"
                    )
            else:
                logger.error(f"Error resolving incident {incident_urn}: {e}")
                raise

    @app.action("reopen_incident")
    def handle_reopen_incident(
        ack: Ack, respond: Respond, action: dict, body: dict
    ) -> None:
        logger.debug(f"body: {body}")
        logger.debug(f"view: {body}")
        ack()

        context = IncidentContext(**json.loads(action["value"]))
        incident_urn = context.urn

        require_oauth = get_require_slack_oauth_binding()
        slack_user_id = body["user"]["id"]

        # Resolve user with shared logic
        resolution = resolve_slack_user_to_datahub(
            slack_user_id=slack_user_id,
            require_oauth_binding=require_oauth,
        )

        # If OAuth is required but user hasn't connected, prompt them
        if resolution.should_prompt_connection:
            logger.info(
                f"User {slack_user_id} needs to connect their account to reopen incidents"
            )
            text, blocks = build_connect_account_blocks(
                slack_user_id, action_description="manage incidents"
            )
            respond(text=text, blocks=blocks, replace_original=False)
            return

        # Fall back to email-based lookup if OAuth not required
        user_urn = resolution.user_urn
        if not user_urn and not require_oauth:
            email, user_urn, user_urns = get_user_information(
                app,
                slack_user_id,
                require_oauth_binding=False,
            )
            logger.debug(f"matched user urns: {user_urns}")

            if not user_urn:
                logger.warning(
                    f"Could not find corresponding DataHub user with email {email}. Reopening incident as system user."
                )
            if len(user_urns) > 1:
                logger.warning(
                    f"Found multiple corresponding DataHub users with email {email}. Reopening incident as system user."
                )

        impersonation_graph = graph_as_user(user_urn) if user_urn else graph_as_system()

        input = {
            "state": "ACTIVE",
        }

        try:
            data = impersonation_graph.execute_graphql(
                UPDATE_INCIDENT_STATUS_MUTATION,
                variables={"urn": incident_urn, "input": input},
            )
            logger.debug(f"reopened incident!: {data}")
            _track_notification_slack_action(
                user_urn=user_urn,
                notification_id=incident_urn,
                notification_type=NotificationType.INCIDENT,
                action=NotificationSlackAction.REOPEN,
                success=bool(data),
            )
        except GraphError as e:
            error_str = str(e)
            if "UNAUTHORIZED" in error_str or "403" in error_str:
                logger.warning(
                    f"User {slack_user_id} (urn={user_urn}) is not authorized to reopen incident {incident_urn}"
                )
                text, blocks = build_authorization_error_blocks(
                    slack_user_id, action_description="reopen this incident"
                )
                respond(text=text, blocks=blocks, replace_original=False)
            else:
                logger.error(f"Error reopening incident {incident_urn}: {e}")
                raise

    @app.action("select_incident_stage")
    def handle_select_incident_stage(
        ack: Ack, respond: Respond, action: dict, body: dict
    ) -> None:
        logger.debug(f"body: {body}")
        logger.debug(f"view: {body}")
        ack()

        option = IncidentSelectOption(**json.loads(action["selected_option"]["value"]))
        context = option.context
        new_stage = option.value

        require_oauth = get_require_slack_oauth_binding()
        slack_user_id = body["user"]["id"]

        # Resolve user with shared logic
        resolution = resolve_slack_user_to_datahub(
            slack_user_id=slack_user_id,
            require_oauth_binding=require_oauth,
        )

        # If OAuth is required but user hasn't connected, prompt them
        if resolution.should_prompt_connection:
            logger.info(
                f"User {slack_user_id} needs to connect their account to change incident stage"
            )
            text, blocks = build_connect_account_blocks(
                slack_user_id, action_description="manage incidents"
            )
            respond(text=text, blocks=blocks, replace_original=False)
            return

        # Fall back to email-based lookup if OAuth not required
        user_urn = resolution.user_urn
        if not user_urn and not require_oauth:
            email, user_urn, user_urns = get_user_information(
                app,
                slack_user_id,
                require_oauth_binding=False,
            )

            if not user_urn:
                logger.warning(
                    f"Could not find corresponding DataHub user with email {email}. Selecting incident stage as system user."
                )
            if len(user_urns) > 1:
                logger.warning(
                    f"Found multiple corresponding DataHub users with email {email}. Selecting incident stage as system user."
                )

        impersonation_graph = graph_as_user(user_urn) if user_urn else graph_as_system()

        input = {"stage": new_stage}
        try:
            data = impersonation_graph.execute_graphql(
                UPDATE_INCIDENT_STATUS_MUTATION,
                variables={"urn": context.urn, "input": input},
            )
            logger.debug(f"updated incident stage: {data}")
        except GraphError as e:
            error_str = str(e)
            if "UNAUTHORIZED" in error_str or "403" in error_str:
                logger.warning(
                    f"User {slack_user_id} (urn={user_urn}) is not authorized to update incident stage for {context.urn}"
                )
                text, blocks = build_authorization_error_blocks(
                    slack_user_id, action_description="update this incident's stage"
                )
                respond(text=text, blocks=blocks, replace_original=False)
            else:
                logger.error(f"Error updating incident stage for {context.urn}: {e}")
                raise

    @app.action("select_incident_priority")
    def handle_select_incident_priority(
        ack: Ack, respond: Respond, action: dict, body: dict
    ) -> None:
        logger.debug(f"body: {body}")
        logger.debug(f"view: {body}")
        ack()

        option = IncidentSelectOption(**json.loads(action["selected_option"]["value"]))
        context = option.context
        new_priority = option.value

        require_oauth = get_require_slack_oauth_binding()
        slack_user_id = body["user"]["id"]

        # Resolve user with shared logic
        resolution = resolve_slack_user_to_datahub(
            slack_user_id=slack_user_id,
            require_oauth_binding=require_oauth,
        )

        # If OAuth is required but user hasn't connected, prompt them
        if resolution.should_prompt_connection:
            logger.info(
                f"User {slack_user_id} needs to connect their account to change incident priority"
            )
            text, blocks = build_connect_account_blocks(
                slack_user_id, action_description="manage incidents"
            )
            respond(text=text, blocks=blocks, replace_original=False)
            return

        # Fall back to email-based lookup if OAuth not required
        user_urn = resolution.user_urn
        if not user_urn and not require_oauth:
            email, user_urn, user_urns = get_user_information(
                app,
                slack_user_id,
                require_oauth_binding=False,
            )

            if not user_urn:
                logger.warning(
                    f"Could not find corresponding DataHub user with email {email}. Updating incident priority as system user."
                )
            if len(user_urns) > 1:
                logger.warning(
                    f"Found multiple corresponding DataHub users with email {email}. Updating incident priority as system user."
                )

        impersonation_graph = graph_as_user(user_urn) if user_urn else graph_as_system()

        try:
            data = impersonation_graph.execute_graphql(
                UPDATE_INCIDENT_PRIORITY_MUTATION,
                variables={"urn": context.urn, "priority": new_priority},
            )
            logger.debug(f"updated incident priority: {data}")
        except GraphError as e:
            error_str = str(e)
            if "UNAUTHORIZED" in error_str or "403" in error_str:
                logger.warning(
                    f"User {slack_user_id} (urn={user_urn}) is not authorized to update incident priority for {context.urn}"
                )
                text, blocks = build_authorization_error_blocks(
                    slack_user_id, action_description="update this incident's priority"
                )
                respond(text=text, blocks=blocks, replace_original=False)
            else:
                logger.error(f"Error updating incident priority for {context.urn}: {e}")
                raise

    @app.action("external_redirect")
    def handle_actions(ack: Ack, respond: Respond, _action: dict) -> None:
        # Main action is to redirect user, this is required to acknowledge the action
        ack()

    @app.shortcut("attach_to_asset")
    def handle_shortcuts(ack: Ack, event: dict, say: Say) -> None:
        ack()
        say(
            f"Hey <@{event['user']}>! DataHub shortcut commands are coming soon!",
            icon_url=DATAHUB_SLACK_ICON_URL,
        )

    return app


def get_slack_request_handler() -> SlackRequestHandler:
    # Attach the slack event handler to the app.
    app = get_slack_app(
        slack_config.get_connection(), slack_config.get_global_settings()
    )
    app_handler = SlackRequestHandler(app)
    return app_handler


@public_router.post("/slack/events")
async def slack_event_endpoint(req: fastapi.Request) -> fastapi.Response:
    body = await req.body()
    logger.debug(f"Received slack event: {body!r}")

    return await get_slack_request_handler().handle(req)


@public_router.post("/slack/actions")
async def slack_action_endpoint(req: fastapi.Request) -> fastapi.Response:
    body = await req.body()
    logger.debug(f"Received slack action: {body!r}")

    return await get_slack_request_handler().handle(req)


@public_router.post("/slack/commands")
async def slack_command_endpoint(req: fastapi.Request) -> fastapi.Response:
    body = await req.body()
    # Workaround issue in frontend proxy
    # headers = req.headers.mutablecopy()
    # headers["content-type"] = "application/x-www-form-urlencoded"
    # req._headers = headers
    logger.debug(f"Received slack command: {body!r}")

    return await get_slack_request_handler().handle(req)


@dataclasses.dataclass
class SlackMessageUrl:
    workspace_name: str
    conversation_id: str
    message_id: str
    thread_ts: Optional[str]


def parse_slack_message_url(url: str) -> Optional[SlackMessageUrl]:
    # Parse the url using regex.
    # https://regex101.com/r/QxS5d3/2

    regex = r"^https://([a-zA-Z0-9_\-]+)\.slack\.com/archives/([CD]\w+)/p(\d+)(?:\?.*thread_ts=([\d.]+).*)?$"

    matches = re.fullmatch(regex, url)
    if not matches:
        return None

    workspace_name, conversation_id, message_id, thread_ts = matches.groups()

    return SlackMessageUrl(
        workspace_name=workspace_name,
        conversation_id=conversation_id,
        message_id=message_id,
        thread_ts=thread_ts,
    )


class SlackLinkPreview(BaseModel):
    url: str

    timestamp: int  # unix timestamp in milliseconds
    text: str

    authorName: str
    authorImageUrl: Optional[str] = None

    workspaceName: str
    channelName: str

    # Only present if the message is in a thread.
    # These fields refer to the entire thread.
    isPartOfThread: bool = False
    replyCount: Optional[int] = None
    threadBaseMessageText: Optional[str] = None


def get_slack_link_preview(raw_url: str) -> SlackLinkPreview:
    app = get_slack_app(
        slack_config.get_connection(), slack_config.get_global_settings()
    )

    url = parse_slack_message_url(raw_url)
    if not url:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid slack message url")

    try:
        # First, get the conversation (channel/DM/MPIM) info.
        # TODO: Add caching around this.
        conversation = app.client.conversations_info(
            channel=url.conversation_id
        ).validate()
        conversation_name = conversation["channel"]["name"]

        # Next, get the message info.
        slack_message_ts: str = url.message_id[:-6] + "." + url.message_id[-6:]
        oldest = None
        if url.thread_ts:
            slack_message_ts, oldest = url.thread_ts, slack_message_ts

        messages = app.client.conversations_replies(
            channel=url.conversation_id,
            ts=slack_message_ts,
            oldest=oldest,
            limit=1,
            inclusive=True,
            include_all_metadata=True,
        ).validate()["messages"]

        if url.thread_ts and len(messages) == 2:
            thread_base_message, message = messages
        elif messages[0].get("thread_ts"):
            thread_base_message = message = messages[0]
        else:
            thread_base_message = None
            message = messages[0]

        # Get more details about the message author.
        if "user" in message:
            user_id = message["user"]

            # TODO: Add caching around this.
            user_details = app.client.users_info(user=user_id).validate()["user"]
            author_name = (
                user_details["profile"]["display_name_normalized"]
                or user_details["profile"]["display_name"]
                or user_details["profile"]["real_name_normalized"]
                or user_details["profile"]["real_name"]
            )
            author_image_url = user_details["profile"]["image_72"]
        else:
            author_name = message["username"]
            author_image_url = message["icons"].get("image_48")

        preview = SlackLinkPreview(
            url=raw_url,
            timestamp=datetime_to_ts_millis(slack_ts_to_datetime(message["ts"])),
            text=message["text"],
            authorName=author_name,
            authorImageUrl=author_image_url,
            workspaceName=url.workspace_name,
            channelName=conversation_name,
        )
        if thread_base_message:
            preview.isPartOfThread = True
            preview.replyCount = thread_base_message["reply_count"]
            preview.threadBaseMessageText = thread_base_message["text"]

        return preview
    except slack_sdk.errors.SlackApiError as e:
        error_message = e.response["error"]
        logger.exception(f"Error getting slack message preview: {error_message}")
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"You do not have permission to view conversation {url.conversation_id} message {url.message_id}: {error_message}",
        ) from e


# ============================================================================
# Slack Personal Notifications OAuth Endpoints
# ============================================================================


def store_user_slack_settings(
    user_urn: str,
    slack_user_id: str,
    display_name: Optional[str] = None,
) -> bool:
    """
    Store Slack user binding in DataHub user settings.

    This is the essential binding that links a Slack user to a DataHub user.
    The slack_user_id is the primary identifier used for user resolution.

    Args:
        user_urn: DataHub user URN to bind
        slack_user_id: Slack user ID (e.g., "U12345678")
        display_name: Optional display name for UI purposes

    Returns:
        True if successful, False otherwise
    """
    logger.debug(f"Storing Slack binding: {user_urn} -> {slack_user_id}")

    try:
        from datahub.emitter.mcp import MetadataChangeProposalWrapper
        from datahub.metadata.com.linkedin.pegasus2avro.identity import (
            CorpUserAppearanceSettingsClass,
            CorpUserSettingsClass,
        )
        from datahub.metadata.schema_classes import (
            NotificationSettingsClass,
            SlackNotificationSettingsClass,
            SlackUserClass,
        )

        # Get or create user settings
        existing_settings = graph.get_aspect(
            entity_urn=user_urn, aspect_type=CorpUserSettingsClass
        )

        if existing_settings:
            corp_settings = existing_settings
            notification_settings = (
                corp_settings.notificationSettings
                or NotificationSettingsClass(sinkTypes=[])
            )
        else:
            corp_settings = CorpUserSettingsClass(
                appearance=CorpUserAppearanceSettingsClass(showSimplifiedHomepage=False)
            )
            notification_settings = NotificationSettingsClass(sinkTypes=[])

        # Create the SlackUser binding
        slack_user = SlackUserClass(
            slackUserId=slack_user_id,
            displayName=display_name,
            lastUpdated=int(time.time() * 1000),
        )

        # Merge with existing slack settings to preserve channels / userHandle
        existing_slack = (
            notification_settings.slackSettings or SlackNotificationSettingsClass()
        )
        existing_slack.user = slack_user
        notification_settings.slackSettings = existing_slack
        corp_settings.notificationSettings = notification_settings

        # Emit the binding
        mcp = MetadataChangeProposalWrapper(
            entityUrn=user_urn,
            aspect=corp_settings,
        )
        graph.emit(mcp)

        logger.info(f"✅ Bound Slack user {slack_user_id} to {user_urn}")
        return True

    except Exception as e:
        logger.error(f"Failed to store Slack binding: {e}")
        return False


def _is_safe_redirect_path(path: Optional[str]) -> bool:
    """Return True only if *path* is a safe, relative redirect target."""
    if not path or not path.startswith("/"):
        return False
    # Block protocol-relative, backslash, encoded-slash, and traversal tricks
    if path.startswith("//") or "://" in path:
        return False
    if "\\" in path or "%2f" in path.lower() or "%5c" in path.lower():
        return False
    if ".." in path:
        return False
    return True


def _build_oauth_error_redirect(redirect_url: str, message: str) -> RedirectResponse:
    """Build a redirect response with a URL-encoded error message."""
    safe_message = quote(message, safe="")
    return RedirectResponse(
        url=f"{redirect_url}?slack_oauth=error&message={safe_message}",
        status_code=302,
    )


def _exchange_slack_oidc_token(
    config: SlackConnection, code: str, redirect_uri: str
) -> Any:
    """
    Exchange an authorization code for OIDC tokens via Slack.

    See: https://docs.slack.dev/authentication/sign-in-with-slack/

    Returns:
        The validated Slack OIDC response.

    Raises:
        slack_sdk.errors.SlackApiError: If the token exchange fails.
    """
    slack_client = slack_sdk.web.WebClient(proxy=SLACK_PROXY)
    assert config.app_details
    assert config.app_details.client_id
    assert config.app_details.client_secret
    return slack_client.openid_connect_token(
        client_id=config.app_details.client_id,
        client_secret=config.app_details.client_secret,
        code=code,
        redirect_uri=redirect_uri,
    ).validate()


def _extract_slack_user_from_oidc(
    oidc_response: Any,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract Slack user ID and display name from an OIDC token response.

    The ``openid.connect.token`` endpoint returns an ``id_token`` JWT whose
    payload contains user claims (``sub``, ``name``, etc.).  Signature
    verification is unnecessary here because the token was received directly
    from Slack's server over HTTPS in a back-channel (server-to-server) call.
    Per OpenID Connect Core §3.1.3.7, TLS server validation may be used in
    place of token signature checking when the token is obtained directly
    from the token endpoint.

    Returns:
        (slack_user_id, display_name) — either may be None.
    """
    id_token: Optional[str] = oidc_response.get("id_token")
    if not id_token:
        return None, None

    try:
        payload_segment = id_token.split(".")[1]
        # JWT base64url encoding may lack padding
        padded = payload_segment + "=" * (-len(payload_segment) % 4)
        claims = json.loads(base64.urlsafe_b64decode(padded))
    except Exception:
        logger.warning("Failed to decode id_token JWT payload", exc_info=True)
        return None, None

    slack_user_id: Optional[str] = claims.get(
        "https://slack.com/user_id"
    ) or claims.get("sub")
    display_name: Optional[str] = claims.get("name") or claims.get("given_name")

    return slack_user_id, display_name


def handle_personal_notifications_oauth(
    code: str,
    user_urn: Optional[str],
    redirect_url: Optional[str],
    state: Optional[str] = None,
) -> Union[RedirectResponse, Dict[str, Any]]:
    """
    Handle OAuth completion for personal notifications flow.

    Orchestrates: validate → OIDC exchange → extract user → store binding → redirect.
    """
    logger.info(f"Handling personal notifications OAuth for user: {user_urn}")

    if not user_urn:
        if redirect_url:
            return _build_oauth_error_redirect(
                redirect_url, "User authentication information is missing"
            )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="user_urn parameter is required for Slack OAuth flow",
        )

    try:
        config = slack_config.get_connection()
        if (
            not config.app_details
            or not config.app_details.client_id
            or not config.app_details.client_secret
        ):
            if redirect_url:
                return _build_oauth_error_redirect(
                    redirect_url, "Slack integration not configured"
                )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Slack app credentials not configured",
            )

        redirect_uri = f"{DATAHUB_FRONTEND_URL}/integrations/slack/oauth_callback"

        # Step 1: Exchange authorization code for OIDC tokens
        try:
            oidc_response = _exchange_slack_oidc_token(config, code, redirect_uri)
        except slack_sdk.errors.SlackApiError as e:
            error_message = e.response.get("error", "Unknown error")
            logger.exception(f"Slack OIDC token exchange failed: {error_message}")
            if redirect_url:
                return _build_oauth_error_redirect(
                    redirect_url, f"OIDC failed: {error_message}"
                )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to complete Slack OIDC: {error_message}",
            ) from e

        # Step 2: Extract user info from OIDC response
        slack_user_id, display_name = _extract_slack_user_from_oidc(oidc_response)

        if not slack_user_id:
            logger.error("OIDC succeeded but no user ID in response or token")
            if redirect_url:
                return _build_oauth_error_redirect(
                    redirect_url, "Failed to retrieve user information"
                )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="OIDC succeeded but no user ID in response or token",
            )

        logger.info(f"Slack OAuth completed - User ID: {slack_user_id}")

        # Step 3: Store the binding
        success = store_user_slack_settings(
            user_urn,
            slack_user_id=slack_user_id,
            display_name=display_name,
        )

        if not success:
            if redirect_url:
                return _build_oauth_error_redirect(
                    redirect_url, "Failed to save settings"
                )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update notification settings for user {user_urn}",
            )

        # Step 4: Redirect with success
        target = redirect_url or "/settings/personal-notifications"
        success_redirect = (
            f"{target}?slack_oauth=success"
            f"&slack_user_id={quote(slack_user_id, safe='')}"
        )
        return RedirectResponse(url=success_redirect, status_code=302)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Unexpected error in personal notifications OAuth: {e}", exc_info=True
        )
        if redirect_url:
            return _build_oauth_error_redirect(redirect_url, "Unexpected error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error during OAuth flow: {str(e)}",
        ) from e
