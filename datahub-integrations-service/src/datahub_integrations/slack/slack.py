import functools
import json
import re
from datetime import datetime, timezone
from typing import Optional, Tuple

import fastapi
import slack_bolt
import slack_sdk.errors
import slack_sdk.web
from fastapi import HTTPException, status
from fastapi.responses import RedirectResponse
from loguru import logger
from pydantic import BaseModel
from slack_bolt import Ack, Respond, Say
from slack_bolt.adapter.fastapi import SlackRequestHandler
from slack_sdk.oauth import AuthorizeUrlGenerator

from datahub_integrations.app import DATAHUB_FRONTEND_URL, EXTERNAL_STATIC_PATH, graph
from datahub_integrations.graphql.incidents import (
    UPDATE_INCIDENT_PRIORITY_MUTATION,
    UPDATE_INCIDENT_STATUS_MUTATION,
)
from datahub_integrations.graphql.slack import SLACK_GET_ENTITY_QUERY
from datahub_integrations.graphql.subscription import (
    CREATE_SUBSCRIPTION,
    DELETE_SUBSCRIPTION,
)
from datahub_integrations.slack.app_manifest import (
    create_app_with_manifest,
    get_slack_app_manifest,
    slack_bot_scopes,
    update_app_with_manifest,
)
from datahub_integrations.slack.command.router import handle_command
from datahub_integrations.slack.command.search import search
from datahub_integrations.slack.config import SLACK_PROXY, SlackConnection, slack_config
from datahub_integrations.slack.context import (
    IncidentContext,
    IncidentSelectOption,
    SearchContext,
)
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
    get_datahub_user,
    get_user_information,
    graph_as_system,
    graph_as_user,
)
from datahub_integrations.slack.utils.entity_extract import (
    ExtractedEntity,
    get_type_url,
)

# 7 days because admins may take some time to approve
_state_store = InMemoryStateStore(expiration_seconds=60 * 60 * 24 * 7)

ACRYL_SLACK_ICON_URL = f"{EXTERNAL_STATIC_PATH}/acryl-slack-icon.png"

private_router = fastapi.APIRouter()
public_router = fastapi.APIRouter()


def get_oauth_url_generator(config: SlackConnection) -> AuthorizeUrlGenerator:
    assert config.app_details
    assert config.app_details.client_id

    return AuthorizeUrlGenerator(
        client_id=config.app_details.client_id,
        scopes=slack_bot_scopes,
        redirect_uri=f"{DATAHUB_FRONTEND_URL}/integrations/slack/oauth_callback",
    )


@private_router.post("/slack/reload_credentials")
def reload_slack_credentials() -> None:
    """Reload Slack credentials from GMS and refreshes existing services appropriately."""

    slack_config.reload()


@public_router.get("/slack/install")
def install_slack_app() -> RedirectResponse:
    config = slack_config.reload()

    # Create the Slack app manifest before attempting to install.
    manifest = get_slack_app_manifest()
    config = create_app_with_manifest(config, manifest)
    slack_config.save_config(config)
    assert config.app_details, "App details should be present after provisioning."

    # Generate the OAuth URL and redirect to it.
    authorize_url_generator = get_oauth_url_generator(config)
    state = _state_store.issue()

    url = authorize_url_generator.generate(state=state)
    logger.debug(f"Redirecting to {url}")

    return RedirectResponse(url=url)


@public_router.get("/slack/refresh-installation")
def refresh_slack_app() -> RedirectResponse:
    config = slack_config.reload()

    # Update the Slack app manifest before attempting to install.
    manifest = get_slack_app_manifest()
    config = update_app_with_manifest(config, manifest)
    slack_config.save_config(config)
    assert config.app_details, "App details should be present after provisioning."

    # Generate the OAuth URL and redirect to it.
    authorize_url_generator = get_oauth_url_generator(config)
    state = _state_store.issue()

    url = authorize_url_generator.generate(state=state)
    logger.debug(f"Redirecting to {url}")

    return RedirectResponse(url=url)


@public_router.get("/slack/oauth_callback")
def oauth_callback(
    state: str,
    code: Optional[str] = None,
    error: Optional[str] = None,
    error_description: Optional[str] = None,
) -> RedirectResponse:
    config = slack_config.get_config()
    assert config.app_details, "App details should be present after provisioning."
    assert (
        config.app_details.client_id
    ), "Client id should be present after provisioning"
    assert (
        config.app_details.client_secret
    ), "Client secret should be present after provisioning"

    if not _state_store.consume(state):
        raise fastapi.HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid state parameter.",
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
    oauth_response = slack_client.oauth_v2_access(
        client_id=config.app_details.client_id,
        client_secret=config.app_details.client_secret,
        redirect_uri=authorize_url_generator.redirect_uri,
        code=code,
    ).validate()

    authed_user = oauth_response["authed_user"]
    logger.info(
        f'Completed app install for team {oauth_response.get("team")}, approved by {authed_user}'
    )

    # Save the new bot token.
    bot_token = oauth_response["access_token"]
    new_config = config.copy(
        update=dict(
            bot_token=bot_token,
        )
    )
    slack_config.save_config(new_config)

    # Send a welcome message to the user who just installed us.
    # TODO: Add more context to this message + a link back to the integrations page.
    app = get_slack_app(new_config)
    app.client.chat_postMessage(
        channel=authed_user["id"],
        text="DataHub has been connected to Slack!",
        icon_url=ACRYL_SLACK_ICON_URL,
    )

    return RedirectResponse(url="/settings/integrations/slack")


@functools.lru_cache(maxsize=1)
def get_slack_app(config: SlackConnection) -> slack_bolt.App:
    if not config.app_details:
        raise fastapi.HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Slack app manifest has not been provisioned yet.",
        )
    if config.bot_token is None:
        raise fastapi.HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Slack app has not been installed into a workspace yet.",
        )

    logger.info("Initializing Slack bolt sdk app.")

    # Initializes your app with your bot token and signing secret
    app = slack_bolt.App(
        client=slack_sdk.web.WebClient(proxy=SLACK_PROXY, token=config.bot_token),
        signing_secret=config.app_details.signing_secret,
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

        # https://<frontend_url>/<entity_type>/<urn>[/asdf]?<suffix_with_slashes>
        urn = link_url.split("/")[4]
        logger.debug(f"URN: {urn}")

        variables = {"urn": urn}
        data = graph.execute_graphql(SLACK_GET_ENTITY_QUERY, variables=variables)
        raw_entity = data["entity"]

        if not raw_entity or not raw_entity["properties"]:
            # If the entity doesn't exist, GMS may "mint" the entity at read time.
            # If that happens, we can expect properties to be None.
            return

        # Call the Slack API method to unfurl the link.
        # See https://api.slack.com/docs/message-link-unfurling#link_unfurling_with_api
        response = app.client.chat_unfurl(
            channel=event["channel"],
            ts=event["message_ts"],
            unfurls={
                link_url: {**render_entity_preview(raw_entity), "color": ACRYL_COLOR}
            },
            # user_auth_url='...',
        )

        # Log the API call response
        logger.debug(response)

    @app.message("test-acryl-bot")
    def handle_test_message(message: dict, say: Say) -> None:
        logger.info(message)
        say(
            f'Hey <@{message["user"]}>, DataHub is available in this channel!',
            icon_url=ACRYL_SLACK_ICON_URL,
        )

    @app.event("message")
    def handle_message_events(body: dict) -> None:
        logger.info(f"message handler: {body}")
        pass

    @app.event("app_mention")
    def handle_app_mention_events(event: dict, say: Say) -> None:
        logger.info(event)
        say(
            f'Hey <@{event["user"]}>! DataHub commands are coming soon!',
            icon_url=ACRYL_SLACK_ICON_URL,
        )

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

        user_urn = get_datahub_user(app, body["user"]["id"])
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
        email, user_urn, _ = get_user_information(app, body["user"]["id"])
        if not user_urn:
            respond(
                f"❗ Unsubscribe failed: could not find corresponding DataHub user with email {email}"
            )
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
        respond = Respond(
            response_url=private_metadata["response_url"],
            proxy=app.client.proxy,
            ssl=app.client.ssl,
        )
        ack()

        email, user_urn, user_urns = get_user_information(app, body["user"]["id"])
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

        email, user_urn, user_urns = get_user_information(app, body["user"]["id"])
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
        data = impersonation_graph.execute_graphql(
            UPDATE_INCIDENT_STATUS_MUTATION,
            variables={"urn": incident_urn, "input": input},
        )

        logger.debug(f"resolved incident!: {data}")

    @app.action("reopen_incident")
    def handle_reopen_incident(
        ack: Ack, respond: Respond, action: dict, body: dict
    ) -> None:
        logger.debug(f"body: {body}")
        logger.debug(f"view: {body}")
        ack()

        context = IncidentContext(**json.loads(action["value"]))
        incident_urn = context.urn

        email, user_urn, user_urns = get_user_information(app, body["user"]["id"])
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
        data = impersonation_graph.execute_graphql(
            UPDATE_INCIDENT_STATUS_MUTATION,
            variables={"urn": incident_urn, "input": input},
        )

        logger.debug(f"reopened incident!: {data}")

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

        email, user_urn, user_urns = get_user_information(app, body["user"]["id"])

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
        data = impersonation_graph.execute_graphql(
            UPDATE_INCIDENT_STATUS_MUTATION,
            variables={"urn": context.urn, "input": input},
        )

        logger.debug(f"updated incident stage: {data}")

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

        email, user_urn, user_urns = get_user_information(app, body["user"]["id"])

        if not user_urn:
            logger.warning(
                f"Could not find corresponding DataHub user with email {email}. Updating incident priority as system user."
            )
        if len(user_urns) > 1:
            logger.warning(
                f"Found multiple corresponding DataHub users with email {email}. Updating incident priority as system user."
            )

        impersonation_graph = graph_as_user(user_urn) if user_urn else graph_as_system()

        data = impersonation_graph.execute_graphql(
            UPDATE_INCIDENT_PRIORITY_MUTATION,
            variables={"urn": context.urn, "priority": new_priority},
        )

        logger.debug(f"updated incident priority: {data}")

    @app.action("external_redirect")
    def handle_actions(ack: Ack, respond: Respond, _action: dict) -> None:
        # Main action is to redirect user, this is required to acknowledge the action
        ack()

    @app.shortcut("attach_to_asset")
    def handle_shortcuts(ack: Ack, event: dict, say: Say) -> None:
        ack()
        say(
            f'Hey <@{event["user"]}>! DataHub shortcut commands are coming soon!',
            icon_url=ACRYL_SLACK_ICON_URL,
        )

    return app


def get_slack_request_handler() -> SlackRequestHandler:
    # Attach the slack event handler to the app.
    app = get_slack_app(slack_config.get_config())
    app_handler = SlackRequestHandler(app)
    return app_handler


@public_router.post("/slack/events")
async def slack_event_endpoint(req: fastapi.Request) -> fastapi.Response:
    body = await req.body()
    logger.debug(f"Received slack event: {body!r}\nHeaders: {req.headers}")

    return await get_slack_request_handler().handle(req)


@public_router.post("/slack/actions")
async def slack_action_endpoint(req: fastapi.Request) -> fastapi.Response:
    body = await req.body()
    logger.debug(f"Received slack action: {body!r}\nHeaders: {req.headers}")

    return await get_slack_request_handler().handle(req)


@public_router.post("/slack/commands")
async def slack_command_endpoint(req: fastapi.Request) -> fastapi.Response:
    body = await req.body()
    # Workaround issue in frontend proxy
    # headers = req.headers.mutablecopy()
    # headers["content-type"] = "application/x-www-form-urlencoded"
    # req._headers = headers
    logger.debug(f"Received slack command: {body!r}\nHeaders: {req.headers}")

    return await get_slack_request_handler().handle(req)


def parse_slack_message_url(url: str) -> Optional[Tuple[str, str, str, Optional[str]]]:
    # Parse the url using regex.
    # https://regex101.com/r/QxS5d3/2

    regex = r"^https://([a-zA-Z0-9_\-]+)\.slack\.com/archives/([CD]\w+)/p(\d+)(?:\?.*thread_ts=([\d.]+).*)?$"

    matches = re.fullmatch(regex, url)
    if not matches:
        return None

    workspace_name, conversation_id, message_id, thread_ts = matches.groups()

    return workspace_name, conversation_id, message_id, thread_ts


class SlackLinkPreview(BaseModel):
    url: str

    timestamp: int  # unix timestamp in milliseconds
    text: str

    authorName: str
    authorImageUrl: Optional[str]

    workspaceName: str
    channelName: str

    # Only present if the message is in a thread.
    # These fields refer to the entire thread.
    isPartOfThread: bool = False
    replyCount: Optional[int]
    threadBaseMessageText: Optional[str]


def get_slack_link_preview(url: str) -> SlackLinkPreview:
    app = get_slack_app(slack_config.get_config())

    parsed_url = parse_slack_message_url(url)
    if not parsed_url:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid slack message url")

    workspace_name, conversation_id, message_id, thread_ts = parsed_url

    try:
        # First, get the conversation (channel/DM/MPIM) info.
        # TODO: Add caching around this.
        conversation = app.client.conversations_info(channel=conversation_id).validate()
        conversation_name = conversation["channel"]["name"]

        # Next, get the message info.
        slack_message_ts: str = message_id[:-6] + "." + message_id[-6:]
        oldest = None
        if thread_ts:
            slack_message_ts, oldest = thread_ts, slack_message_ts

        messages = app.client.conversations_replies(
            channel=conversation_id,
            ts=slack_message_ts,
            oldest=oldest,
            limit=1,
            inclusive=True,
            include_all_metadata=True,
        ).validate()["messages"]

        if thread_ts and len(messages) == 2:
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
            url=url,
            timestamp=int(
                datetime.fromtimestamp(
                    float(message["ts"]), tz=timezone.utc
                ).timestamp()
                * 1000
            ),
            text=message["text"],
            authorName=author_name,
            authorImageUrl=author_image_url,
            workspaceName=workspace_name,
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
            f"You do not have permission to view conversation {conversation_id} message {message_id}: {error_message}",
        ) from e
