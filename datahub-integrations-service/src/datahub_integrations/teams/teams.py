import asyncio
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import fastapi
import httpx
from cachetools import TTLCache
from fastapi import HTTPException, Request, status
from fastapi.responses import Response
from loguru import logger
from pydantic import BaseModel

from datahub_integrations.app import graph
from datahub_integrations.graphql.slack import SLACK_GET_ENTITY_QUERY
from datahub_integrations.teams.config import (
    TeamsAppDetails,
    TeamsConnection,
    teams_config,
)
from datahub_integrations.teams.exceptions import (
    TeamsErrorCodes,
    TeamsMessageSendException,
)
from datahub_integrations.teams.render.render_entity import render_entity_card
from datahub_integrations.teams.search import SearchResponse, get_search_service
from datahub_integrations.teams.url_utils import extract_urn_from_url

public_router = fastapi.APIRouter()
private_router = fastapi.APIRouter()

# TTL Configuration for Teams message update locks
TEAMS_LOCK_CACHE_TTL_SECONDS_ENV_VAR = (
    "INTEGRATION_SERVICE_TEAMS_LOCK_CACHE_TTL_SECONDS"
)
TEAMS_LOCK_CACHE_TTL_SECONDS_VALUE: int = int(
    os.getenv(TEAMS_LOCK_CACHE_TTL_SECONDS_ENV_VAR) or "60"  # 1 minute default
)
TEAMS_LOCK_CACHE_MAX_SIZE = 100

# TTL-based cache to track locks for Teams message updates
# This prevents race conditions when multiple updates happen to the same message
# Locks are automatically cleaned up after TTL to prevent memory leaks
_message_update_locks: TTLCache[str, asyncio.Lock] = TTLCache(
    maxsize=TEAMS_LOCK_CACHE_MAX_SIZE, ttl=TEAMS_LOCK_CACHE_TTL_SECONDS_VALUE
)

# Global set to track messages that have received their final response
# Progress updates for these messages should be dropped
_finalized_messages: set[str] = set()


def mark_message_finalized(activity_id: str) -> None:
    """Mark a Teams message as finalized (final response sent)."""
    _finalized_messages.add(activity_id)
    logger.debug(f"Marked Teams message {activity_id} as finalized")


def is_message_finalized(activity_id: str) -> bool:
    """Check if a Teams message has been finalized."""
    return activity_id in _finalized_messages


def get_conversation_id(activity: "TeamsActivity") -> Optional[str]:
    """Extract conversation ID from Teams activity (thread-aware for channels)."""
    from datahub_integrations.teams.conversation_utils import get_teams_conversation_id

    # Convert TeamsActivity to Bot Framework Activity-like object for compatibility
    class ActivityLike:
        def __init__(self, teams_activity: Any) -> None:
            self.conversation = teams_activity.conversation
            self.channel_data = getattr(teams_activity, "channel_data", None)

    activity_like = ActivityLike(activity)
    return get_teams_conversation_id(activity_like)


async def safe_send_teams_message(
    service_url: str, conversation_id: str, message: dict, config: TeamsConnection
) -> bool:
    """Send Teams message with error handling."""
    try:
        await send_teams_message(service_url, conversation_id, message, config)
        return True
    except TeamsMessageSendException as e:
        # Log the error with detailed information and metrics
        e.log_and_track()
        # Re-raise the exception so callers can handle it appropriately
        # This allows the user-friendly message to be surfaced to the end user
        raise
    except Exception as e:
        logger.error(f"Unexpected error sending Teams message: {e}")
        return False


async def safe_update_teams_message(
    service_url: str,
    conversation_id: str,
    activity_id: str,
    new_message: dict,
    config: TeamsConnection,
) -> bool:
    """Update Teams message with error handling."""
    try:
        await update_teams_message(
            service_url, conversation_id, activity_id, new_message, config
        )
        return True
    except TeamsMessageSendException as e:
        # Log the error with detailed information and metrics
        e.log_and_track()
        # Re-raise the exception so callers can handle it appropriately
        # This allows the user-friendly message to be surfaced to the end user
        raise
    except Exception as e:
        logger.error(f"Unexpected error updating Teams message: {e}")
        return False


async def safe_update_teams_final_message(
    service_url: str,
    conversation_id: str,
    activity_id: str,
    new_message: dict,
    config: TeamsConnection,
) -> bool:
    """Update Teams message with final content and error handling."""
    try:
        await update_teams_final_message(
            service_url, conversation_id, activity_id, new_message, config
        )
        return True
    except Exception as e:
        logger.error(f"Failed to update final Teams message: {e}")
        return False


async def safe_update_teams_progress_message(
    service_url: str,
    conversation_id: str,
    activity_id: str,
    new_message: dict,
    config: TeamsConnection,
) -> bool:
    """Update Teams progress message with error handling."""
    try:
        await update_teams_progress_message(
            service_url, conversation_id, activity_id, new_message, config
        )
        return True
    except Exception as e:
        logger.error(f"Failed to update Teams progress message: {e}")
        return False


def cleanup_message_state(activity_id: str) -> None:
    """Clean up message-specific state."""
    # TTL cache will automatically clean up locks, but we can manually remove if needed
    if activity_id in _message_update_locks:
        del _message_update_locks[activity_id]
    _finalized_messages.discard(activity_id)


class TeamsActivity(BaseModel):
    """Represents a Microsoft Teams activity."""

    id: Optional[str] = None
    type: str
    timestamp: Optional[str] = None
    serviceUrl: Optional[str] = None
    channelId: Optional[str] = None
    from_: Optional[Dict[str, Any]] = None
    conversation: Optional[Dict[str, Any]] = None
    recipient: Optional[Dict[str, Any]] = None
    text: Optional[str] = None
    textType: Optional[str] = None
    inputHint: Optional[str] = None
    entities: Optional[List[Dict[str, Any]]] = None
    attachments: Optional[List[Dict[str, Any]]] = None
    membersAdded: Optional[List[Dict[str, Any]]] = None
    membersRemoved: Optional[List[Dict[str, Any]]] = None
    value: Optional[Dict[str, Any]] = None
    name: Optional[str] = None
    action: Optional[str] = None
    is_followup_question: Optional[bool] = False

    class Config:
        allow_population_by_field_name = True

    def __init__(self, **data: Any) -> None:
        # Handle the 'from' field which is a Python keyword
        if "from" in data:
            data["from_"] = data.pop("from")
        super().__init__(**data)


class TeamsResponse(BaseModel):
    """Represents a Teams response."""

    status: int
    body: Optional[str] = None


async def get_azure_ad_token(app_id: str, app_password: str, tenant_id: str) -> str:
    """Get Azure AD token for Microsoft Graph API access."""
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    data = {
        "client_id": app_id,
        "client_secret": app_password,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(token_url, data=data)
        response.raise_for_status()
        token_data = response.json()
        return token_data["access_token"]


async def send_teams_message(
    service_url: str, conversation_id: str, message: dict, config: TeamsConnection
) -> None:
    """Send a message to a Teams conversation."""
    if (
        not config.app_details
        or not config.app_details.app_id
        or not config.app_details.app_password
        or not config.app_details.tenant_id
    ):
        raise ValueError("Teams app credentials not configured")

    # Get access token
    access_token = await get_azure_ad_token(
        config.app_details.app_id,
        config.app_details.app_password,
        config.app_details.tenant_id,
    )

    # Send message
    url = f"{service_url}/v3/conversations/{conversation_id}/activities"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=message, headers=headers)
        if response.status_code != 201:
            # Map HTTP status codes to specific error codes
            if response.status_code == 403:
                error_code = TeamsErrorCodes.MESSAGE_SEND_FORBIDDEN
                technical_message = (
                    f"Bot not authorized to send message (403): {response.text}"
                )
            elif response.status_code == 401:
                error_code = TeamsErrorCodes.MESSAGE_SEND_UNAUTHORIZED
                technical_message = f"Authentication failed (401): {response.text}"
            elif response.status_code == 404:
                error_code = TeamsErrorCodes.MESSAGE_SEND_NOT_FOUND
                technical_message = f"Conversation not found (404): {response.text}"
            else:
                error_code = TeamsErrorCodes.MESSAGE_SEND_FAILED
                technical_message = f"Failed to send Teams message ({response.status_code}): {response.text}"

            raise TeamsMessageSendException(
                message=technical_message,
                error_code=error_code,
                status_code=response.status_code,
                response_text=response.text,
            )


async def update_teams_message(
    service_url: str,
    conversation_id: str,
    activity_id: str,
    new_message: dict,
    config: TeamsConnection,
) -> None:
    """Update an existing Teams message."""
    await _update_teams_message_impl(
        service_url, conversation_id, activity_id, new_message, config
    )


async def _update_teams_message_impl(
    service_url: str,
    conversation_id: str,
    activity_id: str,
    new_message: dict,
    config: TeamsConnection,
) -> None:
    """Implementation for updating Teams messages."""
    if (
        not config.app_details
        or not config.app_details.app_id
        or not config.app_details.app_password
        or not config.app_details.tenant_id
    ):
        raise ValueError("Teams app credentials not configured")

    # Get access token
    access_token = await get_azure_ad_token(
        config.app_details.app_id,
        config.app_details.app_password,
        config.app_details.tenant_id,
    )

    # Update message
    url = f"{service_url}/v3/conversations/{conversation_id}/activities/{activity_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient() as client:
        response = await client.put(url, json=new_message, headers=headers)
        if response.status_code not in [200, 204]:
            # Map HTTP status codes to specific error codes for message updates
            if response.status_code == 403:
                error_code = TeamsErrorCodes.MESSAGE_SEND_FORBIDDEN
                technical_message = (
                    f"Bot not authorized to update message (403): {response.text}"
                )
            elif response.status_code == 401:
                error_code = TeamsErrorCodes.MESSAGE_SEND_UNAUTHORIZED
                technical_message = (
                    f"Authentication failed for update (401): {response.text}"
                )
            elif response.status_code == 404:
                error_code = TeamsErrorCodes.MESSAGE_SEND_NOT_FOUND
                technical_message = (
                    f"Message or conversation not found (404): {response.text}"
                )
            else:
                error_code = TeamsErrorCodes.MESSAGE_SEND_FAILED
                technical_message = f"Failed to update Teams message ({response.status_code}): {response.text}"

            raise TeamsMessageSendException(
                message=technical_message,
                error_code=error_code,
                status_code=response.status_code,
                response_text=response.text,
            )


async def update_teams_progress_message(
    service_url: str,
    conversation_id: str,
    activity_id: str,
    new_message: dict,
    config: TeamsConnection,
) -> None:
    """Update Teams message with progress information."""
    # Check if message is finalized - don't update if it is
    if is_message_finalized(activity_id):
        logger.debug(f"Skipping progress update for finalized message {activity_id}")
        return

    # Use message-specific lock to prevent race conditions
    if activity_id not in _message_update_locks:
        _message_update_locks[activity_id] = asyncio.Lock()

    async with _message_update_locks[activity_id]:
        # Double-check finalization status inside the lock
        if is_message_finalized(activity_id):
            logger.debug(f"Message {activity_id} was finalized while waiting for lock")
            return

        await _update_teams_message_impl(
            service_url, conversation_id, activity_id, new_message, config
        )


async def update_teams_final_message(
    service_url: str,
    conversation_id: str,
    activity_id: str,
    new_message: dict,
    config: TeamsConnection,
) -> None:
    """Update Teams message with final content and mark as finalized."""
    # Use message-specific lock to ensure atomicity
    if activity_id not in _message_update_locks:
        _message_update_locks[activity_id] = asyncio.Lock()

    async with _message_update_locks[activity_id]:
        await _update_teams_message_impl(
            service_url, conversation_id, activity_id, new_message, config
        )

        # Mark message as finalized to prevent further updates
        mark_message_finalized(activity_id)

    # Schedule cleanup after a delay
    asyncio.create_task(_cleanup_message_after_delay(activity_id, delay=300))


async def _cleanup_message_after_delay(activity_id: str, delay: int = 300) -> None:
    """Clean up message state after a delay."""
    await asyncio.sleep(delay)
    cleanup_message_state(activity_id)


@public_router.post("/teams/webhook")
async def teams_webhook(request: Request) -> Response:
    """Handle incoming Teams webhook events using Bot Framework SDK."""
    from datahub_integrations.teams.adapter import get_bot_adapter

    try:
        adapter = get_bot_adapter()
        return await adapter.process_request(request)
    except Exception as e:
        logger.error(f"Error in Teams webhook: {e}")
        return Response(status_code=500)


# Message handling functions removed - Bot Framework implementation in bot.py is now the primary approach
# The following functions are now handled by DataHubTeamsBot in bot.py:
# - handle_teams_message -> DataHubTeamsBot.on_message_activity
# - handle_teams_mention -> DataHubTeamsBot._handle_mention
# - handle_teams_slash_command -> DataHubTeamsBot._handle_command
# - handle_teams_action_submission -> DataHubTeamsBot._handle_action_submission


async def handle_url_unfurling(
    activity: TeamsActivity, config: TeamsConnection
) -> None:
    """Handle URL unfurling for DataHub entities."""
    # This function may still be needed for URL unfurling functionality
    conversation_id = get_conversation_id(activity)
    if conversation_id is None:
        logger.error("Could not get conversation ID for URL unfurling, cannot process")
        return

    # Extract URN from URL
    if not activity.text:
        logger.info("No text in activity, skipping unfurling")
        return
    urn = extract_urn_from_url(activity.text)
    logger.info(f"Extracted URN from text: {urn}")
    if not urn:
        logger.info("No URN found in text, skipping unfurling")
        return

    try:
        # Get entity details
        variables = {"urn": urn}
        data = graph.execute_graphql(SLACK_GET_ENTITY_QUERY, variables=variables)
        raw_entity = data.get("entity")

        if not raw_entity or not raw_entity.get("properties"):
            return

        # Create Teams card for the entity
        card = render_entity_card(raw_entity)
        logger.info(f"Successfully created entity card for URN: {urn}")

        response = {"type": "message", "attachments": [card]}

        if not activity.serviceUrl:
            logger.error("No serviceUrl in activity, cannot send message")
            return
        await send_teams_message(
            service_url=activity.serviceUrl,
            conversation_id=conversation_id,
            message=response,
            config=config,
        )
        logger.info(f"Successfully sent unfurled card for URN: {urn}")

    except Exception as e:
        logger.error(f"Error unfurling URL: {e}")


async def handle_teams_invoke(activity: TeamsActivity) -> Optional[Response]:
    """Handle Teams invoke actions (button clicks, unfurl requests, etc.)."""

    logger.debug(f"Teams invoke action: {activity.action}")
    logger.debug(f"Teams invoke name: {activity.name}")

    # Handle URL unfurling requests from compose extensions
    if activity.name == "composeExtension/queryLink":
        logger.info("Processing URL unfurling request from compose extension")

        # Extract URL from the invoke value
        url = None
        if activity.value and "url" in activity.value:
            url = activity.value["url"]

        if url:
            logger.info(f"Attempting to unfurl URL from invoke request: {url}")

            # Extract URN from URL using our flexible matching
            urn = extract_urn_from_url(url)
            logger.info(f"Extracted URN from URL: {urn}")

            if not urn:
                logger.info("No URN found in URL, skipping unfurling")
                return None

            try:
                # Get entity details from DataHub
                variables = {"urn": urn}
                data = graph.execute_graphql(
                    SLACK_GET_ENTITY_QUERY, variables=variables
                )
                raw_entity = data.get("entity")

                if not raw_entity or not raw_entity.get("properties"):
                    logger.warning("No entity data found for URN")
                    return None

                # Create adaptive card for the entity
                card = render_entity_card(raw_entity)
                logger.info(f"Successfully created entity card for URN: {urn}")

                # Return composeExtension response for unfurling
                unfurl_response = {
                    "composeExtension": {
                        "type": "result",
                        "attachmentLayout": "list",
                        "attachments": [
                            {
                                "contentType": "application/vnd.microsoft.card.adaptive",
                                "content": card["content"],
                                "preview": {
                                    "contentType": "application/vnd.microsoft.card.thumbnail",
                                    "content": {
                                        "title": raw_entity.get("properties", {}).get(
                                            "name", "DataHub Entity"
                                        ),
                                        "subtitle": f"From {raw_entity.get('platform', {}).get('properties', {}).get('displayName', 'Unknown Platform')}",
                                        "text": raw_entity.get("properties", {}).get(
                                            "description", ""
                                        ),
                                    },
                                },
                            }
                        ],
                    }
                }

                logger.info("Successfully created compose extension unfurl response")
                return Response(
                    content=json.dumps(unfurl_response),
                    status_code=200,
                    media_type="application/json",
                )

            except Exception as e:
                logger.error(f"Error processing unfurl invoke: {e}")
                return None

    # Handle other invoke types as needed
    logger.info(f"Unhandled invoke type: {activity.name}")
    return None


@private_router.post("/teams/reload_credentials")
def reload_teams_credentials() -> None:
    """Reload Teams credentials from GMS."""
    teams_config.reload()


# Search and utility endpoints
class ChannelSearchRequest(BaseModel):
    query: str
    limit: int = 25
    team_ids: Optional[str] = None


class UserSearchRequest(BaseModel):
    query: str
    limit: int = 25


class SearchRequest(BaseModel):
    query: str
    limit: int = 25
    type: str = "all"
    team_ids: Optional[str] = None


@private_router.get("/teams/search/channels", response_model=SearchResponse)
async def search_channels_get(
    query: str, limit: int = 25, team_ids: Optional[str] = None
) -> SearchResponse:
    """Search for Teams channels (GET endpoint)."""
    try:
        team_ids_list = team_ids.split(",") if team_ids else []
        search_service = get_search_service()
        return await search_service.search_channels(
            query=query, limit=limit, team_ids=team_ids_list
        )
    except Exception as e:
        logger.error(f"Error searching channels: {e}")
        return SearchResponse(
            results=[],
            hasMore=False,
            totalCount=0,
        )


@private_router.post("/teams/search/channels", response_model=SearchResponse)
async def search_channels_post(request: ChannelSearchRequest) -> SearchResponse:
    """Search for Teams channels (POST endpoint)."""
    try:
        team_ids_list = request.team_ids.split(",") if request.team_ids else []
        search_service = get_search_service()
        return await search_service.search_channels(
            query=request.query, limit=request.limit, team_ids=team_ids_list
        )
    except Exception as e:
        logger.error(f"Error searching channels: {e}")
        return SearchResponse(
            results=[],
            hasMore=False,
            totalCount=0,
        )


@private_router.get("/teams/channels/list", response_model=SearchResponse)
async def list_all_channels(limit: int = 100) -> SearchResponse:
    """List all available channels from all accessible teams."""
    try:
        search_service = get_search_service()
        return await search_service.list_all_channels(limit=limit)
    except Exception as e:
        logger.error(f"Error listing channels: {e}")
        return SearchResponse(
            results=[],
            hasMore=False,
            totalCount=0,
        )


@private_router.get("/teams/search/users", response_model=SearchResponse)
async def search_users_get(query: str, limit: int = 25) -> SearchResponse:
    """Search for users (GET endpoint)."""
    try:
        search_service = get_search_service()
        return await search_service.search_users(query=query, limit=limit)
    except Exception as e:
        logger.error(f"Error searching users: {e}")
        return SearchResponse(
            results=[],
            hasMore=False,
            totalCount=0,
        )


@private_router.post("/teams/search/users", response_model=SearchResponse)
async def search_users_post(request: UserSearchRequest) -> SearchResponse:
    """Search for users (POST endpoint)."""
    try:
        search_service = get_search_service()
        return await search_service.search_users(
            query=request.query, limit=request.limit
        )
    except Exception as e:
        logger.error(f"Error searching users: {e}")
        return SearchResponse(
            results=[],
            hasMore=False,
            totalCount=0,
        )


@private_router.get("/teams/search", response_model=SearchResponse)
async def search_all_get(
    query: str, limit: int = 25, type: str = "all", team_ids: Optional[str] = None
) -> SearchResponse:
    """Search for users and/or channels (GET endpoint)."""
    try:
        team_ids_list = team_ids.split(",") if team_ids else []
        search_service = get_search_service()

        if type == "channels":
            return await search_service.search_channels(
                query=query, limit=limit, team_ids=team_ids_list
            )
        elif type == "users":
            return await search_service.search_users(query=query, limit=limit)
        else:
            # Search both users and channels
            channels_result = await search_service.search_channels(
                query=query, limit=limit // 2, team_ids=team_ids_list
            )
            users_result = await search_service.search_users(
                query=query, limit=limit // 2
            )

            combined_results = channels_result.results + users_result.results
            return SearchResponse(
                results=combined_results,
                hasMore=channels_result.hasMore or users_result.hasMore,
                totalCount=channels_result.totalCount + users_result.totalCount,
            )

    except Exception as e:
        logger.error(f"Error searching: {e}")
        return SearchResponse(
            results=[],
            hasMore=False,
            totalCount=0,
        )


@private_router.post("/teams/search", response_model=SearchResponse)
async def search_all_post(request: SearchRequest) -> SearchResponse:
    """Search for users and/or channels (POST endpoint)."""
    try:
        team_ids_list = request.team_ids.split(",") if request.team_ids else []
        search_service = get_search_service()

        if request.type == "channels":
            return await search_service.search_channels(
                query=request.query, limit=request.limit, team_ids=team_ids_list
            )
        elif request.type == "users":
            return await search_service.search_users(
                query=request.query, limit=request.limit
            )
        else:
            # Search both users and channels
            channels_result = await search_service.search_channels(
                query=request.query, limit=request.limit // 2, team_ids=team_ids_list
            )
            users_result = await search_service.search_users(
                query=request.query, limit=request.limit // 2
            )

            combined_results = channels_result.results + users_result.results
            return SearchResponse(
                results=combined_results,
                hasMore=channels_result.hasMore or users_result.hasMore,
                totalCount=channels_result.totalCount + users_result.totalCount,
            )

    except Exception as e:
        logger.error(f"Error searching: {e}")
        return SearchResponse(
            results=[],
            hasMore=False,
            totalCount=0,
        )


@private_router.get("/teams/permissions/check")
async def check_teams_permissions() -> Dict[str, Any]:
    """Check Microsoft Graph API permissions for Teams integration."""
    try:
        config = teams_config.get_config()
        if not config or not config.app_details:
            return {"error": "Teams configuration not available"}

        # Get access token and check permissions
        if not config.app_details or not all(
            [
                config.app_details.app_id,
                config.app_details.app_password,
                config.app_details.tenant_id,
            ]
        ):
            return {"error": "Teams app credentials not configured properly"}

        # mypy type narrowing
        assert config.app_details.app_id is not None
        assert config.app_details.app_password is not None
        assert config.app_details.tenant_id is not None

        access_token = await get_azure_ad_token(
            config.app_details.app_id,
            config.app_details.app_password,
            config.app_details.tenant_id,
        )

        # Test various Microsoft Graph API endpoints
        headers = {"Authorization": f"Bearer {access_token}"}
        permissions_status = {}

        async with httpx.AsyncClient() as client:
            # Test teams access
            try:
                response = await client.get(
                    "https://graph.microsoft.com/v1.0/teams", headers=headers
                )
                permissions_status["teams"] = {
                    "status": "success" if response.status_code == 200 else "limited",
                    "status_code": response.status_code,
                }
            except Exception as e:
                permissions_status["teams"] = {"status": "error", "error": str(e)}

        return {
            "app_id": config.app_details.app_id,
            "tenant_id": config.app_details.tenant_id,
            "permissions": permissions_status,
        }

    except Exception as e:
        logger.error(f"Error checking Teams permissions: {e}")
        return {"error": f"Permission check failed: {str(e)}"}


@private_router.get("/teams/permissions/list")
async def list_teams_permissions() -> Dict[str, Any]:
    """List the actual permissions granted to this Teams application."""
    try:
        config = teams_config.get_config()
        if not config or not config.app_details:
            return {"error": "Teams configuration not available"}

        # This would require additional Microsoft Graph API calls to get detailed permissions
        # For now, return basic information
        return {
            "app_id": config.app_details.app_id,
            "tenant_id": config.app_details.tenant_id,
            "note": "Detailed permission listing requires additional Graph API integration",
        }

    except Exception as e:
        logger.error(f"Error listing Teams permissions: {e}")
        return {"error": f"Permission listing failed: {str(e)}"}


@private_router.get("/teams/discover")
async def discover_teams_resources() -> Dict[str, Any]:
    """Discover available Teams resources (teams, channels) for debugging."""
    try:
        config = teams_config.get_config()
        if not config or not config.app_details:
            return {"error": "Teams configuration not available"}

        # Get access token
        if not config.app_details or not all(
            [
                config.app_details.app_id,
                config.app_details.app_password,
                config.app_details.tenant_id,
            ]
        ):
            return {"error": "Teams app credentials not configured properly"}

        # mypy type narrowing
        assert config.app_details.app_id is not None
        assert config.app_details.app_password is not None
        assert config.app_details.tenant_id is not None

        access_token = await get_azure_ad_token(
            config.app_details.app_id,
            config.app_details.app_password,
            config.app_details.tenant_id,
        )

        resources = {}
        headers = {"Authorization": f"Bearer {access_token}"}

        async with httpx.AsyncClient() as client:
            # Discover teams
            try:
                response = await client.get(
                    "https://graph.microsoft.com/v1.0/teams", headers=headers
                )
                if response.status_code == 200:
                    resources["teams"] = response.json()
                else:
                    resources["teams"] = {"error": f"Status {response.status_code}"}
            except Exception as e:
                resources["teams"] = {"error": str(e)}

        return resources

    except Exception as e:
        logger.error(f"Error discovering Teams resources: {e}")
        return {"error": f"Resource discovery failed: {str(e)}"}


# OAuth and configuration endpoints continue...
# (The rest of the OAuth endpoints would be included here but truncated for brevity)


@private_router.post("/teams/oauth/config")
async def get_teams_oauth_config() -> Dict[str, Any]:
    """Get OAuth configuration for Teams integration setup"""
    return {
        "app_id": os.getenv("DATAHUB_TEAMS_APP_ID"),
        "redirect_uri": os.getenv(
            "DATAHUB_TEAMS_OAUTH_REDIRECT_URI",
            "https://router.datahub.com/public/teams/oauth/callback",
        ),
        "scopes": "openid profile https://graph.microsoft.com/.default offline_access",
        "base_auth_url": "https://login.microsoftonline.com",
    }


async def exchange_oauth_code_for_tokens(
    authorization_code: str,
    app_id: str,
    app_secret: str,
    redirect_uri: str,
    tenant_id: str,
) -> Dict[str, Any]:
    """Exchange authorization code for Microsoft Graph access tokens"""
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "client_id": app_id,
        "client_secret": app_secret,
        "code": authorization_code,
        "grant_type": "authorization_code",
        "redirect_uri": redirect_uri,
        "scope": "openid profile https://graph.microsoft.com/.default offline_access",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        logger.info(f"🌐 Making token request to: {token_url}")
        async with httpx.AsyncClient() as client:
            response = await client.post(
                token_url, data=data, headers=headers, timeout=30.0
            )
            logger.info(f"📡 Token response status: {response.status_code}")

            if response.status_code == 200:
                token_data = response.json()

                # Extract user info and tenant ID from id_token if available
                extracted_tenant_id = None
                extracted_user_info = {}
                if "id_token" in token_data:
                    try:
                        # Parse JWT payload to extract user and tenant info
                        import base64
                        import json as json_lib

                        # JWT has 3 parts separated by dots
                        jwt_parts = token_data["id_token"].split(".")
                        if len(jwt_parts) >= 2:
                            # Decode payload (add padding if needed)
                            payload_b64 = jwt_parts[1]
                            payload_b64 += "=" * (4 - len(payload_b64) % 4)
                            payload_bytes = base64.b64decode(payload_b64)
                            payload = json_lib.loads(payload_bytes.decode("utf-8"))

                            extracted_user_info = {
                                "id": payload.get("oid"),  # Azure AD user ID
                                "displayName": payload.get("name"),
                                "mail": payload.get("email"),
                                "userPrincipalName": payload.get("upn"),
                                "tenant_id": payload.get("tid"),
                            }
                            extracted_tenant_id = payload.get("tid")
                            logger.info(
                                f"✅ Extracted user info from JWT: {extracted_user_info}"
                            )
                    except Exception as e:
                        logger.warning(f"Could not parse JWT token: {e}")

                # Return combined token data with user info
                return {
                    "access_token": token_data.get("access_token"),
                    "refresh_token": token_data.get("refresh_token"),
                    "expires_in": token_data.get("expires_in", 3600),
                    "tenant_id": extracted_tenant_id,
                    "user_info": extracted_user_info,
                }
            else:
                logger.error(
                    f"Token exchange failed: {response.status_code} - {response.text}"
                )
                return {"error": response.text}

    except Exception as e:
        logger.error(f"Error during token exchange: {e}")
        return {"error": str(e)}


async def get_teams_user_info(access_token: str) -> Dict[str, Any]:
    """Get user information from Microsoft Graph API"""
    logger.info("📊 Getting user info from Microsoft Graph")

    try:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            # Get basic user info
            response = await client.get(
                "https://graph.microsoft.com/v1.0/me", headers=headers
            )

            if response.status_code == 200:
                user_data = response.json()
                azure_user_id = user_data.get("id")
                logger.info(f"✅ Retrieved user info from Graph API: {azure_user_id}")

                # Try to get Teams-specific user ID
                teams_user_id = None
                try:
                    # Get user's joined teams to extract Teams user ID
                    teams_response = await client.get(
                        "https://graph.microsoft.com/v1.0/me/joinedTeams",
                        headers=headers,
                    )
                    if teams_response.status_code == 200:
                        teams_data = teams_response.json()
                        if teams_data.get("value"):
                            first_team_id = teams_data["value"][0].get("id")
                            if first_team_id:
                                # Get team members to find this user's Teams ID
                                members_response = await client.get(
                                    f"https://graph.microsoft.com/v1.0/teams/{first_team_id}/members",
                                    headers=headers,
                                )
                                if members_response.status_code == 200:
                                    members_data = members_response.json()
                                    for member in members_data.get("value", []):
                                        if member.get("userId") == azure_user_id:
                                            teams_user_id = member.get("id")
                                            logger.info(
                                                f"Found Teams user ID: {teams_user_id}"
                                            )
                                            break
                except Exception as e:
                    logger.warning(f"Could not get Teams-specific user ID: {e}")

                user_data["teams_user_id"] = teams_user_id
                return user_data
            else:
                logger.error(
                    f"Failed to get user info: {response.status_code} - {response.text}"
                )
                return {}

    except Exception as e:
        logger.error(f"Error getting user info: {e}")
        return {}


async def store_user_teams_settings(
    user_urn: str,
    azure_user_id: str,
    teams_user_id: Optional[str] = None,
    display_name: Optional[str] = None,
    email: Optional[str] = None,
    tenant_id: Optional[str] = None,
) -> bool:
    """Store Teams user information in user's notification settings"""
    logger.debug(
        f"Storing Teams settings for user: {user_urn}, azure_user_id: {azure_user_id}, teams_user_id: {teams_user_id}"
    )

    try:
        # Import required models
        import time

        from datahub.metadata.com.linkedin.pegasus2avro.identity import (
            CorpUserAppearanceSettingsClass,
            CorpUserSettingsClass,
        )
        from datahub.metadata.schema_classes import (
            NotificationSettingsClass,
            TeamsNotificationSettingsClass,
            TeamsUserClass,
        )

        # Get existing CorpUserSettings
        existing_corp_settings = graph.get_aspect(
            entity_urn=user_urn, aspect_type=CorpUserSettingsClass
        )

        if existing_corp_settings:
            corp_settings = existing_corp_settings
            notification_settings = (
                corp_settings.notificationSettings
                or NotificationSettingsClass(sinkTypes=[])
            )
        else:
            # Create default corp user settings
            corp_settings = CorpUserSettingsClass(
                appearance=CorpUserAppearanceSettingsClass(showSimplifiedHomepage=False)
            )
            notification_settings = NotificationSettingsClass(sinkTypes=[])

        # Create TeamsUser with available information
        teams_user = TeamsUserClass(
            azureUserId=azure_user_id,
            teamsUserId=teams_user_id,
            displayName=display_name,
            email=email,
            lastUpdated=int(time.time() * 1000),
        )

        teams_settings = TeamsNotificationSettingsClass(
            user=teams_user,
            channels=[],  # Personal notifications don't use channels
        )

        # Update notification settings
        notification_settings.teamsSettings = teams_settings
        corp_settings.notificationSettings = notification_settings

        # Save updated settings
        from datahub.emitter.mcp import MetadataChangeProposalWrapper

        # Create MCP and emit
        mcp = MetadataChangeProposalWrapper(
            entityUrn=user_urn,
            aspect=corp_settings,
        )

        graph.emit(mcp)
        logger.info(f"✅ Successfully stored Teams settings for user: {user_urn}")
        return True

    except Exception as e:
        logger.error(f"Error storing Teams settings: {e}")
        return False


async def handle_personal_notifications_oauth(
    code: str,
    user_urn: Optional[str],
    redirect_url: Optional[str],
    state: Optional[str] = None,
) -> Union[fastapi.responses.RedirectResponse, Dict[str, Any]]:
    """Handle OAuth completion for personal notifications flow"""
    from .exceptions import TeamsErrorCodes, TeamsOAuthException

    logger.info(f"🔐 Handling personal notifications OAuth for user: {user_urn}")

    # Validate required parameters
    if not user_urn:
        raise TeamsOAuthException(
            message="user_urn parameter is required for Teams OAuth flow",
            error_code=TeamsErrorCodes.MISSING_USER_URN,
            redirect_url=redirect_url,
            user_friendly_message="User authentication information is missing. Please log in and try again.",
        )

    try:
        # Get app credentials
        app_id = os.environ.get("DATAHUB_TEAMS_APP_ID")
        app_secret = os.environ.get("DATAHUB_TEAMS_APP_PASSWORD")
        if not app_id or not app_secret:
            raise TeamsOAuthException(
                message="DATAHUB_TEAMS_APP_ID or DATAHUB_TEAMS_APP_PASSWORD environment variables not configured",
                error_code=TeamsErrorCodes.MISSING_APP_CREDENTIALS,
                redirect_url=redirect_url,
                user_friendly_message="Teams integration is not properly configured. Please contact your administrator.",
            )

        # Exchange authorization code for tokens
        redirect_uri = os.environ.get(
            "DATAHUB_TEAMS_OAUTH_REDIRECT_URI",
            "https://router.datahub.com/public/teams/oauth/callback",
        )

        # Extract tenant ID from state parameter - REQUIRED
        if not state:
            raise TeamsOAuthException(
                message="No state parameter provided - cannot extract tenant ID",
                error_code=TeamsErrorCodes.OAUTH_INVALID_STATE,
                redirect_url=redirect_url,
                user_friendly_message="Authentication failed due to missing tenant information. Please try again.",
            )

        try:
            import base64
            import json as json_lib

            # Add padding to base64 if needed
            missing_padding = len(state) % 4
            if missing_padding:
                state_padded = state + "=" * (4 - missing_padding)
            else:
                state_padded = state

            state_decoded = base64.b64decode(state_padded).decode("utf-8")
            state_data = json_lib.loads(state_decoded)
            tenant_id_for_token = state_data.get("tenant_id")

            if not tenant_id_for_token:
                raise TeamsOAuthException(
                    message="No tenant_id found in state parameter",
                    error_code=TeamsErrorCodes.OAUTH_INVALID_STATE,
                    redirect_url=redirect_url,
                    user_friendly_message="Authentication failed due to missing tenant information. Please try again.",
                )

            logger.info(f"🔑 Using tenant ID from state: {tenant_id_for_token}")
        except TeamsOAuthException:
            raise
        except Exception as e:
            raise TeamsOAuthException(
                message=f"Failed to decode state parameter: {e}",
                error_code=TeamsErrorCodes.OAUTH_INVALID_STATE,
                redirect_url=redirect_url,
                user_friendly_message="Authentication failed due to invalid tenant information. Please try again.",
            ) from e

        token_result = await exchange_oauth_code_for_tokens(
            authorization_code=code,
            app_id=app_id,
            app_secret=app_secret,
            redirect_uri=redirect_uri,
            tenant_id=tenant_id_for_token,
        )

        access_token = token_result.get("access_token")
        if not access_token:
            raise TeamsOAuthException(
                message="OAuth token exchange succeeded but no access_token in response",
                error_code=TeamsErrorCodes.OAUTH_TOKEN_EXCHANGE_FAILED,
                redirect_url=redirect_url,
                user_friendly_message="Failed to authenticate with Microsoft Teams. Please try again.",
            )

        # Try to get user info from JWT token first, fallback to Graph API if needed
        user_info = token_result.get("user_info", {})
        if user_info.get("id"):
            # Use user info from JWT token (preferred method)
            azure_user_id = user_info.get("id")
            teams_user_id = user_info.get("teams_user_id")
            display_name = user_info.get("displayName")
            email = user_info.get("mail") or user_info.get("userPrincipalName")
            tenant_id = user_info.get("tenant_id") or token_result.get("tenant_id")
            logger.info(
                f"🎯 Using user info from JWT token - Azure ID: {azure_user_id}"
            )
        else:
            # Fallback: Get user info from Microsoft Graph API
            logger.info("⚠️ JWT user info not available, falling back to Graph API call")
            user_info = await get_teams_user_info(access_token)
            azure_user_id = user_info.get("id")
            teams_user_id = user_info.get("teams_user_id")
            display_name = user_info.get("displayName")
            email = user_info.get("mail") or user_info.get("userPrincipalName")
            tenant_id = user_info.get("tenant_id") or token_result.get("tenant_id")

        if not azure_user_id:
            raise TeamsOAuthException(
                message="Microsoft Graph API call succeeded but no user ID in response",
                error_code=TeamsErrorCodes.TEAMS_USER_ID_NOT_FOUND,
                redirect_url=redirect_url,
                user_friendly_message="Failed to retrieve your Teams user information. Please try again.",
            )

        logger.info(
            f"✅ Successfully extracted Teams user info - Azure ID: {azure_user_id}"
        )

        # Store Teams user information in DataHub GMS
        success = await store_user_teams_settings(
            user_urn,
            azure_user_id=azure_user_id,
            teams_user_id=teams_user_id,
            display_name=display_name,
            email=email,
            tenant_id=tenant_id,
        )

        if not success:
            raise TeamsOAuthException(
                message=f"Failed to update notification settings for user {user_urn} with Teams ID {teams_user_id}",
                error_code=TeamsErrorCodes.DATAHUB_SETTINGS_UPDATE_FAILED,
                redirect_url=redirect_url,
                user_friendly_message="Failed to save Teams integration settings. Please try again.",
            )

        logger.info(f"✅ Successfully stored Teams settings for user: {user_urn}")

        # Redirect back to personal notifications with success
        success_redirect = f"{redirect_url or '/settings/personal-notifications'}?teams_oauth=success&teams_user_id={teams_user_id or azure_user_id}"
        logger.info(f"🔄 Redirecting to: {success_redirect}")

        return fastapi.responses.RedirectResponse(url=success_redirect, status_code=302)

    except TeamsOAuthException:
        # Re-raise TeamsOAuthException to be handled by the router
        raise
    except Exception as e:
        # Handle unexpected errors by wrapping in TeamsOAuthException
        logger.error(
            f"Unexpected error in personal notifications OAuth: {e}", exc_info=True
        )
        raise TeamsOAuthException(
            message=f"Unexpected error during OAuth flow: {str(e)}",
            error_code="oauth_unexpected_error",
            redirect_url=redirect_url,
            user_friendly_message="An unexpected error occurred during Teams integration setup. Please try again.",
        ) from e


@public_router.get("/teams/oauth/complete", response_model=None)
async def complete_teams_oauth(
    code: str,
    state: Optional[str] = None,
    flow_type: Optional[str] = None,
    user_urn: Optional[str] = None,
    redirect_url: Optional[str] = None,
) -> Union[fastapi.responses.RedirectResponse, Dict[str, Any]]:
    """Complete Microsoft OAuth flow and save Teams integration configuration"""
    from .exceptions import TeamsOAuthException

    logger.info("🔐 Completing Teams OAuth flow")
    logger.info(
        f"   Authorization code: {code[:20]}..." if code else "   No authorization code"
    )
    logger.info(f"   State: {state}")
    logger.info(f"   Flow type: {flow_type}")

    # Handle personal notifications flow differently
    if flow_type == "personal_notifications":
        logger.info("📱 Handling personal notifications OAuth flow")
        try:
            return await handle_personal_notifications_oauth(
                code, user_urn, redirect_url, state
            )
        except TeamsOAuthException as e:
            # Convert TeamsOAuthException to redirect response
            return e.to_redirect_response()
        except Exception as e:
            logger.error(f"Personal notifications OAuth failed: {e}")
            # Return redirect response on error
            if redirect_url:
                error_url = f"{redirect_url}?teams_oauth=error&message=Personal+notifications+OAuth+failed"
                return fastapi.responses.RedirectResponse(url=error_url)
            else:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Personal notifications OAuth failed",
                ) from e

    try:
        if not code:
            logger.error("Missing authorization code in OAuth callback")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing authorization code",
            )

        # Extract tenant ID and other info from state
        tenant_id = None
        state_data = {}
        if state:
            try:
                import base64
                import json as json_lib

                # Add padding to base64 if needed
                missing_padding = len(state) % 4
                if missing_padding:
                    state_padded = state + "=" * (4 - missing_padding)
                else:
                    state_padded = state
                state_decoded = base64.b64decode(state_padded).decode("utf-8")
                state_data = json_lib.loads(state_decoded)
                tenant_id = state_data.get("tenant_id")
                logger.info(f"🔑 Extracted tenant ID from state: {tenant_id}")
            except Exception as e:
                logger.warning(f"Could not decode state parameter: {e}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid state parameter",
                ) from e

        if not tenant_id:
            logger.error("No tenant ID found in OAuth state")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing tenant ID in OAuth state",
            )

        # Get current Teams configuration
        current_config = teams_config.get_config()

        # Extract app credentials from config (which handles env vars and DataHub storage)
        if (
            not current_config.app_details
            or not current_config.app_details.app_id
            or not current_config.app_details.app_password
        ):
            logger.error(
                "Teams app credentials not configured in DataHub or environment variables"
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Teams app credentials not configured",
            )

        app_id = current_config.app_details.app_id
        app_secret = current_config.app_details.app_password

        # Get redirect URI for token exchange
        redirect_uri = os.environ.get(
            "DATAHUB_TEAMS_OAUTH_REDIRECT_URI",
            "https://router.datahub.com/public/teams/oauth/callback",
        )

        logger.info("🔄 Exchanging OAuth authorization code for tokens")

        # Exchange authorization code for access token
        token_result = await exchange_oauth_code_for_tokens(
            authorization_code=code,
            app_id=app_id,
            app_secret=app_secret,
            redirect_uri=redirect_uri,
            tenant_id=tenant_id,
        )

        if "error" in token_result:
            logger.error(f"Token exchange failed: {token_result['error']}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"OAuth token exchange failed: {token_result['error']}",
            )

        access_token = token_result.get("access_token")
        if not access_token:
            logger.error("No access token received from OAuth exchange")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="OAuth token exchange succeeded but no access token received",
            )

        logger.info("✅ Successfully obtained OAuth access token")

        # Create Teams configuration with the obtained credentials
        teams_connection = TeamsConnection(
            app_details=TeamsAppDetails(
                app_id=app_id,
                app_password=app_secret,
                tenant_id=tenant_id,
            ),
            webhook_url=None,  # Will be set by router or admin
            enable_conversation_history=True,  # Enable by default
        )

        # Save the Teams configuration to DataHub
        logger.info("💾 Saving Teams configuration to DataHub")
        teams_config.save_config(teams_connection)

        logger.info("✅ Teams OAuth integration configured and saved successfully")

        return {
            "status": "success",
            "message": "Teams OAuth integration configured successfully",
            "tenant_id": tenant_id,
            "app_id": app_id,
            "timestamp": datetime.now().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error completing Teams OAuth flow: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to complete OAuth flow: {str(e)}",
        ) from e
