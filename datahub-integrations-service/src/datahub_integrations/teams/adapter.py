"""
Bot Framework adapter for DataHub Teams integration.

This module provides the Bot Framework adapter that connects FastAPI
with the Bot Framework SDK.
"""

import json
from typing import Any, Dict, Optional

from botbuilder.core import (
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
)
from botbuilder.schema import Activity
from fastapi import Request, Response
from loguru import logger

from datahub_integrations.teams.bot import DataHubTeamsBot
from datahub_integrations.teams.config import teams_config
from datahub_integrations.teams.exceptions import (
    TeamsErrorCodes,
    TeamsMessageSendException,
)


class DataHubBotAdapter:
    """
    Adapter that bridges FastAPI webhook with Bot Framework SDK.

    This handles the authentication and message routing between
    Teams webhooks and the Bot Framework ActivityHandler.
    """

    def __init__(self) -> None:
        self._bot = DataHubTeamsBot()
        self._adapter: Optional[BotFrameworkAdapter] = None
        self._initialized = False

    def _ensure_initialized(self) -> None:
        """Ensure the Bot Framework adapter is initialized with current config."""
        if self._initialized:
            return

        config = teams_config.get_config()
        if not config or not config.app_details:
            logger.error("Teams configuration not available for Bot Framework adapter")
            return

        app_details = config.app_details
        if not app_details.app_id or not app_details.app_password:
            logger.error("Teams app credentials not configured")
            return

        # Create Bot Framework adapter settings (following the official pattern)
        settings = BotFrameworkAdapterSettings(
            app_id=app_details.app_id,
            app_password=app_details.app_password,
            channel_auth_tenant=app_details.app_tenant_id,
        )

        logger.debug(
            f"Bot Framework settings: app_id={app_details.app_id}, has_password={bool(app_details.app_password)}"
        )

        # Create the adapter with proper credential provider
        self._adapter = BotFrameworkAdapter(settings)
        self._initialized = True

        # Initialize bot name with app_id - will be updated with display name on first message
        self._bot.set_bot_name_from_config(app_details.app_id)
        logger.info(
            "📝 Bot initialized with app_id - display name will be updated from first message"
        )

        logger.info("Bot Framework adapter initialized successfully")

    async def process_request(self, request: Request) -> Response:
        """
        Process incoming Teams webhook request using Bot Framework SDK.

        Args:
            request: FastAPI request object

        Returns:
            FastAPI response object
        """
        try:
            self._ensure_initialized()

            if not self._adapter:
                logger.error("Bot Framework adapter not initialized")
                return Response(status_code=500)

            # Get request body and parse activity
            body = await request.body()
            # Debug: Log all headers to see what Teams is sending
            logger.debug(f"Incoming Teams webhook headers: {dict(request.headers)}")

            # Note: Teams sends "Authorization" header (capital A)
            auth_header = request.headers.get(
                "Authorization", request.headers.get("authorization", "")
            )
            logger.debug(
                f"Extracted auth header: {'present' if auth_header else 'missing'}"
            )

            # Parse the JSON into an Activity object (following aiohttp integration pattern)
            body_json = body.decode()
            activity_data = json.loads(body_json)
            activity = Activity().deserialize(activity_data)

            # Define bot logic that routes to our bot handlers
            async def bot_logic(turn_context: TurnContext) -> None:
                # Route to appropriate handler based on activity type
                if turn_context.activity.type == "message":
                    await self._bot.on_message_activity(turn_context)
                elif turn_context.activity.type == "invoke":
                    await self._bot.on_invoke_activity(turn_context)
                else:
                    # Handle other activity types as needed
                    logger.debug(
                        f"Unhandled activity type: {turn_context.activity.type}"
                    )

            # Process the activity using Bot Framework adapter (following official pattern)
            try:
                invoke_response = await self._adapter.process_activity(
                    activity, auth_header, bot_logic
                )
            except Exception as e:
                logger.error(f"Bot Framework process_activity failed: {e}")
                # Log more details for debugging
                logger.error(
                    f"Activity type: {activity.type}, Auth header present: {bool(auth_header)}"
                )
                raise

            # Handle invoke responses (for Teams invoke activities)
            if invoke_response:
                return Response(
                    content=invoke_response.body,
                    status_code=invoke_response.status,
                    media_type="application/json",
                )

            return Response(status_code=200)

        except ValueError as e:
            logger.error(f"Invalid activity data in Teams webhook: {e}")
            return Response(status_code=400)
        except Exception as e:
            logger.error(f"Error processing Teams webhook with Bot Framework: {e}")
            return Response(status_code=500)

    async def send_proactive_message(
        self, conversation_reference: Dict[str, Any], message_text: str
    ) -> bool:
        """
        Send a proactive message to a Teams conversation.

        Args:
            conversation_reference: Bot Framework conversation reference
            message_text: Message to send

        Returns:
            True if message was sent successfully
        """
        try:
            self._ensure_initialized()

            if not self._adapter:
                logger.error("Bot Framework adapter not initialized")
                return False

            # Send proactive message using Bot Framework
            async def bot_logic(turn_context: TurnContext) -> None:
                await turn_context.send_activity(message_text)

            await self._adapter.continue_conversation(
                reference=conversation_reference, callback=bot_logic
            )

            return True

        except TeamsMessageSendException as e:
            # Log the detailed error and re-raise so callers can access the user-friendly message
            e.log_and_track()
            raise
        except Exception as e:
            # Check if this is a Bot Framework error that we can map to our exception system
            if "403" in str(e) or "Forbidden" in str(e):
                teams_error = TeamsMessageSendException(
                    message=f"Bot Framework proactive message failed (403): {e}",
                    error_code=TeamsErrorCodes.MESSAGE_SEND_FORBIDDEN,
                    status_code=403,
                    response_text=str(e),
                )
                teams_error.log_and_track()
                raise teams_error from e
            elif "401" in str(e) or "Unauthorized" in str(e):
                teams_error = TeamsMessageSendException(
                    message=f"Bot Framework proactive message failed (401): {e}",
                    error_code=TeamsErrorCodes.MESSAGE_SEND_UNAUTHORIZED,
                    status_code=401,
                    response_text=str(e),
                )
                teams_error.log_and_track()
                raise teams_error from e
            else:
                logger.error(f"Unexpected error sending proactive Teams message: {e}")
                return False


# Global adapter instance
_bot_adapter: Optional[DataHubBotAdapter] = None


def get_bot_adapter() -> DataHubBotAdapter:
    """Get the global Bot Framework adapter instance."""
    global _bot_adapter
    if _bot_adapter is None:
        _bot_adapter = DataHubBotAdapter()
    return _bot_adapter
