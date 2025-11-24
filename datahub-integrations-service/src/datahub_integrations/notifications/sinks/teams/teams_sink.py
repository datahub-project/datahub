import asyncio
import json
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    IncidentNotificationDetailsClass,
    MetadataChangeProposalClass,
    NotificationRecipientClass,
    NotificationRequestClass,
)
from loguru import logger

# DataHubGraph import moved to function scope to avoid circular imports
from datahub_integrations.identity.identity_provider import IdentityProvider
from datahub_integrations.notifications.constants import (
    DATAHUB_BASE_URL,
    MAX_NOTIFICATION_RETRIES,
    STATEFUL_TEAMS_INCIDENT_MESSAGES_ENABLED,
)
from datahub_integrations.notifications.sinks.context import NotificationContext
from datahub_integrations.notifications.sinks.shared.incident_message_builder import (
    IncidentMessageBuilder,
)
from datahub_integrations.notifications.sinks.shared.interactive_buttons import (
    IncidentContext,
    create_incident_buttons,
)
from datahub_integrations.notifications.sinks.sink import NotificationSink
from datahub_integrations.notifications.sinks.teams.error_handling import (
    TeamsError,
    TeamsErrorCategory,
    categorize_teams_error,
    get_retry_delay_for_error,
    log_teams_error,
    should_retry_teams_error,
)
from datahub_integrations.notifications.sinks.teams.template_utils import (
    create_actors_tag_string,
)
from datahub_integrations.notifications.sinks.teams.types import TeamsMessageDetails
from datahub_integrations.notifications.sinks.utils import retry_with_backoff
from datahub_integrations.teams.config import TeamsConnection, teams_config
from datahub_integrations.teams.exceptions import (
    TeamsErrorCodes,
    TeamsMessageSendException,
    map_teams_api_error_to_error_code,
)

# Generate the complete entity card using existing renderer
from datahub_integrations.teams.render.render_entity import (
    EntityCardRenderField,
    render_entity_card,
)
from datahub_integrations.teams.url_utils import get_type_url

# Constants
VIEW_IN_DATAHUB_TITLE = "View in DataHub"


def map_priority_to_readable(raw_priority: Any) -> str:
    """Map numeric priority to readable text, similar to Slack implementation."""
    if raw_priority is None:
        return "Not Set"

    # Handle both string and numeric priorities
    priority_str = str(raw_priority)
    priority_mapping = {
        "0": "Critical",
        "1": "High",
        "2": "Medium",
        "3": "Low",
        "CRITICAL": "Critical",
        "HIGH": "High",
        "MEDIUM": "Medium",
        "LOW": "Low",
    }
    return priority_mapping.get(priority_str, f"Unknown ({raw_priority})")


def map_stage_to_readable(raw_stage: Any) -> str:
    """Map stage value to readable text, similar to Slack implementation."""
    if raw_stage is None:
        return "Not Set"

    stage_mapping = {
        "NO_ACTION_REQUIRED": "No Action Required",
        "TRIAGE": "Triage",
        "INVESTIGATION": "Investigation",
        "WORK_IN_PROGRESS": "Work in Progress",
        "FIXED": "Fixed",
    }
    return stage_mapping.get(str(raw_stage), str(raw_stage))


def map_incident_type_to_readable(raw_type: str) -> str:
    """Map incident type to readable text, similar to Slack implementation."""
    if not raw_type:
        return "Unknown"

    type_mapping = {
        "FRESHNESS": "Freshness",
        "OPERATIONAL": "Operational",
        "DATA_SCHEMA": "Schema",
        "VOLUME": "Volume",
        "FIELD": "Column",
        "SQL": "Custom SQL",
    }
    return type_mapping.get(raw_type, raw_type)


class RetryMode(Enum):
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"


class TeamsNotificationSink(NotificationSink):
    """
    A Microsoft Teams notification sink.
    """

    def __init__(self) -> None:
        self.base_url: Optional[str] = None
        self.graph: Optional[DataHubGraph] = None
        self.identity_provider: Optional[IdentityProvider] = None
        self.teams_connection_config: Optional[TeamsConnection] = None

    def type(self) -> str:
        return (
            "TEAMS"  # Use string for now since PDL classes haven't been generated yet
        )

    def supported_notification_recipient_types(self) -> List[str]:
        return [
            "TEAMS_CHANNEL",
            "TEAMS_DM",
        ]

    def init(self) -> None:
        # Init Teams client.
        from datahub_integrations.app import graph

        self.base_url = DATAHUB_BASE_URL
        self.graph = graph
        self.identity_provider = IdentityProvider(graph)

    def _check_is_teams_config_valid(self, config: Optional[TeamsConnection]) -> bool:
        return (
            config is not None
            and config.app_details is not None
            and config.app_details.app_id is not None
            and config.app_details.app_password is not None
            and config.app_details.tenant_id is not None
        )

    def send(
        self, request: NotificationRequestClass, context: NotificationContext
    ) -> None:
        template_type: str = str(request.message.template)

        # Force refresh config for test notifications
        if template_type == "CUSTOM" and self._is_test_notification(
            request.message.parameters or {}
        ):
            logger.info("Test notification detected - forcing Teams config refresh")
            self._maybe_reload_teams_config(force_refresh=True)
        else:
            # Maybe reload teams config if it's stale (normal behavior)
            self._maybe_reload_teams_config()

        if not self._check_is_teams_config_valid(self.teams_connection_config):
            config_error = TeamsError(
                "Teams configuration is invalid or missing",
                category=TeamsErrorCategory.CONFIGURATION_ERROR,
                details={"config_present": self.teams_connection_config is not None},
                retryable=False,
            )
            log_teams_error(
                config_error,
                operation="configuration_check",
                additional_context={"template_type": template_type},
            )
            logger.warning("Skipping Teams notification due to invalid configuration")
            return

        # Route to appropriate handler based on template type with retry logic
        try:
            max_attempts = MAX_NOTIFICATION_RETRIES

            if template_type == "BROADCAST_NEW_INCIDENT":
                message_details = retry_with_backoff(
                    lambda: asyncio.run(self._send_new_incident_notification(request)),
                    max_attempts=max_attempts,
                    backoff_factor=2,
                    initial_backoff=1,
                )
                if self._should_save_message_details(template_type):
                    # Save message details
                    self._save_message_details(
                        request, template_type, message_details or []
                    )
            elif template_type == "BROADCAST_NEW_INCIDENT_UPDATE":
                retry_with_backoff(
                    lambda: asyncio.run(
                        self._update_new_incident_notifications(request)
                    ),
                    max_attempts=max_attempts,
                    backoff_factor=2,
                    initial_backoff=1,
                )
            elif template_type == "BROADCAST_INCIDENT_STATUS_CHANGE":
                retry_with_backoff(
                    lambda: asyncio.run(
                        self._send_incident_status_change_notification(request)
                    ),
                    max_attempts=max_attempts,
                    backoff_factor=2,
                    initial_backoff=1,
                )
            elif template_type == "BROADCAST_COMPLIANCE_FORM_PUBLISH":
                retry_with_backoff(
                    lambda: asyncio.run(
                        self._send_compliance_form_publish_notification(request)
                    ),
                    max_attempts=max_attempts,
                    backoff_factor=2,
                    initial_backoff=1,
                )
            elif template_type == "BROADCAST_ENTITY_CHANGE":
                retry_with_backoff(
                    lambda: asyncio.run(self._send_entity_change_notification(request)),
                    max_attempts=max_attempts,
                    backoff_factor=2,
                    initial_backoff=1,
                )
            elif template_type == "CUSTOM":
                retry_with_backoff(
                    lambda: asyncio.run(self._send_custom_notification(request)),
                    max_attempts=max_attempts,
                    backoff_factor=2,
                    initial_backoff=1,
                )
            elif template_type == "BROADCAST_NEW_PROPOSAL":
                retry_with_backoff(
                    lambda: asyncio.run(self._send_new_proposal_notification(request)),
                    max_attempts=max_attempts,
                    backoff_factor=2,
                    initial_backoff=1,
                )
            elif template_type == "BROADCAST_PROPOSAL_STATUS_CHANGE":
                retry_with_backoff(
                    lambda: asyncio.run(
                        self._send_proposal_status_change_notification(request)
                    ),
                    max_attempts=max_attempts,
                    backoff_factor=2,
                    initial_backoff=1,
                )
            elif template_type == "BROADCAST_INGESTION_RUN_CHANGE":
                retry_with_backoff(
                    lambda: asyncio.run(
                        self._send_ingestion_run_change_notification(request)
                    ),
                    max_attempts=max_attempts,
                    backoff_factor=2,
                    initial_backoff=1,
                )
            elif template_type == "BROADCAST_ASSERTION_STATUS_CHANGE":
                retry_with_backoff(
                    lambda: asyncio.run(
                        self._send_assertion_status_change_notification(request)
                    ),
                    max_attempts=max_attempts,
                    backoff_factor=2,
                    initial_backoff=1,
                )
            elif template_type == "BROADCAST_NEW_ACTION_WORKFLOW_FORM_REQUEST":
                retry_with_backoff(
                    lambda: asyncio.run(
                        self._send_workflow_form_request_notification(request)
                    ),
                    max_attempts=max_attempts,
                    backoff_factor=2,
                    initial_backoff=1,
                )
            elif (
                template_type == "BROADCAST_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE"
            ):
                retry_with_backoff(
                    lambda: asyncio.run(
                        self._send_workflow_form_status_change_notification(request)
                    ),
                    max_attempts=max_attempts,
                    backoff_factor=2,
                    initial_backoff=1,
                )
            else:
                logger.warning(
                    f"Unsupported notification template type: {template_type}"
                )
                return
        except Exception as e:
            logger.error(
                f"Failed to send Teams notification after {max_attempts} attempts: {e}"
            )
            raise

    async def _send_new_incident_notification(
        self, request: NotificationRequestClass
    ) -> List[TeamsMessageDetails]:
        # Build rich adaptive card for new incident
        adaptive_card = self._build_incident_message(request)
        return await self._send_teams_adaptive_card_with_details(
            request.recipients, adaptive_card
        )

    async def _update_new_incident_notifications(
        self, request: NotificationRequestClass
    ) -> None:
        # First, try to find the messages that need to be updated for the recipients.
        message_details = self._get_saved_message_details(request)

        # Build the new message adaptive card.
        adaptive_card = self._build_incident_update_message(request)

        await self._update_teams_messages(
            message_details,
            adaptive_card,
        )

    async def _send_incident_status_change_notification(
        self, request: NotificationRequestClass
    ) -> None:
        # Build rich adaptive card for incident status change
        adaptive_card = self._build_incident_status_change_message(request)
        await self._send_teams_adaptive_card(request.recipients, adaptive_card)

    async def _send_compliance_form_publish_notification(
        self, request: NotificationRequestClass
    ) -> None:
        # Build Teams message for compliance form
        message_text = self._build_compliance_form_message(request)
        await self._send_teams_message(request.recipients, message_text)
        # Note: Compliance form notifications don't need post-send actions

    async def _send_entity_change_notification(
        self, request: NotificationRequestClass
    ) -> None:
        # Build Teams Adaptive Card for entity change
        adaptive_card = self._build_entity_change_message(request)
        await self._send_teams_adaptive_card_with_request(
            request.recipients, adaptive_card, request
        )

    async def _send_teams_message(
        self, recipients: List[NotificationRecipientClass], message_text: str
    ) -> bool:
        """Send a Teams message to all recipients with retry logic.

        Returns:
            bool: True if all messages were sent successfully, False otherwise
        """
        teams_recipients = self._get_teams_recipients(recipients)
        success = True

        for recipient in teams_recipients:
            try:
                # Apply retry logic to each individual message send
                await self._send_message_with_retry(recipient, message_text)
            except Exception as e:
                logger.error(
                    f"Failed to send Teams message to {recipient} after all retries: {e}"
                )
                success = False

        return success

    async def _send_teams_adaptive_card(
        self, recipients: List[NotificationRecipientClass], adaptive_card: dict
    ) -> None:
        """Send a Teams Adaptive Card to all recipients with retry logic."""
        teams_recipients = self._get_teams_recipients(recipients)

        for recipient in teams_recipients:
            try:
                await self._send_adaptive_card_with_retry(recipient, adaptive_card)
            except Exception as e:
                logger.error(
                    f"Failed to send Teams adaptive card to {recipient} after all retries: {e}"
                )
                raise

    async def _send_teams_adaptive_card_with_request(
        self,
        recipients: List[NotificationRecipientClass],
        adaptive_card: dict,
        request: NotificationRequestClass,
    ) -> None:
        """Send a Teams Adaptive Card to all recipients with notification request for rich activity feed."""
        teams_recipients = self._get_teams_recipients(recipients)

        for recipient in teams_recipients:
            try:
                await self._send_adaptive_card_to_recipient_with_request(
                    recipient, adaptive_card, request
                )
            except Exception as e:
                logger.error(f"Failed to send Teams adaptive card to {recipient}: {e}")
                raise

    def _deserialize_json_list(self, serialized_list: str) -> List[str]:
        """
        Deserialize a JSON string to a list of strings.
        """
        try:
            return json.loads(serialized_list)
        except json.JSONDecodeError:
            return []

    def _parse_teams_recipient_id(self, recipient_id: str) -> Dict[str, str]:
        """Parse structured Teams recipient ID into components.

        Args:
            recipient_id: Either a JSON string with multiple identifiers or a simple string

        Returns:
            Dictionary mapping identifier types to values
        """
        try:
            # Try JSON format first: {"teams":"user-123","azure":"uuid-456","email":"john@example.com"}
            if recipient_id.startswith("{") and recipient_id.endswith("}"):
                return json.loads(recipient_id)
        except json.JSONDecodeError:
            logger.debug(
                f"Failed to parse JSON recipient ID: {recipient_id}, treating as simple ID"
            )

        # Fallback: treat as simple identifier
        # Try to detect type based on format, with Teams IDs taking priority over emails
        if recipient_id.startswith("19:") or recipient_id.startswith("28:"):
            return {"teams": recipient_id}  # Common Teams ID patterns
        elif (
            "@" in recipient_id
            and "." in recipient_id
            and not recipient_id.startswith(("19:", "28:"))
        ):
            return {"email": recipient_id}
        else:
            # Could be Azure ID or Teams ID, default to Teams for backwards compatibility
            return {"teams": recipient_id}

    def _extract_user_guid_from_recipient(self, recipient_id: str) -> Optional[str]:
        """Extract a valid user GUID from recipient ID for Microsoft Graph API calls.

        Args:
            recipient_id: Either a JSON string with multiple identifiers or a simple string

        Returns:
            Valid user GUID string that can be used in Microsoft Graph API calls, or None if not found
        """
        identifiers = self._parse_teams_recipient_id(recipient_id)

        # Try identifiers in priority order: azure (GUID), then teams, then email
        for id_type in ["azure", "teams", "email"]:
            if id_type in identifiers:
                identifier_value = identifiers[id_type]
                # Azure IDs are GUIDs and work directly with Graph API
                if id_type == "azure" and identifier_value:
                    return identifier_value
                # Teams IDs might be GUIDs if they don't have the 19:/28: prefix
                elif id_type == "teams" and identifier_value:
                    # If it's a simple GUID (no prefix), use it
                    if not identifier_value.startswith(("19:", "28:")):
                        return identifier_value
                    # Teams prefixed IDs are not valid for Graph API calls
                    else:
                        continue
                # Email addresses can also be used as user identifiers in Graph API
                elif id_type == "email" and identifier_value:
                    # Don't use emails that are actually Teams IDs misidentified
                    if not identifier_value.startswith(("19:", "28:")):
                        return identifier_value

        # Fallback: if the recipient looks like a GUID, use it directly
        if len(recipient_id) == 36 and "-" in recipient_id:
            return recipient_id

        return None

    async def _send_message_to_recipient(
        self, recipient: str, message_text: str
    ) -> None:
        """Send a message to a specific Teams recipient using appropriate method based on recipient type."""
        if (
            not self.teams_connection_config
            or not self.teams_connection_config.app_details
        ):
            error_msg = "Teams configuration not available for sending notification"
            logger.error(error_msg)
            raise Exception(error_msg)

        try:
            # Determine if this is a channel or user
            if self._is_channel_recipient(recipient):
                # For channels, send via Bot Framework conversation API
                logger.info(f"Sending message to channel {recipient}")
                await self._send_message_to_conversation(recipient, message_text)
            else:
                # For users, parse recipient ID to get all available identifiers
                identifiers = self._parse_teams_recipient_id(recipient)
                logger.info(
                    f"Sending message to user with identifiers: {list(identifiers.keys())}"
                )

                # Try identifiers in priority order: teams, azure, email
                for id_type in ["teams", "azure", "email"]:
                    if id_type in identifiers:
                        identifier_value = identifiers[id_type]
                        logger.debug(f"Trying {id_type} identifier: {identifier_value}")

                        success = await self._try_send_with_identifier(
                            identifier_value, id_type, message_text
                        )

                        if success:
                            logger.info(
                                f"Successfully sent notification using {id_type} identifier"
                            )
                            return

                # If we get here, all identifiers failed
                logger.error(
                    f"Failed to send Teams message using any available identifier: {identifiers}"
                )

        except Exception as e:
            logger.error(f"Failed to send Teams notification to {recipient}: {e}")
            raise

    async def _try_send_with_identifier(
        self, identifier: str, id_type: str, message_text: str
    ) -> bool:
        """Try to send a message using a specific identifier type.

        Args:
            identifier: The user identifier value
            id_type: Type of identifier ('teams', 'azure', or 'email')
            message_text: The message to send

        Returns:
            True if message was sent successfully, False otherwise
        """
        try:
            if id_type == "teams":
                # Try activity feed first, then Bot Framework
                success = await self._send_activity_feed_notification(
                    identifier, message_text
                )
                if success:
                    return True

                # Fallback to direct message
                await self._send_direct_message(identifier, message_text)
                return True

            elif id_type == "azure":
                # For Azure IDs, primarily use Graph API activity feed
                success = await self._send_activity_feed_notification(
                    identifier, message_text
                )
                return success

            elif id_type == "email":
                # For emails, try to resolve to Teams user then send activity feed
                # For now, treat email as potential Teams identifier
                success = await self._send_activity_feed_notification(
                    identifier, message_text
                )
                return success

            else:
                logger.warning(f"Unknown identifier type: {id_type}")
                return False

        except Exception as e:
            logger.debug(
                f"Failed to send with {id_type} identifier '{identifier}': {e}"
            )
            return False

    async def _send_adaptive_card_to_recipient(
        self, recipient: str, adaptive_card: dict
    ) -> None:
        """Send an Adaptive Card to a specific Teams recipient using appropriate method based on recipient type."""
        if (
            not self.teams_connection_config
            or not self.teams_connection_config.app_details
        ):
            error_msg = "Teams configuration not available for sending notification"
            logger.error(error_msg)
            raise Exception(error_msg)

        try:
            # Determine if this is a channel or user
            if self._is_channel_recipient(recipient):
                # For channels, send directly via Bot Framework
                logger.info(f"Sending adaptive card to channel {recipient}")
                await self._send_direct_adaptive_card(recipient, adaptive_card)
            else:
                # For users, use the robust conversation-based approach
                logger.info(f"Sending adaptive card to user {recipient}")
                await self._send_direct_adaptive_card(recipient, adaptive_card)

        except Exception as e:
            logger.error(f"Failed to send Teams adaptive card to {recipient}: {e}")
            raise

    async def _try_send_adaptive_card_with_identifier(
        self, identifier: str, id_type: str, adaptive_card: dict
    ) -> bool:
        """Try to send an adaptive card using a specific identifier type.

        Args:
            identifier: The user identifier value
            id_type: Type of identifier ('teams', 'azure', or 'email')
            adaptive_card: The adaptive card to send

        Returns:
            True if card was sent successfully, False otherwise
        """
        try:
            if id_type == "teams":
                # Try activity feed first, then Bot Framework
                success = await self._send_activity_feed_notification_with_card(
                    identifier, adaptive_card
                )
                if success:
                    return True

                # Fallback to direct adaptive card
                await self._send_direct_adaptive_card(identifier, adaptive_card)
                return True

            elif id_type == "azure":
                # For Azure IDs, primarily use Graph API activity feed
                success = await self._send_activity_feed_notification_with_card(
                    identifier, adaptive_card
                )
                return success

            elif id_type == "email":
                # For emails, try to resolve to Teams user then send activity feed
                success = await self._send_activity_feed_notification_with_card(
                    identifier, adaptive_card
                )
                return success

            else:
                logger.warning(f"Unknown identifier type for adaptive card: {id_type}")
                return False

        except Exception as e:
            logger.debug(
                f"Failed to send adaptive card with {id_type} identifier '{identifier}': {e}"
            )
            return False

    async def _send_adaptive_card_to_recipient_with_request(
        self, recipient: str, adaptive_card: dict, request: NotificationRequestClass
    ) -> None:
        """Send an Adaptive Card to a specific Teams recipient with rich activity feed using request parameters."""
        if (
            not self.teams_connection_config
            or not self.teams_connection_config.app_details
        ):
            error_msg = "Teams configuration not available for sending notification"
            logger.error(error_msg)
            raise Exception(error_msg)

        try:
            # Determine if this is a channel or user
            if self._is_channel_recipient(recipient):
                # For channels, send directly via Bot Framework
                logger.info(f"Sending adaptive card to channel {recipient}")
                await self._send_direct_adaptive_card(recipient, adaptive_card)
            else:
                # For users, try activity feed first with rich information, then fallback to direct message
                logger.info(f"Sending adaptive card to user {recipient}")
                success = await self._send_activity_feed_notification_with_request(
                    recipient, request
                )
                if not success:
                    logger.info(
                        f"Activity feed failed, falling back to direct message with card for user {recipient}"
                    )
                    await self._send_direct_adaptive_card(recipient, adaptive_card)

        except Exception as e:
            logger.error(f"Failed to send Teams adaptive card to {recipient}: {e}")
            raise

    async def _send_activity_feed_notification(
        self, recipient: str, message_text: str
    ) -> bool:
        """Send activity feed notification to a user."""
        try:
            from datahub_integrations.teams.graph_api import GraphApiClient

            if not self.teams_connection_config:
                logger.error(
                    "Teams configuration not available for activity feed notification"
                )
                return False

            # Create Graph API client
            graph_client = GraphApiClient(self.teams_connection_config)

            # Determine activity type based on message content
            activity_type = self._determine_activity_type(message_text)

            # Extract preview text (first 100 characters)
            preview_text = (
                message_text[:100] + "..." if len(message_text) > 100 else message_text
            )

            # Extract template parameters based on activity type
            template_parameters = self._extract_template_parameters(
                activity_type, message_text
            )

            # Send activity feed notification
            success = await graph_client.send_activity_feed_notification(
                user_id=recipient,
                activity_type=activity_type,
                preview_text=preview_text,
                template_parameters=template_parameters,
            )

            return success

        except Exception as e:
            logger.error(
                f"Failed to send activity feed notification to {recipient}: {e}"
            )
            return False

    async def _send_activity_feed_notification_with_request(
        self, recipient: str, request: NotificationRequestClass
    ) -> bool:
        """Send activity feed notification to a user using rich information from notification request."""
        try:
            from datahub_integrations.teams.graph_api import GraphApiClient

            if not self.teams_connection_config:
                logger.error(
                    "Teams configuration not available for activity feed notification"
                )
                return False

            # Create Graph API client
            graph_client = GraphApiClient(self.teams_connection_config)

            # Extract rich information from request parameters
            params = request.message.parameters or {}

            # Generate rich activity feed message
            activity_type, preview_text, template_parameters, web_url = (
                self._build_activity_feed_message(params)
            )

            # Send activity feed notification
            success = await graph_client.send_activity_feed_notification(
                user_id=recipient,
                activity_type=activity_type,
                preview_text=preview_text,
                template_parameters=template_parameters,
                web_url=web_url,
            )

            return success

        except Exception as e:
            logger.error(
                f"Failed to send activity feed notification to {recipient}: {e}"
            )
            return False

    def _determine_activity_type(self, message_text: str) -> str:
        """Determine activity type based on message content."""
        message_lower = message_text.lower()

        if "incident" in message_lower:
            return "incidentNotification"
        elif "dataset" in message_lower or "data" in message_lower:
            return "datasetUpdated"
        elif "compliance" in message_lower:
            return "complianceNotification"
        elif "entity" in message_lower or "changed" in message_lower:
            return "entityChanged"
        else:
            return "generalNotification"

    def _extract_template_parameters(
        self, activity_type: str, message_text: str
    ) -> dict:
        """Extract template parameters based on activity type."""
        if activity_type == "datasetUpdated":
            return {"datasetName": self._extract_entity_name(message_text)}
        elif activity_type == "entityChanged":
            return {"entityName": self._extract_entity_name(message_text)}
        elif activity_type == "incidentNotification":
            return {"incidentTitle": self._extract_incident_title(message_text)}
        elif activity_type == "complianceNotification":
            return {"formName": self._extract_form_name(message_text)}
        elif activity_type == "generalNotification":
            return {"message": message_text}
        else:
            return {"message": message_text}

    def _build_activity_feed_message(
        self, params: dict
    ) -> tuple[str, str, dict, Optional[str]]:
        """Build rich activity feed message from notification parameters."""
        # Extract change information
        actor_name = params.get("actorName", "Someone")
        operation = params.get("operation", "changed")
        modifier_type = params.get("modifierType", "property")
        modifier_name = params.get("modifier0Name", "")

        # Extract entity information
        entity_name = self._extract_entity_name_from_params(params)
        platform_name = self._extract_platform_name_from_params(params)
        entity_type = self._extract_entity_type_from_params(params)
        container_breadcrumbs = self._extract_container_breadcrumbs_from_params(params)

        # Build entity context for template parameter
        full_entity_path = self._build_full_entity_path(
            platform_name, entity_type, container_breadcrumbs, entity_name
        )

        # Build action context for preview text
        preview_text = self._format_action_message(
            actor_name, operation, modifier_type, modifier_name
        )

        # Determine activity type based on operation and modifier
        activity_type = self._determine_activity_type_from_params(
            operation, modifier_type
        )

        # Build template parameters with entity context
        template_parameters = self._build_template_parameters_from_params(
            activity_type, params, full_entity_path
        )

        # Extract entity URL for clickable notification
        # TODO: Teams activity feed webUrl has restrictions - investigate proper deep link format
        entity_url: Optional[str] = (
            None  # Disable for now until we determine correct format
        )

        return activity_type, preview_text, template_parameters, entity_url

    def _extract_entity_name_from_params(self, params: dict) -> str:
        """Extract entity name from notification parameters."""
        # Try to get entity name from parameters
        entity_name = params.get("entityName", "")
        if entity_name:
            return entity_name

        # Try to extract from entity path
        entity_path = params.get("entityPath", "")
        if entity_path:
            # Extract entity name from path like "/dataset/urn%3Ali%3Adataset%3A%28..."
            import urllib.parse

            try:
                if "/dataset/" in entity_path:
                    urn_part = entity_path.split("/dataset/")[1].split("?")[0]
                    decoded_urn = urllib.parse.unquote(urn_part)
                    return self._extract_entity_name(decoded_urn)
            except Exception:
                pass

        # Fallback to platform info
        platform = params.get("entityPlatform", "")
        if platform:
            return f"{platform} entity"

        return "entity"

    def _extract_platform_name_from_params(self, params: dict) -> str:
        """Extract platform name from notification parameters."""
        platform = params.get("entityPlatform", "")
        if platform:
            return platform.title()  # Capitalize first letter
        return ""

    def _extract_entity_type_from_params(self, params: dict) -> str:
        """Extract entity type from notification parameters."""
        entity_type = params.get("entityType", "")
        if entity_type:
            # Convert from DATASET to table, CHART to chart, etc.
            type_mapping = {
                "DATASET": "table",
                "CHART": "chart",
                "DASHBOARD": "dashboard",
                "DATA_JOB": "job",
                "DATA_FLOW": "flow",
                "CONTAINER": "container",
                "DOMAIN": "domain",
                "DATA_PRODUCT": "product",
                "GLOSSARY_TERM": "term",
                "TAG": "tag",
                "CORP_USER": "user",
                "CORP_GROUP": "group",
            }
            return type_mapping.get(entity_type, entity_type.lower())
        return ""

    def _extract_container_breadcrumbs_from_params(self, params: dict) -> str:
        """Extract container breadcrumbs from notification parameters."""
        # Try to get container info from various parameter fields
        entity_path = params.get("entityPath", "")
        entity_name = params.get("entityName", "")

        # Try to extract from URN if available
        if entity_path and "/dataset/" in entity_path:
            import urllib.parse

            try:
                urn_part = entity_path.split("/dataset/")[1].split("?")[0]
                decoded_urn = urllib.parse.unquote(urn_part)
                return self._extract_breadcrumbs_from_urn(decoded_urn)
            except Exception:
                pass

        # Try to extract from entity name if it contains dots
        if entity_name and "." in entity_name:
            parts = entity_name.split(".")
            if len(parts) > 1:
                return ".".join(parts[:-1])  # All parts except the last one

        return ""

    def _extract_breadcrumbs_from_urn(self, urn: str) -> str:
        """Extract breadcrumbs from URN."""
        try:
            # Example URN: urn:li:dataset:(urn:li:dataPlatform:workday,workday.compensation,PROD)
            if urn.startswith("urn:li:dataset:"):
                # Remove the prefix and get the dataset part
                dataset_part = urn[len("urn:li:dataset:") :]

                # Handle parentheses
                if dataset_part.startswith("(") and dataset_part.endswith(")"):
                    dataset_part = dataset_part[1:-1]

                # Split by commas and get the middle part (dataset name)
                parts = dataset_part.split(",")
                if len(parts) >= 2:
                    dataset_name = parts[1].strip()
                    if "." in dataset_name:
                        name_parts = dataset_name.split(".")
                        if len(name_parts) > 1:
                            return ".".join(
                                name_parts[:-1]
                            )  # All parts except the last one
        except Exception:
            pass

        return ""

    def _build_full_entity_path(
        self,
        platform_name: str,
        entity_type: str,
        container_breadcrumbs: str,
        entity_name: str,
    ) -> str:
        """Build full entity path with platform and container context."""
        # Start with platform and entity type
        parts = []

        if platform_name:
            parts.append(platform_name)

        if entity_type:
            parts.append(entity_type)

        # Add container breadcrumbs and entity name
        if container_breadcrumbs:
            if entity_name:
                full_name = f"{container_breadcrumbs}.{entity_name}"
            else:
                full_name = container_breadcrumbs
        else:
            full_name = entity_name if entity_name else "entity"

        if parts:
            return f"{' '.join(parts)} {full_name}"
        else:
            return full_name

    def _extract_entity_url_from_params(self, params: dict) -> Optional[str]:
        """Extract entity URL from notification parameters."""
        # Try to get entity type and URN from parameters
        entity_type = params.get("entityType", "")
        entity_urn = params.get("entityUrn", "")

        # Try to extract from entity path if not in parameters
        if not entity_urn:
            entity_path = params.get("entityPath", "")
            if entity_path:
                # Extract URN from path like "/dataset/urn%3Ali%3Adataset%3A%28..."
                import urllib.parse

                try:
                    if "/dataset/" in entity_path:
                        urn_part = entity_path.split("/dataset/")[1].split("?")[0]
                        entity_urn = urllib.parse.unquote(urn_part)
                        entity_type = "DATASET"
                    elif "/chart/" in entity_path:
                        urn_part = entity_path.split("/chart/")[1].split("?")[0]
                        entity_urn = urllib.parse.unquote(urn_part)
                        entity_type = "CHART"
                    elif "/dashboard/" in entity_path:
                        urn_part = entity_path.split("/dashboard/")[1].split("?")[0]
                        entity_urn = urllib.parse.unquote(urn_part)
                        entity_type = "DASHBOARD"
                    # Add more entity types as needed
                except Exception:
                    pass

        # Use the existing helper to generate URL
        if entity_type and entity_urn:
            return get_type_url(entity_type, entity_urn)

        # Fallback to building URL from entity path
        entity_path = params.get("entityPath", "")
        if entity_path:
            from datahub_integrations.app import DATAHUB_FRONTEND_URL

            return f"{DATAHUB_FRONTEND_URL}{entity_path}"

        return None

    def _format_change_message(
        self,
        actor_name: str,
        operation: str,
        modifier_type: str,
        modifier_name: str,
        entity_name: str,
        platform_name: str,
        entity_type: str,
        container_breadcrumbs: str,
    ) -> str:
        """Format a human-readable change message with platform and container context."""
        # Get operation verb and emoji
        if operation == "added":
            verb = "added"
            emoji = "➕"
        elif operation == "removed":
            verb = "removed"
            emoji = "➖"
        elif operation == "updated" or operation == "changed":
            verb = "updated"
            emoji = "✏️"
        else:
            verb = "changed"
            emoji = "📝"

        # Format modifier type
        if modifier_type == "tags" or modifier_type == "Tag(s)":
            modifier_desc = f"tag {modifier_name}"
        elif modifier_type == "terms" or modifier_type == "Term(s)":
            modifier_desc = f"term {modifier_name}"
        elif modifier_type == "description":
            modifier_desc = "description"
        elif modifier_type == "schema":
            modifier_desc = "schema"
        elif modifier_type == "ownership":
            modifier_desc = "ownership"
        else:
            modifier_desc = (
                f"{modifier_type} {modifier_name}" if modifier_name else modifier_type
            )

        # Build full entity path with platform and containers
        full_entity_path = self._build_full_entity_path(
            platform_name, entity_type, container_breadcrumbs, entity_name
        )

        # Build message using template: Actor operation modifier modifier-name from/to platform-name subtype/base-type container1-name.container2-name.entity-name
        preposition = (
            "from" if operation == "removed" else "to" if operation == "added" else "on"
        )

        return f"{emoji} {actor_name} {verb} {modifier_desc} {preposition} {full_entity_path}"

    def _format_action_message(
        self,
        actor_name: str,
        operation: str,
        modifier_type: str,
        modifier_name: str,
    ) -> str:
        """Format a concise action message for preview text."""
        # Get operation verb and emoji
        if operation == "added":
            verb = "added"
            emoji = "➕"
        elif operation == "removed":
            verb = "removed"
            emoji = "➖"
        elif operation == "updated" or operation == "changed":
            verb = "updated"
            emoji = "✏️"
        else:
            verb = "changed"
            emoji = "📝"

        # Format modifier type
        if modifier_type == "tags" or modifier_type == "Tag(s)":
            modifier_desc = f"tag {modifier_name}"
        elif modifier_type == "terms" or modifier_type == "Term(s)":
            modifier_desc = f"term {modifier_name}"
        elif modifier_type == "description":
            modifier_desc = "description"
        elif modifier_type == "schema":
            modifier_desc = "schema"
        elif modifier_type == "ownership":
            modifier_desc = "ownership"
        else:
            modifier_desc = (
                f"{modifier_type} {modifier_name}" if modifier_name else modifier_type
            )

        # Build concise action message: "Actor verb modifier"
        return f"{emoji} {actor_name} {verb} {modifier_desc}"

    def _determine_activity_type_from_params(
        self, operation: str, modifier_type: str
    ) -> str:
        """Determine activity type based on operation and modifier type."""
        # For entity changes, use entityChanged for most operations
        if modifier_type in ["tags", "terms", "description", "schema", "ownership"]:
            return "entityChanged"
        elif "dataset" in modifier_type.lower():
            return "datasetUpdated"
        else:
            return "generalNotification"

    def _build_template_parameters_from_params(
        self, activity_type: str, params: dict, full_entity_path: str
    ) -> dict:
        """Build template parameters based on activity type and notification parameters."""
        if activity_type == "entityChanged":
            return {"entityName": full_entity_path}
        elif activity_type == "datasetUpdated":
            return {"datasetName": full_entity_path}
        elif activity_type == "generalNotification":
            return {"message": full_entity_path}
        else:
            return {"message": full_entity_path}

    def _extract_entity_name(self, message_text: str) -> str:
        """Extract entity name from message text."""
        import re

        # Look for URN patterns
        urn_pattern = r"urn:li:\w+:([^,\s]+)"
        match = re.search(urn_pattern, message_text)
        if match:
            return match.group(1)

        # Look for quoted entity names
        quoted_pattern = r'"([^"]+)"'
        match = re.search(quoted_pattern, message_text)
        if match:
            return match.group(1)

        # Fallback to first 50 characters
        return message_text[:50]

    def _extract_incident_title(self, message_text: str) -> str:
        """Extract incident title from message text."""
        lines = message_text.split("\n")
        return lines[0] if lines else message_text[:50]

    def _extract_form_name(self, message_text: str) -> str:
        """Extract form name from message text."""
        import re

        # Look for form-related patterns
        form_pattern = r"form[:\s]+([^\n,]+)"
        match = re.search(form_pattern, message_text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Fallback to first line
        lines = message_text.split("\n")
        return lines[0] if lines else message_text[:50]

    def _is_channel_recipient(self, recipient: str) -> bool:
        """
        Determine if recipient is a channel or user.

        Teams channels typically have format: 19:channelId@thread.teams
        Users have format: Azure AD user ID (UUID format) or JSON format with email/azure fields
        """
        # Check if recipient is a JSON object (user format)
        if recipient.startswith("{") and recipient.endswith("}"):
            return False

        # HACK: Treat specific Teams user ID as a user, not channel
        if recipient == "ZvlMSQ7TPFiXCkay1BLqPVyXH2BNxoTfJHmhM3lRhh0":
            logger.info(
                f"🔧 HACK: Treating Teams user ID {recipient} as a user (not channel)"
            )
            return False

        # Check if it looks like a channel ID (contains @ and thread)
        if (
            "@thread.teams" in recipient
            or "@thread.skype" in recipient
            or "@thread.tacv2" in recipient
        ):
            return True

        # Check if it looks like a UUID (Azure AD user ID)
        import re

        uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        if re.match(uuid_pattern, recipient, re.IGNORECASE):
            return False

        # Default to treating as user if we can't determine (safer for DM notifications)
        return False

    async def _send_direct_message(self, recipient: str, message_text: str) -> None:
        """Send a direct message using hybrid Graph API + Bot Framework approach."""
        try:
            # Parse recipient ID to extract user identifier for conversation lookup
            recipient_info = self._parse_teams_recipient_id(recipient)
            user_id = (
                recipient_info.get("azure") or recipient_info.get("email") or recipient
            )

            # Try to find existing conversation via Microsoft Graph API
            conversation_id = await self._find_existing_bot_conversation(user_id)

            if conversation_id:
                logger.info(
                    f"Found existing conversation for {recipient}: {conversation_id}"
                )
                await self._send_message_to_conversation(conversation_id, message_text)
                logger.info(f"Successfully sent Teams direct message to {recipient}")
                return
            else:
                error_msg = (
                    f"No existing conversation found for {recipient}. "
                    f"User may need to install the bot as a personal app or interact with it first."
                )
                logger.error(error_msg)
                raise Exception(error_msg)

        except Exception as e:
            logger.error(f"Failed to send direct message to {recipient}: {e}")
            raise

    async def _send_direct_adaptive_card(
        self, recipient: str, adaptive_card: dict
    ) -> None:
        """Send an adaptive card via Microsoft Graph Chat API or Bot Framework fallback."""

        # For channels, use Bot Framework directly with channel ID as conversation ID
        if self._is_channel_recipient(recipient):
            try:
                logger.info(
                    f"Sending adaptive card to channel {recipient} via Bot Framework"
                )
                await self._send_adaptive_card_to_conversation(recipient, adaptive_card)
                logger.info(
                    f"Successfully sent Teams adaptive card to channel {recipient}"
                )
                return
            except Exception as e:
                logger.error(
                    f"Failed to send adaptive card to channel {recipient}: {e}"
                )
                raise

        # For users, try Graph Chat API first, then fallback to Bot Framework
        try:
            # Try Microsoft Graph Chat API first (uses Chat.ReadWrite permission)
            logger.info(
                f"Attempting to send via Microsoft Graph Chat API to {recipient}"
            )
            await self._send_graph_chat_message(recipient, adaptive_card)
            logger.info(
                f"Successfully sent Teams adaptive card via Graph API to {recipient}"
            )
            return
        except Exception as e:
            logger.warning(
                f"Graph Chat API failed for {recipient}: {e}, falling back to Bot Framework"
            )

        # Fallback to new hybrid Bot Framework approach
        try:
            # Parse recipient ID to extract user identifier for conversation lookup
            recipient_info = self._parse_teams_recipient_id(recipient)
            user_id = (
                recipient_info.get("azure") or recipient_info.get("email") or recipient
            )

            # Try to find existing conversation via Microsoft Graph API
            conversation_id = await self._find_existing_bot_conversation(user_id)

            if conversation_id:
                logger.info(
                    f"Found existing conversation for {recipient}: {conversation_id}"
                )
                message_id = await self._send_adaptive_card_to_conversation(
                    conversation_id, adaptive_card
                )
                if message_id:
                    logger.info(f"Successfully sent Teams adaptive card to {recipient}")
                    return
                else:
                    logger.error(
                        f"Failed to send Teams adaptive card to {recipient} - no message ID returned"
                    )
                    raise Exception("Failed to send adaptive card to conversation")
            else:
                error_msg = (
                    f"No existing conversation found for {recipient}. "
                    f"User may need to install the bot as a personal app or interact with it first."
                )
                logger.error(error_msg)
                raise Exception(error_msg)

        except Exception as e:
            logger.error(f"Failed to send adaptive card to {recipient}: {e}")
            raise

    async def _send_graph_chat_message(
        self, recipient: str, adaptive_card: dict
    ) -> None:
        """Send adaptive card using Microsoft Graph Chat API (requires Chat.ReadWrite permission)."""
        import httpx

        from datahub_integrations.teams.graph_api import GraphApiClient

        # DEBUG: Log config values for Graph API
        if self.teams_connection_config and self.teams_connection_config.app_details:
            app_details = self.teams_connection_config.app_details
            logger.info(f"DEBUG Graph API - app_id: {app_details.app_id is not None}")
            logger.info(
                f"DEBUG Graph API - app_password: {app_details.app_password is not None}"
            )
            logger.info(f"DEBUG Graph API - tenant_id: {repr(app_details.tenant_id)}")
            logger.info(
                f"DEBUG Graph API - app_tenant_id: {repr(app_details.app_tenant_id)}"
            )
        else:
            logger.info("DEBUG Graph API - No app details found")

        # Get access token for Microsoft Graph
        teams_config = self.teams_connection_config
        if not teams_config:
            raise Exception("No Teams connection config available")

        # Create GraphApiClient and get access token
        graph_client = GraphApiClient(teams_config)
        token = await graph_client._get_graph_access_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        # Create a simple text message with adaptive card as attachment
        # Graph API supports different message formats than Bot Framework
        message_content = self._adaptive_card_to_text_preview(adaptive_card)

        message_payload = {"body": {"contentType": "text", "content": message_content}}

        # Send message directly to user via Graph API
        # Use the chat messages endpoint for the user
        async with httpx.AsyncClient() as client:
            # Try to send a message by creating a chat first
            # Use the app ID directly since we're using application permissions
            # For Microsoft Graph Chat API, just use the plain app ID (not Bot Framework format)
            teams_config = self.teams_connection_config
            if not teams_config or not teams_config.app_details:
                raise Exception("No Teams app configuration available")

            app_user_id = teams_config.app_details.app_id

            chat_payload = {
                "chatType": "oneOnOne",
                "members": [
                    {
                        "@odata.type": "#microsoft.graph.aadUserConversationMember",
                        "user@odata.bind": f"https://graph.microsoft.com/v1.0/users('{app_user_id}')",
                        "roles": ["owner"],
                    },
                    {
                        "@odata.type": "#microsoft.graph.aadUserConversationMember",
                        "user@odata.bind": f"https://graph.microsoft.com/v1.0/users('{recipient}')",
                        "roles": ["owner"],
                    },
                ],
            }

            # Create or get chat
            chat_response = await client.post(
                "https://graph.microsoft.com/v1.0/chats",
                json=chat_payload,
                headers=headers,
                timeout=30.0,
            )

            if chat_response.status_code in [200, 201]:
                chat_data = chat_response.json()
                chat_id = chat_data.get("id")

                # Send message to the chat
                message_response = await client.post(
                    f"https://graph.microsoft.com/v1.0/chats/{chat_id}/messages",
                    json=message_payload,
                    headers=headers,
                    timeout=30.0,
                )

                if message_response.status_code not in [200, 201]:
                    raise Exception(
                        f"Failed to send message: {message_response.status_code} {message_response.text}"
                    )

                logger.info(f"Successfully sent Graph API message to chat {chat_id}")
            else:
                raise Exception(
                    f"Failed to create chat: {chat_response.status_code} {chat_response.text}"
                )

    def _adaptive_card_to_text_preview(self, adaptive_card: dict) -> str:
        """Convert adaptive card to text preview for Graph API message."""
        # Extract text content from adaptive card
        content = adaptive_card.get("content", {})
        body = content.get("body", [])

        text_parts = []
        for item in body:
            if item.get("type") == "TextBlock":
                text = item.get("text", "")
                # Clean up markdown formatting
                text = text.replace("**", "").replace("*", "")
                if text:
                    text_parts.append(text)

        # Add action button URL if available
        actions = content.get("actions", [])
        for action in actions:
            if action.get("type") == "Action.OpenUrl":
                url = action.get("url", "")
                title = action.get("title", VIEW_IN_DATAHUB_TITLE)
                if url:
                    text_parts.append(f"{title}: {url}")

        return "\n".join(text_parts) if text_parts else "DataHub Notification"

    async def _send_activity_feed_notification_with_card(
        self, recipient: str, adaptive_card: dict
    ) -> bool:
        """Send activity feed notification with adaptive card content."""
        try:
            # Extract preview text from adaptive card
            preview_text = self._extract_preview_from_card(adaptive_card)

            # For now, send as regular activity feed notification
            # Activity feed doesn't directly support adaptive cards, so we extract key info
            return await self._send_activity_feed_notification(recipient, preview_text)

        except Exception as e:
            logger.error(
                f"Failed to send activity feed notification with card to {recipient}: {e}"
            )
            return False

    def _extract_preview_from_card(self, adaptive_card: dict | None) -> str:
        """Extract preview text from adaptive card."""
        if not adaptive_card:
            return ""

        try:
            # Navigate through the adaptive card structure to find text content
            # Handle both structures: cards with content.body and direct body
            if "content" in adaptive_card:
                body = adaptive_card["content"].get("body", [])
            else:
                body = adaptive_card.get("body", [])

            # Extract all text from the body (including nested containers)
            def extract_text_recursive(items: list) -> list[str]:
                texts = []
                for item in items:
                    if item.get("type") == "TextBlock":
                        text = item.get("text", "")
                        if text and len(text.strip()) > 0:
                            texts.append(text.strip())
                    elif item.get("type") == "Container":
                        # Recursively extract from container items
                        container_items = item.get("items", [])
                        texts.extend(extract_text_recursive(container_items))
                return texts

            all_texts = extract_text_recursive(body)
            if all_texts:
                # Join all texts with spaces and clean up formatting
                combined_text = " ".join(all_texts)

                # Clean up markdown formatting for preview
                import re

                clean_text = re.sub(
                    r"\*\*(.*?)\*\*", r"\1", combined_text
                )  # Remove bold
                clean_text = re.sub(r"\*(.*?)\*", r"\1", clean_text)  # Remove italic
                clean_text = re.sub(
                    r"[🔄➕➖🚨📢✅📋]", "", clean_text
                )  # Remove emojis
                return clean_text.strip()

            # Fallback to generic message
            return "DataHub notification"

        except Exception as e:
            logger.error(f"Failed to extract preview from adaptive card: {e}")
            return "DataHub notification"

    async def _send_message_with_retry(self, recipient: str, message_text: str) -> None:
        """Send message to recipient with enhanced retry logic and error handling."""
        attempt = 0
        max_attempts = MAX_NOTIFICATION_RETRIES

        while attempt < max_attempts:
            try:
                await self._send_message_to_recipient(recipient, message_text)
                logger.info(
                    f"Successfully sent Teams message to {recipient} (attempt {attempt + 1})"
                )
                return  # Success, exit the retry loop
            except Exception as e:
                # Categorize the error for better handling
                teams_error = categorize_teams_error(
                    e, f"sending message to {recipient}"
                )

                # Log the error with structured information
                log_teams_error(
                    teams_error,
                    recipient=recipient,
                    operation="send_message",
                    additional_context={
                        "attempt": attempt + 1,
                        "max_attempts": max_attempts,
                    },
                )

                # Check if we should retry
                if not should_retry_teams_error(teams_error, attempt, max_attempts):
                    logger.error(
                        f"Non-retryable error sending message to {recipient}: {teams_error}"
                    )
                    raise teams_error from e

                if attempt == max_attempts - 1:
                    logger.error(
                        f"Failed to send message to {recipient} after {max_attempts} attempts"
                    )
                    raise teams_error from e

                # Calculate backoff time based on error type
                backoff_time = get_retry_delay_for_error(teams_error, attempt)
                logger.info(
                    f"Retrying message send to {recipient} in {backoff_time}s (attempt {attempt + 2}/{max_attempts})"
                )
                await asyncio.sleep(backoff_time)
                attempt += 1

    async def _send_adaptive_card_with_retry(
        self, recipient: str, adaptive_card: dict
    ) -> None:
        """Send adaptive card to recipient with enhanced retry logic and error handling."""
        attempt = 0
        max_attempts = MAX_NOTIFICATION_RETRIES

        while attempt < max_attempts:
            try:
                await self._send_adaptive_card_to_recipient(recipient, adaptive_card)
                logger.info(
                    f"Successfully sent Teams adaptive card to {recipient} (attempt {attempt + 1})"
                )
                return  # Success, exit the retry loop
            except Exception as e:
                # Categorize the error for better handling
                teams_error = categorize_teams_error(
                    e, f"sending adaptive card to {recipient}"
                )

                # Log the error with structured information
                log_teams_error(
                    teams_error,
                    recipient=recipient,
                    operation="send_adaptive_card",
                    additional_context={
                        "attempt": attempt + 1,
                        "max_attempts": max_attempts,
                    },
                )

                # Check if we should retry
                if not should_retry_teams_error(teams_error, attempt, max_attempts):
                    logger.error(
                        f"Non-retryable error sending adaptive card to {recipient}: {teams_error}"
                    )
                    raise teams_error from e

                if attempt == max_attempts - 1:
                    logger.error(
                        f"Failed to send adaptive card to {recipient} after {max_attempts} attempts"
                    )
                    raise teams_error from e

                # Calculate backoff time based on error type
                backoff_time = get_retry_delay_for_error(teams_error, attempt)
                logger.info(
                    f"Retrying adaptive card send to {recipient} in {backoff_time}s (attempt {attempt + 2}/{max_attempts})"
                )
                await asyncio.sleep(backoff_time)
                attempt += 1

    def _should_save_message_details(self, template_type: str) -> bool:
        """Check if message details should be saved for this template type."""
        if (
            STATEFUL_TEAMS_INCIDENT_MESSAGES_ENABLED
            and template_type == "BROADCAST_NEW_INCIDENT"
        ):
            return True
        return False

    def _save_message_details(
        self,
        request: NotificationRequestClass,
        template_type: str,
        message_details: List[TeamsMessageDetails],
    ) -> None:
        """Save message details to DataHub for later use (e.g., updating incident messages)."""
        if template_type == "BROADCAST_NEW_INCIDENT":
            request_parameters = request.message.parameters or {}
            incident_urn = request_parameters.get("incidentUrn")
            if incident_urn is not None:
                self._save_new_incident_message_details(incident_urn, message_details)

    def _save_new_incident_message_details(
        self, incident_urn: str, message_details: List[TeamsMessageDetails]
    ) -> None:
        """Save Teams incident message details to DataHub."""
        if len(message_details) != 0 and self.graph:
            self.graph.emit_mcp(
                self._build_message_details_patch_proposal(
                    incident_urn, message_details
                )
            )

    def _build_message_details_patch_proposal(
        self, incident_urn: str, message_details: List[TeamsMessageDetails]
    ) -> MetadataChangeProposalClass:
        """Build metadata patch proposal for Teams message details."""
        patch = MetadataPatchProposal(
            urn=incident_urn,
        )
        for message_detail in message_details:
            # Save the information about the Teams incident notification
            patch._add_patch(
                aspect_name="incidentNotificationDetails",
                op="add",
                path=("teams", "messages", message_detail.message_id),
                value={
                    "conversationId": message_detail.conversation_id,
                    "conversationName": message_detail.conversation_name,
                    "messageId": message_detail.message_id,
                    "recipientId": message_detail.recipient_id,
                },
            )
        return list(patch.build())[0]

    def _get_saved_message_details(
        self,
        request: NotificationRequestClass,
    ) -> List[TeamsMessageDetails]:
        """Retrieve saved Teams message details from DataHub."""
        message_details = []
        # Step 1: Extract the incident urn
        request_parameters = request.message.parameters or {}
        incident_urn = request_parameters.get("incidentUrn")
        if incident_urn is not None:
            # Step 2: Find the recipient ids for the recipients.
            recipient_ids = [recipient.id for recipient in request.recipients]
            # Step 3: Fetch the saved notification details for the recipients
            notification_details = None
            if self.graph:
                notification_details = self.graph.get_aspect(
                    entity_urn=incident_urn,
                    aspect_type=IncidentNotificationDetailsClass,
                )
            # Step 4: Attempt to resolve the ids, to determine which messages should be updated.
            if (
                notification_details is not None
                and hasattr(notification_details, "teams")
                and notification_details.teams is not None
            ):
                sent_messages = notification_details.teams.messages
                for sent_message in sent_messages:
                    # Match based on recipientId or conversationId
                    if (
                        sent_message.recipientId in recipient_ids
                        or sent_message.conversationId in recipient_ids
                        or sent_message.conversationName in recipient_ids
                    ):
                        message_detail = TeamsMessageDetails(
                            conversation_id=sent_message.conversationId,
                            conversation_name=sent_message.conversationName or None,
                            message_id=sent_message.messageId,
                            recipient_id=sent_message.recipientId,
                        )
                        message_details.append(message_detail)
        return message_details

    async def _send_teams_adaptive_card_with_details(
        self, recipients: List[NotificationRecipientClass], adaptive_card: dict
    ) -> List[TeamsMessageDetails]:
        """Send a Teams Adaptive Card to all recipients and return message details."""
        teams_recipients = self._get_teams_recipients(recipients)
        message_details = []

        for recipient in teams_recipients:
            try:
                message_detail = await self._send_adaptive_card_with_retry_and_details(
                    recipient, adaptive_card
                )
                if message_detail:
                    message_details.append(message_detail)
            except Exception as e:
                logger.error(
                    f"Failed to send Teams adaptive card to {recipient} after all retries: {e}"
                )

        return message_details

    async def _send_adaptive_card_with_retry_and_details(
        self, recipient: str, adaptive_card: dict
    ) -> Optional[TeamsMessageDetails]:
        """Send adaptive card to recipient with retry logic and return message details."""
        attempt = 0
        max_attempts = MAX_NOTIFICATION_RETRIES
        backoff_factor = 2
        initial_backoff = 1

        while attempt < max_attempts:
            try:
                message_detail = (
                    await self._send_adaptive_card_to_recipient_with_details(
                        recipient, adaptive_card
                    )
                )
                return message_detail  # Success, return the message details
            except Exception as e:
                if attempt == max_attempts - 1:
                    logger.error(
                        f"Failed to send adaptive card to {recipient} after {max_attempts} attempts: {e}"
                    )
                    raise  # Re-raise the last exception after all retries have failed
                else:
                    backoff_time = initial_backoff * (backoff_factor**attempt)
                    logger.warning(
                        f"Failed to send adaptive card to {recipient} (attempt {attempt + 1}/{max_attempts}), retrying in {backoff_time}s: {e}"
                    )
                    await asyncio.sleep(backoff_time)
                    attempt += 1
        return None

    async def _send_adaptive_card_to_recipient_with_details(
        self, recipient: str, adaptive_card: dict
    ) -> Optional[TeamsMessageDetails]:
        """Send an Adaptive Card to a specific Teams recipient and capture real message details."""
        try:
            # For channels, use recipient directly as conversation ID
            if self._is_channel_recipient(recipient):
                message_id = await self._send_adaptive_card_to_conversation(
                    recipient, adaptive_card
                )
                if message_id:
                    return TeamsMessageDetails(
                        conversation_id=recipient,
                        conversation_name=None,  # Could be enhanced to get actual channel name
                        message_id=message_id,
                        recipient_id=recipient,
                    )
            else:
                # For users, find or create conversation first
                conversation_id = await self._find_existing_bot_conversation(recipient)
                if not conversation_id:
                    conversation_id = await self._create_or_get_conversation(recipient)

                if conversation_id:
                    message_id = await self._send_adaptive_card_to_conversation(
                        conversation_id, adaptive_card
                    )
                    if message_id:
                        return TeamsMessageDetails(
                            conversation_id=conversation_id,
                            conversation_name=None,  # Could be enhanced to get user display name
                            message_id=message_id,
                            recipient_id=recipient,
                        )
                else:
                    logger.warning(f"Could not establish conversation with {recipient}")

            return None
        except Exception as e:
            logger.error(f"Failed to send adaptive card to {recipient}: {e}")
            raise

    async def _update_teams_messages(
        self,
        message_details: List[TeamsMessageDetails],
        adaptive_card: dict,
    ) -> None:
        """Update existing Teams messages with new adaptive card content."""
        for message_detail in message_details:
            try:
                await self._update_teams_message_with_retry(
                    message_detail, adaptive_card
                )
            except Exception as e:
                logger.error(
                    f"Failed to update Teams message {message_detail.message_id}: {e}"
                )

    async def _update_teams_message_with_retry(
        self, message_detail: TeamsMessageDetails, adaptive_card: dict
    ) -> None:
        """Update a Teams message with retry logic."""
        attempt = 0
        max_attempts = MAX_NOTIFICATION_RETRIES
        backoff_factor = 2
        initial_backoff = 1

        while attempt < max_attempts:
            try:
                await self._update_teams_message(message_detail, adaptive_card)
                return  # Success, exit the retry loop
            except Exception as e:
                if attempt == max_attempts - 1:
                    logger.error(
                        f"Failed to update Teams message {message_detail.message_id} after {max_attempts} attempts: {e}"
                    )
                    raise  # Re-raise the last exception after all retries have failed
                else:
                    backoff_time = initial_backoff * (backoff_factor**attempt)
                    logger.warning(
                        f"Failed to update Teams message {message_detail.message_id} (attempt {attempt + 1}/{max_attempts}), retrying in {backoff_time}s: {e}"
                    )
                    await asyncio.sleep(backoff_time)
                    attempt += 1

    async def _update_teams_message(
        self, message_detail: TeamsMessageDetails, adaptive_card: dict
    ) -> None:
        """Update a specific Teams message with new adaptive card content using Bot Framework API."""
        try:
            import httpx

            if (
                not self.teams_connection_config
                or not self.teams_connection_config.app_details
            ):
                logger.error("Teams configuration not available")
                raise Exception("Teams configuration not available")

            app_details = self.teams_connection_config.app_details

            # Get Azure AD token using proper tenant_id
            if not app_details.tenant_id:
                logger.error("Teams tenant_id not configured")
                return None

            token_url = f"https://login.microsoftonline.com/{app_details.tenant_id}/oauth2/v2.0/token"

            data = {
                "grant_type": "client_credentials",
                "client_id": app_details.app_id,
                "client_secret": app_details.app_password,
                "scope": "https://api.botframework.com/.default",
            }

            headers = {"Content-Type": "application/x-www-form-urlencoded"}

            async with httpx.AsyncClient() as client:
                response = await client.post(token_url, data=data, headers=headers)

                if response.status_code != 200:
                    logger.error(
                        f"Failed to get Bot Framework token: {response.status_code} {response.text}"
                    )
                    raise Exception(
                        f"Failed to get Bot Framework token: {response.status_code}"
                    )

                token_data = response.json()
                bot_token = token_data["access_token"]

                # Update the message using Bot Framework API
                # Handle both wrapped and unwrapped adaptive card formats
                if (
                    "contentType" in adaptive_card
                    and adaptive_card["contentType"]
                    == "application/vnd.microsoft.card.adaptive"
                ):
                    # Already properly wrapped (from render_entity_card)
                    attachment = adaptive_card
                else:
                    # Raw adaptive card (from test notifications) - needs wrapping
                    attachment = {
                        "contentType": "application/vnd.microsoft.card.adaptive",
                        "content": adaptive_card,
                    }
                update_payload = {"type": "message", "attachments": [attachment]}

                update_headers = {
                    "Authorization": f"Bearer {bot_token}",
                    "Content-Type": "application/json",
                }

                service_url = "https://smba.trafficmanager.net/apis/v3"
                # Use PUT to update the existing message
                update_url = f"{service_url}/conversations/{message_detail.conversation_id}/activities/{message_detail.message_id}"

                logger.info(
                    f"Updating Teams message {message_detail.message_id} in conversation {message_detail.conversation_id}"
                )

                update_response = await client.put(
                    update_url, json=update_payload, headers=update_headers
                )

                if update_response.status_code in [200, 201, 202]:
                    logger.info(
                        f"Successfully updated Teams message {message_detail.message_id}"
                    )
                else:
                    # Map HTTP status codes to specific error codes for message updates
                    if update_response.status_code == 403:
                        error_code = TeamsErrorCodes.MESSAGE_SEND_FORBIDDEN
                        technical_message = f"Bot not authorized to update message (403): {update_response.text}"
                    elif update_response.status_code == 401:
                        error_code = TeamsErrorCodes.MESSAGE_SEND_UNAUTHORIZED
                        technical_message = f"Authentication failed for update (401): {update_response.text}"
                    elif update_response.status_code == 404:
                        error_code = TeamsErrorCodes.MESSAGE_SEND_NOT_FOUND
                        technical_message = f"Message not found for update (404): {update_response.text}"
                    else:
                        error_code = TeamsErrorCodes.MESSAGE_SEND_FAILED
                        technical_message = f"Failed to update message ({update_response.status_code}): {update_response.text}"

                    teams_exception = TeamsMessageSendException(
                        message=technical_message,
                        error_code=error_code,
                        status_code=update_response.status_code,
                        response_text=update_response.text,
                    )
                    teams_exception.log_and_track()
                    raise teams_exception

        except Exception as e:
            logger.error(
                f"Error updating Teams message {message_detail.message_id}: {e}"
            )
            raise

    async def _create_or_get_conversation(self, teams_id: str) -> Optional[str]:
        """Create or get a conversation ID for a Teams user ID using Bot Framework API."""
        try:
            import httpx

            from datahub_integrations.teams.teams import get_azure_ad_token

            if (
                not self.teams_connection_config
                or not self.teams_connection_config.app_details
            ):
                logger.error(
                    "Teams configuration not available for creating conversation"
                )
                return None

            app_details = self.teams_connection_config.app_details
            if (
                not app_details
                or not app_details.app_id
                or not app_details.app_password
                or not app_details.tenant_id
            ):
                logger.error("Missing Teams app credentials")
                return None
            access_token = await get_azure_ad_token(
                app_details.app_id, app_details.app_password, app_details.tenant_id
            )

            # Create a new conversation with the user using Bot Framework API
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }

            # Use Bot Framework API to create a conversation
            service_url = "https://smba.trafficmanager.net/apis/"
            create_conversation_url = f"{service_url}v3/conversations"

            # Parse the teams_id to get the appropriate member ID for Bot Framework
            # Bot Framework expects the raw identifier, not necessarily a GUID
            identifiers = self._parse_teams_recipient_id(teams_id)

            # For Bot Framework conversation creation, prefer the original format or teams ID
            member_id = teams_id  # Use original format for Bot Framework compatibility
            member_name = teams_id

            # If we have structured identifiers, log for debugging
            if len(identifiers) > 1:
                logger.debug(
                    f"Creating conversation with structured ID {teams_id}, parsed identifiers: {list(identifiers.keys())}"
                )

            # Create conversation payload
            conversation_payload = {
                "bot": {"id": f"28:{app_details.app_id}", "name": "DataHub Bot"},
                "members": [{"id": member_id, "name": member_name}],
                "channelData": {"tenant": {"id": app_details.tenant_id}},
            }

            logger.info(f"Creating conversation with payload: {conversation_payload}")

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    create_conversation_url, json=conversation_payload, headers=headers
                )

                if response.status_code in [200, 201]:
                    conversation_data = response.json()
                    conversation_id = conversation_data.get("id")
                    logger.info(f"Created conversation with ID: {conversation_id}")
                    return conversation_id
                else:
                    logger.error(
                        f"Failed to create conversation: {response.status_code} {response.text}"
                    )
                    return None

        except Exception as e:
            logger.error(f"Failed to create conversation for {teams_id}: {e}")
            return None

    def _build_incident_message(self, request: NotificationRequestClass) -> dict:
        """Build rich adaptive card for incident notifications."""
        return self._build_incident_adaptive_card(request, "🚨 New Incident")

    def _build_incident_update_message(self, request: NotificationRequestClass) -> dict:
        """Build rich adaptive card for incident updates."""
        return self._build_incident_adaptive_card(request, "📢 Incident Update")

    def _build_incident_status_change_message(
        self, request: NotificationRequestClass
    ) -> dict:
        """Build rich adaptive card for incident status changes."""
        params = request.message.parameters or {}
        new_status = params.get("newStatus", "Unknown Status")

        # Use appropriate emoji and title based on status (matching Slack format)
        if new_status == "RESOLVED":
            emoji = "✅"
            title = "Incident Resolved"
        elif new_status == "ACTIVE":
            emoji = "🔴"
            title = "Incident Reopened"
        else:
            emoji = "📝"
            title = "Incident Status Changed"

        return self._build_incident_adaptive_card(request, f"{emoji} {title}")

    def _build_incident_adaptive_card(
        self, request: NotificationRequestClass, title: str
    ) -> dict:
        """Build rich adaptive card for incident notifications with detailed information."""
        params = request.message.parameters or {}

        # Use shared message builder to get incident context
        base_url = self.base_url or "http://localhost:9002"
        context = IncidentMessageBuilder.build_incident_message_context(
            params, base_url
        )

        # Extract additional information needed for Teams
        urn = params.get("incidentUrn", "")

        # Extract owner information for tagging
        owner_urns = self._deserialize_json_list(params.get("owners", "[]"))
        downstream_owner_urns = self._deserialize_json_list(
            params.get("downstreamOwners", "[]")
        )

        # Get actors for owner tagging
        actors: dict[str, Any] = {}
        if (owner_urns or downstream_owner_urns) and self.identity_provider:
            # Use identity provider to resolve actors
            all_owner_urns = set(owner_urns + downstream_owner_urns)
            actors = self.identity_provider.batch_get_actors(all_owner_urns)

        # Build DataHub URL (now using context)
        incident_url = context.incident_url

        # Use shared message builder to get status information
        emoji, status_title, action_verb = (
            IncidentMessageBuilder.get_incident_status_info(
                context.new_status, context.prev_status
            )
        )

        # Build header text using shared logic
        header_text = f"{emoji} {status_title}"

        # Build incident message using shared logic
        incident_message = IncidentMessageBuilder.build_incident_summary_message(
            context, action_verb
        )

        # Get appropriate color using shared logic
        is_resolved = context.new_status == "RESOLVED"
        status_color = IncidentMessageBuilder.get_status_color(is_resolved)

        card_body = [
            {
                "type": "TextBlock",
                "text": header_text,
                "weight": "Bolder",
                "size": "Large",
                "color": status_color,
            },
            {
                "type": "TextBlock",
                "text": incident_message,
                "wrap": True,
                "spacing": "Small",
            },
        ]

        # Add Note if there's a meaningful resolution message (right after main message)
        if (
            context.message
            and context.message.strip()
            and context.message.strip() != "None"
        ):
            card_body.append(
                {
                    "type": "TextBlock",
                    "text": f"**Note**\n{context.message.strip()}",
                    "wrap": True,
                    "spacing": "Medium",
                }
            )

        # Add incident title
        card_body.append(
            {
                "type": "TextBlock",
                "text": f"**{context.incident_title}**",
                "weight": "Bolder",
                "size": "Medium",
                "wrap": True,
                "spacing": "Medium",
            }
        )

        # Add reason right after the title
        if context.incident_description and context.incident_description != "None":
            # Always show the original incident description as "Reason"
            card_body.append(
                {
                    "type": "TextBlock",
                    "text": f"**Reason**\n{context.incident_description}",
                    "wrap": True,
                    "spacing": "Medium",
                }
            )

        # Add Category, Priority, Stage on separate lines with bold labels
        # Add category
        incident_type = params.get("incidentType", "")
        if incident_type:
            readable_type = map_incident_type_to_readable(incident_type)
            card_body.append(
                {
                    "type": "TextBlock",
                    "text": f"**Category**: {readable_type}",
                    "wrap": True,
                    "spacing": "Small",
                }
            )

        # Add priority
        if context.priority:
            readable_priority = map_priority_to_readable(context.priority)
            card_body.append(
                {
                    "type": "TextBlock",
                    "text": f"**Priority**: {readable_priority}",
                    "wrap": True,
                    "spacing": "Small",
                }
            )

        # Add stage
        if context.stage:
            readable_stage = map_stage_to_readable(context.stage)
            card_body.append(
                {
                    "type": "TextBlock",
                    "text": f"**Stage**: {readable_stage}",
                    "wrap": True,
                    "spacing": "Small",
                }
            )

        # Add owner information in two-column layout like Slack
        owner_facts = []

        # Add asset owners (left column)
        if owner_urns and actors:
            owners_raw = [actors.get(urn) for urn in owner_urns if urn in actors]
            owners = [owner for owner in owners_raw if owner is not None]
            if owners:
                owners_str = create_actors_tag_string(
                    owners, self.teams_connection_config
                )
                owner_facts.append(
                    {"title": "Asset Owners:", "value": owners_str or "None"}
                )
        else:
            owner_facts.append({"title": "Asset Owners:", "value": "None"})

        # Add impacted asset count (right column) - moved here since category is now above
        downstream_asset_count = params.get("downstreamAssetCount", "")
        if downstream_asset_count:
            owner_facts.append(
                {"title": "Impacted Asset Count:", "value": str(downstream_asset_count)}
            )
        else:
            owner_facts.append({"title": "Impacted Asset Count:", "value": "None"})

        # Add owner information as a fact set (first row)
        if owner_facts:
            card_body.append(
                {"type": "FactSet", "facts": owner_facts, "spacing": "Medium"}
            )

        # Add second row: Impacted Owners (deduplicated)
        impact_facts = []

        # Add downstream owners (left column) with deduplication
        if downstream_owner_urns and actors:
            downstream_owners_raw = [
                actors.get(urn) for urn in downstream_owner_urns if urn in actors
            ]
            downstream_owners = [
                owner for owner in downstream_owners_raw if owner is not None
            ]
            if downstream_owners and downstream_owner_urns != owner_urns:
                # Deduplicate downstream owners while preserving order
                seen = set()
                deduplicated_downstream_owners = []
                for owner in downstream_owners:
                    # Use a unique identifier for deduplication (email or display name)
                    owner_id = (
                        getattr(owner, "email", None)
                        or getattr(owner, "display_name", None)
                        or str(owner)
                    )
                    if owner_id not in seen:
                        seen.add(owner_id)
                        deduplicated_downstream_owners.append(owner)

                downstream_owners_str = create_actors_tag_string(
                    deduplicated_downstream_owners, self.teams_connection_config
                )
                impact_facts.append(
                    {
                        "title": "Impacted Owners:",
                        "value": downstream_owners_str or "None",
                    }
                )
        else:
            impact_facts.append({"title": "Impacted Owners:", "value": "None"})

        # Add impact information as a fact set (second row)
        if impact_facts:
            card_body.append(
                {"type": "FactSet", "facts": impact_facts, "spacing": "Medium"}
            )

        # Skip adding redundant status information - the title already indicates the status change

        # Create incident context for interactive buttons
        incident_context = IncidentContext(urn=urn, stage=context.stage)

        # Create appropriate interactive buttons based on incident status
        interactive_buttons = create_incident_buttons(
            incident_context, context.new_status, incident_url
        )

        # Convert to Teams format
        actions = [button.to_teams_format() for button in interactive_buttons]

        return {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": card_body,
                "actions": actions,
            },
        }

    def _build_compliance_form_message(self, request: NotificationRequestClass) -> str:
        """Build message text for compliance form notifications."""
        params = request.message.parameters or {}
        return f"📋 New Compliance Form: {params.get('formName', 'Unknown')}"

    async def _send_custom_notification(
        self, request: NotificationRequestClass
    ) -> None:
        """Send custom notification."""
        params = request.message.parameters or {}

        # Check if this is a test notification and send adaptive card instead
        if self._is_test_notification(params):
            logger.info("Detected test notification, sending adaptive card")
            adaptive_card = self._build_test_notification_card(params)
            try:
                await self._send_teams_adaptive_card_with_request(
                    request.recipients, adaptive_card, request
                )
                # Execute post-send actions for test notifications (mark as completed)
                await self._execute_post_send_actions(request, True)
            except Exception as e:
                logger.error(f"Failed to send test notification: {e}")
                # Mark as failed
                await self._execute_post_send_actions(request, False, e)
                raise
        else:
            message_text = self._build_custom_message(request)
            success = await self._send_teams_message(request.recipients, message_text)

            # Check for post-send actions (e.g., test notifications)
            await self._execute_post_send_actions(request, success)

    async def _send_new_proposal_notification(
        self, request: NotificationRequestClass
    ) -> None:
        """Send new proposal notification."""
        adaptive_card = self._build_proposal_message(request)
        await self._send_teams_adaptive_card(request.recipients, adaptive_card)

    async def _send_proposal_status_change_notification(
        self, request: NotificationRequestClass
    ) -> None:
        """Send proposal status change notification."""
        adaptive_card = self._build_proposal_status_change_message(request)
        await self._send_teams_adaptive_card(request.recipients, adaptive_card)

    async def _send_ingestion_run_change_notification(
        self, request: NotificationRequestClass
    ) -> None:
        """Send ingestion run status change notification."""
        adaptive_card = self._build_ingestion_run_change_message(request)
        await self._send_teams_adaptive_card(request.recipients, adaptive_card)

    async def _send_assertion_status_change_notification(
        self, request: NotificationRequestClass
    ) -> None:
        """Send assertion status change notification."""
        adaptive_card = self._build_assertion_status_change_message(request)
        await self._send_teams_adaptive_card(request.recipients, adaptive_card)

    async def _send_workflow_form_request_notification(
        self, request: NotificationRequestClass
    ) -> None:
        """Send workflow form request notification."""
        adaptive_card = self._build_workflow_form_request_message(request)
        await self._send_teams_adaptive_card(request.recipients, adaptive_card)

    async def _send_workflow_form_status_change_notification(
        self, request: NotificationRequestClass
    ) -> None:
        """Send workflow form status change notification."""
        adaptive_card = self._build_workflow_form_status_change_message(request)
        await self._send_teams_adaptive_card(request.recipients, adaptive_card)

    def _build_custom_message(self, request: NotificationRequestClass) -> str:
        """Build message text for custom notifications."""
        params = request.message.parameters or {}
        title = params.get("title", "DataHub Notification")
        body = params.get("body", "")
        return f"**{title}**\n{body}" if body else title

    def _is_test_notification(self, params: dict) -> bool:
        """Check if this is a test notification."""
        request_name = params.get("requestName", "")
        return request_name == "notificationConnectionTest"

    def _build_test_notification_card(self, params: dict) -> dict:
        """Build adaptive card for test notifications with improved formatting and button."""
        title = params.get("title", "Hello from DataHub!")
        body = params.get("body", "")

        # Get frontend URL for the button
        frontend_url = self._get_frontend_url()

        return {
            "type": "AdaptiveCard",
            "body": [
                {
                    "type": "TextBlock",
                    "text": title,
                    "weight": "Bolder",
                    "size": "Medium",
                },
                {"type": "TextBlock", "text": body, "wrap": True, "spacing": "Medium"},
            ],
            "actions": [
                {
                    "type": "Action.OpenUrl",
                    "title": "Go to DataHub",
                    "url": frontend_url,
                }
            ],
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.3",
        }

    def _get_frontend_url(self) -> str:
        """Get the DataHub frontend URL for button links."""
        # Use the same base_url that other methods use for consistency
        return self.base_url or "http://localhost:9002"

    def _build_proposal_message(self, request: NotificationRequestClass) -> dict:
        """Build Teams Adaptive Card for new proposal notifications."""
        params = request.message.parameters or {}

        actor_name = params.get("actorName", "Someone")
        entity_name = params.get("entityName", "UnknownEntity")
        operation = params.get("operation", "add")
        modifier_type = params.get("modifierType", "UnknownModifier")
        entity_path = params.get("entityPath", "")

        # Build entity URL
        entity_url = f"{self.base_url}{entity_path}" if entity_path else ""
        details_url = f"{self.base_url}/requests/proposals"

        return {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": "📬 **New Proposal Raised**",
                        "weight": "Bolder",
                        "size": "Medium",
                        "color": "Accent",
                    },
                    {
                        "type": "TextBlock",
                        "text": f"**{actor_name}** has proposed to {operation} {modifier_type} for {entity_name}",
                        "wrap": True,
                        "spacing": "Medium",
                    },
                ],
                "actions": [
                    {
                        "type": "Action.OpenUrl",
                        "title": "View Proposal Details",
                        "url": details_url,
                    }
                ]
                + (
                    [
                        {
                            "type": "Action.OpenUrl",
                            "title": VIEW_IN_DATAHUB_TITLE,
                            "url": entity_url,
                        }
                    ]
                    if entity_url
                    else []
                ),
            },
        }

    def _build_proposal_status_change_message(
        self, request: NotificationRequestClass
    ) -> dict:
        """Build Teams Adaptive Card for proposal status change notifications."""
        params = request.message.parameters or {}

        actor_name = params.get("actorName", "Someone")
        entity_name = params.get("entityName", "UnknownEntity")
        operation = params.get("operation", "add")
        modifier_type = params.get("modifierType", "UnknownModifier")
        action = params.get("action", "accepted")
        entity_path = params.get("entityPath", "")

        # Build entity URL
        entity_url = f"{self.base_url}{entity_path}" if entity_path else ""
        details_url = f"{self.base_url}/requests/proposals"

        # Determine status emoji and color
        status_emoji = (
            "✅" if action == "accepted" else "❌" if action == "rejected" else "📝"
        )
        status_color = (
            "Good"
            if action == "accepted"
            else "Attention"
            if action == "rejected"
            else "Default"
        )

        return {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": f"{status_emoji} **Proposal Status Changed**",
                        "weight": "Bolder",
                        "size": "Medium",
                        "color": status_color,
                    },
                    {
                        "type": "TextBlock",
                        "text": f"**{actor_name}** has **{action}** the proposal to {operation} {modifier_type} for {entity_name}",
                        "wrap": True,
                        "spacing": "Medium",
                    },
                ],
                "actions": [
                    {
                        "type": "Action.OpenUrl",
                        "title": "View Proposal Details",
                        "url": details_url,
                    }
                ]
                + (
                    [
                        {
                            "type": "Action.OpenUrl",
                            "title": VIEW_IN_DATAHUB_TITLE,
                            "url": entity_url,
                        }
                    ]
                    if entity_url
                    else []
                ),
            },
        }

    def _build_ingestion_run_change_message(
        self, request: NotificationRequestClass
    ) -> dict:
        """Build Teams Adaptive Card for ingestion run change notifications."""
        params = request.message.parameters or {}

        source_name = params.get("sourceName", "Unknown Source")
        source_type = params.get("sourceType", "unknown")
        status_text = params.get("statusText", "changed")

        ingestion_url = f"{self.base_url}/ingestion"

        # Determine status emoji and color based on status
        if "failed" in status_text.lower():
            status_emoji = "❌"
            status_color = "Attention"
        elif "completed" in status_text.lower() or "successful" in status_text.lower():
            status_emoji = "✅"
            status_color = "Good"
        elif "started" in status_text.lower() or "running" in status_text.lower():
            status_emoji = "🔄"
            status_color = "Accent"
        else:
            status_emoji = "⚠️"
            status_color = "Warning"

        return {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": f"{status_emoji} **Ingestion Status Update**",
                        "weight": "Bolder",
                        "size": "Medium",
                        "color": status_color,
                    },
                    {
                        "type": "TextBlock",
                        "text": f"Ingestion source **{source_name}** of type **{source_type}** has {status_text}",
                        "wrap": True,
                        "spacing": "Medium",
                    },
                ],
                "actions": [
                    {
                        "type": "Action.OpenUrl",
                        "title": "🔗 View Ingestion Sources",
                        "url": ingestion_url,
                    }
                ],
            },
        }

    def _build_assertion_status_change_message(
        self, request: NotificationRequestClass
    ) -> dict:
        """Build Teams Adaptive Card for assertion status change notifications."""
        params = request.message.parameters or {}

        assertion_type = params.get("assertionType", "assertion")
        entity_name = params.get("entityName", "Unknown Entity")
        entity_path = params.get("entityPath", "")
        result = params.get("result", "unknown")
        result_reason = params.get("resultReason", "")
        description = params.get("description", "")
        external_url = params.get("externalUrl")
        external_platform = params.get("externalPlatform")

        # Build URLs
        entity_url = f"{self.base_url}{entity_path}" if entity_path else ""

        # Determine status based on result
        if result.upper() == "SUCCESS":
            status_emoji = "✅"
            status_color = "Good"
            status_text = "passed"
        elif result.upper() == "FAILURE":
            status_emoji = "❌"
            status_color = "Attention"
            status_text = "failed"
        else:
            status_emoji = "⚠️"
            status_color = "Warning"
            status_text = "changed"

        # Build actions
        actions = []
        if external_url and external_platform:
            actions.append(
                {
                    "type": "Action.OpenUrl",
                    "title": f"View in {external_platform}",
                    "url": external_url,
                }
            )
        elif entity_url:
            actions.append(
                {
                    "type": "Action.OpenUrl",
                    "title": VIEW_IN_DATAHUB_TITLE,
                    "url": entity_url,
                }
            )

        return {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": f"{status_emoji} **Assertion {status_text.title()}**",
                        "weight": "Bolder",
                        "size": "Medium",
                        "color": status_color,
                    },
                    {
                        "type": "TextBlock",
                        "text": f"**{assertion_type.title()}** `{description}` has **{status_text}** for **{entity_name}**",
                        "wrap": True,
                        "spacing": "Medium",
                    },
                ]
                + (
                    [
                        {
                            "type": "TextBlock",
                            "text": f"*{result_reason}*",
                            "wrap": True,
                            "isSubtle": True,
                            "spacing": "Small",
                        }
                    ]
                    if result_reason
                    else []
                ),
                "actions": actions,
            },
        }

    def _build_workflow_form_request_message(
        self, request: NotificationRequestClass
    ) -> dict:
        """Build Teams Adaptive Card for workflow form request notifications."""
        params = request.message.parameters or {}

        # Extract parameters - handle both legacy formName and new workflowName patterns
        if "formName" in params:
            # Legacy pattern: use generic "Workflow Form" in header regardless of actual form name
            header_text = "Workflow Form"
            actual_form_name = params.get("formName", "Workflow Form")
        else:
            # New pattern: use actual workflow name in header
            header_text = params.get("workflowName", "Workflow Form")
            actual_form_name = header_text

        actor_name = params.get("actorName", "Someone")
        entity_name = params.get("entityName", "")
        entity_type = params.get("entityType", "")
        entity_platform = params.get("entityPlatform", "")
        assignee_name = params.get("assigneeName", "you")
        due_date = params.get("dueDate", "")

        # Build entity information similar to Slack implementation
        entity_info = ""
        if entity_name:
            if entity_type:
                entity_info = f" for **{entity_type}** **{entity_name}**"
                if entity_platform:
                    entity_info += f" on **{entity_platform}**"
            else:
                entity_info = f" for **{entity_name}**"
                if entity_platform:
                    entity_info += f" on **{entity_platform}**"

        return {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": f"📋 **New {header_text} {'Assignment' if assignee_name and assignee_name != 'you' else 'Request'}**",
                        "weight": "Bolder",
                        "size": "Medium",
                        "color": "Accent",
                    },
                    {
                        "type": "TextBlock",
                        "text": f"**{actor_name}** has created a new **{actual_form_name}** request{entity_info}"
                        + (
                            f" assigned to **{assignee_name}**"
                            if assignee_name != "you"
                            else ""
                        ),
                        "wrap": True,
                        "spacing": "Medium",
                    },
                ]
                + (
                    [
                        {
                            "type": "TextBlock",
                            "text": f"**Due:** {due_date}",
                            "wrap": True,
                            "weight": "Bolder",
                            "spacing": "Small",
                        }
                    ]
                    if due_date
                    else []
                ),
                "actions": [
                    {
                        "type": "Action.OpenUrl",
                        "title": "🔗 View Form",
                        "url": f"{self.base_url}/requests/workflows",
                    }
                ],
            },
        }

    def _build_workflow_form_status_change_message(
        self, request: NotificationRequestClass
    ) -> dict:
        """Build Teams Adaptive Card for workflow form status change notifications."""
        params = request.message.parameters or {}

        # Extract parameters - handle both legacy (formName/status) and new (workflowName/result) patterns
        if "formName" in params:
            # Legacy pattern: use generic "Workflow Form" in header regardless of actual form name
            header_text = "Workflow Form"
            actual_form_name = params.get("formName", "Workflow Form")
            result = params.get("status", "updated")  # Legacy uses "status" parameter
        else:
            # New pattern: use actual workflow name in header
            header_text = params.get("workflowName", "Workflow Form")
            actual_form_name = header_text
            result = params.get("result", "updated")  # New uses "result" parameter

        actor_name = params.get("actorName", "Someone")
        entity_name = params.get("entityName", "")
        entity_type = params.get("entityType", "")
        entity_platform = params.get("entityPlatform", "")

        # Determine status emoji and color based on result
        if result.upper() in ["COMPLETED", "APPROVED", "DONE"]:
            status_emoji = "✅"
            status_color = "Good"
        elif result.upper() in ["REJECTED", "CANCELLED", "FAILED"]:
            status_emoji = "❌"
            status_color = "Attention"
        else:
            status_emoji = "📝"
            status_color = "Default"

        # Build entity information
        entity_info = ""
        if entity_name:
            if entity_type:
                entity_info = f" for **{entity_type}** **{entity_name}**"
                if entity_platform:
                    entity_info += f" on **{entity_platform}**"
            else:
                entity_info = f" for **{entity_name}**"
                if entity_platform:
                    entity_info += f" on **{entity_platform}**"

        # Generate header text based on pattern
        if "formName" in params:
            # Legacy pattern expects "Workflow Form Status Changed"
            header_text_full = f"{status_emoji} **{header_text} Status Changed**"
        else:
            # New pattern shows specific result status
            header_text_full = f"{status_emoji} **{header_text} Request {result.upper()}** by **{actor_name}**"

        # Generate detail text based on pattern
        if "formName" in params:
            # Legacy pattern: show actor, form name, status, and entity info
            detail_text = f"**{actor_name}** has **{result.lower()}** the **{actual_form_name}** request{entity_info}"
        else:
            # New pattern: show form and entity info with result
            detail_text = f"The **{actual_form_name}** request{entity_info} has been **{result.lower()}**"

        return {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": header_text_full,
                        "weight": "Bolder",
                        "size": "Medium",
                        "color": status_color,
                    },
                    {
                        "type": "TextBlock",
                        "text": detail_text,
                        "wrap": True,
                        "spacing": "Medium",
                    },
                ],
                "actions": [
                    {
                        "type": "Action.OpenUrl",
                        "title": "🔗 View Form",
                        "url": f"{self.base_url}/requests/workflows",
                    }
                ],
            },
        }

    def _get_render_field_from_modifier_type(
        self, modifier_type: str
    ) -> Optional[EntityCardRenderField]:
        logger.info(f"Getting render field from modifier type: {modifier_type}")
        if modifier_type.lower() == "tag(s)":
            return EntityCardRenderField.TAG
        elif modifier_type.lower() == "glossary term(s)":
            return EntityCardRenderField.TERM
        elif modifier_type.lower() == "owner(s)":
            return EntityCardRenderField.OWNERSHIP
        elif modifier_type == "deprecation":
            return EntityCardRenderField.DEPRECATED
        elif modifier_type == "unhealthy":
            return EntityCardRenderField.UNHEALTHY
        elif modifier_type == "health_indicators":
            return EntityCardRenderField.HEALTH_INDICATORS
        elif modifier_type == "domain":
            return EntityCardRenderField.DOMAIN
        elif modifier_type == "usage_stats":
            return EntityCardRenderField.USAGE_STATS
        elif modifier_type == "lineage_counts":
            return EntityCardRenderField.LINEAGE_COUNTS
        else:
            return None

    def _build_entity_change_message(self, request: NotificationRequestClass) -> dict:
        """Build rich notification card by fetching full entity details and adding change banner."""
        logger.info(f"Building entity change message for request: {request}")
        params = request.message.parameters or {}

        # Extract change information
        # actor_name = params.get('actorName', 'Unknown User')
        # operation = params.get('operation', 'changed')
        # modifier_type = params.get('modifierType', 'property')
        # modifier_name = params.get('modifier0Name', '')
        # entity_path = params.get('entityPath', '')

        # Extract entity URN to fetch full details
        entity_urn = self._extract_entity_urn_from_params(params)
        if not entity_urn:
            logger.error("Could not extract entity URN from notification parameters")
            return self._build_fallback_notification_card(params)

        # Fetch full entity details using GraphQL
        entity_data = self._fetch_entity_details(entity_urn)
        if not entity_data:
            logger.warning(
                f"Could not fetch entity details for {entity_urn}, using fallback"
            )
            return self._build_fallback_notification_card(params)

        modifier_type = params.get("modifierType", "property")
        render_field = self._get_render_field_from_modifier_type(modifier_type)
        entity_card = render_entity_card(
            entity_data, fields=[render_field] if render_field else []
        )

        if not entity_card:
            logger.error(f"Failed to render entity card for {entity_urn}")
            return self._build_fallback_notification_card(params)

        # Add change notification banner to the existing entity card
        return self._add_change_banner_to_entity_card(entity_card, params)

    def _extract_entity_urn_from_params(self, params: dict) -> Optional[str]:
        """Extract entity URN from notification parameters."""
        # The URN might be in entityPath or we can reconstruct it
        entity_path = params.get("entityPath", "")
        if "/dataset/" in entity_path:
            # Extract URN from path like /dataset/urn%3Ali%3Adataset%3A%28...
            import urllib.parse

            urn_part = entity_path.split("/dataset/")[1].split("?")[0]
            return urllib.parse.unquote(urn_part)

        # Fallback: try to reconstruct from available parameters
        entity_name = params.get("entityName")
        entity_platform = params.get("entityPlatform")
        if entity_name and entity_platform:
            # This is a basic reconstruction - might not always work
            return f"urn:li:dataset:(urn:li:dataPlatform:{entity_platform.lower()},{entity_name},PROD)"

        return None

    def _fetch_entity_details(self, entity_urn: str) -> Optional[dict]:
        """Fetch full entity details from DataHub GraphQL API."""
        try:
            from datahub_integrations.graphql.teams import TEAMS_GET_ENTITY_QUERY

            variables = {"urn": entity_urn}
            if self.graph is None:
                logger.error("DataHub graph client is not initialized")
                return None
            data = self.graph.execute_graphql(
                TEAMS_GET_ENTITY_QUERY, variables=variables
            )
            return data.get("entity") if data else None
        except Exception as e:
            logger.error(f"Error fetching entity details for {entity_urn}: {e}")
            return None

    def _add_change_banner_to_entity_card(
        self, entity_card: dict, params: dict
    ) -> dict:
        """Add a change notification banner to an existing entity card."""
        if not entity_card or "content" not in entity_card:
            return entity_card

        # Extract change details
        actor_name = params.get("actorName", "Unknown User")
        actor_urn = params.get("actorUrn", "")
        operation = params.get("operation", "changed")
        modifier_type = params.get("modifierType", "property")
        modifier_name = params.get("modifier0Name", "")
        note = params.get("note", "")

        # Resolve actor for tagging if available
        tagged_actor_name = actor_name
        if actor_urn and self.identity_provider:
            actors = self.identity_provider.batch_get_actors({actor_urn})
            actor = actors.get(actor_urn)
            if actor:
                # Use template utils to create a tagged actor string
                tagged_actor_name = create_actors_tag_string(
                    [actor], self.teams_connection_config
                )
            else:
                # Fallback to the original actor name
                tagged_actor_name = actor_name

        # Create operation styling
        if operation == "added":
            operation_icon = "➕"
            operation_text = f"added **{modifier_name}**"
            operation_color = "Good"
        elif operation == "removed":
            operation_icon = "➖"
            operation_text = f"removed **{modifier_name}**"
            operation_color = "Warning"
        else:
            logger.info(f"Operation: {operation}")
            operation_icon = ""
            operation_text = (
                f"{operation} **{modifier_name}**" if modifier_name else operation
            )
            operation_color = "Accent"

        # Create change banner
        change_banner = {
            "type": "Container",
            "style": "accent",
            "items": [
                {
                    "type": "ColumnSet",
                    "columns": [
                        {
                            "type": "Column",
                            "width": "auto",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": operation_icon,
                                    "size": "Medium",
                                    "weight": "Bolder",
                                    "color": operation_color,
                                }
                            ],
                        },
                        {
                            "type": "Column",
                            "width": "stretch",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": f"**{self._format_modifier_type(modifier_type)} Changed**",
                                    "weight": "Bolder",
                                    "size": "Small",
                                    "color": "Default",
                                    "wrap": True,
                                },
                                {
                                    "type": "TextBlock",
                                    "text": f"{tagged_actor_name} {operation_text}",
                                    "size": "Small",
                                    "color": "Default",
                                    "wrap": True,
                                },
                            ],
                        },
                    ],
                }
            ],
            "spacing": "Medium",
        }

        # Insert change banner before the entity header (first item)
        content = entity_card["content"]
        if "body" in content and content["body"]:
            content["body"].insert(0, change_banner)

        # Add note section if note is present
        if note and note.strip():
            note_section = {
                "type": "Container",
                "style": "default",
                "items": [
                    {
                        "type": "ColumnSet",
                        "columns": [
                            {
                                "type": "Column",
                                "width": "auto",
                                "items": [
                                    {
                                        "type": "TextBlock",
                                        "text": "│",
                                        "size": "Small",
                                        "color": "Light",
                                        "spacing": "None",
                                    }
                                ],
                                "spacing": "None",
                            },
                            {
                                "type": "Column",
                                "width": "stretch",
                                "items": [
                                    {
                                        "type": "TextBlock",
                                        "text": "**Note**",
                                        "size": "Small",
                                        "weight": "Bolder",
                                        "color": "Default",
                                        "spacing": "None",
                                    },
                                    {
                                        "type": "TextBlock",
                                        "text": note,
                                        "size": "Small",
                                        "color": "Default",
                                        "wrap": True,
                                        "spacing": "Small",
                                    },
                                ],
                            },
                        ],
                    }
                ],
                "spacing": "Medium",
                "separator": True,
            }

            # Insert note section after the change banner
            content["body"].insert(1, note_section)

        return entity_card

    def _build_fallback_notification_card(self, params: dict) -> dict:
        """Build a simple fallback notification card when entity details can't be fetched."""
        entity_name = params.get("entityName", "Unknown Entity")
        actor_name = params.get("actorName", "Unknown User")
        operation = params.get("operation", "changed")
        modifier_name = params.get("modifier0Name", "")
        entity_path = params.get("entityPath", "")

        from datahub_integrations.app import DATAHUB_FRONTEND_URL

        entity_url = f"{DATAHUB_FRONTEND_URL}{entity_path}" if entity_path else ""

        return {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": f"🔄 **{entity_name}** {operation}",
                        "weight": "Bolder",
                        "size": "Medium",
                        "wrap": True,
                    },
                    {
                        "type": "TextBlock",
                        "text": f"{actor_name} {operation} {modifier_name}",
                        "size": "Small",
                        "wrap": True,
                    },
                ],
                "actions": [
                    {
                        "type": "Action.OpenUrl",
                        "title": VIEW_IN_DATAHUB_TITLE,
                        "url": entity_url,
                    }
                ]
                if entity_url
                else [],
            },
        }

    def _format_modifier_type(self, modifier_type: str) -> str:
        """Format modifier type with proper capitalization."""
        # Handle special cases
        logger.info(f"Formatting modifier type: {modifier_type}")
        if modifier_type.lower() == "tag(s)":
            return "Tag(s)"
        elif modifier_type.lower() == "term(s)":
            return "Term(s)"
        elif modifier_type.lower() == "owner(s)":
            return "Owner(s)"
        elif modifier_type.lower() == "glossary term(s)":
            return "Glossary Term(s)"
        elif modifier_type.lower() == "deprecation":
            return "Deprecation Status"
        else:
            # For other types, use title case but preserve parentheses
            return modifier_type.title()

    def _get_teams_recipients(
        self, recipients: List[NotificationRecipientClass]
    ) -> List[str]:
        """Filter and extract Teams-specific recipients."""
        teams_recipients = []
        for recipient in recipients:
            # Use string comparison for now since the PDL classes haven't been generated yet
            if recipient.type in [
                "TEAMS_CHANNEL",
                "TEAMS_DM",
            ]:
                if recipient.id:
                    recipient_id = recipient.id
                    teams_recipients.append(recipient_id)
        return teams_recipients

    def _maybe_reload_teams_config(self, force_refresh: bool = False) -> None:
        """Get Teams configuration with time-based throttling handled by config manager.

        Args:
            force_refresh: If True, always reload from DataHub regardless of cache state
        """
        try:
            # Always call teams_config.get_config() - it handles time-based throttling internally
            current_config = teams_config.get_config(force_refresh=force_refresh)

            # Update our cached instance
            self.teams_connection_config = current_config

            # DEBUG: Log the loaded config values
            if current_config and current_config.app_details:
                app_details = current_config.app_details
                logger.info(f"DEBUG Reload - app_id: {repr(app_details.app_id)}")
                logger.info(f"DEBUG Reload - tenant_id: {repr(app_details.tenant_id)}")
                logger.info(
                    f"DEBUG Reload - app_tenant_id: {repr(app_details.app_tenant_id)}"
                )
            else:
                logger.info("DEBUG Reload - No app details found")
        except Exception as e:
            logger.error(f"Failed to reload Teams configuration: {e}")

    async def _find_existing_bot_conversation(self, recipient: str) -> Optional[str]:
        """Find existing bot conversation ID via Microsoft Graph API with fast retry."""
        import asyncio

        # Fast retry: 2 attempts max, 100ms between retries = ~200ms max overhead
        for attempt in range(2):
            try:
                return await self._find_existing_bot_conversation_impl(recipient)
            except Exception as e:
                if attempt == 0:  # Only retry once
                    logger.warning(
                        f"Find conversation attempt {attempt + 1} failed, retrying: {e}"
                    )
                    await asyncio.sleep(0.1)  # 100ms delay
                else:
                    logger.error(
                        f"Find conversation failed after {attempt + 1} attempts: {e}"
                    )
                    raise
        return None

    async def _find_existing_bot_conversation_impl(
        self, recipient: str
    ) -> Optional[str]:
        """Implementation of finding existing bot conversation ID via Microsoft Graph API."""
        try:
            import httpx

            if (
                not self.teams_connection_config
                or not self.teams_connection_config.app_details
            ):
                logger.error("Teams configuration not available")
                return None

            app_details = self.teams_connection_config.app_details
            if (
                not app_details
                or not app_details.app_id
                or not app_details.app_password
                or not app_details.tenant_id
            ):
                logger.error("Missing Teams app credentials")
                return None

            # Extract valid user GUID from recipient ID using centralized parser
            user_guid = self._extract_user_guid_from_recipient(recipient)
            if not user_guid:
                logger.error(
                    f"Could not extract valid user GUID from recipient: {recipient}"
                )
                return None

            logger.debug(
                f"Using user GUID {user_guid} extracted from recipient {recipient}"
            )

            # Get Microsoft Graph token (not Bot Framework token)
            token_url = f"https://login.microsoftonline.com/{app_details.tenant_id}/oauth2/v2.0/token"

            data = {
                "grant_type": "client_credentials",
                "client_id": app_details.app_id,
                "client_secret": app_details.app_password,
                "scope": "https://graph.microsoft.com/.default",  # Microsoft Graph scope, not Bot Framework
            }

            token_headers = {"Content-Type": "application/x-www-form-urlencoded"}

            async with httpx.AsyncClient() as token_client:
                token_response = await token_client.post(
                    token_url, data=data, headers=token_headers
                )

                if token_response.status_code != 200:
                    logger.error(
                        f"Failed to get Microsoft Graph token: {token_response.status_code} {token_response.text}"
                    )
                    return None

                token_data = token_response.json()
                access_token = token_data["access_token"]

            headers = {"Authorization": f"Bearer {access_token}"}

            # Get user's chats to find existing bot conversation
            chats_url = f"https://graph.microsoft.com/v1.0/users/{user_guid}/chats"

            async with httpx.AsyncClient() as client:
                response = await client.get(chats_url, headers=headers)

                if response.status_code == 200:
                    chats_data = response.json()
                    logger.info(
                        f"Found {len(chats_data.get('value', []))} chats for user {user_guid}"
                    )

                    # Look for existing bot conversations
                    for chat in chats_data.get("value", []):
                        chat_id = chat.get("id")
                        if not chat_id:
                            continue

                        # Check if this chat involves our bot
                        # Bot conversation IDs typically contain both user ID and app ID
                        if app_details.app_id in chat_id and user_guid in chat_id:
                            logger.info(f"Found existing bot conversation: {chat_id}")
                            return chat_id

                    logger.warning(
                        f"No existing bot conversation found for user {user_guid}"
                    )
                    return None
                else:
                    logger.error(
                        f"Failed to get user chats: {response.status_code} - {response.text}"
                    )
                    return None

        except Exception as e:
            logger.error(
                f"Error finding existing bot conversation for {recipient}: {e}"
            )
            return None

    async def _send_message_to_conversation(
        self, conversation_id: str, message_text: str
    ) -> Optional[str]:
        """Send message to existing conversation using Bot Framework and return message ID."""
        try:
            import httpx

            if (
                not self.teams_connection_config
                or not self.teams_connection_config.app_details
            ):
                logger.error("Teams configuration not available")
                return None

            app_details = self.teams_connection_config.app_details

            # Get Azure AD token using proper tenant_id
            if not app_details.tenant_id:
                logger.error("Teams tenant_id not configured")
                return None

            token_url = f"https://login.microsoftonline.com/{app_details.tenant_id}/oauth2/v2.0/token"

            data = {
                "grant_type": "client_credentials",
                "client_id": app_details.app_id,
                "client_secret": app_details.app_password,
                "scope": "https://api.botframework.com/.default",
            }

            headers = {"Content-Type": "application/x-www-form-urlencoded"}

            async with httpx.AsyncClient() as client:
                response = await client.post(token_url, data=data, headers=headers)

                if response.status_code != 200:
                    logger.error(
                        f"Failed to get Bot Framework token: {response.status_code} {response.text}"
                    )
                    return None

                token_data = response.json()
                bot_token = token_data["access_token"]

                # Send message to conversation
                message_payload = {"type": "message", "text": message_text}

                message_headers = {
                    "Authorization": f"Bearer {bot_token}",
                    "Content-Type": "application/json",
                }

                service_url = "https://smba.trafficmanager.net/apis/v3"
                message_url = (
                    f"{service_url}/conversations/{conversation_id}/activities"
                )

                logger.info(f"Sending message to conversation {conversation_id}")
                message_response = await client.post(
                    message_url, json=message_payload, headers=message_headers
                )

                if message_response.status_code in [200, 201]:
                    response_data = message_response.json()
                    message_id = response_data.get("id")
                    logger.info(
                        f"Successfully sent message to conversation {conversation_id}, message ID: {message_id}"
                    )
                    return message_id
                else:
                    # Map HTTP status codes to specific error codes for message sending
                    if message_response.status_code == 403:
                        error_code = TeamsErrorCodes.MESSAGE_SEND_FORBIDDEN
                        technical_message = f"Bot not authorized to send message (403): {message_response.text}"
                    elif message_response.status_code == 401:
                        error_code = TeamsErrorCodes.MESSAGE_SEND_UNAUTHORIZED
                        technical_message = f"Authentication failed for message (401): {message_response.text}"
                    elif message_response.status_code == 404:
                        error_code = TeamsErrorCodes.MESSAGE_SEND_NOT_FOUND
                        technical_message = (
                            f"Conversation not found (404): {message_response.text}"
                        )
                    else:
                        error_code = TeamsErrorCodes.MESSAGE_SEND_FAILED
                        technical_message = f"Failed to send message ({message_response.status_code}): {message_response.text}"

                    teams_exception = TeamsMessageSendException(
                        message=technical_message,
                        error_code=error_code,
                        status_code=message_response.status_code,
                        response_text=message_response.text,
                    )
                    teams_exception.log_and_track()
                    raise teams_exception

        except Exception as e:
            logger.error(
                f"Error sending message to conversation {conversation_id}: {e}"
            )
            raise

    async def _send_adaptive_card_to_conversation(
        self, conversation_id: str, adaptive_card: dict
    ) -> Optional[str]:
        """Send adaptive card to existing conversation using Bot Framework and return message ID."""
        try:
            import httpx

            if (
                not self.teams_connection_config
                or not self.teams_connection_config.app_details
            ):
                raise TeamsMessageSendException(
                    message="Teams configuration not available",
                    error_code=TeamsErrorCodes.TEAMS_CONFIG_INVALID,
                    user_friendly_message="Teams integration is not properly configured. Please contact your administrator.",
                )

            app_details = self.teams_connection_config.app_details

            # DEBUG: Log config values for Bot Framework
            logger.info(f"DEBUG Bot Framework - app_id: {repr(app_details.app_id)}")
            logger.info(
                f"DEBUG Bot Framework - app_password: {app_details.app_password is not None}"
            )
            logger.info(
                f"DEBUG Bot Framework - tenant_id: {repr(app_details.tenant_id)}"
            )
            logger.info(
                f"DEBUG Bot Framework - app_tenant_id: {repr(app_details.app_tenant_id)}"
            )

            # Get Azure AD token using proper tenant_id
            if not app_details.tenant_id:
                logger.error("Teams tenant_id not configured")
                return None

            token_url = f"https://login.microsoftonline.com/{app_details.app_tenant_id}/oauth2/v2.0/token"
            logger.info(f"DEBUG Bot Framework - token_url: {token_url}")

            data = {
                "grant_type": "client_credentials",
                "client_id": app_details.app_id,
                "client_secret": app_details.app_password,
                "scope": "https://api.botframework.com/.default",
            }

            headers = {"Content-Type": "application/x-www-form-urlencoded"}

            async with httpx.AsyncClient() as client:
                response = await client.post(token_url, data=data, headers=headers)

                if response.status_code != 200:
                    error_code = map_teams_api_error_to_error_code(
                        status_code=response.status_code,
                        response_text=response.text,
                        error_context="token_request",
                    )
                    raise TeamsMessageSendException(
                        message=f"Failed to get Bot Framework token: {response.status_code} {response.text}",
                        error_code=error_code,
                        status_code=response.status_code,
                        response_text=response.text,
                    )

                token_data = response.json()
                bot_token = token_data["access_token"]

                # Send adaptive card to conversation
                # Handle both wrapped and unwrapped adaptive card formats
                if (
                    "contentType" in adaptive_card
                    and adaptive_card["contentType"]
                    == "application/vnd.microsoft.card.adaptive"
                ):
                    # Already properly wrapped (from render_entity_card)
                    attachment = adaptive_card
                else:
                    # Raw adaptive card (from test notifications) - needs wrapping
                    attachment = {
                        "contentType": "application/vnd.microsoft.card.adaptive",
                        "content": adaptive_card,
                    }
                message_payload = {"type": "message", "attachments": [attachment]}

                message_headers = {
                    "Authorization": f"Bearer {bot_token}",
                    "Content-Type": "application/json",
                }

                service_url = "https://smba.trafficmanager.net/apis/v3"
                message_url = (
                    f"{service_url}/conversations/{conversation_id}/activities"
                )

                logger.info(f"Sending adaptive card to conversation {conversation_id}")
                message_response = await client.post(
                    message_url, json=message_payload, headers=message_headers
                )

                if message_response.status_code in [200, 201]:
                    response_data = message_response.json()
                    message_id = response_data.get("id")
                    logger.info(
                        f"Successfully sent adaptive card to conversation {conversation_id}, message ID: {message_id}"
                    )
                    return message_id
                else:
                    # Map HTTP status codes to specific error codes
                    if message_response.status_code == 403:
                        error_code = TeamsErrorCodes.MESSAGE_SEND_FORBIDDEN
                        technical_message = f"Bot not authorized to send adaptive card (403): {message_response.text}"
                    elif message_response.status_code == 401:
                        error_code = TeamsErrorCodes.MESSAGE_SEND_UNAUTHORIZED
                        technical_message = f"Authentication failed for adaptive card (401): {message_response.text}"
                    elif message_response.status_code == 404:
                        error_code = TeamsErrorCodes.MESSAGE_SEND_NOT_FOUND
                        technical_message = (
                            f"Conversation not found (404): {message_response.text}"
                        )
                    else:
                        error_code = TeamsErrorCodes.MESSAGE_SEND_FAILED
                        technical_message = f"Failed to send adaptive card ({message_response.status_code}): {message_response.text}"

                    teams_exception = TeamsMessageSendException(
                        message=technical_message,
                        error_code=error_code,
                        status_code=message_response.status_code,
                        response_text=message_response.text,
                    )
                    teams_exception.log_and_track()
                    raise teams_exception

        except Exception as e:
            logger.error(
                f"Error sending adaptive card to conversation {conversation_id}: {e}"
            )
            raise

    async def _execute_post_send_actions(
        self,
        request: NotificationRequestClass,
        success: bool,
        error: Optional[Exception] = None,
    ) -> None:
        """Execute post-send actions for the notification request."""
        params = request.message.parameters or {}
        request_name = params.get("requestName")
        test_notification_urn = params.get("testNotificationExecutionRequestUrn")

        # Handle test notification completion
        if request_name == "notificationConnectionTest" and test_notification_urn:
            await self._complete_test_notification(
                test_notification_urn, success, error
            )

    def _get_user_friendly_error_message(self, error: Exception) -> str:
        """Convert technical error to user-friendly message with actionable guidance."""
        error_str = str(error).lower()

        # Configuration errors
        if (
            "missing teams app credentials" in error_str
            or "app_id" in error_str
            or "app_password" in error_str
            or "tenant_id" in error_str
        ):
            return "Teams app configuration is incomplete. Please verify that App ID, App Password, and Tenant ID are properly configured in Settings > Integrations > Teams."

        # Authentication/OAuth errors
        if (
            "aadsts" in error_str
            or "invalid_request" in error_str
            or "tenant identifier" in error_str
        ):
            if "none" in error_str or "tenant identifier 'none'" in error_str:
                return "Teams Tenant ID is missing or invalid. Please check that your Tenant ID is set to a valid Azure AD tenant identifier (GUID format) in Settings > Integrations > Teams."
            return "Teams authentication failed. Please check your App ID, App Password, and Tenant ID configuration in Settings > Integrations > Teams."

        # Bot Framework token errors
        if "failed to get bot framework token" in error_str:
            return "Teams Bot Framework authentication failed. Please verify your App Password and Tenant ID are correct in Settings > Integrations > Teams."

        # Graph API errors
        if "graph api" in error_str or "microsoft graph" in error_str:
            return "Microsoft Graph API access failed. Please check your App ID has the required permissions and your credentials are valid in Settings > Integrations > Teams."

        # Network/connectivity errors
        if (
            "connection" in error_str
            or "timeout" in error_str
            or "network" in error_str
        ):
            return "Network connection to Teams failed. Please check your internet connectivity and try again."

        # Generic fallback with some guidance
        return f"Teams notification failed: {str(error)[:100]}{'...' if len(str(error)) > 100 else ''}. Please check your Teams configuration in Settings > Integrations > Teams."

    async def _complete_test_notification(
        self,
        execution_request_urn: str,
        success: bool,
        error: Optional[Exception] = None,
    ) -> None:
        """Complete a test notification execution request with result."""
        try:
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.metadata.schema_classes import (
                ExecutionRequestResultClass,
                StructuredExecutionReportClass,
            )

            from datahub_integrations.app import graph

            # Generate user-friendly error message if there was a failure
            error_message = None
            user_message = "Teams test notification sent successfully"

            if not success and error:
                error_message = self._get_user_friendly_error_message(error)
                user_message = error_message
            elif not success:
                error_message = "Teams notification failed"
                user_message = "Failed to send Teams test notification"

            # Prepare the execution result - must match Slack's format (array of reports)
            report_entry = {
                "error": error_message,
                "warning": None,
                "timestamp": str(int(datetime.now().timestamp() * 1000)),
                "message": user_message,
            }

            # Create array of reports (to match Slack format that UI expects)
            reports_array = [report_entry]

            result = ExecutionRequestResultClass(
                status="SUCCESS" if success else "ERROR",
                structuredReport=StructuredExecutionReportClass(
                    type="TEST_CONNECTION",
                    serializedValue=json.dumps(reports_array),
                    contentType="application/json",
                ),
            )

            # Create the metadata change proposal
            mcp = MetadataChangeProposalWrapper(
                entityUrn=execution_request_urn,
                aspect=result,
            )

            # Emit the MCP using the existing DataHubGraph client
            graph.emit_mcp(mcp)
            logger.info(
                f"Successfully completed test notification execution request: {execution_request_urn}"
            )

        except Exception as e:
            logger.error(
                f"Failed to complete test notification execution request {execution_request_urn}: {e}"
            )
            # Don't raise - this is a best-effort completion
