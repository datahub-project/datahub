"""
Teams event handlers for interactive card submissions and incident management.
"""

import logging
from typing import Any, Dict, Optional

from datahub.ingestion.graph.client import DataHubGraph

from datahub_integrations.identity.identity_provider import IdentityProvider
from datahub_integrations.notifications.sinks.shared.incident_manager import (
    IncidentManager,
)

logger = logging.getLogger(__name__)


class TeamsIncidentEventHandler:
    """Handle Teams interactive card events for incident management."""

    def __init__(self, graph_client: DataHubGraph, identity_provider: IdentityProvider):
        self.graph_client = graph_client
        self.identity_provider = identity_provider
        self.incident_manager = IncidentManager(graph_client)

    async def handle_incident_action(
        self, action_data: Dict[str, Any], datahub_user_urn: Optional[str]
    ) -> Dict[str, Any]:
        """
        Handle incident action from Teams adaptive card.

        Args:
            action_data: Data from Teams Action.Submit containing action details
            datahub_user_urn: DataHub user URN for impersonation (or None for system user)

        Returns:
            Dictionary with success status and message
        """
        action = action_data.get("action")
        incident_urn = action_data.get("incident_urn")

        if not incident_urn:
            return {"success": False, "error": "Missing incident URN"}

        if not action:
            return {"success": False, "error": "Missing action"}

        try:
            user_info = datahub_user_urn or "system"
            logger.info(
                f"Processing Teams incident action: {action} for incident {incident_urn} by user {user_info}"
            )

            if action == "resolve_incident":
                result = await self.incident_manager.resolve_incident(
                    incident_urn, datahub_user_urn
                )
                return {
                    "success": result.success,
                    "message": result.message,
                    "action": "resolve_incident",
                    "incident_urn": result.incident_urn,
                    "new_status": result.new_status,
                }
            elif action == "reopen_incident":
                result = await self.incident_manager.reopen_incident(
                    incident_urn, datahub_user_urn
                )
                return {
                    "success": result.success,
                    "message": result.message,
                    "action": "reopen_incident",
                    "incident_urn": result.incident_urn,
                    "new_status": result.new_status,
                }
            else:
                return {"success": False, "error": f"Unknown action: {action}"}
        except Exception as e:
            logger.error(f"Failed to handle incident action {action}: {e}")
            return {"success": False, "error": str(e)}

    def parse_teams_submit_data(self, submit_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse Teams Action.Submit data to extract incident action information.

        Args:
            submit_data: Raw data from Teams Action.Submit

        Returns:
            Parsed action data with standardized field names
        """
        # Teams sends the data directly in the submit payload
        # Extract the relevant fields
        parsed_data = {
            "action": submit_data.get("action"),
            "incident_urn": submit_data.get("incident_urn"),
            "incident_stage": submit_data.get("incident_stage"),
        }

        logger.debug(f"Parsed Teams submit data: {parsed_data}")
        return parsed_data

    def validate_action_data(self, action_data: Dict[str, Any]) -> Optional[str]:
        """
        Validate that action data contains required fields.

        Args:
            action_data: Parsed action data

        Returns:
            Error message if validation fails, None if valid
        """
        if not action_data.get("action"):
            return "Missing required field: action"

        if not action_data.get("incident_urn"):
            return "Missing required field: incident_urn"

        valid_actions = ["resolve_incident", "reopen_incident"]
        if action_data["action"] not in valid_actions:
            return f"Invalid action: {action_data['action']}. Must be one of {valid_actions}"

        return None  # No validation errors
