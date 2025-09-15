"""
Shared incident message builder for consistent messaging across platforms.

This module provides shared logic for building incident messages that can be
used by both Teams and Slack integrations to ensure consistency.
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class IncidentMessageContext:
    """Context information for building incident messages."""

    incident_title: str
    incident_description: str
    entity_name: str
    entity_platform: str
    entity_type: str
    entity_path: str
    incident_url: str
    actor_name: str
    new_status: Optional[str] = None
    prev_status: Optional[str] = None
    priority: str = ""
    stage: str = ""
    message: str = ""
    owners: Optional[List[Any]] = None
    downstream_owners: Optional[List[Any]] = None
    downstream_asset_count: str = ""


class IncidentMessageBuilder:
    """
    Shared builder for incident messages across different platforms.

    This class provides common logic for determining message content
    that can be used by both Teams and Slack integrations.
    """

    @staticmethod
    def build_incident_message_context(
        request_params: Dict[str, Any], base_url: str
    ) -> IncidentMessageContext:
        """
        Build incident message context from request parameters.

        Args:
            request_params: Parameters from the notification request
            base_url: Base URL for building incident URLs

        Returns:
            IncidentMessageContext with all the incident information
        """
        # Extract basic incident information
        incident_title = request_params.get("incidentTitle", "Unknown Incident")
        incident_description = request_params.get("incidentDescription", "")
        entity_name = request_params.get("entityName", "Unknown Entity")
        entity_platform = request_params.get("entityPlatform", "")
        entity_type = request_params.get("entityType", "")
        entity_path = request_params.get("entityPath", "")
        actor_name = request_params.get("actorName", "")

        # Extract status information
        new_status = request_params.get("newStatus")
        prev_status = request_params.get("prevStatus")

        # Extract other details
        priority = request_params.get("incidentPriority", "")
        stage = request_params.get("incidentStage", "")
        message = request_params.get("message", "")
        downstream_asset_count = request_params.get("downstreamAssetCount", "")

        # Build incident URL
        incident_url = (
            f"{base_url}{entity_path}/Incidents"
            if entity_path
            else f"{base_url}/Incidents"
        )

        return IncidentMessageContext(
            incident_title=incident_title,
            incident_description=incident_description,
            entity_name=entity_name,
            entity_platform=entity_platform,
            entity_type=entity_type,
            entity_path=entity_path,
            incident_url=incident_url,
            actor_name=actor_name,
            new_status=new_status,
            prev_status=prev_status,
            priority=priority,
            stage=stage,
            message=message,
            downstream_asset_count=downstream_asset_count,
        )

    @staticmethod
    def get_incident_status_info(
        new_status: Optional[str], prev_status: Optional[str]
    ) -> Tuple[str, str, str]:
        """
        Get incident status information for message building.

        Args:
            new_status: Current status of the incident
            prev_status: Previous status of the incident

        Returns:
            Tuple of (emoji, title, action_verb) for the incident status
        """
        if new_status == "RESOLVED":
            return "✅", "Incident Resolved", "resolved"
        elif new_status == "ACTIVE" and prev_status:
            return "🔴", "Incident Reopened", "reopened"
        elif new_status == "ACTIVE":
            return "🚨", "New Incident", "raised"
        else:
            return "📝", "Incident Status Changed", "updated"

    @staticmethod
    def build_incident_summary_message(
        context: IncidentMessageContext, action_verb: str
    ) -> str:
        """
        Build the main incident summary message.

        Args:
            context: Incident message context
            action_verb: The action verb (raised, resolved, reopened, updated)

        Returns:
            Formatted incident summary message
        """
        # Build entity context string
        entity_context = f"{context.entity_platform} {context.entity_type}".strip()
        if entity_context:
            entity_context += " "

        # Build actor context
        actor_context = f" by {context.actor_name}" if context.actor_name else ""

        # Build the message based on action verb
        if action_verb == "resolved":
            return f"An incident has been resolved for {entity_context}[{context.entity_name}]({context.incident_url}){actor_context}."
        elif action_verb == "reopened":
            return f"An incident has been reopened for {entity_context}[{context.entity_name}]({context.incident_url}){actor_context}."
        elif action_verb == "raised":
            return f"An incident has been raised on {entity_context}[{context.entity_name}]({context.incident_url}){actor_context}."
        else:  # updated or other
            return f"An incident has been updated for {entity_context}[{context.entity_name}]({context.incident_url}){actor_context}."

    @staticmethod
    def get_status_color(is_resolved: bool) -> str:
        """
        Get the appropriate color for the incident status.

        Args:
            is_resolved: Whether the incident is resolved

        Returns:
            Color string appropriate for the platform
        """
        return "good" if is_resolved else "attention"

    @staticmethod
    def build_status_change_text(
        new_status: Optional[str], prev_status: Optional[str]
    ) -> str:
        """
        Build status change text for display.

        Args:
            new_status: Current status
            prev_status: Previous status

        Returns:
            Formatted status change text
        """
        if new_status and prev_status:
            return f"Status: {new_status} (was {prev_status})"
        elif new_status:
            return f"Status: {new_status}"
        else:
            return "Status: Unknown"
