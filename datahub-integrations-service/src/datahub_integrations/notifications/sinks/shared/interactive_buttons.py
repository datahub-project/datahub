"""
Shared interactive button components for notification sinks.

This module provides base classes and implementations for interactive buttons
that can be used across different notification platforms (Slack, Teams, etc.)
while maintaining platform-specific formatting.
"""

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class IncidentContext:
    """Context information for incident-related interactive buttons."""

    urn: str
    stage: Optional[str] = None


class InteractiveButton(ABC):
    """Base class for interactive buttons across different platforms."""

    def __init__(self, incident_context: IncidentContext):
        self.incident_context = incident_context

    @abstractmethod
    def to_slack_format(self) -> Dict[str, Any]:
        """Convert to Slack button format."""
        pass

    @abstractmethod
    def to_teams_format(self) -> Dict[str, Any]:
        """Convert to Teams Action.Submit format."""
        pass

    def get_action_id(self) -> str:
        """Get the action identifier for this button."""
        return self._get_action_id()

    @abstractmethod
    def _get_action_id(self) -> str:
        """Get the platform-specific action identifier."""
        pass


class ResolveIncidentButton(InteractiveButton):
    """Button for resolving incidents."""

    def to_slack_format(self) -> Dict[str, Any]:
        """Convert to Slack button format."""
        return {
            "type": "button",
            "text": {"type": "plain_text", "text": "Mark as Resolved"},
            "style": "primary",
            "value": json.dumps(self.incident_context.__dict__),
            "action_id": "resolve_incident",
        }

    def to_teams_format(self) -> Dict[str, Any]:
        """Convert to Teams Action.Submit format."""
        return {
            "type": "Action.Submit",
            "title": "Mark as Resolved",
            "data": {
                "action": "resolve_incident",
                "incident_urn": self.incident_context.urn,
                "incident_stage": self.incident_context.stage,
            },
        }

    def _get_action_id(self) -> str:
        return "resolve_incident"


class ReopenIncidentButton(InteractiveButton):
    """Button for reopening incidents."""

    def to_slack_format(self) -> Dict[str, Any]:
        """Convert to Slack button format."""
        return {
            "type": "button",
            "text": {"type": "plain_text", "text": "Reopen Incident"},
            "style": "primary",
            "value": json.dumps(self.incident_context.__dict__),
            "action_id": "reopen_incident",
        }

    def to_teams_format(self) -> Dict[str, Any]:
        """Convert to Teams Action.Submit format."""
        return {
            "type": "Action.Submit",
            "title": "Reopen Incident",
            "data": {
                "action": "reopen_incident",
                "incident_urn": self.incident_context.urn,
                "incident_stage": self.incident_context.stage,
            },
        }

    def _get_action_id(self) -> str:
        return "reopen_incident"


class ViewDetailsButton(InteractiveButton):
    """Button for viewing incident details."""

    def __init__(self, incident_context: IncidentContext, url: str):
        super().__init__(incident_context)
        self.url = url

    def to_slack_format(self) -> Dict[str, Any]:
        """Convert to Slack button format."""
        return {
            "type": "button",
            "text": {"type": "plain_text", "text": "View Details"},
            "url": self.url,
            "action_id": "external_redirect",
        }

    def to_teams_format(self) -> Dict[str, Any]:
        """Convert to Teams Action.OpenUrl format."""
        return {
            "type": "Action.OpenUrl",
            "title": "View Details",
            "url": self.url,
        }

    def _get_action_id(self) -> str:
        return "external_redirect"


def create_incident_buttons(
    incident_context: IncidentContext, status: Optional[str], details_url: str
) -> List[InteractiveButton]:
    """
    Create appropriate incident buttons based on status.

    Args:
        incident_context: Context information for the incident
        status: Current incident status ("ACTIVE", "RESOLVED", etc.)
        details_url: URL for viewing incident details

    Returns:
        List of interactive buttons appropriate for the incident status
    """
    buttons: List[InteractiveButton] = []

    # Always add view details button
    buttons.append(ViewDetailsButton(incident_context, details_url))

    # Add status-specific button
    if status == "ACTIVE" or not status:
        buttons.append(ResolveIncidentButton(incident_context))
    else:
        buttons.append(ReopenIncidentButton(incident_context))

    return buttons
