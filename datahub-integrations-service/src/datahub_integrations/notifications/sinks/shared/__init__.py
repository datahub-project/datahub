"""
Shared components for notification sinks.

This module contains shared infrastructure for building interactive buttons
and other common functionality across different notification platforms.
"""

from .incident_manager import (
    IncidentActionResult,
    IncidentManager,
)
from .incident_message_builder import (
    IncidentMessageBuilder,
    IncidentMessageContext,
)
from .interactive_buttons import (
    IncidentContext,
    InteractiveButton,
    ReopenIncidentButton,
    ResolveIncidentButton,
    ViewDetailsButton,
    create_incident_buttons,
)

__all__ = [
    "IncidentContext",
    "InteractiveButton",
    "ResolveIncidentButton",
    "ReopenIncidentButton",
    "ViewDetailsButton",
    "create_incident_buttons",
    "IncidentManager",
    "IncidentActionResult",
    "IncidentMessageBuilder",
    "IncidentMessageContext",
]
