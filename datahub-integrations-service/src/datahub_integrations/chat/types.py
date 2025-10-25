"""
Chat types and enums.

This module provides type definitions for different chat contexts.
"""

from enum import Enum


class ChatType(Enum):
    """Chat type enumeration for different chat contexts."""

    DATAHUB_UI = "datahub_ui"  # DataHub web interface
    SLACK = "slack"  # Slack integration
    TEAMS = "teams"  # Microsoft Teams integration
    DEFAULT = "default"  # Fallback context
