"""
Chat types and enums.

This module provides type definitions for different chat contexts.
"""

from enum import Enum
from typing import List

from loguru import logger
from pydantic import BaseModel, field_validator


class ChatType(Enum):
    """Chat type enumeration for different chat contexts."""

    DATAHUB_UI = "datahub_ui"  # DataHub web interface
    SLACK = "slack"  # Slack integration
    TEAMS = "teams"  # Microsoft Teams integration
    DEFAULT = "default"  # Fallback context


_MAX_SUGGESTIONS = 4


class NextMessage(BaseModel):
    """Response message with optional follow-up suggestions."""

    text: str
    suggestions: List[str] = []

    @field_validator("suggestions", mode="after")
    @classmethod
    def validate_suggestions(cls, v: List[str]) -> List[str]:
        """
        Validate and warn about suggestion count limits.

        Args:
            v: List of suggestion strings

        Returns:
            Validated suggestions list
        """
        if len(v) > _MAX_SUGGESTIONS:
            logger.warning(
                f"Model provided {len(v)} suggestions, but only {_MAX_SUGGESTIONS} are allowed. Truncating to {_MAX_SUGGESTIONS}."
            )
            return v[:_MAX_SUGGESTIONS]

        return v
