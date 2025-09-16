"""
Teams utilities for DataHub integration.

This module contains utility functions for Teams integration,
including user mapping and other helper functions.
"""

from .datahub_user import (
    get_datahub_user_from_email,
    get_datahub_user_from_teams_id,
    get_user_information_from_email,
    get_user_information_from_teams_id,
    graph_as_system,
    graph_as_user,
)

__all__ = [
    "graph_as_system",
    "graph_as_user",
    "get_datahub_user_from_email",
    "get_datahub_user_from_teams_id",
    "get_user_information_from_email",
    "get_user_information_from_teams_id",
]
