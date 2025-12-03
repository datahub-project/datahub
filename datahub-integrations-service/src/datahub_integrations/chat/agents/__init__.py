"""
Agent factories for DataHub chat agents.

This module provides factory functions for creating configured AgentRunner instances
for different agent types (DataCatalog Explorer, etc.).
"""

# Import factory functions from agent-specific modules
from datahub_integrations.chat.agents.data_catalog_agent import (
    create_data_catalog_explorer_agent,
)
from datahub_integrations.chat.agents.ingestion_troubleshooting_agent import (
    create_ingestion_troubleshooting_agent,
)

# Re-export types for convenience
from datahub_integrations.chat.types import ChatType, NextMessage

# Export the factory function and utilities
__all__ = [
    "create_data_catalog_explorer_agent",
    "create_ingestion_troubleshooting_agent",
    "NextMessage",  # Re-export for convenience
    "ChatType",  # Re-export for convenience
]
