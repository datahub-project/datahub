"""
Example agents demonstrating how to use the agent infrastructure.

This module contains example implementations showing how to build
specialized agents and subagents using the reusable infrastructure.

Examples:
- schema_comparison_agent: Simple focused agent for comparing schemas
- ingestion_troubleshooting_agent: Proactive agent with multi-step investigation
"""

from datahub_integrations.chat.examples.ingestion_troubleshooting_agent import (
    create_ingestion_troubleshooting_agent,
)
from datahub_integrations.chat.examples.schema_comparison_agent import (
    create_schema_comparison_agent,
)

__all__ = [
    "create_schema_comparison_agent",
    "create_ingestion_troubleshooting_agent",
]
