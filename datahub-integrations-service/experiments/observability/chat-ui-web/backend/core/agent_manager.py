"""
Agent Manager - Manages embedded DataHub agent instances.

Extracted from chat_ui.py to provide a reusable agent management layer
for embedded agent mode (running agent directly in the process).
"""

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from loguru import logger

# Add integrations service to path for embedded agent imports
integrations_src = Path(__file__).parent.parent.parent.parent.parent.parent / "src"
if integrations_src.exists() and str(integrations_src) not in sys.path:
    sys.path.insert(0, str(integrations_src))
    logger.info(f"Added integrations source to path: {integrations_src}")

if TYPE_CHECKING:
    from datahub_integrations.chat.agents import AgentRunner


class AgentManager:
    """Manages embedded DataHub agent instances."""

    def __init__(self) -> None:
        """Initialize agent manager."""
        self.agents: dict[str, "AgentRunner"] = {}
        # Track credentials for each agent to detect when they change
        self._agent_credentials: dict[str, tuple[str, Optional[str]]] = {}  # agent_key -> (gms_url, gms_token)

    def create_agent(
        self, gms_url: str, gms_token: Optional[str] = None
    ) -> Optional["AgentRunner"]:
        """
        Create a new embedded agent instance.

        Args:
            gms_url: DataHub GMS URL
            gms_token: Optional GMS authentication token

        Returns:
            AgentRunner instance or None if creation failed
        """
        logger.info("Creating embedded agent...")
        try:
            # Fix URL format: The correct GMS URL for new Acryl Cloud is /gms not /api/gms
            # /api/gms returns 401, but /gms works correctly for both REST and GraphQL
            server_url = gms_url
            if "/api/gms" in server_url:
                server_url = server_url.replace("/api/gms", "/gms")
                logger.info(f"Fixed GMS URL format: /api/gms -> /gms")
                logger.info(f"  Original URL: {gms_url}")
                logger.info(f"  Fixed URL: {server_url}")

            # Set environment variables BEFORE importing agent modules
            logger.info(f"Setting DataHub environment variables with URL: {server_url}")
            os.environ["DATAHUB_GMS_URL"] = server_url
            if gms_token:
                # Set both token env vars - app.py uses DATAHUB_GMS_API_TOKEN
                os.environ["DATAHUB_GMS_TOKEN"] = gms_token
                os.environ["DATAHUB_GMS_API_TOKEN"] = gms_token
            logger.info(f"Set DATAHUB_GMS_URL={server_url}, token={'set' if gms_token else 'not set'}")

            # Import agent dependencies (only when needed)
            from datahub.sdk.main_client import DataHubClient
            from datahub_integrations.chat.agents import create_data_catalog_explorer_agent
            from datahub_integrations.mcp.mcp_server import mcp

            logger.info(f"Creating embedded agent with GMS URL: {gms_url}")

            # Create DataHub client with workaround URL
            logger.info(
                f"Creating DataHubClient with server={server_url}, token={'***' if gms_token else 'None'}"
            )
            client = DataHubClient(
                server=server_url,
                token=gms_token,
            )
            logger.info(f"DataHubClient created successfully")

            # Create agent with MCP tools
            logger.info("Creating agent with MCP tools...")
            agent = create_data_catalog_explorer_agent(client=client, tools=[mcp])

            # Disable prompt caching - not supported in all regions/configurations
            agent.config.use_prompt_caching = False
            logger.info("Created embedded agent successfully (prompt caching disabled)")

            return agent

        except ImportError as e:
            logger.error(f"Agent dependencies not available: {e}")
            raise RuntimeError(
                "Agent dependencies not available - install datahub-integrations package"
            ) from e
        except Exception as e:
            logger.error(f"Failed to create embedded agent: {e}")
            raise RuntimeError(f"Failed to create agent: {e}") from e

    def get_or_create_agent(
        self,
        conversation_id: str,
        gms_url: str,
        gms_token: Optional[str] = None,
        reuse_agents: bool = False,
    ) -> Optional["AgentRunner"]:
        """
        Get existing agent or create new one for a conversation.

        Args:
            conversation_id: ID of the conversation
            gms_url: DataHub GMS URL
            gms_token: Optional GMS authentication token
            reuse_agents: If True, reuse agents across conversations (use a single shared agent)

        Returns:
            AgentRunner instance or None if creation failed
        """
        # If reusing agents, use a single shared agent key
        agent_key = "shared" if reuse_agents else conversation_id

        logger.info(f"Getting or creating agent for {agent_key}: gms_url={gms_url}, token={'***' if gms_token else 'None'}")

        # Check if credentials changed for existing agent
        current_credentials = (gms_url, gms_token)
        cached_credentials = self._agent_credentials.get(agent_key)

        if agent_key in self.agents:
            if cached_credentials != current_credentials:
                logger.info(f"Credentials changed for {agent_key}, refreshing DataHub client")
                # Refresh the client with new credentials instead of recreating entire agent
                from datahub.sdk.main_client import DataHubClient

                # Fix URL format
                server_url = gms_url.replace("/api/gms", "/gms") if "/api/gms" in gms_url else gms_url

                # Create new client with updated credentials
                new_client = DataHubClient(server=server_url, token=gms_token)

                # Update the agent's client
                self.agents[agent_key].client = new_client

                # Update tracked credentials
                self._agent_credentials[agent_key] = current_credentials
                logger.info(f"Refreshed DataHub client for {agent_key}")
            else:
                logger.info(f"Using cached agent for {agent_key} (credentials unchanged)")
        else:
            logger.info(f"Agent not found in cache, creating new agent for {agent_key}")
            self.agents[agent_key] = self.create_agent(gms_url, gms_token)
            self._agent_credentials[agent_key] = current_credentials

        return self.agents.get(agent_key)

    def reset_agent(self, conversation_id: str) -> None:
        """
        Reset (delete) the agent for a conversation.

        Args:
            conversation_id: ID of the conversation
        """
        if conversation_id in self.agents:
            del self.agents[conversation_id]
            logger.info(f"Reset agent for conversation: {conversation_id}")

        # Also clean up credentials tracking
        if conversation_id in self._agent_credentials:
            del self._agent_credentials[conversation_id]

    def reset_all_agents(self) -> None:
        """Reset all agents."""
        self.agents.clear()
        self._agent_credentials.clear()
        logger.info("Reset all agents")

    def has_agent(self, conversation_id: str, reuse_agents: bool = False) -> bool:
        """
        Check if an agent exists for a conversation.

        Args:
            conversation_id: ID of the conversation
            reuse_agents: If True, check for shared agent

        Returns:
            True if agent exists
        """
        agent_key = "shared" if reuse_agents else conversation_id
        return agent_key in self.agents
