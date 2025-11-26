"""
ChatSessionManager for managing persistent chat sessions.

Provides a simple, instance-based API to create/load chat sessions and
send messages while handling persistence concerns via DataHubAiConversationClient.
"""

import queue
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterator, List, Optional, Protocol, Sequence

from datahub.metadata.schema_classes import (
    DataHubAiConversationActorTypeClass,
    DataHubAiConversationMessageTypeClass,
)
from datahub.sdk.main_client import DataHubClient
from fastmcp import FastMCP
from loguru import logger

from datahub_integrations.chat.agent import AgentRunner
from datahub_integrations.chat.agent.progress_tracker import ProgressUpdate
from datahub_integrations.chat.agents import create_data_catalog_explorer_agent
from datahub_integrations.chat.chat_history import ChatHistory
from datahub_integrations.chat.datahub_ai_conversation_client import (
    DataHubAiConversationClient,
)
from datahub_integrations.chat.types import ChatType, NextMessage
from datahub_integrations.mcp.mcp_server import mcp
from datahub_integrations.mcp_integration.tool import ToolWrapper


@dataclass
class ChatMessageEvent:
    """Domain event for chat messages - transport-agnostic."""

    message_type: str  # TEXT, THINKING, TOOL_CALL, TOOL_RESULT
    text: str
    conversation_urn: str
    timestamp: int
    # Optional fields
    user_urn: Optional[str] = None  # Set for user messages
    error: Optional[str] = None  # Set if there was an error


class AgentFactory(Protocol):
    """
    Protocol for agent factory functions.

    Defines the expected signature for all agent factories in AGENT_FACTORIES.
    This provides better IDE support and type safety.
    """

    def __call__(
        self,
        client: DataHubClient,
        history: Optional[ChatHistory] = None,
        extra_instructions_override: Optional[str] = None,
        chat_type: ChatType = ChatType.DEFAULT,
        tools: Optional[Sequence[ToolWrapper | FastMCP]] = None,
    ) -> AgentRunner:
        """
        Create a configured AgentRunner instance.

        Args:
            client: DataHub client for tool execution
            history: Optional existing chat history
            extra_instructions_override: Optional override for extra instructions
            chat_type: Type of chat context (UI, Slack, Teams, etc.)
            tools: Optional tools to use

        Returns:
            Configured AgentRunner instance
        """
        ...


# Agent factory mapping
AGENT_FACTORIES: Dict[str, AgentFactory] = {
    "DataCatalogExplorer": create_data_catalog_explorer_agent,
    # Future agents can be added here
}


class ChatSessionManager:
    """
    Manager/factory for chat sessions and persistence.

    This manager uses two separate DataHub clients:
    - system_client: Used for conversation persistence (system-level operations)
    - tools_client: Used for tool execution (user-level permissions)

    Clean API:
      - create_session(agent_type, chat_type)
      - load_session(conversation_urn, agent_type)
      - add_user_message(agent, text)
      - send_message(text, user_urn, conversation_urn) - High-level streaming API
    """

    def __init__(self, system_client: DataHubClient, tools_client: DataHubClient):
        self.system_client = system_client
        self.tools_client = tools_client
        # Conversation management uses system credentials
        self.conversation_manager = DataHubAiConversationClient(system_client)
        logger.info("Initialized ChatSessionManager with system and tools clients")

    def create_session(
        self,
        agent_type: str = "DataCatalogExplorer",
        chat_type: ChatType = ChatType.DATAHUB_UI,
    ) -> AgentRunner:
        """Create a new agent session with the specified agent type.

        Uses tools_client for tool execution with user permissions.

        Args:
            agent_type: Type of agent to create (e.g., "DataCatalogExplorer")
            chat_type: Chat context type (UI, Slack, Teams, etc.)

        Returns:
            Configured AgentRunner instance

        Raises:
            ValueError: If agent_type is not recognized
        """
        if agent_type not in AGENT_FACTORIES:
            raise ValueError(
                f"Unknown agent type: {agent_type}. "
                f"Available types: {list(AGENT_FACTORIES.keys())}"
            )

        factory = AGENT_FACTORIES[agent_type]
        return factory(client=self.tools_client, chat_type=chat_type, tools=[mcp])

    def load_session(
        self, conversation_urn: str, agent_type: str = "DataCatalogExplorer"
    ) -> AgentRunner:
        """Load an agent session from GraphQL using conversation history.

        Conversation history is loaded with system_client, but tool execution
        uses tools_client for user permissions.

        Args:
            conversation_urn: URN of the conversation to load
            agent_type: Type of agent to create (e.g., "DataCatalogExplorer")

        Returns:
            Configured AgentRunner instance with loaded history

        Raises:
            ValueError: If agent_type is not recognized
        """
        if agent_type not in AGENT_FACTORIES:
            raise ValueError(
                f"Unknown agent type: {agent_type}. "
                f"Available types: {list(AGENT_FACTORIES.keys())}"
            )

        history, chat_type = self.conversation_manager.load_conversation_with_metadata(
            conversation_urn
        )
        factory = AGENT_FACTORIES[agent_type]
        return factory(
            client=self.tools_client, chat_type=chat_type, history=history, tools=[mcp]
        )

    def add_user_message(self, agent: AgentRunner, text: str) -> None:
        """Add a user message to the agent's history."""
        from datahub_integrations.chat.chat_history import HumanMessage

        agent.history.add_message(HumanMessage(text=text))

    def _generate_with_progress(
        self,
        agent: AgentRunner,
        progress_callback: Optional[Callable[[List[ProgressUpdate]], None]] = None,
    ) -> NextMessage:
        """Generate the next message for a given agent with optional progress callback."""
        if progress_callback:
            with agent.set_progress_callback(progress_callback):
                return agent.generate_formatted_message()
        return agent.generate_formatted_message()

    def send_message(
        self,
        text: str,
        user_urn: str,
        conversation_urn: str,
    ) -> Iterator[ChatMessageEvent]:
        """
        Send a message and stream progress updates.

        This is the high-level API that combines session loading,
        message posting, and progress streaming into a single call.

        Yields ChatMessageEvent domain objects (transport-agnostic).
        The caller (e.g., chat_api) wraps these for SSE.
        """
        # Load existing session
        agent = self.load_session(conversation_urn)

        # Queue for progress updates (None signals completion)
        progress_q: queue.Queue[Optional[ChatMessageEvent]] = queue.Queue()

        # Flag to track if conversation stream completed successfully
        is_active = True

        # Yield initial user message immediately for minimal latency
        user_message_timestamp = int(time.time() * 1000)

        yield ChatMessageEvent(
            message_type="TEXT",
            text=text,
            conversation_urn=conversation_urn,
            timestamp=user_message_timestamp,
            user_urn=user_urn,
        )

        # Save user message to backend
        self.conversation_manager.save_message_to_conversation(
            conversation_urn=conversation_urn,
            actor_urn=user_urn,
            actor_type=DataHubAiConversationActorTypeClass.USER,  # type: ignore[arg-type]
            message_type=DataHubAiConversationMessageTypeClass.TEXT,  # type: ignore[arg-type]
            text=text,
            timestamp=user_message_timestamp,
        )

        # Track how many updates we've already sent to avoid duplicates
        sent_update_count = 0

        # Progress callback that enqueues structured updates and saves THINKING messages
        def progress_callback(updates: List[ProgressUpdate]) -> None:
            nonlocal sent_update_count
            nonlocal is_active

            # Only emit new updates that haven't been sent yet
            new_updates = updates[sent_update_count:]
            for update in new_updates:
                timestamp = int(time.time() * 1000)
                progress_q.put(
                    ChatMessageEvent(
                        message_type=update.message_type,
                        text=update.text,
                        conversation_urn=conversation_urn,
                        timestamp=timestamp,
                    )
                )

                # Save THINKING messages to backend
                if is_active and update.message_type == "THINKING":
                    self.conversation_manager.save_message_to_conversation(
                        conversation_urn=conversation_urn,
                        actor_urn="urn:li:corpuser:datahub-ai",
                        actor_type=DataHubAiConversationActorTypeClass.AGENT,  # type: ignore[arg-type]
                        message_type=DataHubAiConversationMessageTypeClass.THINKING,  # type: ignore[arg-type]
                        text=update.text,
                        timestamp=timestamp,
                    )
            sent_update_count = len(updates)

        # Add user message to history
        self.add_user_message(agent, text)

        # Run generation in a background thread
        next_message_container: List[Optional[NextMessage]] = [None]
        error_container: List[Optional[Exception]] = [None]

        def run_generation():
            try:
                result = self._generate_with_progress(agent, progress_callback)
                next_message_container[0] = result
            except Exception as e:
                logger.exception("Error during message generation")
                error_container[0] = e
            finally:
                # Signal completion with None sentinel
                progress_q.put(None)

        generation_thread = threading.Thread(target=run_generation, daemon=True)
        generation_thread.start()

        # Yield progress updates as they arrive
        # Track completion to know if we should save the final message
        completed_successfully = False
        try:
            while True:
                try:
                    event = progress_q.get(timeout=0.05)  # 50ms for low latency
                    if event is None:  # None signals completion
                        completed_successfully = True
                        break
                    yield event
                except queue.Empty:
                    # Timeout - continue waiting (keepalives handled by caller)
                    continue
        finally:
            # Mark as inactive if stream didn't complete successfully
            if not completed_successfully:
                is_active = False

        # Wait for thread to complete
        generation_thread.join(timeout=1.0)

        # Check for errors
        if error_container[0]:
            yield ChatMessageEvent(
                message_type="TEXT",
                text="",
                conversation_urn=conversation_urn,
                timestamp=int(time.time() * 1000),
                error=str(error_container[0]),
            )
            return

        # Save and yield final assistant message
        next_message = next_message_container[0]
        if next_message:
            ai_message_timestamp = int(time.time() * 1000)

            # Only save AI message if stream completed successfully
            if is_active:
                self.conversation_manager.save_message_to_conversation(
                    conversation_urn=conversation_urn,
                    actor_urn="urn:li:corpuser:datahub-ai",
                    actor_type=DataHubAiConversationActorTypeClass.AGENT,  # type: ignore[arg-type]
                    message_type=DataHubAiConversationMessageTypeClass.TEXT,  # type: ignore[arg-type]
                    text=next_message.text,
                    timestamp=ai_message_timestamp,
                )

            yield ChatMessageEvent(
                message_type="TEXT",
                text=next_message.text,
                conversation_urn=conversation_urn,
                timestamp=ai_message_timestamp,
            )
