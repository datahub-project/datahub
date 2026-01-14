"""
ChatSessionManager for managing persistent chat sessions.

Provides a simple, instance-based API to create/load chat sessions and
send messages while handling persistence concerns via DataHubAiConversationClient.
"""

import queue
import threading
import time
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Protocol,
    Sequence,
)

from datahub_integrations.chat.agent.agent_runner import enrich_event_with_agent_data

if TYPE_CHECKING:
    from datahub_integrations.chat.chat_api import ChatContext

from datahub.metadata.schema_classes import (
    DataHubAiConversationActorTypeClass,
    DataHubAiConversationMessageTypeClass,
)
from datahub.sdk.main_client import DataHubClient
from datahub.utilities.perf_timer import PerfTimer
from fastmcp import FastMCP
from loguru import logger

from datahub_integrations.chat.agent import AgentRunner
from datahub_integrations.chat.agent.progress_tracker import ProgressUpdate
from datahub_integrations.chat.agents import (
    create_data_catalog_explorer_agent,
    create_ingestion_troubleshooting_agent,
)
from datahub_integrations.chat.chat_history import ChatHistory
from datahub_integrations.chat.config import CHAT_SSE_KEEPALIVE_INTERVAL_SECONDS
from datahub_integrations.chat.datahub_ai_conversation_client import (
    DataHubAiConversationClient,
)
from datahub_integrations.chat.types import ChatType, NextMessage
from datahub_integrations.chat.utils import combine_contexts
from datahub_integrations.mcp.mcp_server import mcp
from datahub_integrations.mcp_integration.tool import ToolWrapper
from datahub_integrations.telemetry.chat_events import (
    ChatbotInteractionEvent,
    ui_chat_id,
    ui_message_id,
)
from datahub_integrations.telemetry.telemetry import track_saas_event


@dataclass
class ChatMessageEvent:
    """Domain event for chat messages - transport-agnostic."""

    message_type: str  # TEXT, THINKING, TOOL_CALL, TOOL_RESULT, KEEPALIVE
    text: str
    conversation_urn: str
    timestamp: int
    # Optional fields
    user_urn: Optional[str] = None  # Set for user messages
    error: Optional[str] = None  # Set if there was an error
    is_keepalive: bool = False  # True for keepalive-only events

    @classmethod
    def keepalive(cls, conversation_urn: str) -> "ChatMessageEvent":
        """Create a keepalive event to prevent SSE connection timeouts."""
        return cls(
            message_type="KEEPALIVE",
            text="",
            conversation_urn=conversation_urn,
            timestamp=int(time.time() * 1000),
            is_keepalive=True,
        )


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
        context: Optional[str] = None,
    ) -> AgentRunner:
        """
        Create a configured AgentRunner instance.

        Args:
            client: DataHub client for tool execution
            history: Optional existing chat history
            extra_instructions_override: Optional override for extra instructions
            chat_type: Type of chat context (UI, Slack, Teams, etc.)
            tools: Optional tools to use
            context: Optional natural language context about the conversation

        Returns:
            Configured AgentRunner instance
        """
        ...


# Agent factory mapping
AGENT_FACTORIES: Dict[str, AgentFactory] = {
    "DataCatalogExplorer": create_data_catalog_explorer_agent,
    "IngestionTroubleshooter": create_ingestion_troubleshooting_agent,
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
        self,
        conversation_urn: str,
        agent_type: str = "DataCatalogExplorer",
        message_context: Optional["ChatContext"] = None,
    ) -> AgentRunner:
        """Load an agent session from GraphQL using conversation history.

        Conversation history is loaded with system_client, but tool execution
        uses tools_client for user permissions.

        Args:
            conversation_urn: URN of the conversation to load
            agent_type: Type of agent to create (e.g., "DataCatalogExplorer")
            message_context: Optional message-level context to combine with conversation context

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

        (
            history,
            chat_type,
            conversation_context,
        ) = self.conversation_manager.load_conversation_with_metadata(conversation_urn)

        # Combine conversation context with message context
        combined_context = combine_contexts(conversation_context, message_context)

        factory = AGENT_FACTORIES[agent_type]
        return factory(
            client=self.tools_client,
            chat_type=chat_type,
            history=history,
            tools=[mcp],
            context=combined_context,
        )

    def add_user_message(self, agent: AgentRunner, text: str) -> None:
        """Add a user message to the agent's history."""
        from datahub_integrations.chat.chat_history import HumanMessage

        agent.add_message(HumanMessage(text=text))

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
        agent_name: str | None = None,
        message_context: Optional["ChatContext"] = None,  # type: ignore
    ) -> Iterator[ChatMessageEvent]:
        """
        Send a message and stream progress updates.

        This is the high-level API that combines session loading,
        message posting, and progress streaming into a single call.

        Yields ChatMessageEvent domain objects (transport-agnostic).
        The caller (e.g., chat_api) wraps these for SSE.

        Args:
            text: Message text
            user_urn: URN of the user sending the message
            conversation_urn: URN of the conversation
            agent_name: Optional agent name/type to use
            message_context: Optional message-level context that will be combined with conversation context
        """
        timer = PerfTimer()
        timer.start()
        agent = None
        message_text = text

        # Load existing session
        # Use agent_name as agent_type if provided, otherwise default to DataCatalogExplorer
        agent_type = agent_name if agent_name else "DataCatalogExplorer"
        agent = self.load_session(conversation_urn, agent_type, message_context)

        # Queue for progress updates (None signals completion)
        progress_q: queue.Queue[Optional[ChatMessageEvent]] = queue.Queue()

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
                if update.message_type == "THINKING":
                    self.conversation_manager.save_message_to_conversation(
                        conversation_urn=conversation_urn,
                        actor_urn="urn:li:corpuser:datahub-ai",
                        actor_type=DataHubAiConversationActorTypeClass.AGENT,  # type: ignore[arg-type]
                        message_type=DataHubAiConversationMessageTypeClass.THINKING,  # type: ignore[arg-type]
                        text=update.text,
                        timestamp=timestamp,
                        agent_name=agent_type,
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
        while True:
            try:
                event = progress_q.get(timeout=CHAT_SSE_KEEPALIVE_INTERVAL_SECONDS)
                if event is None:  # None signals completion
                    break
                yield event
            except queue.Empty:
                # Timeout expired - send keepalive to prevent connection timeout
                yield ChatMessageEvent.keepalive(conversation_urn)

        # Wait for thread to complete
        generation_thread.join(timeout=1.0)

        # Determine response text and error based on outcome
        error = error_container[0]
        next_message = next_message_container[0]

        if error:
            # Use the same error message that the frontend displays for consistency
            # TODO: Ideally the frontend should read the error message from SSE payload
            # instead of hardcoding it, then we could customize messages per error type
            response_text = "Oops! An unexpected error occurred. 🥹 Please try again in a little while."
        elif next_message:
            response_text = next_message.text
        else:
            # No message to save (shouldn't happen normally)
            return

        response_timestamp = int(time.time() * 1000)

        # Save message to backend
        self.conversation_manager.save_message_to_conversation(
            conversation_urn=conversation_urn,
            actor_urn="urn:li:corpuser:datahub-ai",
            actor_type=DataHubAiConversationActorTypeClass.AGENT,  # type: ignore[arg-type]
            message_type=DataHubAiConversationMessageTypeClass.TEXT,  # type: ignore[arg-type]
            text=response_text,
            timestamp=response_timestamp,
            agent_name=agent_type,
        )

        # Yield the message event
        yield ChatMessageEvent(
            message_type="TEXT",
            text=response_text,
            conversation_urn=conversation_urn,
            timestamp=response_timestamp,
            error=str(error) if error else None,
        )

        # Emit analytics event
        is_followup_question = (
            agent.history.is_followup_datahub_ask_question
            if agent and agent.history and not error
            else False
        )
        timestamp_str = str(response_timestamp)

        event_data = ChatbotInteractionEvent(
            user_urn=user_urn,
            chat_id=ui_chat_id(conversation_urn, timestamp_str),
            message_id=ui_message_id(conversation_urn, timestamp_str),
            chatbot="datahub_ui",
            ui_user_urn=user_urn,
            ui_conversation_urn=conversation_urn,
            message_contents=message_text,
            response_error=f"{type(error).__name__}: {str(error)}" if error else None,
            response_contents=response_text if not error else None,
            response_length=len(response_text) if not error else None,
            response_generation_duration_sec=timer.elapsed_seconds(),
            is_followup_question=is_followup_question,
        )

        enrich_event_with_agent_data(event_data, agent)
        track_saas_event(event_data)
