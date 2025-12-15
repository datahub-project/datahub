"""
DataHub AI Conversation Client for chat sessions.

This module provides only the essential functionality needed for the streaming chat API.
All conversation CRUD operations are handled by the GraphQL API, not this Python service.
"""

from typing import Any, Dict, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import EmitMode
from datahub.metadata.schema_classes import (
    DataHubAiConversationActorTypeClass,
    DataHubAiConversationInfoClass,
    DataHubAiConversationMessageTypeClass,
)
from datahub.sdk.main_client import DataHubClient
from loguru import logger

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
    Message,
    ReasoningMessage,
)
from datahub_integrations.chat.config import CHAT_MAX_CONVERSATION_MESSAGES
from datahub_integrations.chat.types import ChatType


def map_origin_type_to_chat_type(origin_type: Optional[str]) -> ChatType:
    """
    Map GraphQL originType to ChatType.

    Args:
        origin_type: The originType from GraphQL (e.g., "DATAHUB_UI")

    Returns:
        Corresponding ChatType enum value
    """
    if not origin_type:
        return ChatType.DEFAULT

    # Map GraphQL originType values to ChatType
    origin_type_mapping = {
        "DATAHUB_UI": ChatType.DATAHUB_UI,
        "SLACK": ChatType.SLACK,
        "TEAMS": ChatType.TEAMS,
    }

    return origin_type_mapping.get(origin_type, ChatType.DEFAULT)


# GraphQL query constant
GET_DATAHUB_AI_CONVERSATION_QUERY = """
query GetDataHubAiConversation($urn: String!) {
    getDataHubAiConversation(urn: $urn) {
        urn
        originType
        context {
            text
            entityUrns
        }
        messages {
            type
            time
            actor {
                type
                actor
            }
            content {
                text
            }
            agentName
        }
    }
}
"""


def convert_graphql_message_to_chat_message(
    graphql_msg: Dict[str, Any],
) -> Optional[Message]:
    """
    Convert a GraphQL message to a ChatHistory message.

    Args:
        graphql_msg: Message from GraphQL response

    Returns:
        Corresponding ChatHistory message or None if invalid
    """
    message_type = graphql_msg.get("type", "TEXT")
    actor_type = graphql_msg.get("actor", {}).get("type", "USER")
    text = graphql_msg.get("content", {}).get("text", "")

    if not text:
        return None

    # Map GraphQL message to ChatHistory message based on actor type
    if actor_type == "USER":
        return HumanMessage(text=text)
    elif actor_type == "AGENT":
        return _convert_agent_message(message_type, text)

    return None


def _convert_agent_message(message_type: str, text: str) -> Message:
    """
    Convert an agent message based on its type.

    Args:
        message_type: Type of message (TEXT, THINKING, TOOL_CALL, TOOL_RESULT)
        text: Message text

    Returns:
        Appropriate ChatHistory message
    """
    if message_type == "THINKING":
        return ReasoningMessage(text=text)
    elif message_type in ("TOOL_CALL", "TOOL_RESULT"):
        # For tool messages, store as reasoning for now
        # TODO: Reconstruct proper ToolCallRequest/ToolResult objects
        return ReasoningMessage(text=text)
    else:
        # Regular AI response (TEXT type)
        return AssistantMessage(text=text)


def convert_graphql_messages_to_chat_history(
    graphql_messages: List[Dict[str, Any]],
) -> ChatHistory:
    """
    Convert a list of GraphQL messages to a ChatHistory object.

    Limits the conversation to the most recent CHAT_MAX_CONVERSATION_MESSAGES
    to prevent unbounded memory growth and maintain reasonable context windows.

    Args:
        graphql_messages: List of messages from GraphQL response

    Returns:
        ChatHistory with converted messages (limited to most recent N)
    """
    messages = []
    for msg in graphql_messages:
        converted = convert_graphql_message_to_chat_message(msg)
        if converted:
            messages.append(converted)

    # Limit to most recent messages if we exceed the maximum
    if len(messages) > CHAT_MAX_CONVERSATION_MESSAGES:
        original_count = len(messages)
        messages = messages[-CHAT_MAX_CONVERSATION_MESSAGES:]
        logger.info(
            f"Conversation history truncated after exceeding max number of messages: {original_count} messages -> {len(messages)} (most recent {CHAT_MAX_CONVERSATION_MESSAGES})"
        )

    return ChatHistory(messages=messages)


class DataHubAiConversationClient:
    """Minimal conversation client for chat sessions."""

    def __init__(self, client: DataHubClient):
        self.client = client
        # Simple in-memory storage for active chat sessions only
        self._conversations: Dict[str, ChatHistory] = {}

    def _fetch_conversation_from_graphql(
        self, conversation_urn: str
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch conversation data from GraphQL.

        Args:
            conversation_urn: URN of the conversation to fetch

        Returns:
            Conversation data dict or None if not found/error
        """
        try:
            variables = {"urn": conversation_urn}
            logger.info(f"Fetching conversation from GraphQL: {conversation_urn}")

            # Access the underlying DataHubGraph instance for GraphQL queries
            result = self.client._graph.execute_graphql(
                GET_DATAHUB_AI_CONVERSATION_QUERY, variables=variables
            )

            if not result:
                logger.warning(f"No data returned from GraphQL for {conversation_urn}")
                return None

            logger.info(f"Result: {result}")

            logger.info(f"Result type: {type(result)}")

            conversation = result.get("getDataHubAiConversation")
            if not conversation:
                logger.info(f"Conversation {conversation_urn} not found in GraphQL")
                return None

            return conversation

        except Exception as e:
            logger.error(
                f"Failed to fetch conversation from GraphQL: {conversation_urn}: {e}"
            )
            return None

    def load_chat_history_from_graphql(self, conversation_urn: str) -> ChatHistory:
        """
        Load chat history from Java server state store via GraphQL.

        This fetches the conversation from the Java GraphQL server, which maintains
        the authoritative state of the conversation including all messages.

        Args:
            conversation_urn: URN of the conversation to load

        Returns:
            ChatHistory with messages, or empty history if not found/error
        """
        conversation = self._fetch_conversation_from_graphql(conversation_urn)

        logger.info(f"Conversation: {conversation}")

        if not conversation:
            return ChatHistory(messages=[])

        graphql_messages = conversation.get("messages", [])
        logger.info(
            f"Found {len(graphql_messages)} messages in conversation {conversation_urn}"
        )

        chat_history = convert_graphql_messages_to_chat_history(graphql_messages)
        logger.info(
            f"Loaded {len(chat_history.messages)} messages for conversation {conversation_urn}"
        )

        logger.info(f"Chat history: {chat_history}")

        return chat_history

    def load_conversation_with_metadata(
        self, conversation_urn: str
    ) -> tuple[ChatHistory, ChatType, Optional[str]]:
        """
        Load chat history, origin type, and context from Java server state store via GraphQL.

        Args:
            conversation_urn: URN of the conversation to load

        Returns:
            Tuple of (ChatHistory, ChatType, context_text) or (empty ChatHistory, DEFAULT, None) if not found/error
        """
        conversation = self._fetch_conversation_from_graphql(conversation_urn)

        if not conversation:
            return ChatHistory(messages=[]), ChatType.DEFAULT, None

        # Extract origin type and map to ChatType
        origin_type = conversation.get("originType")
        chat_type = map_origin_type_to_chat_type(origin_type)

        # Extract context text
        context_text = None
        context = conversation.get("context")
        if context and context.get("text"):
            context_text = context.get("text")
            logger.info(
                f"Conversation {conversation_urn} has context: {context_text[:100]}..."
            )

        logger.info(
            f"Conversation {conversation_urn} has originType: {origin_type} -> ChatType: {chat_type}"
        )

        # Load chat history
        graphql_messages = conversation.get("messages", [])
        chat_history = convert_graphql_messages_to_chat_history(graphql_messages)

        logger.info(
            f"Loaded conversation {conversation_urn} with {len(chat_history.messages)} messages and type {chat_type}"
        )

        return chat_history, chat_type, context_text

    def load_chat_history(self, conversation_urn: str) -> ChatHistory:
        """
        Load chat history from conversation.

        This now always fetches from the Java server state store via GraphQL
        to ensure we have the latest conversation state.
        """
        return self.load_chat_history_from_graphql(conversation_urn)

    def save_conversation(
        self, conversation_urn: str, chat_history: ChatHistory
    ) -> bool:
        """Save chat history to conversation."""
        try:
            self._conversations[conversation_urn] = chat_history
            logger.info(
                f"Saved conversation {conversation_urn} with {len(chat_history.messages)} messages"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to save conversation {conversation_urn}: {e}")
            return False

    def save_message_to_conversation(
        self,
        conversation_urn: str,
        actor_urn: str,
        actor_type: DataHubAiConversationActorTypeClass,
        message_type: DataHubAiConversationMessageTypeClass,
        text: str,
        timestamp: int,
        agent_name: str | None = None,
    ) -> None:
        """
        Save a message to the conversation aspect.

        This appends the message to the messages array in DataHubAiConversationInfo
        and emits the aspect synchronously to ensure immediate persistence.
        Sets the conversation title to the first user message if not already set.

        Args:
            conversation_urn: URN of the conversation
            actor_urn: URN of the actor (user or agent)
            actor_type: Type of actor (USER or AGENT)
            message_type: Type of message (TEXT, THINKING, TOOL_CALL, TOOL_RESULT)
            text: Message content
            timestamp: Message timestamp in milliseconds
        """
        try:
            logger.info(
                f"Attempting to save message to conversation {conversation_urn}: "
                f"actor={actor_urn}, type={actor_type}, message_type={message_type}, "
                f"text_length={len(text)}"
            )

            # Get the current conversation aspect
            conversation_info = self.client._graph.get_aspect(
                entity_urn=conversation_urn,
                aspect_type=DataHubAiConversationInfoClass,
            )

            # If no conversation exists yet, raise an error
            if conversation_info is None:
                logger.error(
                    f"Conversation {conversation_urn} not found in DataHub - cannot save message"
                )
                raise ValueError(
                    f"Conversation with urn {conversation_urn} is not found"
                )

            logger.info(
                f"Found existing conversation {conversation_urn}: "
                f"current_messages={len(conversation_info.messages) if conversation_info.messages else 0}, "
                f"title={getattr(conversation_info, 'title', None)}"
            )

            # Determine title for the new aspect
            title = getattr(conversation_info, "title", None)
            if (
                actor_type == DataHubAiConversationActorTypeClass.USER
                and message_type == DataHubAiConversationMessageTypeClass.TEXT
                and not title
            ):
                # Truncate title to first 500 characters for readability
                title = text[:500]
                logger.info(
                    f"Setting conversation title to first user message: {title}"
                )

            # Convert existing aspect to dict and back to ensure clean serialization
            # This preserves all required fields while removing problematic state
            aspect_dict = conversation_info.to_obj()

            # Get existing messages (already as dicts from to_obj)
            existing_messages_dicts = aspect_dict.get("messages", [])

            # Build new message as dict directly to avoid Avro serialization issues
            new_message_dict = {
                "type": str(message_type),
                "time": timestamp,
                "actor": {
                    "type": str(actor_type),
                    "actor": actor_urn,
                },
                "content": {
                    "text": text,
                },
            }

            # Add agent name if provided
            if agent_name is not None:
                new_message_dict["agentName"] = agent_name

            # Append new message to existing messages
            aspect_dict["messages"] = existing_messages_dicts + [new_message_dict]

            if title is not None:
                aspect_dict["title"] = title

            logger.info(
                f"Appended message to conversation. New message count: {len(aspect_dict['messages'])}"
            )

            new_aspect = DataHubAiConversationInfoClass.from_obj(aspect_dict)

            # Emit the aspect
            mcp = MetadataChangeProposalWrapper(
                entityUrn=conversation_urn,
                aspect=new_aspect,
            )

            logger.info(
                f"Emitting conversation aspect to DataHub for {conversation_urn}"
            )
            try:
                logger.info(
                    f"authorization request headers: {dict(self.client._graph._session.headers)}"
                )
            except Exception:
                # Silently skip header logging if it fails (e.g., in tests with mocks)
                pass
            self.client._graph.emit(mcp, emit_mode=EmitMode.SYNC_PRIMARY)
            logger.info(
                f"Successfully saved {actor_type} message to conversation {conversation_urn}. "
                f"Total messages: {len(aspect_dict['messages'])}"
            )

        except Exception as e:
            logger.error(
                f"Failed to save message to conversation {conversation_urn}: {e}",
                exc_info=True,
            )
