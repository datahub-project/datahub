"""
Conversation Manager - Manages conversation lifecycle and state.

Extracted from chat_ui.py to provide a reusable conversation management layer
that can be used by both Streamlit and FastAPI frontends.
"""

import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from loguru import logger


@dataclass
class ProcessingState:
    """Processing state for a conversation."""

    is_processing: bool = False
    pending_message: Optional[Dict[str, Any]] = None
    current_progress_display: Optional[str] = None
    last_progress_display: Optional[str] = None


@dataclass
class Conversation:
    """A single conversation with its messages and metadata."""

    id: str
    urn: str
    messages: List[Dict[str, Any]] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    title: str = "New conversation"
    history: Optional[Any] = None  # ChatHistory object (from datahub_integrations)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "urn": self.urn,
            "messages": self.messages,
            "created_at": self.created_at.isoformat(),
            "title": self.title,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Conversation":
        """Create from dictionary."""
        return cls(
            id=data["id"],
            urn=data["urn"],
            messages=data.get("messages", []),
            created_at=datetime.fromisoformat(data["created_at"])
            if isinstance(data.get("created_at"), str)
            else data.get("created_at", datetime.now()),
            title=data.get("title", "New conversation"),
        )


class ConversationManager:
    """Manages conversations and their lifecycle."""

    def __init__(self) -> None:
        """Initialize conversation manager."""
        self.conversations: Dict[str, Conversation] = {}
        self.processing_states: Dict[str, ProcessingState] = {}
        self.active_conversation_id: Optional[str] = None

    def create_conversation(self, title: str = "New conversation") -> Conversation:
        """
        Create a new conversation.

        Args:
            title: Title for the conversation

        Returns:
            Created conversation
        """
        timestamp = int(time.time())
        rand_num = random.randint(1000, 9999)
        conv_id = f"conv-{timestamp}-{rand_num}"
        urn = f"urn:li:conversation:ui-{timestamp}-{rand_num}"

        conv = Conversation(
            id=conv_id,
            urn=urn,
            title=title,
        )

        self.conversations[conv_id] = conv
        self.processing_states[conv_id] = ProcessingState()

        # Set as active if this is the first conversation
        if self.active_conversation_id is None:
            self.active_conversation_id = conv_id

        logger.info(f"Created conversation: {conv_id}")
        return conv

    def get_conversation(self, conv_id: str) -> Optional[Conversation]:
        """
        Get a conversation by ID.

        Args:
            conv_id: Conversation ID

        Returns:
            Conversation or None if not found
        """
        return self.conversations.get(conv_id)

    def list_conversations(self) -> List[Conversation]:
        """
        List all conversations.

        Returns:
            List of conversations
        """
        return list(self.conversations.values())

    def delete_conversation(self, conv_id: str) -> bool:
        """
        Delete a conversation.

        Args:
            conv_id: Conversation ID to delete

        Returns:
            True if deleted, False if not found
        """
        if conv_id not in self.conversations:
            logger.warning(f"Conversation not found: {conv_id}")
            return False

        # Prevent deleting active conversation if processing
        proc_state = self.processing_states.get(conv_id)
        if proc_state and proc_state.is_processing:
            logger.warning(f"Cannot delete conversation {conv_id} - currently processing")
            return False

        del self.conversations[conv_id]
        if conv_id in self.processing_states:
            del self.processing_states[conv_id]

        # If deleted active conversation, switch to another
        if self.active_conversation_id == conv_id:
            if self.conversations:
                self.active_conversation_id = list(self.conversations.keys())[0]
            else:
                self.active_conversation_id = None

        logger.info(f"Deleted conversation: {conv_id}")
        return True

    def get_active_conversation(self) -> Optional[Conversation]:
        """
        Get the active conversation.

        Returns:
            Active conversation or None
        """
        if self.active_conversation_id:
            return self.conversations.get(self.active_conversation_id)
        return None

    def set_active_conversation(self, conv_id: str) -> bool:
        """
        Set the active conversation.

        Args:
            conv_id: Conversation ID to set as active

        Returns:
            True if set successfully, False if conversation not found
        """
        if conv_id not in self.conversations:
            logger.warning(f"Cannot set active conversation - not found: {conv_id}")
            return False

        self.active_conversation_id = conv_id
        logger.debug(f"Set active conversation: {conv_id}")
        return True

    def get_processing_state(self, conv_id: str) -> ProcessingState:
        """
        Get processing state for a conversation.

        Args:
            conv_id: Conversation ID

        Returns:
            Processing state (creates new if doesn't exist)
        """
        if conv_id not in self.processing_states:
            self.processing_states[conv_id] = ProcessingState()
        return self.processing_states[conv_id]

    def add_user_message(
        self, conv_id: str, message: str, is_auto: bool = False
    ) -> bool:
        """
        Add a user message to a conversation.

        Args:
            conv_id: Conversation ID
            message: Message text
            is_auto: Whether this is an auto-generated message

        Returns:
            True if added successfully, False if conversation not found
        """
        conv = self.conversations.get(conv_id)
        if not conv:
            logger.warning(f"Cannot add message - conversation not found: {conv_id}")
            return False

        user_msg = {
            "role": "user",
            "content": message,
            "timestamp": datetime.now(),
            "is_auto": is_auto,
        }

        conv.messages.append(user_msg)

        # Update conversation title from first user message
        if conv.title == "New conversation" and message:
            # Use first user message as title (truncated)
            max_length = 60
            if len(message) > max_length:
                conv.title = message[:max_length] + "..."
            else:
                conv.title = message
            logger.info(f"Updated conversation title to: {conv.title}")

        # Initialize ChatHistory if needed
        if conv.history is None:
            try:
                from datahub_integrations.chat.chat_history import ChatHistory

                conv.history = ChatHistory()
            except ImportError:
                logger.warning("ChatHistory not available - message tracking limited")

        # Add to ChatHistory
        if conv.history is not None:
            try:
                from datahub_integrations.chat.chat_history import HumanMessage

                conv.history.add_message(HumanMessage(text=message))
            except ImportError:
                pass

        logger.debug(f"Added user message to conversation {conv_id}")
        return True

    def add_assistant_message(
        self,
        conv_id: str,
        content: str,
        duration: float = 0.0,
        event_count: int = 0,
        success: bool = True,
        is_auto: bool = False,
    ) -> bool:
        """
        Add an assistant message to a conversation.

        Args:
            conv_id: Conversation ID
            content: Message content
            duration: Processing duration in seconds
            event_count: Number of events in the response
            success: Whether the message was successful
            is_auto: Whether this is an auto-generated response

        Returns:
            True if added successfully, False if conversation not found
        """
        conv = self.conversations.get(conv_id)
        if not conv:
            logger.warning(f"Cannot add message - conversation not found: {conv_id}")
            return False

        assistant_msg = {
            "role": "assistant",
            "content": content,
            "timestamp": datetime.now(),
            "duration": duration,
            "event_count": event_count,
            "success": success,
            "is_auto": is_auto,
        }

        conv.messages.append(assistant_msg)
        logger.debug(f"Added assistant message to conversation {conv_id}")
        return True

    def clear_conversation(self, conv_id: str) -> bool:
        """
        Clear all messages from a conversation.

        Args:
            conv_id: Conversation ID

        Returns:
            True if cleared successfully, False if conversation not found
        """
        conv = self.conversations.get(conv_id)
        if not conv:
            logger.warning(f"Cannot clear conversation - not found: {conv_id}")
            return False

        conv.messages.clear()

        # Reset ChatHistory
        if conv.history is not None:
            try:
                from datahub_integrations.chat.chat_history import ChatHistory

                conv.history = ChatHistory()
            except ImportError:
                conv.history = None

        logger.info(f"Cleared conversation: {conv_id}")
        return True
