import threading
from typing import Dict, List, Sequence

import cachetools
from loguru import logger

from datahub_integrations.chat.chat_history import (
    ChatHistory,
    Message,
)

# Teams conversation history management
#
# Teams doesn't have built-in threading like Slack, but we can track conversations
# using conversation IDs and message timestamps to maintain context.
#
# We want our agent to be able to see:
# 1. the history of messages in the conversation
# 2. the intermediate tool calls and results, which aren't visible to the user
#
# This mechanism is fully in-memory, and therefore needs to be best-effort
# and degrade gracefully. However, it significantly improves the common case UX.
# Eventually it might make sense to persist this info back to GMS.


class TeamsConversationHistory:
    def __init__(self, conversation_id: str) -> None:
        self.conversation_id = conversation_id

        self._is_full_history = False

        # (message_ts) -> teams message
        self._teams_messages: Dict[str, Message] = {}
        # (message_ts) -> incremental chat history associated with this output message
        self._thinking: Dict[str, List[Message]] = {}

    def add_message(
        self, message_ts: str, message: Message, is_latest_message: bool = False
    ) -> None:
        # We assume that messages are added in order. This should be the case, since
        # messages on a single conversation should come in from Teams in order.
        self._teams_messages[message_ts] = message
        if is_latest_message:
            # For simplicity, we'll assume we have full history when we add the first message
            # In a more sophisticated implementation, we could try to fetch conversation history
            self._is_full_history = True

    def add_thinking(self, message_ts: str, thinking: Sequence[Message]) -> None:
        self._thinking[message_ts] = list(thinking)

    def set_full_history(self, messages: Dict[str, Message]) -> None:
        self._teams_messages = messages
        self._is_full_history = True

    def message_count(self) -> int:
        return len(self._teams_messages)

    def is_limited_history(self) -> bool:
        return not self._is_full_history

    def get_chat_history(self) -> ChatHistory:
        # We're relying on dicts being ordered for correctness here.
        messages: Dict[str, Message | List[Message]] = {
            message_ts: message for message_ts, message in self._teams_messages.items()
        }

        for message_ts, thinking in self._thinking.items():
            if message_ts in messages:
                # The thinking messages should include the final "respond to user" call,
                # so it's ok to ignore the message we fetched from Teams.
                messages[message_ts] = thinking
            else:
                logger.debug(
                    f"Skipping thinking message {message_ts} because it's not in the original messages"
                )

        linearized_messages = []
        for message in messages.values():
            if isinstance(message, List):
                linearized_messages.extend(message)
            else:
                linearized_messages.append(message)

        return ChatHistory(
            messages=linearized_messages,
            extra_properties=dict(
                conversation_id=self.conversation_id,
                is_limited_history=self.is_limited_history(),
            ),
        )


# I'd estimate that a typical Teams chat history will be ~40k tokens, and a
# max size will one will be ~200k tokens. Assuming ~4 characters per token,
# that's an avg 160kb and max 800kb per chat.
_DEFAULT_TEAMS_HISTORY_CACHE_MAXSIZE = 200


class TeamsHistoryCache:
    def __init__(
        self,
        maxsize: int = _DEFAULT_TEAMS_HISTORY_CACHE_MAXSIZE,
    ) -> None:
        assert maxsize > 0

        self._lock = threading.Lock()

        # (conversation_id) -> TeamsConversationHistory
        self._conversations: cachetools.LRUCache[str, TeamsConversationHistory] = (
            cachetools.LRUCache(maxsize=maxsize)
        )

    def get_conversation(self, conversation_id: str) -> TeamsConversationHistory:
        with self._lock:
            return self._conversations.setdefault(
                conversation_id, TeamsConversationHistory(conversation_id)
            )
