import threading
from typing import Dict, List, Sequence

import cachetools
from loguru import logger

from datahub_integrations.chat.chat_history import (
    ChatHistory,
    Message,
)

# We want our agent to be able to see:
# 1. the history of messages in the thread
# 2. the intermediate tool calls and results, which aren't visible to the user
#
# 1 is further complicated by the fact that we don't always have the permissions to
# read all of the messages in the thread. In those cases, we have to save each
# user message and bot response ourselves.
# 2 is a bit complicated because we have to send the message first before we
# know the message_ts and can save it into the thread's history. This isn't too
# bad though, since we send the message first to show the user our progress anyways.
#
# This entire mechanism is fully in-memory, and therefore needs to be best-effort
# and degrade gracefully. However, it significantly improves the common case UX.
# Eventually it might make sense to persist this info back to GMS.


class SlackThreadHistory:
    def __init__(self, channel_id: str, thread_ts: str) -> None:
        self.channel_id = channel_id
        self.thread_ts = thread_ts

        self._is_full_history = False

        # (message_ts) -> slack message
        self._slack_messages: Dict[str, Message] = {}
        # (message_ts) -> incremental chat history associated with this output message
        self._thinking: Dict[str, List[Message]] = {}

    def add_message(
        self, message_ts: str, message: Message, is_latest_message: bool = False
    ) -> None:
        # We assume that messages are added in order. This should be the case, since
        # messages on a single thread should come in from Slack in order.
        self._slack_messages[message_ts] = message
        if is_latest_message:
            # If message_ts == thread_ts, then the thread has a single message total.
            # Otherwise, the thread may have older messages that we don't have access to.
            self._is_full_history = message_ts == self.thread_ts

    def add_thinking(self, message_ts: str, thinking: Sequence[Message]) -> None:
        self._thinking[message_ts] = list(thinking)

    def set_full_history(self, messages: Dict[str, Message]) -> None:
        self._slack_messages = messages
        self._is_full_history = True

    def message_count(self) -> int:
        return len(self._slack_messages)

    def is_limited_history(self) -> bool:
        return not self._is_full_history

    def get_chat_history(self) -> ChatHistory:
        # We're relying on dicts being ordered for correctness here.
        messages: Dict[str, Message | List[Message]] = {
            message_ts: message for message_ts, message in self._slack_messages.items()
        }

        for message_ts, thinking in self._thinking.items():
            if message_ts in messages:
                # The thinking messages should include the final "respond to user" call,
                # so it's ok to ignore the message we fetched from Slack.
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
                channel_id=self.channel_id,
                thread_ts=self.thread_ts,
                is_limited_history=self.is_limited_history(),
            ),
        )


# I'd estimate that a typical chat history will be ~40k tokens, and a
# max size will one will be ~200k tokens. Assuming ~4 characters per token,
# that's an avg 160kb and max 800kb per chat.
_DEFAULT_SLACK_HISTORY_CACHE_MAXSIZE = 200


class SlackHistoryCache:
    def __init__(
        self,
        maxsize: int = _DEFAULT_SLACK_HISTORY_CACHE_MAXSIZE,
    ) -> None:
        assert maxsize > 0

        self._lock = threading.Lock()

        # (channel_id, thread_ts) -> SlackThreadHistory
        self._threads: cachetools.LRUCache[tuple[str, str], SlackThreadHistory] = (
            cachetools.LRUCache(maxsize=maxsize)
        )

    def get_thread(self, channel_id: str, thread_ts: str) -> SlackThreadHistory:
        with self._lock:
            return self._threads.setdefault(
                (channel_id, thread_ts), SlackThreadHistory(channel_id, thread_ts)
            )
