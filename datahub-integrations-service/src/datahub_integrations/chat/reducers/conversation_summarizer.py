from typing import TYPE_CHECKING, List, Sequence

from loguru import logger
from typing_extensions import override

from datahub_integrations.gen_ai.bedrock_converse import converse_with_bedrock

if TYPE_CHECKING:
    from mypy_boto3_bedrock_runtime.type_defs import MessageUnionTypeDef


from datahub_integrations.chat.chat_history import (
    ChatHistory,
    Message,
    SummaryMessage,
    ToolCallRequest,
)
from datahub_integrations.chat.context_reducer import (
    ChatContextReducer,
    ContextReducerConfig,
    TokenCountEstimator,
)
from datahub_integrations.gen_ai.model_config import BedrockModel

_CREATE_SUMMARY_SYSTEM_PROMPT = """\
You are a helpful AI assistant tasked with summarizing conversations.
Provide a concise and complete summary of the entire conversation.
Focus on information that would be helpful for continuing the conversation, such as
user's query, assistant's reasoning, key findings from tool results relevant to answering user's query.
You must include important DataHub entities with their human-readable identifiers (qualifiedName for datasets, dashboardId for dashboards, etc.) from existing messages.
You must not include information that is not present in the existing messages.
"""

_UPDATE_SUMMARY_SYSTEM_PROMPT = """\
You are a helpful AI assistant tasked with summarizing conversations.
Extend existing summary by taking into account the new messages in conversation.
Focus on information that would be helpful for continuing the conversation, such as
user's query, assistant's reasoning, key findings from tool results relevant to answering user's query.
You must include important DataHub entities with their human-readable identifiers (qualifiedName for datasets, dashboardId for dashboards, etc.) from existing messages.
You must not include information that is not present in the existing messages.
"""


class ConversationSummarizer(ChatContextReducer):
    """Summarizes older messages when hitting token limit.

    This strategy preserves high-level context and conversation flow by:
    1. Keeping recent messages intact
    2. Summarizing older messages into a single summary message
    3. Maintaining conversation continuity while reducing token count

    The summarization preserves:
    - User intent and conversation goals
    - Key decisions and outcomes
    - Important context from tool results
    - Overall conversation flow

    This approach is most effective for long conversations with multiple topics
    where preserving the high-level context is more important than specific details.
    """

    def __init__(
        self,
        token_estimator: TokenCountEstimator,
        config: ContextReducerConfig,
        num_recent_messages_to_keep: int,
        summarization_model: BedrockModel | str,
    ) -> None:
        super().__init__(token_estimator, config)
        self.num_recent_messages_to_keep = num_recent_messages_to_keep
        self.summarization_model = summarization_model

    @override
    def _reduce(self, history: ChatHistory) -> List[Message]:
        split_index = self.split_at_context_fit(history.context_messages)

        assert split_index < len(history.context_messages), (
            "No older messages to summarize"
        )

        messages_to_summarize = history.context_messages[:split_index]
        remaining_messages = history.context_messages[split_index:]

        logger.info(f"Summarizing {len(messages_to_summarize)} messages")
        logger.info(f"Remaining messages: {len(remaining_messages)}")

        # If old summary existed, it would be at head of old_messages
        summary_text = self._create_or_update_summary(messages_to_summarize)

        if (
            len(remaining_messages) < self.num_recent_messages_to_keep
            and len(history.context_messages) > self.num_recent_messages_to_keep
        ):
            # NOTE: In this cases, there is overlap in summary and recent messages
            # This should not be a problem, but is subject to validation
            remaining_messages = history.context_messages[
                -self.num_recent_messages_to_keep :
            ]

        remaining_messages = self.adjust_remaining_messages(remaining_messages)
        reduced_messages = [SummaryMessage(text=summary_text)] + remaining_messages

        return reduced_messages

    def split_at_context_fit(self, messages: List[Message]) -> int:
        """
        Determines the index at which to split the message history so that the
        messages up to (but not including) this index fit within the allowed context window.

        This method iteratively removes messages from the end of the list until the
        remaining messages fit within the token limit as determined by the context reducer.
        It also ensures that a ToolCallRequest is not left dangling at the end of the
        message list without a corresponding ToolResult, to avoid model errors.

        Args:
            messages (List[Message]): The list of messages to consider for context fitting.

        Returns:
            int: The index at which to split the messages. Messages before this index
                 should be summarized, and messages from this index onward are kept as-is.
        """
        # Start with all messages, process until right split point is found
        # Typically this will be right before last message, i.e. at len - 1
        messages_that_fit = messages.copy()
        index = len(messages_that_fit)

        while self.needs_reduction(ChatHistory(messages=messages_that_fit)):
            messages_that_fit.pop()
            index -= 1

        # Do not include last tool call request as it may trigger summarizer to return tool's output
        if messages_that_fit and isinstance(messages_that_fit[-1], ToolCallRequest):
            index -= 1

        return index

    def _create_or_update_summary(self, messages: List[Message]) -> str:
        # Past summary, if present, will be first message
        past_summary_message = (
            messages[0] if isinstance(messages[0], SummaryMessage) else None
        )
        # there should always at most one summary message and not more
        if past_summary_message:
            return self._update_summary_text(past_summary_message.text, messages[1:])
        else:
            return self._create_summary_text(messages)

    def _prepare_conversation_text(self, messages: List[Message]) -> str:
        formatted_messages = [message.to_obj() for message in messages]
        text = "\n".join(
            [
                f"{fmt_message['role']}: {fmt_message['content']}"
                for fmt_message in formatted_messages
            ]
        )
        return text

    def _create_summary_text(self, messages: List[Message]) -> str:
        bedrock_messages: Sequence["MessageUnionTypeDef"] = [
            {
                "role": "user",
                "content": [{"text": self._prepare_conversation_text(messages)}],
            }
        ]

        return converse_with_bedrock(
            {"text": _CREATE_SUMMARY_SYSTEM_PROMPT},
            bedrock_messages,
            self.summarization_model,
            temperature=0.3,
            max_tokens=1024,
        )

    def _update_summary_text(
        self, past_summary: str, new_messages: List[Message]
    ) -> str:
        bedrock_messages: Sequence["MessageUnionTypeDef"] = [
            {
                "role": "assistant",
                "content": [{"text": f"Existing summary:\n {past_summary}"}],
            },
            {
                "role": "user",
                "content": [{"text": self._prepare_conversation_text(new_messages)}],
            },
        ]
        return converse_with_bedrock(
            {"text": _UPDATE_SUMMARY_SYSTEM_PROMPT},
            bedrock_messages,
            self.summarization_model,
            temperature=0.3,
            max_tokens=2048,
        )
