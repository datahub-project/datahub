from typing import List

from typing_extensions import override

from datahub_integrations.chat.chat_history import (
    ChatHistory,
    HumanMessage,
    Message,
    SummaryMessage,
)
from datahub_integrations.chat.context_reducer import (
    ChatContextReducer,
    ContextReducerConfig,
)
from datahub_integrations.mcp._token_estimator import TokenCountEstimator


class SlidingWindowReducer(ChatContextReducer):
    """Keeps only the most recent N messages, discarding older ones.

    This strategy provides a reliable backstop that guarantees token limit compliance
    by simply truncating the conversation to the most recent messages. It's extremely
    simple, fast, and predictable.

    This approach is most effective as a final fallback when other strategies
    are insufficient, ensuring that context window errors never occur.

    Pros:
    - Extremely simple and fast implementation
    - Predictable token reduction
    - No LLM calls required
    - Preserves exact recent context
    - Low latency and cost
    - Reliable backstop - guarantees token limit compliance

    Cons:
    - Loses all historical context
    - May break conversation continuity
    - User may need to repeat previous context
    - Not suitable for long-running conversations
    - May lose important earlier decisions or context
    """

    def __init__(
        self,
        token_estimator: TokenCountEstimator,
        config: ContextReducerConfig,
        max_messages: int,
    ) -> None:
        super().__init__(token_estimator, config)
        self.max_messages = max_messages

    @override
    def _reduce(self, history: ChatHistory) -> List[Message]:
        # Keep only the most recent messages
        # Maybe add info in prompt as to this being partial conversation ?

        if len(history.context_messages) <= self.max_messages:
            return history.context_messages  # NOOP

        reduced_messages = history.context_messages[-self.max_messages :]

        # Adds upto 2 additional messages
        # One for balancing tool result and other for including human/summary message
        reduced_messages = self.adjust_remaining_messages(reduced_messages)

        # Without information about user's intent, agent will loose its direction.
        if not any(
            isinstance(m, (HumanMessage, SummaryMessage)) for m in reduced_messages
        ):
            last_human_message = next(
                (
                    m
                    for m in reversed(history.context_messages)
                    if isinstance(m, (HumanMessage))
                ),
                None,
            )
            last_summary_message = next(
                (
                    m
                    for m in reversed(history.context_messages)
                    if isinstance(m, (SummaryMessage))
                ),
                None,
            )
            last_intent_message = last_human_message or last_summary_message
            assert last_intent_message is not None, (
                "Neither human message nor summary message found in history"
            )  # IMPOSSIBLE
            reduced_messages = [last_intent_message] + reduced_messages

        return reduced_messages
