import json
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import List, Union

import mlflow
from datahub.utilities.perf_timer import PerfTimer
from loguru import logger

from datahub_integrations.chat.chat_history import (
    ChatHistory,
    Message,
    ToolResult,
    ToolResultError,
)
from datahub_integrations.mcp._token_estimator import TokenCountEstimator


@dataclass
class ContextReducerConfig:
    llm_token_limit: int
    safety_buffer: int
    system_message_tokens: int
    tool_config_tokens: int


@dataclass
class ReductionMetadata:
    reducer_name: str
    time_in_seconds: float

    num_tokens_before: int
    num_tokens_after: int

    num_messages_before: int
    num_messages_after: int


class ChatContextReducer(ABC):
    """
    Context reducers implement different strategies for managing token limits
    by reducing the size of chat history when it approaches model limits.
    """

    def __init__(
        self, token_estimator: TokenCountEstimator, config: ContextReducerConfig
    ):
        """Initialize the context reducer.

        Args:
            token_estimator: Token estimator for measuring context size
            config: Configuration for reduction behavior
        """
        self.token_estimator = token_estimator
        self.config = config

    @abstractmethod
    def _reduce(self, history: ChatHistory) -> List[Message]:
        """Prepare chat context, applying reduction strategy if needed.

        This method should check if the history exceeds token limits and
        apply the appropriate reduction strategy to bring it within limits.

        Also updates reduction metadata

        Args:
            history: The original chat history

        """

        # Be aware of below error cases in converse API when adding implementation to this method
        # 1. The model returned the following errors: messages: final assistant content cannot end with trailing whitespace
        # 2. The number of toolResult blocks at messages.1.content exceeds the number of toolUse blocks of previous turn.
        # 3. The model returned the following errors: messages.3: `tool_use` ids were found without `tool_result` blocks immediately after: tooluse_tgDj5j6-RSukk5YqMKVOng. Each `tool_use` block must have a corresponding `tool_result` block in the next message.

        # Use something like adjust_remaining_messages for case 2
        pass

    def reduce(self, history: ChatHistory) -> None:
        if not self.needs_reduction(history):
            return

        num_messages_before = len(history.context_messages)
        num_tokens_before = self._estimate_tokens(history.context_messages)

        logger.info(f"Estimated tokens: {num_tokens_before}")

        with (
            mlflow.start_span(
                f"reduce_history_{self.__class__.__name__}",
                span_type=mlflow.entities.SpanType.TOOL,
                attributes={"reducer_name": self.__class__.__name__},
            ) as span,
            PerfTimer() as perf_timer,
        ):
            logger.info(f"Reducing history with {self.__class__.__name__}")
            try:
                reduced_history = self._reduce(history)

                num_messages_after = len(reduced_history)
                num_tokens_after = self._estimate_tokens(reduced_history)

                reduction_metadata = ReductionMetadata(
                    reducer_name=self.__class__.__name__,
                    time_in_seconds=perf_timer.elapsed_seconds(),
                    num_tokens_before=num_tokens_before,
                    num_tokens_after=num_tokens_after,
                    num_messages_before=num_messages_before,
                    num_messages_after=num_messages_after,
                )
                span.set_attributes(asdict(reduction_metadata))
                history.set_reduced_history(reduced_history, asdict(reduction_metadata))
            except Exception as e:
                logger.error(
                    f"Error reducing history with {self.__class__.__name__}: {e}"
                )

    def adjust_remaining_messages(
        self, remaining_messages: List[Message]
    ) -> List[Message]:
        if len(remaining_messages) > 0 and isinstance(
            remaining_messages[0], (ToolResult, ToolResultError)
        ):
            return [remaining_messages[0].tool_request] + remaining_messages
        return remaining_messages

    def _estimate_tokens(self, history: Union[ChatHistory, List[Message]]) -> int:
        return (
            sum(
                self.token_estimator.estimate_tokens(json.dumps(message.to_obj()))
                for message in (
                    history.context_messages
                    if isinstance(history, ChatHistory)
                    else history
                )
            )
            + self.config.safety_buffer
            + self.config.system_message_tokens
            + self.config.tool_config_tokens
        )

    def needs_reduction(self, history: ChatHistory) -> bool:
        estimated_tokens = self._estimate_tokens(history.context_messages)

        return estimated_tokens > self.config.llm_token_limit
