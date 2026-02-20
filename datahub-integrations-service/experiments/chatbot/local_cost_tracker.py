"""Local cost tracker for accumulating token usage in the chat UI.

This module provides a CostTrackerProtocol implementation that accumulates
token usage locally for display in the Streamlit UI, while delegating to
a real CostTracker for OTEL metrics.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, NamedTuple

from loguru import logger

from datahub_integrations.observability.cost import (
    CostEstimate,
    CostTracker,
    CostTrackerProtocol,
    TokenUsage,
)
from datahub_integrations.observability.metrics_constants import AIModule

if TYPE_CHECKING:
    from datahub_integrations.observability.bot_metrics import BotPlatform


class ModelKey(NamedTuple):
    """Key for identifying a provider/model pair."""

    provider: str
    model: str


@dataclass
class ModelUsageStats:
    """Usage statistics for a single provider/model.

    Tracks all TokenUsage properties separately:
    - prompt_tokens: Non-cached input tokens
    - completion_tokens: Output tokens
    - cache_read_tokens: Input tokens served from cache
    - cache_write_tokens: Input tokens written to cache
    - total_cost: Accumulated cost in USD
    """

    prompt_tokens: int = 0
    completion_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    total_cost: float = 0.0

    @property
    def input_tokens(self) -> int:
        """Total input tokens (prompt + cache_read + cache_write)."""
        return self.prompt_tokens + self.cache_read_tokens + self.cache_write_tokens


class LocalCostTracker(CostTrackerProtocol):
    """CostTrackerProtocol implementation that accumulates usage locally for UI display.

    Uses composition to delegate to a real CostTracker for OTEL metrics while
    also tracking turn-level and session-total token counts and costs locally,
    broken down by provider/model.
    """

    def __init__(self) -> None:
        # Delegate to real CostTracker for OTEL metrics
        self._delegate = CostTracker()

        # Per-model usage, keyed by ModelKey(provider, model)
        self.turn_usage: dict[ModelKey, ModelUsageStats] = {}
        self.total_usage: dict[ModelKey, ModelUsageStats] = {}

    # ===== Aggregated properties for convenience =====

    @property
    def turn_total_cost(self) -> float:
        """Total cost for current turn across all models."""
        return sum(s.total_cost for s in self.turn_usage.values())

    @property
    def total_total_cost(self) -> float:
        """Total cost for session across all models."""
        return sum(s.total_cost for s in self.total_usage.values())

    @property
    def turn_input_tokens(self) -> int:
        """Total input tokens for current turn across all models."""
        return sum(s.input_tokens for s in self.turn_usage.values())

    @property
    def turn_completion_tokens(self) -> int:
        """Total completion tokens for current turn across all models."""
        return sum(s.completion_tokens for s in self.turn_usage.values())

    @property
    def total_input_tokens(self) -> int:
        """Total input tokens for session across all models."""
        return sum(s.input_tokens for s in self.total_usage.values())

    @property
    def total_completion_tokens(self) -> int:
        """Total completion tokens for session across all models."""
        return sum(s.completion_tokens for s in self.total_usage.values())

    # ===== CostTrackerProtocol implementation =====

    def record_llm_call(
        self,
        provider: str,
        model: str,
        usage: TokenUsage,
        ai_module: AIModule,
        success: bool = True,
    ) -> CostEstimate | None:
        """Record LLM call and accumulate tokens/costs locally per model."""
        # Delegate to real tracker for OTEL metrics (also calculates cost)
        result = self._delegate.record_llm_call(
            provider, model, usage, ai_module, success
        )

        if success:
            key = ModelKey(provider=provider, model=model)

            # Get or create stats for this model
            turn_stats = self.turn_usage.setdefault(key, ModelUsageStats())
            total_stats = self.total_usage.setdefault(key, ModelUsageStats())

            # Accumulate tokens
            turn_stats.prompt_tokens += usage.prompt_tokens
            turn_stats.completion_tokens += usage.completion_tokens
            turn_stats.cache_read_tokens += usage.cache_read_tokens
            turn_stats.cache_write_tokens += usage.cache_write_tokens

            total_stats.prompt_tokens += usage.prompt_tokens
            total_stats.completion_tokens += usage.completion_tokens
            total_stats.cache_read_tokens += usage.cache_read_tokens
            total_stats.cache_write_tokens += usage.cache_write_tokens

            # Accumulate cost from returned CostEstimate
            if result:
                turn_stats.total_cost += result.total_cost
                total_stats.total_cost += result.total_cost

            logger.debug(
                f"LocalCostTracker: {key.provider}/{key.model} - "
                f"{turn_stats.input_tokens:,} in / {turn_stats.completion_tokens:,} out "
                f"(${turn_stats.total_cost:.4f})"
            )

        return result

    def record_user_request(
        self,
        ai_module: AIModule,
        success: bool = True,
        platform: "BotPlatform | None" = None,
    ) -> None:
        """Record a user request (Tier 1)."""
        self._delegate.record_user_request(ai_module, success, platform)

    def record_tool_call(
        self,
        *,
        ai_module: AIModule,
        tool: str,
        success: bool = True,
        is_external: bool = False,
    ) -> None:
        """Record a tool invocation by the LLM (Tier 3)."""
        self._delegate.record_tool_call(
            ai_module=ai_module, tool=tool, success=success, is_external=is_external
        )

    def record_user_request_latency(
        self,
        duration_seconds: float,
        ai_module: AIModule,
        success: bool = True,
        platform: "BotPlatform | None" = None,
    ) -> None:
        """Record latency for a user request (Tier 1)."""
        self._delegate.record_user_request_latency(
            duration_seconds, ai_module, success, platform
        )

    def record_llm_call_latency(
        self,
        duration_seconds: float,
        provider: str,
        model: str,
        ai_module: AIModule,
        success: bool = True,
    ) -> None:
        """Record latency for an LLM API call (Tier 2)."""
        self._delegate.record_llm_call_latency(
            duration_seconds, provider, model, ai_module, success
        )

    def record_tool_call_latency(
        self,
        *,
        duration_seconds: float,
        ai_module: AIModule,
        tool: str,
        success: bool = True,
        is_external: bool = False,
    ) -> None:
        """Record latency for a tool invocation (Tier 3)."""
        self._delegate.record_tool_call_latency(
            duration_seconds=duration_seconds,
            ai_module=ai_module,
            tool=tool,
            success=success,
            is_external=is_external,
        )

    # ===== Local tracking methods (not part of protocol) =====

    def reset_turn(self) -> None:
        """Reset turn counters (call before each user message)."""
        self.turn_usage.clear()

    def reset_all(self) -> None:
        """Reset all counters (call when clearing the chat session)."""
        self.turn_usage.clear()
        self.total_usage.clear()
