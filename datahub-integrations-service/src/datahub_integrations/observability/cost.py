"""Cost tracking utilities for GenAI operations.

This module provides cost estimation and tracking for LLM API calls,
supporting multiple providers and models.

Configuration Precedence:
1. File-based config (GENAI_MODEL_PRICING_FILE) - for Helm ConfigMap deployments
2. Code defaults (_MODEL_PRICING) - always available as fallback

The file-based config uses per-model merging, so you only need to specify
models that differ from defaults or are custom additions.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol

from loguru import logger
from opentelemetry import metrics

from datahub_integrations.observability.metrics_constants import (
    GENAI_LLM_CALL_DURATION,
    GENAI_LLM_CALLS_TOTAL,
    GENAI_LLM_COST_PER_CALL,
    GENAI_LLM_COST_TOTAL,
    GENAI_LLM_TOKENS_TOTAL,
    GENAI_TOOL_CALL_DURATION,
    GENAI_TOOL_CALLS_TOTAL,
    GENAI_USER_MESSAGE_DURATION,
    GENAI_USER_MESSAGES_TOTAL,
    LABEL_AI_MODULE,
    LABEL_MODEL,
    LABEL_PLATFORM,
    LABEL_PROVIDER,
    LABEL_STATUS,
    LABEL_TOKEN_TYPE,
    LABEL_TOOL,
    TOKEN_TYPE_CACHE_READ,
    TOKEN_TYPE_CACHE_WRITE,
    TOKEN_TYPE_COMPLETION,
    TOKEN_TYPE_PROMPT,
    AIModule,
)

if TYPE_CHECKING:
    from datahub_integrations.observability.bot_metrics import BotPlatform

# Default pricing data as of December 2025 (USD per 1M tokens)
# Sources: OpenAI, Anthropic, AWS Bedrock pricing pages
# Cache pricing: cache_read is typically 10% of prompt cost, cache_write equals prompt cost
# Note: Bedrock pricing may include regional premiums (+10%) and optimized latency (+25%)
#
# To override or extend pricing:
# - Create a YAML/JSON file with custom pricing
# - Set GENAI_MODEL_PRICING_FILE environment variable to file path
# - Per-model merging: only specify models that differ from defaults
_MODEL_PRICING: dict[str, dict[str, dict[str, float]]] = {
    "openai": {
        "gpt-4": {
            "prompt": 30.0,
            "completion": 60.0,
            "cache_read": 3.0,
            "cache_write": 30.0,
        },
        "gpt-4-turbo": {
            "prompt": 10.0,
            "completion": 30.0,
            "cache_read": 1.0,
            "cache_write": 10.0,
        },
        "gpt-4o": {
            "prompt": 2.5,
            "completion": 10.0,
            "cache_read": 0.25,
            "cache_write": 2.5,
        },
        "gpt-4o-mini": {
            "prompt": 0.15,
            "completion": 0.60,
            "cache_read": 0.015,
            "cache_write": 0.15,
        },
        "gpt-3.5-turbo": {
            "prompt": 0.5,
            "completion": 1.5,
            "cache_read": 0.05,
            "cache_write": 0.5,
        },
        "gpt-3.5-turbo-16k": {
            "prompt": 3.0,
            "completion": 4.0,
            "cache_read": 0.3,
            "cache_write": 3.0,
        },
    },
    "anthropic": {
        "claude-3-5-sonnet": {
            "prompt": 3.0,
            "completion": 15.0,
            "cache_read": 0.30,
            "cache_write": 3.0,
        },
        "claude-3-opus": {
            "prompt": 15.0,
            "completion": 75.0,
            "cache_read": 1.5,
            "cache_write": 15.0,
        },
        "claude-3-sonnet": {
            "prompt": 3.0,
            "completion": 15.0,
            "cache_read": 0.30,
            "cache_write": 3.0,
        },
        "claude-3-haiku": {
            "prompt": 0.25,
            "completion": 1.25,
            "cache_read": 0.025,
            "cache_write": 0.25,
        },
        "claude-3-5-haiku": {
            "prompt": 0.80,
            "completion": 4.0,
            "cache_read": 0.08,
            "cache_write": 0.80,
        },
        "claude-2": {
            "prompt": 8.0,
            "completion": 24.0,
            "cache_read": 0.8,
            "cache_write": 8.0,
        },
        "claude-instant": {
            "prompt": 0.8,
            "completion": 2.4,
            "cache_read": 0.08,
            "cache_write": 0.8,
        },
    },
    "bedrock": {
        # AWS Bedrock pricing (varies by region)
        "us.anthropic.claude-sonnet-4-5": {
            "prompt": 3.0,
            "completion": 15.0,
            "cache_read": 0.30,
            "cache_write": 3.0,
        },
        "us.anthropic.claude-3-haiku": {
            "prompt": 0.25,
            "completion": 1.25,
            "cache_read": 0.025,
            "cache_write": 0.25,
        },
        "us.anthropic.claude-3-5-haiku": {
            "prompt": 0.80,
            "completion": 4.0,
            "cache_read": 0.08,
            "cache_write": 0.80,
        },
        "anthropic.claude-3-5-sonnet-v2": {
            "prompt": 3.0,
            "completion": 15.0,
            "cache_read": 0.30,
            "cache_write": 3.0,
        },
        "anthropic.claude-3-opus": {
            "prompt": 15.0,
            "completion": 75.0,
            "cache_read": 1.5,
            "cache_write": 15.0,
        },
        "anthropic.claude-3-sonnet": {
            "prompt": 3.0,
            "completion": 15.0,
            "cache_read": 0.30,
            "cache_write": 3.0,
        },
        "anthropic.claude-3-haiku": {
            "prompt": 0.25,
            "completion": 1.25,
            "cache_read": 0.025,
            "cache_write": 0.25,
        },
        "anthropic.claude-v2": {
            "prompt": 8.0,
            "completion": 24.0,
            "cache_read": 0.8,
            "cache_write": 8.0,
        },
        "anthropic.claude-instant-v1": {
            "prompt": 0.8,
            "completion": 2.4,
            "cache_read": 0.08,
            "cache_write": 0.8,
        },
        "meta.llama3-70b": {
            "prompt": 0.99,
            "completion": 0.99,
            "cache_read": 0.099,
            "cache_write": 0.99,
        },
        "meta.llama3-8b": {
            "prompt": 0.3,
            "completion": 0.3,
            "cache_read": 0.03,
            "cache_write": 0.3,
        },
    },
    "azure": {
        # Azure OpenAI pricing (varies by region)
        "gpt-4": {
            "prompt": 30.0,
            "completion": 60.0,
            "cache_read": 3.0,
            "cache_write": 30.0,
        },
        "gpt-4-turbo": {
            "prompt": 10.0,
            "completion": 30.0,
            "cache_read": 1.0,
            "cache_write": 10.0,
        },
        "gpt-35-turbo": {
            "prompt": 0.5,
            "completion": 1.5,
            "cache_read": 0.05,
            "cache_write": 0.5,
        },
    },
}


def _deep_merge_pricing(
    base: dict[str, dict[str, dict[str, float]]],
    override: dict[str, dict[str, dict[str, float]]],
) -> dict[str, dict[str, dict[str, float]]]:
    """Deep merge pricing dictionaries at the model level.

    Merging strategy:
    - Provider level: Union of providers from base and override
    - Model level: Override models replace base models with same name
    - Other models from base are preserved

    Example:
        base = {"anthropic": {"sonnet": {...}, "haiku": {...}}}
        override = {"anthropic": {"haiku": {...new...}, "custom": {...}}}
        result = {"anthropic": {"sonnet": {...}, "haiku": {...new...}, "custom": {...}}}

    Args:
        base: Base pricing (typically _MODEL_PRICING)
        override: Override pricing (from file)

    Returns:
        Merged pricing with overrides applied per-model
    """
    merged = {}

    # Copy all base providers
    for provider, models in base.items():
        merged[provider] = models.copy()

    # Apply overrides per-model
    for provider, models in override.items():
        if provider not in merged:
            merged[provider] = {}
        # Update/add models (per-model merge)
        merged[provider].update(models)

    return merged


def _load_pricing_from_file(
    file_path: str,
) -> dict[str, dict[str, dict[str, float]]] | None:
    """Load pricing from a YAML or JSON file.

    Expected file format (YAML):
        anthropic:
          claude-custom-model:
            prompt: 5.0
            completion: 10.0
            cache_read: 0.5
            cache_write: 5.0

    Or (JSON):
        {
          "anthropic": {
            "claude-custom-model": {
              "prompt": 5.0,
              "completion": 10.0,
              "cache_read": 0.5,
              "cache_write": 5.0
            }
          }
        }

    Args:
        file_path: Path to pricing file (YAML or JSON)

    Returns:
        Pricing dictionary, or None if file doesn't exist or is invalid
    """
    path = Path(file_path)

    if not path.exists():
        logger.debug(f"Pricing file not found: {file_path}")
        return None

    try:
        with path.open() as f:
            if path.suffix in (".yaml", ".yml"):
                try:
                    import yaml

                    data = yaml.safe_load(f)
                except ImportError:
                    logger.warning(
                        f"PyYAML not installed, cannot load {file_path}. "
                        "Install with: pip install pyyaml"
                    )
                    return None
            elif path.suffix == ".json":
                data = json.load(f)
            else:
                logger.warning(
                    f"Unsupported pricing file format: {path.suffix}. "
                    "Use .yaml, .yml, or .json"
                )
                return None

        # Validate structure
        if not isinstance(data, dict):
            logger.error(f"Invalid pricing file format in {file_path}: expected dict")
            return None

        logger.info(f"Loaded pricing from {file_path} for {len(data)} providers")
        return data

    except Exception as e:
        logger.error(f"Failed to load pricing from {file_path}: {e}")
        return None


def _load_custom_pricing() -> dict[str, dict[str, dict[str, float]]]:
    """Load pricing with file-based overrides.

    Precedence:
    1. File overrides (GENAI_MODEL_PRICING_FILE)
    2. Code defaults (_MODEL_PRICING)

    Merge strategy: Per-model merge (see _deep_merge_pricing)

    Environment:
        GENAI_MODEL_PRICING_FILE: Path to YAML/JSON pricing file
            Default: /etc/datahub/integrations/model-pricing.yaml

    Returns:
        Final pricing dictionary
    """
    file_path = os.getenv(
        "GENAI_MODEL_PRICING_FILE", "/etc/datahub/integrations/model-pricing.yaml"
    )

    file_pricing = _load_pricing_from_file(file_path)
    if file_pricing:
        logger.info(f"Merging pricing overrides from {file_path}")
        return _deep_merge_pricing(_MODEL_PRICING, file_pricing)

    logger.debug("Using default pricing (no file overrides)")
    return _MODEL_PRICING


def _detect_bedrock_optimized_latency() -> bool:
    """Detect if Bedrock optimized latency is enabled (+25% premium).

    Returns:
        True if ENABLE_BEDROCK_OPTIMIZED_LATENCY environment variable is set to true.
    """
    value = os.getenv("ENABLE_BEDROCK_OPTIMIZED_LATENCY", "false").lower()
    return value in ("true", "1", "yes")


def _detect_bedrock_regional_endpoint() -> bool:
    """Detect if using regional Bedrock endpoints (+10% premium).

    Cross-region inference profiles (us.*, eu.*, apac.*) have a 10% premium
    over global endpoints.

    Returns:
        True if ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX is set (defaults to "us").
    """
    prefix = os.getenv("ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX", "us")
    # Regional prefixes are: us, eu, apac
    # If no prefix or empty, it's a global endpoint (no premium)
    return prefix in ("us", "eu", "apac")


def _apply_bedrock_premiums(
    base_pricing: dict[str, float],
    provider: str,
) -> dict[str, float]:
    """Apply Bedrock-specific pricing premiums.

    Args:
        base_pricing: Base pricing dictionary for a model.
        provider: Provider name (e.g., "bedrock").

    Returns:
        Pricing with premiums applied if applicable.
    """
    if provider.lower() != "bedrock":
        return base_pricing

    multiplier = 1.0

    # Apply regional endpoint premium (+10%)
    if _detect_bedrock_regional_endpoint():
        multiplier *= 1.10

    # Apply optimized latency premium (+25%)
    if _detect_bedrock_optimized_latency():
        multiplier *= 1.25

    if multiplier > 1.0:
        logger.debug(
            f"Applying {multiplier:.2f}x Bedrock pricing multiplier "
            f"(regional: {_detect_bedrock_regional_endpoint()}, "
            f"optimized: {_detect_bedrock_optimized_latency()})"
        )
        return {k: v * multiplier for k, v in base_pricing.items()}

    return base_pricing


TokenType = Literal["prompt", "completion"]


@dataclass
class TokenUsage:
    """Token usage information from an LLM call.

    Cache tokens represent prompt caching optimization where previously processed
    prompts are cached and reused. Cache read tokens are typically 90% cheaper
    than regular prompt tokens (e.g., $0.30/M vs $3.00/M for Claude 3.5 Sonnet).
    """

    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    cache_read_tokens: int = (
        0  # Tokens served from cache (typically 10% of prompt cost)
    )
    cache_write_tokens: int = 0  # Tokens written to cache (same cost as prompt)


@dataclass
class CostEstimate:
    """Cost estimate for an LLM operation."""

    prompt_cost: float
    completion_cost: float
    cache_read_cost: float = 0.0
    cache_write_cost: float = 0.0
    total_cost: float = 0.0
    currency: str = "USD"

    def __post_init__(self) -> None:
        """Calculate total cost if not explicitly set."""
        if self.total_cost == 0.0:
            self.total_cost = (
                self.prompt_cost
                + self.completion_cost
                + self.cache_read_cost
                + self.cache_write_cost
            )

    def __str__(self) -> str:
        return f"${self.total_cost:.6f} {self.currency}"


class CostCalculator:
    """Calculator for LLM API costs across providers."""

    def __init__(self) -> None:
        self._pricing = _load_custom_pricing()

    def get_model_pricing(
        self,
        provider: str,
        model: str,
    ) -> dict[str, float] | None:
        """Get pricing for a specific model.

        Args:
            provider: Provider name (e.g., "openai", "anthropic").
            model: Model identifier.

        Returns:
            Dict with "prompt" and "completion" pricing per 1M tokens,
            or None if pricing not available.
        """
        provider_lower = provider.lower()
        if provider_lower not in self._pricing:
            logger.warning(
                f"No pricing data available for provider '{provider}'. "
                f"Add pricing to _MODEL_PRICING or set GENAI_MODEL_PRICING environment variable."
            )
            return None

        base_pricing = None

        # Try exact match first
        if model in self._pricing[provider_lower]:
            base_pricing = self._pricing[provider_lower][model]
        else:
            # Try prefix match (e.g., "gpt-4-0613" matches "gpt-4")
            for model_prefix, pricing in self._pricing[provider_lower].items():
                if model.startswith(model_prefix):
                    base_pricing = pricing
                    break

        if base_pricing is None:
            logger.warning(
                f"No pricing data available for {provider}/{model}. "
                f"Token usage will be recorded but cost not estimated. "
                f"Add pricing to _MODEL_PRICING or set GENAI_MODEL_PRICING environment variable."
            )
            return None

        # Apply Bedrock-specific premiums if applicable
        return _apply_bedrock_premiums(base_pricing, provider)

    def calculate_cost(
        self,
        provider: str,
        model: str,
        usage: TokenUsage,
    ) -> CostEstimate | None:
        """Calculate cost for an LLM operation.

        Args:
            provider: Provider name.
            model: Model identifier.
            usage: Token usage from the API call.

        Returns:
            Cost estimate, or None if pricing not available.
        """
        pricing = self.get_model_pricing(provider, model)
        if pricing is None:
            return None

        # Pricing is per 1M tokens
        prompt_cost = (usage.prompt_tokens / 1_000_000) * pricing["prompt"]
        completion_cost = (usage.completion_tokens / 1_000_000) * pricing["completion"]
        cache_read_cost = (usage.cache_read_tokens / 1_000_000) * pricing.get(
            "cache_read", 0.0
        )
        cache_write_cost = (usage.cache_write_tokens / 1_000_000) * pricing.get(
            "cache_write", pricing["prompt"]
        )

        return CostEstimate(
            prompt_cost=prompt_cost,
            completion_cost=completion_cost,
            cache_read_cost=cache_read_cost,
            cache_write_cost=cache_write_cost,
        )


class CostTrackerProtocol(Protocol):
    """Protocol defining the cost tracker interface for dependency injection.

    This protocol enables custom tracker implementations (e.g., for local
    accumulation in UI tools) while maintaining type safety.
    """

    def record_llm_call(
        self,
        provider: str,
        model: str,
        usage: TokenUsage,
        ai_module: AIModule,
        success: bool = True,
    ) -> CostEstimate | None:
        """Record token usage and cost for an LLM call."""
        ...

    def record_user_request(
        self,
        ai_module: AIModule,
        success: bool = True,
        platform: "BotPlatform | None" = None,
    ) -> None:
        """Record a user request (Tier 1)."""
        ...

    def record_tool_call(
        self,
        ai_module: AIModule,
        tool: str,
        success: bool = True,
    ) -> None:
        """Record a tool invocation by the LLM (Tier 3)."""
        ...

    def record_user_request_latency(
        self,
        duration_seconds: float,
        ai_module: AIModule,
        success: bool = True,
        platform: "BotPlatform | None" = None,
    ) -> None:
        """Record latency for a user request (Tier 1)."""
        ...

    def record_llm_call_latency(
        self,
        duration_seconds: float,
        provider: str,
        model: str,
        ai_module: AIModule,
        success: bool = True,
    ) -> None:
        """Record latency for an LLM API call (Tier 2)."""
        ...

    def record_tool_call_latency(
        self,
        duration_seconds: float,
        ai_module: AIModule,
        tool: str,
        success: bool = True,
    ) -> None:
        """Record latency for a tool invocation (Tier 3)."""
        ...


class CostTracker:
    """Tracks and records LLM costs as OpenTelemetry metrics."""

    def __init__(self, meter_name: str = "datahub_integrations.gen_ai"):
        self._calculator = CostCalculator()
        meter = metrics.get_meter(meter_name)

        # Counter for total cost in USD
        self._cost_counter = meter.create_counter(
            name=GENAI_LLM_COST_TOTAL,
            description="Total cost of LLM API calls in USD",
            unit="USD",
        )

        # Histogram for per-call costs
        self._cost_histogram = meter.create_histogram(
            name=GENAI_LLM_COST_PER_CALL,
            description="Cost per LLM API call in USD",
            unit="USD",
        )

        # Counter for token usage
        self._token_counter = meter.create_counter(
            name=GENAI_LLM_TOKENS_TOTAL,
            description="Total tokens consumed by LLM calls",
            unit="tokens",
        )

        # === Three-tier tracking ===

        # Tier 1: User messages (top level - user requests)
        self._user_message_counter = meter.create_counter(
            name=GENAI_USER_MESSAGES_TOTAL,
            description="Total number of user messages/requests processed",
            unit="messages",
        )

        # Tier 2: LLM calls (middle level - API calls to LLMs)
        self._llm_call_counter = meter.create_counter(
            name=GENAI_LLM_CALLS_TOTAL,
            description="Total number of LLM API calls made",
            unit="calls",
        )

        # Tier 3: Tool calls (bottom level - tool invocations by LLM)
        self._tool_call_counter = meter.create_counter(
            name=GENAI_TOOL_CALLS_TOTAL,
            description="Total number of tool calls by LLM",
            unit="calls",
        )

        # === Latency tracking histograms ===

        # Tier 1: User message latency (overall request-response time)
        self._user_message_duration = meter.create_histogram(
            name=GENAI_USER_MESSAGE_DURATION,
            description="Latency of user message processing from request to response",
            unit="s",
        )

        # Tier 2: LLM call latency (per LLM invocation)
        self._llm_call_duration = meter.create_histogram(
            name=GENAI_LLM_CALL_DURATION,
            description="Latency of individual LLM API calls",
            unit="s",
        )

        # Tier 3: Tool call latency (per tool execution)
        self._tool_call_duration = meter.create_histogram(
            name=GENAI_TOOL_CALL_DURATION,
            description="Latency of individual tool executions",
            unit="s",
        )

    def record_llm_call(
        self,
        provider: str,
        model: str,
        usage: TokenUsage,
        ai_module: AIModule,
        success: bool = True,
    ) -> CostEstimate | None:
        """Record token usage and cost for an LLM call.

        Args:
            provider: Provider name.
            model: Model identifier.
            usage: Token usage from the API call.
            ai_module: AI module/feature making the call (e.g., AIModule.CHAT).
            success: Whether the call succeeded.

        Returns:
            Cost estimate if pricing available, None otherwise.
        """
        labels = {
            LABEL_PROVIDER: provider,
            LABEL_MODEL: model,
            LABEL_AI_MODULE: ai_module.value,
            LABEL_STATUS: "success" if success else "error",
        }

        # Record token usage
        self._token_counter.add(
            usage.prompt_tokens,
            attributes={**labels, LABEL_TOKEN_TYPE: TOKEN_TYPE_PROMPT},
        )
        self._token_counter.add(
            usage.completion_tokens,
            attributes={**labels, LABEL_TOKEN_TYPE: TOKEN_TYPE_COMPLETION},
        )
        if usage.cache_read_tokens > 0:
            self._token_counter.add(
                usage.cache_read_tokens,
                attributes={**labels, LABEL_TOKEN_TYPE: TOKEN_TYPE_CACHE_READ},
            )
        if usage.cache_write_tokens > 0:
            self._token_counter.add(
                usage.cache_write_tokens,
                attributes={**labels, LABEL_TOKEN_TYPE: TOKEN_TYPE_CACHE_WRITE},
            )

        # Calculate and record cost
        cost = self._calculator.calculate_cost(provider, model, usage)
        if cost is not None:
            self._cost_counter.add(cost.total_cost, attributes=labels)
            self._cost_histogram.record(cost.total_cost, attributes=labels)

        # Increment LLM call counter (Tier 2: one per LLM API call)
        # This enables calculating average cost per call and calls per user message
        self._llm_call_counter.add(1, attributes=labels)

        return cost

    def record_user_request(
        self,
        ai_module: AIModule,
        success: bool = True,
        platform: BotPlatform | None = None,
    ) -> None:
        """Record a user request (Tier 1).

        Call this once per user request at the entry point (e.g., chat endpoint,
        description generation endpoint). This represents the top-level user action.

        Args:
            ai_module: AI module processing the request.
            success: Whether the request succeeded.
            platform: Bot platform (SLACK or TEAMS) if request came from a bot.
        """
        labels = {
            LABEL_AI_MODULE: ai_module.value,
            LABEL_STATUS: "success" if success else "error",
        }
        if platform is not None:
            labels[LABEL_PLATFORM] = platform.value
        self._user_message_counter.add(1, attributes=labels)

    def record_tool_call(
        self,
        ai_module: AIModule,
        tool: str,
        success: bool = True,
    ) -> None:
        """Record a tool invocation by the LLM (Tier 3).

        Call this each time the LLM invokes a tool (e.g., search_datasets,
        get_schema, etc.). This represents the bottom-level tool usage.

        Args:
            ai_module: AI module invoking the tool.
            tool: Name of the tool being called.
            success: Whether the tool call succeeded.
        """
        labels = {
            LABEL_AI_MODULE: ai_module.value,
            LABEL_TOOL: tool,
            LABEL_STATUS: "success" if success else "error",
        }
        self._tool_call_counter.add(1, attributes=labels)

    def record_user_request_latency(
        self,
        duration_seconds: float,
        ai_module: AIModule,
        success: bool = True,
        platform: BotPlatform | None = None,
    ) -> None:
        """Record latency for a user request (Tier 1).

        Call this at the end of processing a user request with the total duration.

        Args:
            duration_seconds: Duration of the entire request-response cycle in seconds.
            ai_module: AI module that processed the request.
            success: Whether the request succeeded.
            platform: Bot platform (SLACK or TEAMS) if request came from a bot.
        """
        labels = {
            LABEL_AI_MODULE: ai_module.value,
            LABEL_STATUS: "success" if success else "error",
        }
        if platform is not None:
            labels[LABEL_PLATFORM] = platform.value
        self._user_message_duration.record(duration_seconds, attributes=labels)

    def record_llm_call_latency(
        self,
        duration_seconds: float,
        provider: str,
        model: str,
        ai_module: AIModule,
        success: bool = True,
    ) -> None:
        """Record latency for an LLM API call (Tier 2).

        Call this after each LLM API call with the duration.

        Args:
            duration_seconds: Duration of the LLM API call in seconds.
            provider: Provider name (e.g., "anthropic", "openai").
            model: Model identifier.
            ai_module: AI module making the call.
            success: Whether the call succeeded.
        """
        labels = {
            LABEL_PROVIDER: provider,
            LABEL_MODEL: model,
            LABEL_AI_MODULE: ai_module.value,
            LABEL_STATUS: "success" if success else "error",
        }
        self._llm_call_duration.record(duration_seconds, attributes=labels)

    def record_tool_call_latency(
        self,
        duration_seconds: float,
        ai_module: AIModule,
        tool: str,
        success: bool = True,
    ) -> None:
        """Record latency for a tool invocation (Tier 3).

        Call this after each tool execution with the duration.

        Args:
            duration_seconds: Duration of the tool execution in seconds.
            ai_module: AI module invoking the tool.
            tool: Name of the tool that was called.
            success: Whether the tool call succeeded.
        """
        labels = {
            LABEL_AI_MODULE: ai_module.value,
            LABEL_TOOL: tool,
            LABEL_STATUS: "success" if success else "error",
        }
        self._tool_call_duration.record(duration_seconds, attributes=labels)


# Global instance for convenience
_global_tracker: CostTracker | None = None


def get_cost_tracker() -> CostTrackerProtocol:
    """Get the global CostTracker instance.

    Returns the global tracker which implements CostTrackerProtocol.
    Custom implementations can be injected by setting _global_tracker
    before this function is first called.
    """
    global _global_tracker
    if _global_tracker is None:
        _global_tracker = CostTracker()
    return _global_tracker
