from functools import wraps
from typing import Any

from datahub.configuration import DynamicTypedConfig
from datahub.ingestion.graph.client import DataHubGraph
from pydantic import Field
from ratelimit import limits, sleep_and_retry

from datahub_integrations.actions.bulk_bootstrap_action import BulkBootstrapActionConfig
from datahub_integrations.propagation.propagation_v2.config.propagation_rule import (
    PropagationRule,
)


class PropagationV2Config(BulkBootstrapActionConfig):
    enabled: bool = Field(
        True, description="Indicates whether property propagation is enabled."
    )

    propagation_rule: PropagationRule = Field(
        description="Rule for property propagation.",
    )

    sink: DynamicTypedConfig | None = Field(
        default=None,
        description="Configure a different sink; should only be used for testing.",
    )

    max_propagation_depth: int = 5
    max_propagation_fanout: int = 1000
    max_propagation_time_millis: int = 1000 * 60 * 60 * 1  # 1 hour
    rate_limit_propagated_writes: int = 15000  # 15000 writes per 15 seconds (default)
    rate_limit_propagated_writes_period: int = 15  # Every 15 seconds

    def get_rate_limited_emit_mcp(self, emitter: DataHubGraph) -> Any:
        """
        Returns a rate limited emitter that can be used to emit metadata for propagation
        """

        @sleep_and_retry
        @limits(
            calls=self.rate_limit_propagated_writes,
            period=self.rate_limit_propagated_writes_period,
        )
        @wraps(emitter.emit_mcp)
        def wrapper(*args: Any, **kwargs: Any) -> None:
            return emitter.emit_mcp(*args, **kwargs)

        return wrapper
