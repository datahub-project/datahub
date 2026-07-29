from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple, Union

from typing_extensions import TypeAlias

from datahub.emitter.mce_builder import make_ts_millis
from datahub.errors import SdkUsageError
from datahub.metadata.schema_classes import (
    AiContextClass,
    AuditStampClass,
    DialectClass,
    DialectExpressionClass,
    MetricExpressionClass,
)
from datahub.sdk._utils import DEFAULT_ACTOR_URN

__all__ = [
    "AiContextInput",
    "DialectExpressionInput",
    "METRICS_MIN_SAAS_VERSION",
    "MetricExpressionInputType",
    "build_ai_context",
    "build_metric_expression",
    "make_audit_stamp",
    "require_metrics_support",
]

# Minimum DataHub Cloud (SaaS) version supporting semanticModel/metric entities.
# Mirrors the connector gate (snowflake_semantic_model_gate._MIN_SAAS_VERSION);
# do not import from the connector — keep this in the shared SDK location.
METRICS_MIN_SAAS_VERSION: Tuple[int, int, int] = (2, 1, 0)


@dataclass
class AiContextInput:
    """Input container for the first-class ``aiContext`` aspect.

    The aspect is only emitted when at least one field carries content; an
    all-empty ``AiContextInput`` produces no aspect.
    """

    synonyms: Optional[List[str]] = None
    instructions: Optional[str] = None
    examples: Optional[List[str]] = None
    custom_instructions: Optional[str] = None


@dataclass
class DialectExpressionInput:
    """A single (dialect, expression) pair for a metric or field expression."""

    expression: str
    dialect: Union[str, DialectClass] = DialectClass.ANSI_SQL


MetricExpressionInputType: TypeAlias = Union[
    str,
    DialectExpressionInput,
    List[DialectExpressionInput],
    MetricExpressionClass,
]


def build_ai_context(ai: Optional[AiContextInput]) -> Optional[AiContextClass]:
    if ai is None or not (
        ai.synonyms or ai.instructions or ai.examples or ai.custom_instructions
    ):
        return None
    return AiContextClass(
        synonyms=list(ai.synonyms) if ai.synonyms else None,
        instructions=ai.instructions,
        examples=list(ai.examples) if ai.examples else None,
        customInstructions=ai.custom_instructions,
    )


def build_metric_expression(
    expression: MetricExpressionInputType,
    *,
    default_dialect: Union[str, DialectClass] = DialectClass.ANSI_SQL,
) -> MetricExpressionClass:
    if isinstance(expression, MetricExpressionClass):
        return expression
    if isinstance(expression, DialectExpressionInput):
        dialects = [
            DialectExpressionClass(
                dialect=expression.dialect, expression=expression.expression
            )
        ]
    elif isinstance(expression, str):
        dialects = [
            DialectExpressionClass(dialect=default_dialect, expression=expression)
        ]
    elif isinstance(expression, list):
        dialects = [
            DialectExpressionClass(dialect=item.dialect, expression=item.expression)
            for item in expression
        ]
    else:  # pragma: no cover - defensive
        raise TypeError(f"Unsupported expression input type: {type(expression)!r}")
    return MetricExpressionClass(dialects=dialects)


def make_audit_stamp(ts: Optional[datetime]) -> Optional[AuditStampClass]:
    if ts is None:
        return None
    return AuditStampClass(time=make_ts_millis(ts), actor=DEFAULT_ACTOR_URN)


def require_metrics_support(graph: object) -> None:
    """Opt-in preflight check that the connected GMS supports the
    ``semanticModel``/``metric``/logical-``dataset`` entities.

    Mirrors the Snowflake connector gate's asymmetry:

    - **DataHub Cloud (SaaS):** raises :class:`SdkUsageError` when the server
      version is below :data:`METRICS_MIN_SAAS_VERSION`. Does not probe the
      ``metricsEnabled`` kill-switch (an explicit false there surfaces as a
      server-side rejection at emit time, which is loud, not silent).
    - **OSS / self-hosted:** no version signal exists, so this is a no-op (fail
      open). The operator is responsible for running a GMS build that includes
      the semanticModel/metric model.

    Call this before emitting entities when you want a clear, actionable error
    instead of a server-side rejection::

        from datahub.sdk import DataHubClient, require_metrics_support

        client = DataHubClient(server=..., token=...)
        require_metrics_support(client.graph)
    """
    server_config = getattr(graph, "server_config", None)
    if server_config is None:
        # No server config available (e.g. offline/emitter-only); fail open.
        return
    if not server_config.is_datahub_cloud:
        # OSS / self-hosted: no version gate, operator's responsibility.
        return
    if not server_config.is_version_at_least(*METRICS_MIN_SAAS_VERSION):
        version = server_config.service_version
        raise SdkUsageError(
            f"This DataHub Cloud server (v{version}) does not support "
            f"semanticModel/metric entities; requires >= v"
            f"{'.'.join(str(v) for v in METRICS_MIN_SAAS_VERSION)}."
        )
