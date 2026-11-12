from __future__ import annotations

from collections.abc import Sequence as AbcSequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional, TypeVar, Union, overload

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
from datahub.metadata.urns import SemanticModelUrn
from datahub.sdk._utils import DEFAULT_ACTOR_URN
from datahub.utilities.server_config_util import ServiceFeature
from datahub.utilities.urns.error import InvalidUrnError

__all__ = [
    "AiContextInput",
    "DialectExpressionInput",
    "MetricExpressionInputType",
    "as_input_list",
    "build_ai_context",
    "build_metric_expression",
    "make_audit_stamp",
    "require_metrics_support",
    "validate_semantic_model_urn",
]

_T = TypeVar("_T")


@overload
def as_input_list(value: "AbcSequence[_T]") -> List[_T]: ...


@overload
def as_input_list(value: _T) -> List[_T]: ...


def as_input_list(value: Any) -> List[Any]:
    # Implementation for the typed overloads above.
    """Normalize a scalar or sequence into a list.

    A bare ``str`` is a ``Sequence[str]`` and a single URN object is not a
    sequence at all; iterating either where a list is expected misbehaves (a
    URN string gets split character-by-character). Wrap scalars into one-element
    lists so that can't happen.
    """
    if isinstance(value, str):
        return [value]
    if isinstance(value, AbcSequence):
        return list(value)
    return [value]


def validate_semantic_model_urn(value: object) -> str:
    """Reject a blank or malformed semantic-model back-reference.

    The reference is required and must parse as a ``SemanticModelUrn``; an empty
    string is typed-valid but would emit a metric/dataset with no ``ModeledBy``
    edge.
    """
    text = str(value).strip()
    if not text:
        raise SdkUsageError(
            "semantic_model is required and must be a non-empty SemanticModelUrn."
        )
    try:
        SemanticModelUrn.from_string(text)
    except InvalidUrnError as e:
        raise SdkUsageError(
            f"semantic_model must be a valid SemanticModelUrn; got {text!r}."
        ) from e
    return text


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


def _require_nonempty_expression(expression: str) -> str:
    # An empty or whitespace-only expression produces a structurally valid but
    # semantically empty annotation/metric (no SQL to evaluate). Reject it here
    # rather than silently emitting it.
    if not expression or not expression.strip():
        raise SdkUsageError(
            "Metric/field expression must be a non-empty string; got an empty "
            "or whitespace-only value."
        )
    return expression


def _require_dialects(
    dialects: List[DialectExpressionClass],
) -> List[DialectExpressionClass]:
    # A MetricExpression with no dialects carries no evaluable SQL. Require at
    # least one so an empty list input can't silently produce an empty aspect.
    if not dialects:
        raise SdkUsageError(
            "Metric/field expression must contain at least one dialect expression."
        )
    return dialects


def build_metric_expression(
    expression: MetricExpressionInputType,
    *,
    default_dialect: Union[str, DialectClass] = DialectClass.ANSI_SQL,
) -> MetricExpressionClass:
    if isinstance(expression, MetricExpressionClass):
        # A pre-built aspect still has to satisfy the same invariants.
        _require_dialects(expression.dialects)
        for dialect in expression.dialects:
            _require_nonempty_expression(dialect.expression)
        return expression
    if isinstance(expression, DialectExpressionInput):
        dialects = [
            DialectExpressionClass(
                dialect=expression.dialect,
                expression=_require_nonempty_expression(expression.expression),
            )
        ]
    elif isinstance(expression, str):
        dialects = [
            DialectExpressionClass(
                dialect=default_dialect,
                expression=_require_nonempty_expression(expression),
            )
        ]
    elif isinstance(expression, list):
        dialects = [
            DialectExpressionClass(
                dialect=item.dialect,
                expression=_require_nonempty_expression(item.expression),
            )
            for item in expression
        ]
    else:  # pragma: no cover - defensive
        raise TypeError(f"Unsupported expression input type: {type(expression)!r}")
    return MetricExpressionClass(dialects=_require_dialects(dialects))


def make_audit_stamp(ts: Optional[datetime]) -> Optional[AuditStampClass]:
    if ts is None:
        return None
    return AuditStampClass(time=make_ts_millis(ts), actor=DEFAULT_ACTOR_URN)


def require_metrics_support(client_or_graph: object) -> None:
    """Opt-in preflight check that the connected server supports the
    ``semanticModel``/``metric``/logical-``dataset`` entities.

    Accepts either a :class:`~datahub.sdk.main_client.DataHubClient` or a raw
    ``DataHubGraph``; the client's underlying graph is unwrapped automatically.

    Delegates version detection to
    :meth:`RestServiceConfig.supports_feature`
    (``ServiceFeature.SEMANTIC_MODEL_ENTITIES``). Raises :class:`SdkUsageError`
    only when a managed (Cloud) server reports a version below the minimum.
    OSS/self-hosted deployments are not version-gated by the SDK (matching the
    Snowflake connector gate in #18395), and an absent/unreadable version signal
    fails open — the operator is then responsible for running a build that
    includes the semanticModel/metric model.

    Call this before emitting entities when you want a clear, actionable error
    instead of a server-side rejection::

        from datahub.sdk import DataHubClient, require_metrics_support

        client = DataHubClient(server=..., token=...)
        require_metrics_support(client)
    """
    # DataHubClient keeps its DataHubGraph private (behind entities/resolve
    # facades); unwrap it here so callers can pass the client they already hold.
    # Imported lazily to avoid a module-load cycle (main_client -> entity_client
    # -> _all_entities -> semantic_model -> _semantic_shared).
    from datahub.sdk.main_client import DataHubClient

    if isinstance(client_or_graph, DataHubClient):
        graph: object = client_or_graph._graph
    else:
        graph = client_or_graph
    server_config = getattr(graph, "server_config", None)
    if server_config is None:
        # No server config available (e.g. offline/emitter-only); fail open.
        return
    try:
        supported = server_config.supports_feature(
            ServiceFeature.SEMANTIC_MODEL_ENTITIES
        )
    except ValueError:
        # Non-semver build (dev/snapshot/sha tag) — no reliable version signal
        # to gate on; fail open rather than leaking a raw parse error.
        return
    if supported:
        return
    if not getattr(server_config, "is_datahub_cloud", False):
        # OSS/self-hosted: not version-gated by the SDK (SEMANTIC_MODEL_ENTITIES
        # defines no "core" requirement); the operator is responsible for the
        # server build. Fail open rather than block every OSS emit.
        return
    version = getattr(server_config, "service_version", None)
    raise SdkUsageError(
        f"This DataHub server (v{version}) does not support "
        "semanticModel/metric entities. Upgrade to a server build that "
        "includes the semanticModel/metric model."
    )
