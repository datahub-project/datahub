import logging
from dataclasses import dataclass
from typing import Optional, Tuple

from datahub.configuration.common import GraphError
from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

# Minimum DataHub Cloud version supporting semanticModel/metric entities.
_MIN_SAAS_VERSION: Tuple[int, int, int] = (2, 1, 0)

_SEMANTIC_MODEL_ENTITY = "semanticModel"
_SEMANTIC_MODEL_INFO_ASPECT = "semanticModelInfo"
_METRIC_ENTITY = "metric"
_METRIC_INFO_ASPECT = "metricInfo"

_METRICS_ENABLED_OPERATION = "getMetricsEnabled"
_METRICS_ENABLED_QUERY = """
    query getMetricsEnabled {
      appConfig {
        featureFlags {
          metricsEnabled
        }
      }
    }
"""

# A GraphQL error carrying both markers means the server predates metricsEnabled;
# any other error is real and must fail closed.
_FIELD_UNDEFINED_MARKER = "FieldUndefined"
_METRICS_ENABLED_FIELD = "metricsEnabled"


@dataclass
class ResolvedEmitDecision:
    enabled: bool
    reason: str
    is_saas: bool
    version: Optional[str]
    metrics_enabled: Optional[bool]


def _fetch_metrics_enabled(graph: DataHubGraph) -> Optional[bool]:
    """Read the server's metricsEnabled flag.

    Returns None when the flag is absent/null (older server or unset), which
    callers treat as "not the kill-switch". Re-raises non-FieldUndefined errors
    so the resolver fails closed on auth/network problems.
    """
    try:
        response = graph.execute_graphql(
            query=_METRICS_ENABLED_QUERY,
            operation_name=_METRICS_ENABLED_OPERATION,
            strip_unsupported_fields=True,
        )
    except GraphError as e:
        # Fallback for when the graphql-core field stripper is unavailable: older
        # servers reject the query with FieldUndefined; treat as "flag absent"
        # (allow). Propagate everything else to fail closed. The primary path is
        # strip_unsupported_fields=True above, which removes the unknown field and
        # returns an empty featureFlags rather than raising.
        message = str(e)
        if _FIELD_UNDEFINED_MARKER in message and _METRICS_ENABLED_FIELD in message:
            return None
        raise

    feature_flags = (response.get("appConfig") or {}).get("featureFlags")
    if not feature_flags:
        return None
    return feature_flags.get(_METRICS_ENABLED_FIELD)


def _server_supports_entities(graph: DataHubGraph) -> Optional[bool]:
    """Whether the server registry supports semanticModel + metric.

    Returns None when the specs fetch failed; callers must not block on it since
    version and metrics gating already apply.
    """
    specs = graph.get_entity_aspect_specs()
    if specs is None:
        return None
    try:
        return specs.supports(
            _SEMANTIC_MODEL_ENTITY, _SEMANTIC_MODEL_INFO_ASPECT
        ) and specs.supports(_METRIC_ENTITY, _METRIC_INFO_ASPECT)
    except ValueError:
        # supports() raises when the entity type isn't registered at all.
        return False


def resolve_emit_semantic_model_entities(
    graph: Optional[DataHubGraph], recipe_value: Optional[bool]
) -> ResolvedEmitDecision:
    # No graph client: cannot detect the server, so fall back to OSS semantics
    # (recipe must be explicit True).
    if graph is None:
        if recipe_value:
            return ResolvedEmitDecision(
                enabled=True,
                reason="no graph client; enabled by explicit recipe request",
                is_saas=False,
                version=None,
                metrics_enabled=None,
            )
        logger.warning(
            "semantic_views.emit_semantic_model_entities auto-enable requires a "
            "DataHub graph client (SaaS server detection). Without one, it stays "
            "off unless the recipe explicitly sets it to true."
        )
        return ResolvedEmitDecision(
            enabled=False,
            reason="no graph client and recipe did not explicitly request emission",
            is_saas=False,
            version=None,
            metrics_enabled=None,
        )

    server_config = graph.server_config
    is_saas = server_config.is_datahub_cloud
    version = server_config.service_version

    # OSS / self-hosted: recipe-driven only.
    if not is_saas:
        if not recipe_value:
            return ResolvedEmitDecision(
                enabled=False,
                reason="OSS server; recipe did not explicitly request emission",
                is_saas=False,
                version=version,
                metrics_enabled=None,
            )
        # Recipe asked for emission: confirm the registry knows these entities
        # before enabling. None (specs unavailable) does not block.
        if _server_supports_entities(graph) is False:
            return ResolvedEmitDecision(
                enabled=False,
                reason="server registry lacks semanticModel/metric support",
                is_saas=False,
                version=version,
                metrics_enabled=None,
            )
        return ResolvedEmitDecision(
            enabled=True,
            reason="OSS server; enabled by explicit recipe request",
            is_saas=False,
            version=version,
            metrics_enabled=None,
        )

    # SaaS below: recipe force-off wins over any server auto-enable.
    if recipe_value is False:
        return ResolvedEmitDecision(
            enabled=False,
            reason="recipe explicitly set to false (force-off)",
            is_saas=True,
            version=version,
            metrics_enabled=None,
        )

    # Hard veto: server too old, even if the recipe requested emission.
    if not server_config.is_version_at_least(*_MIN_SAAS_VERSION):
        return ResolvedEmitDecision(
            enabled=False,
            reason=(
                f"DataHub Cloud version {version} is below the minimum "
                f"{'.'.join(str(v) for v in _MIN_SAAS_VERSION)} required for "
                "semanticModel/metric entities"
            ),
            is_saas=True,
            version=version,
            metrics_enabled=None,
        )

    # Fail closed: if we can't read the Metrics kill-switch, do not auto-enable.
    try:
        metrics_enabled = _fetch_metrics_enabled(graph)
    except Exception:
        logger.warning(
            "Failed to fetch the DataHub Cloud metricsEnabled feature flag; "
            "failing closed and not auto-enabling semanticModel entities.",
            exc_info=True,
        )
        return ResolvedEmitDecision(
            enabled=False,
            reason="metricsEnabled fetch failed; failing closed",
            is_saas=True,
            version=version,
            metrics_enabled=None,
        )

    # Only explicit false is the kill-switch; absent/None means "not disabled".
    if metrics_enabled is False:
        return ResolvedEmitDecision(
            enabled=False,
            reason="DataHub Cloud Metrics feature is disabled (metricsEnabled=false)",
            is_saas=True,
            version=version,
            metrics_enabled=False,
        )

    # Confirm the registry knows these entities before returning ON. None (specs
    # unavailable) does not block — version + metrics gating already vouched.
    supports = _server_supports_entities(graph)
    if supports is False:
        return ResolvedEmitDecision(
            enabled=False,
            reason="server registry lacks semanticModel/metric support",
            is_saas=True,
            version=version,
            metrics_enabled=metrics_enabled,
        )

    return ResolvedEmitDecision(
        enabled=True,
        reason=(
            "DataHub Cloud auto-enabled (version and Metrics feature satisfied)"
            if recipe_value is None
            else "DataHub Cloud enabled by recipe request (server supports it)"
        ),
        is_saas=True,
        version=version,
        metrics_enabled=metrics_enabled,
    )
