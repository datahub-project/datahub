import logging
from dataclasses import dataclass
from typing import Optional, Tuple

from datahub.configuration.common import GraphError
from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

# Minimum DataHub Cloud version supporting semanticModel/metric entities.
_MIN_SAAS_VERSION: Tuple[int, int, int] = (2, 1, 0)

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
# any other error is re-raised for the resolver, which treats a fetch failure as
# not-disabled (fail-open), never as a veto.
_FIELD_UNDEFINED_MARKER = "FieldUndefined"
_METRICS_ENABLED_FIELD = "metricsEnabled"


@dataclass
class ResolvedEmitDecision:
    enabled: bool
    reason: str
    is_saas: bool
    version: Optional[str]
    metrics_enabled: Optional[bool]
    # Whether the server can accept semanticModel/metric in a
    # structuredPropertyDefinition's entityTypes permission list. Deliberately
    # recipe- and metricsEnabled-independent (SaaS: version only; OSS: recipe
    # value), so the shared definition is identical across recipes and feature
    # flips against the same server and never flaps. Consumed by the tag extractor.
    entity_types_capable: bool


def _fetch_metrics_enabled(graph: DataHubGraph) -> Optional[bool]:
    """Read the server's metricsEnabled flag.

    Returns None when the flag is absent/null (older server or unset), which
    callers treat as "not the kill-switch". Re-raises non-FieldUndefined errors;
    the resolver treats such a failure as not-disabled (fail-open), not a veto.
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
        # (allow). Propagate everything else. The primary path is
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


def resolve_emit_semantic_model_entities(
    graph: Optional[DataHubGraph], recipe_value: Optional[bool]
) -> ResolvedEmitDecision:
    # No graph client: cannot detect the server, so fall back to OSS semantics
    # (recipe must be explicit True). entityTypes capability degrades to the
    # recipe value, as on OSS.
    if graph is None:
        capable = bool(recipe_value)
        if recipe_value:
            return ResolvedEmitDecision(
                enabled=True,
                reason="no graph client; enabled by explicit recipe request",
                is_saas=False,
                version=None,
                metrics_enabled=None,
                entity_types_capable=capable,
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
            entity_types_capable=capable,
        )

    server_config = graph.server_config
    is_saas = server_config.is_datahub_cloud
    version = server_config.service_version

    # Capability for the structuredPropertyDefinition entityTypes permission list.
    # Computed before every early return so it is identical across recipe values
    # against the same server (no definition flap). SaaS: version only; OSS: recipe.
    entity_types_capable = (
        server_config.is_version_at_least(*_MIN_SAAS_VERSION)
        if is_saas
        else bool(recipe_value)
    )

    # OSS / self-hosted: recipe-driven only, no server probing.
    if not is_saas:
        return ResolvedEmitDecision(
            enabled=bool(recipe_value),
            reason=(
                "OSS server; enabled by explicit recipe request"
                if recipe_value
                else "OSS server; recipe did not explicitly request emission"
            ),
            is_saas=False,
            version=version,
            metrics_enabled=None,
            entity_types_capable=entity_types_capable,
        )

    # SaaS below: recipe force-off wins over any server auto-enable.
    if recipe_value is False:
        return ResolvedEmitDecision(
            enabled=False,
            reason="recipe explicitly set to false (force-off)",
            is_saas=True,
            version=version,
            metrics_enabled=None,
            entity_types_capable=entity_types_capable,
        )

    # Hard veto: server too old, even if the recipe requested emission. On the SaaS
    # path entity_types_capable is exactly the version check.
    if not entity_types_capable:
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
            entity_types_capable=entity_types_capable,
        )

    # Fail open: only an explicit metricsEnabled=false is the kill-switch. A probe
    # failure (network/auth/etc.) is not an explicit false, so treat it as
    # "not disabled" (None) and continue.
    try:
        metrics_enabled = _fetch_metrics_enabled(graph)
    except Exception:
        logger.warning(
            "Failed to fetch the DataHub Cloud metricsEnabled feature flag; "
            "treating as not-disabled and continuing.",
            exc_info=True,
        )
        metrics_enabled = None

    # Only explicit false is the kill-switch; absent/None means "not disabled".
    if metrics_enabled is False:
        return ResolvedEmitDecision(
            enabled=False,
            reason="DataHub Cloud Metrics feature is disabled (metricsEnabled=false)",
            is_saas=True,
            version=version,
            metrics_enabled=False,
            entity_types_capable=entity_types_capable,
        )

    return ResolvedEmitDecision(
        enabled=True,
        reason=(
            "DataHub Cloud auto-enabled (version satisfied, Metrics not disabled)"
            if recipe_value is None
            else "DataHub Cloud enabled by recipe request (version satisfied)"
        ),
        is_saas=True,
        version=version,
        metrics_enabled=metrics_enabled,
        entity_types_capable=entity_types_capable,
    )
