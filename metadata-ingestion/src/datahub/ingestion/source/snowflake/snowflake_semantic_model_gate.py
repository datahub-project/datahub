import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

from datahub.configuration.common import GraphError
from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)


class _MetricsProbe(Enum):
    """Outcome of probing the DataHub Cloud metricsEnabled kill-switch."""

    ENABLED = "enabled"  # metricsEnabled=true
    DISABLED = "disabled"  # metricsEnabled=false -> kill-switch veto
    # Field positively absent (older server without the flag) -> fail open.
    FIELD_ABSENT = "field_absent"
    # Operational failure (auth/transport/unexpected) -> fail closed to legacy.
    PROBE_FAILED = "probe_failed"


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


@dataclass(frozen=True)
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
    # True when the SaaS server reported a version string we could not parse, so
    # the capability check failed closed (feature off) rather than crashing the
    # whole Snowflake source. Lets the caller surface it via report.warning even
    # on the auto-enable path, where recipe_value is None.
    version_unparseable: bool = False
    # True when the metricsEnabled kill-switch probe failed operationally
    # (auth/transport/unexpected). We fail closed to legacy rather than enable on
    # an unverified kill-switch; the caller surfaces it via report.warning.
    metrics_probe_failed: bool = False


def _probe_metrics_enabled(graph: DataHubGraph) -> _MetricsProbe:
    """Probe the server's metricsEnabled kill-switch.

    Distinguishes a positively-absent field (older server, FIELD_ABSENT -> fail
    open) from an operational failure (auth/transport/unexpected, PROBE_FAILED ->
    fail closed). Only an explicit true/false yields ENABLED/DISABLED. This split
    matters because the resolver auto-enables on Cloud: an operational probe
    failure must not be mistaken for "kill-switch not set" and silently enable the
    feature on an unverified server.
    """
    try:
        response = graph.execute_graphql(
            query=_METRICS_ENABLED_QUERY,
            operation_name=_METRICS_ENABLED_OPERATION,
            strip_unsupported_fields=True,
        )
    except GraphError as e:
        # Older servers reject the query with FieldUndefined for metricsEnabled;
        # that is a positive "flag absent" signal (fail open). Any other GraphError
        # (auth, permission, malformed query) is an operational failure.
        message = str(e)
        if _FIELD_UNDEFINED_MARKER in message and _METRICS_ENABLED_FIELD in message:
            return _MetricsProbe.FIELD_ABSENT
        logger.warning(
            "metricsEnabled probe failed with a GraphQL error; failing closed to "
            "legacy dataset mode.",
            exc_info=True,
        )
        return _MetricsProbe.PROBE_FAILED
    except Exception:
        # Transport/auth/unexpected failure - cannot verify the kill-switch.
        logger.warning(
            "metricsEnabled probe failed; failing closed to legacy dataset mode.",
            exc_info=True,
        )
        return _MetricsProbe.PROBE_FAILED

    feature_flags = (response.get("appConfig") or {}).get("featureFlags")
    if not feature_flags:
        # Successful response with the field stripped/absent: older server (allow).
        return _MetricsProbe.FIELD_ABSENT
    value = feature_flags.get(_METRICS_ENABLED_FIELD)
    if value is None:
        return _MetricsProbe.FIELD_ABSENT
    return _MetricsProbe.ENABLED if value else _MetricsProbe.DISABLED


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
    version_unparseable = False
    if is_saas:
        try:
            entity_types_capable = server_config.is_version_at_least(*_MIN_SAAS_VERSION)
        except ValueError:
            # is_version_at_least raises on a non-semver service_version (git-sha
            # tag, two-part version, unexpected suffix). This resolver runs by
            # default on Cloud, so an unparseable version must not abort the entire
            # Snowflake source - fail closed to legacy (feature off) instead.
            logger.warning(
                "Could not parse DataHub Cloud version %r for the semanticModel/"
                "metric capability check; treating the server as below the minimum "
                "and keeping the feature off.",
                version,
                exc_info=True,
            )
            entity_types_capable = False
            version_unparseable = True
    else:
        entity_types_capable = bool(recipe_value)

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

    # Hard veto: server too old (or an unparseable version), even if the recipe
    # requested emission. On the SaaS path entity_types_capable is exactly the
    # version check.
    if not entity_types_capable:
        min_version = ".".join(str(v) for v in _MIN_SAAS_VERSION)
        return ResolvedEmitDecision(
            enabled=False,
            reason=(
                f"DataHub Cloud version {version!r} could not be parsed; treating "
                f"as below the minimum {min_version} required for semanticModel/"
                "metric entities"
                if version_unparseable
                else f"DataHub Cloud version {version} is below the minimum "
                f"{min_version} required for semanticModel/metric entities"
            ),
            is_saas=True,
            version=version,
            metrics_enabled=None,
            entity_types_capable=entity_types_capable,
            version_unparseable=version_unparseable,
        )

    probe = _probe_metrics_enabled(graph)

    # Kill-switch: an explicit metricsEnabled=false vetoes.
    if probe is _MetricsProbe.DISABLED:
        return ResolvedEmitDecision(
            enabled=False,
            reason="DataHub Cloud Metrics feature is disabled (metricsEnabled=false)",
            is_saas=True,
            version=version,
            metrics_enabled=False,
            entity_types_capable=entity_types_capable,
        )

    # Operational probe failure: we cannot confirm the kill-switch is off, so fail
    # closed to legacy rather than auto-enable on an unverified server. Only a
    # positively-absent field (older server) is treated as "not disabled".
    if probe is _MetricsProbe.PROBE_FAILED:
        return ResolvedEmitDecision(
            enabled=False,
            reason=(
                "Could not verify the DataHub Cloud metricsEnabled kill-switch "
                "(probe failed); staying in legacy dataset mode"
            ),
            is_saas=True,
            version=version,
            metrics_enabled=None,
            entity_types_capable=entity_types_capable,
            metrics_probe_failed=True,
        )

    # ENABLED or FIELD_ABSENT: not disabled -> enable.
    metrics_enabled = True if probe is _MetricsProbe.ENABLED else None
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
