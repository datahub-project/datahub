import logging
from typing import Dict, List, Optional

import pydantic
from pydantic import Field, field_validator

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    DatasetSourceConfigMixin,
    EnvConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.montecarlo.constants import (
    get_known_data_platforms,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

logger = logging.getLogger(__name__)


def _validate_platform_value(platform: str) -> str:
    """Validate a user-configured warehouse platform name against the known
    DataHub platforms. Raises ``ValueError`` on an unknown value so a typo
    (e.g. ``snowflke``) fails fast at config load instead of producing a
    phantom ``urn:li:dataPlatform:snowflke`` that assertions can never attach
    to. No-ops when the registry cannot be loaded (see
    ``get_known_data_platforms``).
    """
    known = get_known_data_platforms()
    if known is None:
        return platform
    if platform not in known:
        # Suggest the closest match to make a typo self-evident.
        import difflib

        suggestions = difflib.get_close_matches(platform, sorted(known), n=3)
        hint = f" Did you mean one of: {', '.join(suggestions)}?" if suggestions else ""
        raise ValueError(
            f"Unknown DataHub platform {platform!r}. It must match a platform "
            f"name a DataHub source connector emits (e.g. snowflake, bigquery, "
            f"redshift).{hint} If this is a custom platform registered in your "
            f"DataHub instance, the MonteCarlo connector cannot currently "
            f"validate it; see the connector docs."
        )
    return platform


class MonteCarloPlatformDetail(PlatformInstanceConfigMixin, EnvConfigMixin):
    """Maps a Monte Carlo warehouse/connection to a DataHub platform.

    Monte Carlo identifies the warehouse a monitored asset lives in by a
    resource/warehouse UUID, but it does not expose the DataHub platform name
    (e.g. ``snowflake``) directly. This mapping lets users pin the platform,
    platform instance and environment used to build the dataset URN so it lines
    up with the URNs emitted by the corresponding warehouse source. Reuses the
    same ``platform_instance``/``env`` fields (and validation) as other sources'
    connection-to-platform mappings (see e.g. qlik_sense, sigma, trino).
    """

    platform: str = Field(
        description="DataHub platform name for assets in this Monte Carlo warehouse, "
        "e.g. 'snowflake', 'bigquery', 'redshift', 'databricks'.",
    )

    convert_urns_to_lowercase: Optional[bool] = Field(
        default=None,
        description="Override the dataset URN casing for this warehouse only. Set to "
        "true to force lowercase, false to preserve the case Monte Carlo reports (needed "
        "for case-preserving Snowflake/Redshift deployments whose warehouse source runs "
        "with convert_urns_to_lowercase=false). Leave unset to inherit the connector "
        "default: lowercase for snowflake/redshift, case-preserving otherwise, with the "
        "top-level convert_urns_to_lowercase flag forcing lowercase everywhere when true.",
    )

    @field_validator("platform", mode="after")
    @classmethod
    def _validate_platform(cls, v: str) -> str:
        # Catches a typo in a connection_to_platform_map entry before it produces
        # assertion URNs that point at a platform no warehouse source emits.
        return _validate_platform_value(v)


class MonteCarloSourceConfig(
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
):
    api_id: str = Field(
        description="Monte Carlo API key id (the ``mcd_id`` of an API key pair).",
    )
    api_token: pydantic.SecretStr = Field(
        description="Monte Carlo API key token (the ``mcd_token`` of an API key pair).",
    )
    api_endpoint: Optional[str] = Field(
        default=None,
        description="Override for the Monte Carlo MCD GraphQL endpoint. Defaults to the "
        "endpoint baked into the pycarlo client when unset.",
    )

    connection_to_platform_map: Dict[str, MonteCarloPlatformDetail] = Field(
        default={},
        description="Maps a Monte Carlo warehouse resource UUID to a DataHub platform, "
        "platform instance and env, used to build dataset URNs for monitored assets so "
        "they line up with the warehouse source's URNs. The key is the warehouse's "
        "resource UUID (the resource segment of an asset's MCON, "
        "``MCON++<account>++<resource-uuid>++table++...``; also visible on the "
        "warehouse in the Monte Carlo settings UI), not its display name.",
    )
    default_platform: Optional[str] = Field(
        default=None,
        description="Fallback DataHub platform to use when a warehouse is not present in "
        "connection_to_platform_map and the warehouse connection type cannot be mapped "
        "automatically. Leave unset to skip (and warn about) unresolvable assets.",
    )

    @field_validator("default_platform", mode="after")
    @classmethod
    def _validate_default_platform(cls, v: Optional[str]) -> Optional[str]:
        # Same typo guard as MonteCarloPlatformDetail.platform, applied to the
        # fallback platform that covers auto-mapped / unmapped warehouses.
        if v is None:
            return v
        return _validate_platform_value(v)

    target_platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance to stamp on the warehouse dataset URNs built for "
        "warehouses that are auto-mapped (not listed in connection_to_platform_map) or "
        "that fall back to default_platform. This is the warehouse platform's instance, "
        "NOT Monte Carlo's own — the top-level platform_instance field is Monte Carlo's "
        "and must not leak onto warehouse dataset URNs (it would attach assertions to "
        "datasets that do not exist). For warehouses listed in connection_to_platform_map, "
        "set the instance per entry instead. Mirrors the dbt/sqlmesh target_platform_instance "
        "convention. Leave unset for no platform instance on auto-mapped warehouse URNs.",
    )
    target_env: Optional[str] = Field(
        default=None,
        description="Environment to stamp on the warehouse dataset URNs built for auto-mapped "
        "warehouses (those not in connection_to_platform_map). Separate from Monte Carlo's own "
        "env so the warehouse URN namespace can be controlled independently. When unset, falls "
        "back to the top-level env (the values usually coincide). For warehouses listed in "
        "connection_to_platform_map, set the env per entry instead.",
    )

    include_assertions: bool = Field(
        default=True,
        description="Ingest Monte Carlo monitors and custom rules as DataHub assertions.",
    )
    include_alerts: bool = Field(
        default=True,
        description="Ingest Monte Carlo alerts/incidents as assertion run events (failures). "
        "Requires include_assertions, since run events attach to the assertions built from "
        "monitors.",
    )
    alerts_lookback_days: pydantic.PositiveInt = Field(
        default=30,
        description="How many days back to fetch alerts/incidents for. Only applies when "
        "include_alerts is enabled.",
    )

    run_events_lookback_days: Optional[int] = Field(
        default=None,
        description="Ingest Monte Carlo monitor run history (getJobExecutions) plus "
        "measured metric values (getMetricsV4) as AssertionRunEvents. When set to a "
        "positive integer N, emits the latest SUCCESS run(s) per monitor (carrying "
        "the measured value on AssertionResult) for runs within the last N days. "
        "Leave unset (None) to disable — the alert-driven FAILURE-only path "
        "(include_alerts) is the historical behaviour. Requires include_assertions. "
        "Bounds the query window, not the run count (run_events_first caps the "
        "count). Adds ~1 getJobExecutions + ~1 getMetricsV4 call per ingested monitor "
        "per run; set rate_limit_daily to bound this.",
    )
    run_events_first: pydantic.PositiveInt = Field(
        default=5,
        description="Maximum number of most-recent runs to fetch per monitor "
        "(the `first` arg on getJobExecutions). All SUCCESS runs in the page "
        "are emitted as AssertionRunEvents; the latest one carries the measured "
        "metric value from getMetricsV4. Only applies when run_events_lookback_days "
        "is set.",
    )

    monitor_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for monitor/rule names to filter in/out.",
    )
    monitor_type_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for Monte Carlo monitor types (e.g. 'FRESHNESS', "
        "'VOLUME') to filter in/out.",
    )
    domain_ids: List[str] = Field(
        default=[],
        description="Optional list of Monte Carlo domain UUIDs to scope ingestion to.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion configuration. Enables soft-deletion of assertions "
        "whose Monte Carlo monitor no longer exists.",
    )

    rate_limit_requests_per_second: Optional[pydantic.PositiveFloat] = Field(
        default=None,
        description="Sustained token bucket refill rate, in requests/second. Leave unset to "
        "disable client-side rate limiting entirely.",
    )
    rate_limit_burst: Optional[pydantic.PositiveInt] = Field(
        default=None,
        description="Token bucket capacity — the number of requests that can burst above the "
        "sustained rate before throttling kicks in. Only used when "
        "rate_limit_requests_per_second is set.",
    )
    rate_limit_daily: Optional[pydantic.PositiveInt] = Field(
        default=None,
        description="Maximum API calls allowed per UTC calendar day, matching Monte Carlo's "
        "own daily-limit reset behavior. Exceeding it fails the run rather than blocking "
        "until the next day. This is a per-run cap, not a true cross-run daily budget: it "
        "is not shared or coordinated across separate/overlapping ingestion runs, so it "
        "cannot prevent the combined total across runs from exceeding this value. Leave "
        "unset to disable.",
    )

    @pydantic.model_validator(mode="after")
    def _require_rate_for_burst(self) -> "MonteCarloSourceConfig":
        # rate_limit_burst configures the token bucket's capacity, which is only
        # constructed when rate_limit_requests_per_second is set (see
        # MonteCarloClient.__init__) — set alone, it would be silently ignored.
        if (
            self.rate_limit_burst is not None
            and not self.rate_limit_requests_per_second
        ):
            raise ValueError(
                "rate_limit_burst requires rate_limit_requests_per_second to be set."
            )
        return self

    @pydantic.model_validator(mode="after")
    def _require_assertions_for_alerts(self) -> "MonteCarloSourceConfig":
        # Alert run events attach to the assertions built from monitors, so ingesting
        # alerts without assertions would silently produce no run events.
        if self.include_alerts and not self.include_assertions:
            raise ValueError(
                "include_alerts requires include_assertions: alert run events attach to "
                "the assertions built from monitors and cannot be ingested on their own."
            )
        return self

    @pydantic.model_validator(mode="after")
    def _require_assertions_for_run_events(self) -> "MonteCarloSourceConfig":
        # Run events attach to the assertions built from monitors, same as alerts.
        if self.run_events_lookback_days and not self.include_assertions:
            raise ValueError(
                "run_events_lookback_days requires include_assertions: run events "
                "attach to the assertions built from monitors and cannot be "
                "ingested on their own."
            )
        return self
