import logging
from typing import Dict, List, Optional

import pydantic
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    DatasetSourceConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
)
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

logger = logging.getLogger(__name__)


class MonteCarloPlatformDetail(ConfigModel):
    """Maps a Monte Carlo warehouse/connection to a DataHub platform.

    Monte Carlo identifies the warehouse a monitored asset lives in by a
    resource/warehouse UUID, but it does not expose the DataHub platform name
    (e.g. ``snowflake``) directly. This mapping lets users pin the platform,
    platform instance and environment used to build the dataset URN so it lines
    up with the URNs emitted by the corresponding warehouse source.
    """

    platform: str = Field(
        description="DataHub platform name for assets in this Monte Carlo warehouse, "
        "e.g. 'snowflake', 'bigquery', 'redshift', 'databricks'.",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="DataHub platform instance for assets in this warehouse. Must match "
        "the platform_instance used by the warehouse source so URNs align.",
    )
    env: str = Field(
        default=DEFAULT_ENV,
        description="The environment that assets in this warehouse belong to.",
    )


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
        "until the next day. Leave unset to disable.",
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
