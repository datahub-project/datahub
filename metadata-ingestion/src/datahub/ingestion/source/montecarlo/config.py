import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import pydantic
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

logger = logging.getLogger(__name__)


class MonteCarloAlertsConfig(BaseTimeWindowConfig):
    """Time window and toggle for alert/incident ingestion as assertion run events."""

    # Override the parent default (1 day) to 30 days for Monte Carlo alerts.
    start_time: datetime = Field(  # type: ignore[assignment]
        default_factory=lambda: datetime.now(tz=timezone.utc) - timedelta(days=30),
        description=(
            "Earliest alert/incident timestamp to ingest. "
            "Supports relative syntax like '-30 days'. Default: 30 days ago."
        ),
    )
    enabled: bool = Field(
        default=True,
        description="Emit Monte Carlo alerts/incidents as assertion run events (failures).",
    )


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


class MonteCarloSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
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
        description="Maps a Monte Carlo warehouse/resource UUID (or connection name) to a "
        "DataHub platform, platform instance and env. Used to build dataset URNs for "
        "monitored assets so they line up with the warehouse source's URNs.",
    )
    default_platform: Optional[str] = Field(
        default=None,
        description="Fallback DataHub platform to use when a warehouse is not present in "
        "connection_to_platform_map and the warehouse connection type cannot be mapped "
        "automatically. Leave unset to skip (and warn about) unresolvable assets.",
    )

    emit_assertions: bool = Field(
        default=True,
        description="Emit Monte Carlo monitors and custom rules as DataHub assertions.",
    )
    alerts: MonteCarloAlertsConfig = Field(
        default_factory=MonteCarloAlertsConfig,
        description="Configuration for alert/incident ingestion as assertion run events.",
    )

    monitor_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for monitor/rule names to filter in/out.",
    )
    monitor_types_allow: List[str] = Field(
        default=[],
        description="Optional allow-list of Monte Carlo user-defined monitor types. Empty "
        "means all types are ingested.",
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
