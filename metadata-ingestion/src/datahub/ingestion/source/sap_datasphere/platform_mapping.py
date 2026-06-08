"""Platform-mapping resolver for SAP Datasphere.

For each asset, the connector consults this resolver to decide:
  - which DataHub platform to emit the URN under (hana, snowflake, s3, ...)
  - which platform_instance to use
  - which env to use
  - whether to ingest the asset at all (`enabled` flag)

Resolution priority:
  1. Explicit per-connection entry in config.connection_to_platform_map
  2. Per-typeId default in config.platform_type_defaults (which merges user overrides
     on top of the built-in table)
  3. Unknown typeId or unknown connection name -> record warning, return (None, reason).

Skip reasons are surfaced via :class:`ResolveSkipReason` so the source can attribute
each skip to a specific report counter.
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Optional, Set, Tuple, TypedDict

from datahub.ingestion.source.sap_datasphere.config import (
    MANAGED_CONNECTION_KEY,
    ConnectionPlatformConfig,
    SapDatasphereConfig,
)
from datahub.utilities.str_enum import StrEnum


class ConnectionRecord(TypedDict, total=False):
    """Shape of a SAP Datasphere connection record returned by
    ``/api/v1/datasphere/spaces/<space>/connections``.

    The SAP API returns additional fields (id, description, owner, ...) but the
    connector only consumes ``name`` and ``typeId``; using ``total=False`` keeps
    the type permissive at the JSON-boundary.
    """

    name: str
    typeId: str


if TYPE_CHECKING:
    from datahub.ingestion.source.sap_datasphere.report import SapDatasphereReport

logger = logging.getLogger(__name__)

SAP_DATASPHERE_PLATFORM = "sap-datasphere"


class ResolveSkipReason(StrEnum):
    """Why an asset was not resolved to a concrete platform mapping."""

    UNKNOWN_TYPEID = "unknown_typeid"
    UNKNOWN_CONNECTION = "unknown_connection"
    DISABLED = "disabled"


@dataclass(frozen=True)
class ResolvedPlatform:
    """Effective platform mapping for a single asset, with all fallbacks applied.

    The ``enabled`` flag from :class:`ConnectionPlatformConfig` is consumed during
    resolution: a disabled mapping causes ``resolve()`` to return ``(None,
    ResolveSkipReason.DISABLED)`` rather than constructing a ``ResolvedPlatform``,
    so by the time you hold a ``ResolvedPlatform`` it's always enabled.
    """

    platform: str
    platform_instance: Optional[str]
    env: str


class PlatformMappingResolver:
    def __init__(
        self,
        config: SapDatasphereConfig,
        connections_by_name: Dict[str, ConnectionRecord],
        report: Optional["SapDatasphereReport"] = None,
    ) -> None:
        self._config = config
        # Maps connection name -> connection record (must have at least a `typeId` key).
        # The synthetic `_managed` connection has typeId "HANA" by definition.
        self._connections_by_name = connections_by_name
        self._report = report
        self.unknown_typeids_seen: Set[str] = set()
        self.unknown_connection_names_seen: Set[str] = set()

    def resolve(
        self, connection_name: str
    ) -> Tuple[Optional[ResolvedPlatform], Optional[ResolveSkipReason]]:
        """Return (resolved, None) on success, or (None, reason) when skipped."""
        # Step 0: the synthetic `_managed` connection always resolves to the
        # Datasphere platform. Managed assets (Local Tables, native Views,
        # Analytical Models) belong to the Datasphere tenant's identity — not
        # the underlying HANA storage. Federated routing below is unchanged.
        if connection_name == MANAGED_CONNECTION_KEY:
            # Allow the user to disable managed-asset ingestion entirely by
            # setting `_managed: {enabled: false}` in connection_to_platform_map.
            # The other fields (platform, platform_instance, env) are ignored —
            # managed assets are always sap_datasphere with the
            # top-level platform_instance.
            user_override = self._config.connection_to_platform_map.get(connection_name)
            if user_override is not None and not user_override.enabled:
                return None, ResolveSkipReason.DISABLED
            return (
                ResolvedPlatform(
                    platform=SAP_DATASPHERE_PLATFORM,
                    platform_instance=self._config.platform_instance,
                    env=self._config.env,
                ),
                None,
            )
        # Step 1: explicit per-connection entry wins
        raw = self._config.connection_to_platform_map.get(connection_name)
        fallback_reason: Optional[ResolveSkipReason] = None
        if raw is None:
            # Step 2: fall back to typeId default
            raw, fallback_reason = self._typeid_default_for(connection_name)
        if raw is None:
            return None, fallback_reason
        if not raw.enabled:
            return None, ResolveSkipReason.DISABLED
        return (
            ResolvedPlatform(
                platform=raw.platform,
                platform_instance=raw.platform_instance,
                env=raw.env or self._config.env,
            ),
            None,
        )

    def _typeid_default_for(
        self, connection_name: str
    ) -> Tuple[Optional[ConnectionPlatformConfig], Optional[ResolveSkipReason]]:
        # `_managed` has a hard-coded typeId of HANA (the Datasphere tenant's own HANA Cloud).
        if connection_name == MANAGED_CONNECTION_KEY:
            typeid = "HANA"
        else:
            conn = self._connections_by_name.get(connection_name)
            if conn is None:
                if connection_name not in self.unknown_connection_names_seen:
                    self.unknown_connection_names_seen.add(connection_name)
                    logger.warning(
                        "SAP Datasphere connection %r referenced by an asset but not "
                        "found in the connections API response; skipping",
                        connection_name,
                    )
                    if self._report is not None:
                        self._report.warning(
                            title="Unknown SAP Datasphere connection",
                            message=(
                                f"Connection {connection_name!r} is referenced by an "
                                f"asset but is not in the Datasphere connections list; "
                                f"assets routed to this connection will be skipped"
                            ),
                            context=connection_name,
                        )
                return None, ResolveSkipReason.UNKNOWN_CONNECTION
            typeid = conn.get("typeId", "")

        default = self._config.platform_type_defaults.get(typeid)
        if default is None:
            if typeid not in self.unknown_typeids_seen:
                self.unknown_typeids_seen.add(typeid)
                logger.warning(
                    "SAP Datasphere connection %r has typeId %r which is not in "
                    "platform_type_defaults; assets routed to this connection will be "
                    "skipped. Add an entry under platform_type_defaults to ingest them.",
                    connection_name,
                    typeid,
                )
                if self._report is not None:
                    self._report.warning(
                        title="Unknown SAP Datasphere connection typeId",
                        message=(
                            f"Datasphere connection has typeId {typeid!r} which is not "
                            f"in platform_type_defaults. Assets on connections of this "
                            f"type will be skipped. Add an entry under "
                            f"platform_type_defaults to ingest them."
                        ),
                        context=typeid,
                    )
            return None, ResolveSkipReason.UNKNOWN_TYPEID
        return default, None
