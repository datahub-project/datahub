import logging
from typing import TYPE_CHECKING, Dict, Optional, Set

from datahub.ingestion.source.sap_datasphere.config import SapDatasphereConfig
from datahub.ingestion.source.sap_datasphere.constants import (
    MANAGED_CONNECTION_KEY,
    PLATFORM,
)
from datahub.ingestion.source.sap_datasphere.models import (
    ConnectionRecord,
    ResolvedPlatform,
    ResolveResult,
    ResolveSkipReason,
    TypeIdDefault,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.sap_datasphere.report import SapDatasphereReport

logger = logging.getLogger(__name__)

# Kept for callers that imported the platform name from here historically.
SAP_DATASPHERE_PLATFORM = PLATFORM


class PlatformMappingResolver:
    """Resolves each asset's connection to a DataHub platform/instance/env.

    Priority: explicit ``connection_to_platform_map`` entry, then the
    ``platform_type_defaults`` entry for the connection's typeId, else a skip
    with a reason recorded once per unknown connection/typeId.
    """

    def __init__(
        self,
        config: SapDatasphereConfig,
        connections_by_name: Dict[str, ConnectionRecord],
        report: Optional["SapDatasphereReport"] = None,
    ) -> None:
        self._config = config
        self._connections_by_name = connections_by_name
        self._report = report
        self.unknown_typeids_seen: Set[str] = set()
        self.unknown_connection_names_seen: Set[str] = set()

    def resolve(self, connection_name: str) -> ResolveResult:
        # The synthetic _managed connection is always the Datasphere platform:
        # managed assets belong to the tenant's identity, not the underlying HANA
        # storage. The user can disable it via connection_to_platform_map.
        if connection_name == MANAGED_CONNECTION_KEY:
            user_override = self._config.connection_to_platform_map.get(connection_name)
            if user_override is not None and not user_override.enabled:
                return ResolveResult(
                    platform=None, skip_reason=ResolveSkipReason.DISABLED
                )
            return ResolveResult(
                platform=ResolvedPlatform(
                    platform=PLATFORM,
                    platform_instance=self._config.platform_instance,
                    env=self._config.env,
                ),
                skip_reason=None,
            )
        raw = self._config.connection_to_platform_map.get(connection_name)
        fallback_reason: Optional[ResolveSkipReason] = None
        if raw is None:
            td = self._typeid_default_for(connection_name)
            raw = td.config
            fallback_reason = td.skip_reason
        if raw is None:
            return ResolveResult(platform=None, skip_reason=fallback_reason)
        if not raw.enabled:
            return ResolveResult(platform=None, skip_reason=ResolveSkipReason.DISABLED)
        return ResolveResult(
            platform=ResolvedPlatform(
                platform=raw.platform,
                platform_instance=raw.platform_instance,
                env=raw.env or self._config.env,
                convert_urns_to_lowercase=raw.convert_urns_to_lowercase,
                database=raw.database,
            ),
            skip_reason=None,
        )

    def resolve_external(
        self, connection_name: Optional[str], connection_type: Optional[str]
    ) -> ResolveResult:
        # Flow endpoints (esp. replication-flow source/target systems) carry an
        # explicit connectionType that may not appear in the space's connections
        # list, so a plain resolve() by name would spuriously fail. Priority:
        # explicit name mapping, then the endpoint's own typeId, then the normal
        # name-based resolution (which records the unknown-connection reason).
        if connection_name:
            raw = self._config.connection_to_platform_map.get(connection_name)
            if raw is not None:
                if not raw.enabled:
                    return ResolveResult(
                        platform=None, skip_reason=ResolveSkipReason.DISABLED
                    )
                return ResolveResult(
                    platform=ResolvedPlatform(
                        platform=raw.platform,
                        platform_instance=raw.platform_instance,
                        env=raw.env or self._config.env,
                        convert_urns_to_lowercase=raw.convert_urns_to_lowercase,
                        database=raw.database,
                    ),
                    skip_reason=None,
                )
        if connection_type:
            default = self._config.platform_type_defaults.get(connection_type)
            if default is not None and default.enabled:
                return ResolveResult(
                    platform=ResolvedPlatform(
                        platform=default.platform,
                        platform_instance=default.platform_instance,
                        env=default.env or self._config.env,
                        convert_urns_to_lowercase=default.convert_urns_to_lowercase,
                        database=default.database,
                    ),
                    skip_reason=None,
                )
        if connection_name:
            return self.resolve(connection_name)
        return ResolveResult(
            platform=None, skip_reason=ResolveSkipReason.UNKNOWN_CONNECTION
        )

    def _typeid_default_for(self, connection_name: str) -> TypeIdDefault:
        # _managed has a hard-coded typeId of HANA (the tenant's own HANA Cloud).
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
                return TypeIdDefault(
                    config=None, skip_reason=ResolveSkipReason.UNKNOWN_CONNECTION
                )
            # Literal key required: TypedDict.get only type-narrows with a literal.
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
            return TypeIdDefault(
                config=None, skip_reason=ResolveSkipReason.UNKNOWN_TYPEID
            )
        return TypeIdDefault(config=default, skip_reason=None)
