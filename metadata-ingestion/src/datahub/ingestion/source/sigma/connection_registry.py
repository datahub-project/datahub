import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol

logger = logging.getLogger(__name__)

# Maps Sigma connection `type` to DataHub `dataPlatform` name. Values must
# match the canonical platform names in
# metadata-service/configuration/.../data-platforms.yaml; URN construction
# silently produces non-resolving entities otherwise.
SIGMA_TYPE_TO_DATAHUB_PLATFORM_MAP: Dict[str, str] = {
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "athena": "athena",
    "spark": "spark",
    "trino": "trino",
    "presto": "presto",
    # Azure Synapse Dedicated SQL is ingested as `mssql` in DataHub.
    "synapse": "mssql",
    "azure_synapse": "mssql",
}


class _ConnectionRegistryCounters(Protocol):
    """Subset of SigmaSourceReport that build() bumps. Declared structurally
    so test fakes do not need to import the full report class."""

    connections_resolved: int
    connections_unmappable_type: int
    connections_skipped_missing_id: int
    connections_duplicate_id: int


@dataclass(frozen=True)
class SigmaConnectionRecord:
    """Resolved Sigma Connection metadata for warehouse-URN construction.

    ``is_mappable`` defaults to False so records not produced by ``build()``
    are not used for URN construction.
    """

    connection_id: str
    name: str
    sigma_type: str
    datahub_platform: str
    host: Optional[str] = None
    # `account` is more reliable than parsing host for Snowflake URN construction.
    account: Optional[str] = None
    default_database: Optional[str] = None
    default_schema: Optional[str] = None
    # Warehouse / cluster name (Snowflake: COMPUTE_WH, Databricks: cluster id).
    instance_hint: Optional[str] = None
    is_mappable: bool = False


@dataclass
class SigmaConnectionRegistry:
    """In-memory registry indexed by connection_id; built once per ingest run."""

    by_id: Dict[str, SigmaConnectionRecord] = field(default_factory=dict)

    def get(self, connection_id: str) -> Optional[SigmaConnectionRecord]:
        return self.by_id.get(connection_id)

    @classmethod
    def build(
        cls,
        raw_connections: List[Dict[str, Any]],
        *,
        reporter: _ConnectionRegistryCounters,
        type_to_platform_map: Dict[str, str],
    ) -> "SigmaConnectionRegistry":
        """Build the registry from raw /v2/connections payloads.

        Each raw connection is mapped via ``type_to_platform_map`` (Sigma type
        to DataHub platform). Records with unmappable types are kept with
        ``is_mappable=False`` so operators can audit gaps via the
        ``connections_unmappable_type`` counter.
        """
        registry = cls()
        for raw in raw_connections:
            record = _record_from_raw(raw, type_to_platform_map)
            if record is None:
                reporter.connections_skipped_missing_id += 1
                continue

            if record.is_mappable:
                reporter.connections_resolved += 1
            else:
                reporter.connections_unmappable_type += 1

            if record.connection_id in registry.by_id:
                logger.debug(
                    "Duplicate Sigma connectionId %r; later record overwrites earlier.",
                    record.connection_id,
                )
                reporter.connections_duplicate_id += 1
            registry.by_id[record.connection_id] = record

        return registry


def _record_from_raw(
    raw: Dict[str, Any],
    type_to_platform_map: Dict[str, str],
) -> Optional[SigmaConnectionRecord]:
    """Parse one /v2/connections payload into a record. Returns None if the
    payload has no usable id."""
    conn_id = raw.get("connectionId") or raw.get("id", "")
    if not conn_id:
        logger.debug("Skipping Sigma connection payload without id: %s", raw)
        return None

    sigma_type = str(raw.get("type") or raw.get("connectionType") or "").lower()
    datahub_platform = type_to_platform_map.get(sigma_type, "")

    return SigmaConnectionRecord(
        connection_id=conn_id,
        name=raw.get("name", ""),
        sigma_type=sigma_type,
        datahub_platform=datahub_platform,
        host=_first_str(raw, "host"),
        account=_first_str(raw, "account"),
        default_database=_first_str(raw, "database", "defaultDatabase"),
        default_schema=_first_str(raw, "schema", "defaultSchema"),
        instance_hint=_first_str(raw, "warehouse", "cluster"),
        is_mappable=bool(datahub_platform),
    )


def _first_str(raw: Dict[str, Any], *keys: str) -> Optional[str]:
    """First non-empty string value among ``keys`` in ``raw``, else None.

    Sigma's API returns ``""`` for missing optional fields and sometimes
    spells the same field two ways (e.g. ``database`` vs ``defaultDatabase``).
    """
    for key in keys:
        value = raw.get(key)
        if isinstance(value, str) and value:
            return value
    return None
