import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Sigma connection `type` → DataHub `dataPlatform` name. Values must match
# the canonical platform names in
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


@dataclass(frozen=True)
class SigmaConnectionRecord:
    """Resolved Sigma Connection metadata for warehouse-URN construction.

    Built at SigmaSource init from /v2/connections.

    Confidence policy (set by SigmaConnectionRegistry.build):
      1.0  datahub_platform mapping known
      0.0  datahub_platform mapping unknown (record kept for debugging only;
           never used to construct URNs)

    Default is 0.0 so a record constructed outside `build()` is treated as
    untrusted; only records that flow through the build path can claim 1.0.
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
    confidence: float = 0.0


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
        reporter: object,
        type_to_platform_map: Dict[str, str],
    ) -> "SigmaConnectionRegistry":
        """Build the registry from raw /v2/connections payloads.

        Each raw connection is mapped via `type_to_platform_map` (Sigma type →
        DataHub platform). Records with unmappable types are kept with
        confidence=0.0 so operators can audit gaps via the
        `connections_unmappable_type` counter.
        """
        registry = cls()
        for raw in raw_connections:
            conn_id = raw.get("connectionId") or raw.get("id", "")
            if not conn_id:
                logger.debug("Skipping Sigma connection payload without id: %s", raw)
                _increment(reporter, "connections_skipped_missing_id")
                continue

            name = raw.get("name", "")
            sigma_type = str(raw.get("type") or raw.get("connectionType") or "").lower()
            datahub_platform = type_to_platform_map.get(sigma_type, "")

            host = raw.get("host") or None
            account = raw.get("account") or None
            instance_hint = raw.get("warehouse") or raw.get("cluster") or None
            default_database = raw.get("database") or raw.get("defaultDatabase") or None
            default_schema = raw.get("schema") or raw.get("defaultSchema") or None

            if datahub_platform:
                confidence = 1.0
                _increment(reporter, "connections_resolved")
            else:
                confidence = 0.0
                _increment(reporter, "connections_unmappable_type")

            record = SigmaConnectionRecord(
                connection_id=conn_id,
                name=name,
                sigma_type=sigma_type,
                datahub_platform=datahub_platform,
                host=host,
                account=account,
                default_database=default_database,
                default_schema=default_schema,
                instance_hint=instance_hint,
                confidence=confidence,
            )
            if conn_id in registry.by_id:
                logger.debug(
                    "Duplicate Sigma connectionId %r; later record overwrites earlier.",
                    conn_id,
                )
                _increment(reporter, "connections_duplicate_id")
            registry.by_id[conn_id] = record

        return registry


def _increment(reporter: object, counter: str) -> None:
    """Safely increment a named int counter on reporter (no-op if absent).

    Tolerates fake reporters in tests that omit some counters; mistyped
    counter names log at debug so they are not completely silent.
    """
    try:
        setattr(reporter, counter, getattr(reporter, counter) + 1)
    except AttributeError:
        logger.debug("Reporter has no counter named %r; increment skipped.", counter)
