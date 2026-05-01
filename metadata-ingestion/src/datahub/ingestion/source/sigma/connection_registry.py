from dataclasses import dataclass, field
from typing import Dict, List, Optional

# Sigma connection `type` → DataHub `dataPlatform` name.
# P1 probe confirmed: only "snowflake" on the Acryl PoC tenant.
# Other platforms are mapped per Sigma's documented warehouse types.
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
    "synapse": "microsoft-synapse",
    "azure_synapse": "microsoft-synapse",
}

# Fields that must be present for a platform record to reach confidence=1.0.
# P1 probe: Sigma does not expose default_database at the connection level;
# all Snowflake records will be confidence=0.5 until the API changes.
_REQUIRED_FIELDS_PER_PLATFORM: Dict[str, List[str]] = {
    "snowflake": ["host"],
    "bigquery": ["host"],
    "redshift": ["host"],
    "databricks": ["host"],
    "postgres": ["host"],
    "mysql": ["host"],
    "default": ["host"],
}


@dataclass(frozen=True)
class SigmaConnectionRecord:
    """Resolved Sigma Connection metadata for warehouse-URN construction.

    Built at SigmaSource init from /v2/connections; consumed by T4.B (entity-level
    DM→warehouse edges), T4.C (hi-fidelity short-name resolution in chart/DM
    formula resolvers), T4.D (Custom-SQL element platform-instance disambiguation).

    Confidence policy (set by SigmaConnectionRegistry.build):
      1.0  all platform-required fields present
      0.5  some warehouse-URN-shaping fields missing (partial info)
      0.0  datahub_platform mapping unknown (record kept for debugging only;
           never used to construct URNs)
    """

    connection_id: str
    name: str
    sigma_type: str
    datahub_platform: str
    # P1 probe: `host` field present (e.g. "<org>.snowflakecomputing.com").
    host: Optional[str] = None
    # P1 probe: `account` field present for Snowflake (e.g. "<org>-<locator>").
    # More reliable than parsing host for URN construction.
    account: Optional[str] = None
    default_database: Optional[str] = None
    default_schema: Optional[str] = None
    # Warehouse / cluster name (Snowflake: COMPUTE_WH, Databricks: cluster id).
    instance_hint: Optional[str] = None
    confidence: float = 1.0


@dataclass
class SigmaConnectionRegistry:
    """In-memory registry indexed by connection_id; built once per ingest run."""

    by_id: Dict[str, SigmaConnectionRecord] = field(default_factory=dict)

    def get(self, connection_id: str) -> Optional[SigmaConnectionRecord]:
        return self.by_id.get(connection_id)

    def is_resolvable(self, connection_id: str) -> bool:
        rec = self.by_id.get(connection_id)
        return rec is not None and rec.confidence > 0.0

    @classmethod
    def build(
        cls,
        raw_connections: List[Dict],
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
                continue

            name = raw.get("name", "")
            sigma_type = str(raw.get("type") or raw.get("connectionType") or "").lower()
            datahub_platform = type_to_platform_map.get(sigma_type, "")

            host = raw.get("host") or None
            account = raw.get("account") or None
            instance_hint = raw.get("warehouse") or raw.get("cluster") or None
            default_database = raw.get("database") or raw.get("defaultDatabase") or None
            default_schema = raw.get("schema") or raw.get("defaultSchema") or None

            if not datahub_platform:
                confidence = 0.0
                _increment(reporter, "connections_unmappable_type")
            else:
                required = _REQUIRED_FIELDS_PER_PLATFORM.get(
                    datahub_platform, _REQUIRED_FIELDS_PER_PLATFORM["default"]
                )
                field_values = {"host": host}
                present = [f for f in required if field_values.get(f)]
                if len(present) == len(required):
                    confidence = 1.0
                else:
                    confidence = 0.5
                    _increment(reporter, "connections_with_partial_metadata")

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
            registry.by_id[conn_id] = record

            if confidence > 0.0:
                _increment(reporter, "connections_resolved")

        return registry


def _increment(reporter: object, counter: str) -> None:
    """Safely increment a named int counter on reporter (no-op if absent)."""
    try:
        setattr(reporter, counter, getattr(reporter, counter) + 1)
    except AttributeError:
        pass
