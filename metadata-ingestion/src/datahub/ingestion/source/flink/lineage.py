from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set

from datahub.ingestion.source.flink.client import FlinkPlanNode
from datahub.ingestion.source.flink.config import CatalogPlatformDetail
from datahub.ingestion.source.flink.sql_gateway_client import FlinkSQLGatewayClient

logger = logging.getLogger(__name__)

_SOURCE_INDICATORS = ("Source:", "TableSourceScan", "KafkaSource")

_SINK_INDICATORS = (
    "Sink:",
    "Sink(",
    "TableSinkFunction",
    "KafkaSink",
    ": Writer",
    "IcebergSink",
)

_SINGLE_PLATFORM_CATALOG_TYPES = frozenset({"jdbc", "iceberg", "paimon"})

_MIXED_PLATFORM_CATALOG_TYPES = frozenset({"hive", "generic_in_memory"})

_JDBC_PLATFORM_MAP: Dict[str, str] = {
    "jdbc:postgresql": "postgres",
    "jdbc:mysql": "mysql",
    "jdbc:sqlserver": "mssql",
    "jdbc:oracle": "oracle",
    "jdbc:db2": "db2",
    "jdbc:mariadb": "mariadb",
    "jdbc:redshift": "redshift",
    "jdbc:snowflake": "snowflake",
}

_CONNECTOR_PLATFORM_MAP: Dict[str, str] = {
    "kafka": "kafka",
    "jdbc": "jdbc",
    "hive": "hive",
    "filesystem": "hdfs",
    "iceberg": "iceberg",
    "paimon": "paimon",
    "elasticsearch": "elasticsearch",
    "opensearch": "opensearch",
    "mongodb": "mongodb",
    "hbase": "hbase",
    "kinesis": "kinesis",
    "dynamodb": "dynamodb",
    "clickhouse": "clickhouse",
    "doris": "doris",
    "starrocks": "starrocks",
    "cassandra": "cassandra",
    "pulsar": "pulsar",
    "rabbitmq": "rabbitmq",
    "datagen": "datagen",
    "print": "print",
    "blackhole": "blackhole",
}


class NodeRole(Enum):
    SOURCE = "SOURCE"
    SINK = "SINK"
    TRANSFORM = "TRANSFORM"


@dataclass(frozen=True)
class CatalogTableReference:
    """An unresolved table reference from a SQL/Table API plan node.

    Contains the Flink catalog path (catalog, database, table) extracted
    from the plan description. Platform is NOT yet known — it must be
    resolved via PlatformResolver using SQL Gateway catalog introspection.

    When catalog and database are empty strings (SINK_WRITER pattern), the table
    name is all that is known. This pattern cannot be resolved and is reported
    as unresolved in the ingestion report.
    """

    catalog: str
    database: str
    table: str


@dataclass(frozen=True)
class ClassifiedNode:
    node_id: str
    description: str
    role: NodeRole
    platform: Optional[str]
    dataset_name: Optional[str]
    catalog_ref: Optional[CatalogTableReference] = None


@dataclass(frozen=True)
class ExtractorWarning:
    """Warning from a lineage extractor, deferred for main-thread reporting."""

    extractor_name: str
    node_id: str
    exc: BaseException


@dataclass
class LineageResult:
    sources: List[ClassifiedNode] = field(default_factory=list)
    sinks: List[ClassifiedNode] = field(default_factory=list)
    unclassified: List[str] = field(default_factory=list)
    extractor_warnings: List[ExtractorWarning] = field(default_factory=list)


class LineageExtractor(ABC):
    @abstractmethod
    def can_extract(self, description: str, node: FlinkPlanNode) -> bool: ...

    @abstractmethod
    def extract_dataset(
        self, description: str, node: FlinkPlanNode, role: NodeRole
    ) -> Optional[ClassifiedNode]: ...


class DataStreamKafkaExtractor(LineageExtractor):
    """Extract Kafka topic lineage from DataStream API operators only.

    Handles:
      - Source: "KafkaSource-{topic} -> ..."
      - Sink:   "KafkaSink-{topic} -> ..."

    Platform="kafka" is correct here because the connector type IS in the
    operator name (KafkaSource/KafkaSink are Flink's Kafka connector classes).
    """

    KAFKA_DATASTREAM_SOURCE = re.compile(r"KafkaSource-([^\s><]+)")
    # Stop at ':', '[', whitespace, or '>' so that both the legacy format
    # "KafkaSink-topic -> ..." and the Flink 1.19 Kafka Sink V2 formats
    # "KafkaSink-topic: Writer" and "KafkaSink-topic[N]: Writer" are handled.
    KAFKA_DATASTREAM_SINK = re.compile(r"KafkaSink-([^:\s>\[]+)")

    _INDICATORS = ("KafkaSource", "KafkaSink")

    def can_extract(self, description: str, node: FlinkPlanNode) -> bool:
        return any(ind in description for ind in self._INDICATORS)

    def extract_dataset(
        self, description: str, node: FlinkPlanNode, role: NodeRole
    ) -> Optional[ClassifiedNode]:
        if role == NodeRole.SOURCE:
            match = self.KAFKA_DATASTREAM_SOURCE.search(description)
        else:
            match = self.KAFKA_DATASTREAM_SINK.search(description)
        if match:
            return ClassifiedNode(
                node_id=node.id,
                description=description,
                role=role,
                platform="kafka",
                dataset_name=match.group(1),
            )
        return None


class SqlTableExtractor(LineageExtractor):
    """Extract catalog table references from SQL/Table API plan nodes.

    Handles four plan description formats:

      Source:
        "TableSourceScan(table=[[catalog, db, table]])"

      Sink (double-bracket, comma-separated — legacy SinkFunction connectors):
        "Sink(table=[[catalog, db, table]])"

      Sink (single-bracket, dot-separated — JDBC, filesystem, and other Sink V2 connectors):
        "Sink(table=[catalog.db.table])"

      Sink (Writer pattern — Kafka Sink V2, operator chaining):
        "tableName[N]: Writer"

    Does NOT assign platform — returns ClassifiedNode with catalog_ref
    set and platform=None. Platform resolution happens in PlatformResolver.
    """

    # SQL/Table API source: TableSourceScan(table=[[catalog, db, table, ...]])
    TABLE_SOURCE_SCAN = re.compile(
        r"(?:Source:\s*)?(?:\[\d+\]:)?TableSourceScan\("
        r"table=\[\[([^,]+),\s*([^,]+),\s*([^,\]]+)"
    )

    # SQL/Table API sink (double-bracket): Sink(table=[[catalog, db, table]])
    TABLE_SINK_DOUBLE_BRACKET = re.compile(
        r"(?:Sink:\s*)?(?:\[\d+\]:)?(?:TableSinkFunction|Sink)\("
        r"table=\[\[([^,]+),\s*([^,]+),\s*([^\]]+)\]\]"
    )

    # SQL/Table API sink (single-bracket, dotted path): Sink(table=[catalog.db.table])
    # Produced by JDBC, filesystem, and other non-Kafka Sink V2 connectors on Flink 1.15+.
    TABLE_SINK_SINGLE_BRACKET = re.compile(
        r"(?:Sink:\s*)?(?:\[\d+\]:)?Sink\("
        r"table=\[([^.\]]+)\.([^.\]]+)\.([^\],]+)\]"
    )

    # Iceberg sink: "Sink: IcebergSink catalog.database.table"
    # Produced by the Iceberg Flink connector (iceberg-flink-runtime).
    ICEBERG_SINK = re.compile(r"Sink:\s*IcebergSink\s+([\w-]+)\.([\w-]+)\.([\w-]+)")

    # SQL/Table API sink (operator chaining): "tableName[N]: Writer"
    # Produced by Kafka Sink V2. No catalog/database info available.
    SINK_WRITER = re.compile(r"\b([\w-]+)\[\d+\]:\s*Writer\b")

    _INDICATORS = (
        "TableSourceScan",
        "TableSinkFunction",
        ": Writer",
        "Sink(",
        "IcebergSink",
    )

    def can_extract(self, description: str, node: FlinkPlanNode) -> bool:
        return any(ind in description for ind in self._INDICATORS)

    def extract_dataset(
        self, description: str, node: FlinkPlanNode, role: NodeRole
    ) -> Optional[ClassifiedNode]:
        if role == NodeRole.SOURCE:
            return self._extract_source(description, node)
        return self._extract_sink(description, node)

    def _extract_source(
        self, description: str, node: FlinkPlanNode
    ) -> Optional[ClassifiedNode]:
        match = self.TABLE_SOURCE_SCAN.search(description)
        if match:
            ref = CatalogTableReference(
                catalog=match.group(1).strip(),
                database=match.group(2).strip(),
                table=match.group(3).strip(),
            )
            return ClassifiedNode(
                node_id=node.id,
                description=description,
                role=NodeRole.SOURCE,
                platform=None,
                dataset_name=None,
                catalog_ref=ref,
            )
        return None

    def _extract_sink(
        self, description: str, node: FlinkPlanNode
    ) -> Optional[ClassifiedNode]:
        match = self.TABLE_SINK_DOUBLE_BRACKET.search(description)
        if match:
            ref = CatalogTableReference(
                catalog=match.group(1).strip(),
                database=match.group(2).strip(),
                table=match.group(3).strip(),
            )
            return ClassifiedNode(
                node_id=node.id,
                description=description,
                role=NodeRole.SINK,
                platform=None,
                dataset_name=None,
                catalog_ref=ref,
            )

        # Single-bracket dotted: Sink(table=[catalog.db.table])
        # Produced by JDBC, filesystem, and other non-Kafka sinks on Flink 1.15+.
        match = self.TABLE_SINK_SINGLE_BRACKET.search(description)
        if match:
            ref = CatalogTableReference(
                catalog=match.group(1).strip(),
                database=match.group(2).strip(),
                table=match.group(3).strip(),
            )
            return ClassifiedNode(
                node_id=node.id,
                description=description,
                role=NodeRole.SINK,
                platform=None,
                dataset_name=None,
                catalog_ref=ref,
            )

        match = self.ICEBERG_SINK.search(description)
        if match:
            ref = CatalogTableReference(
                catalog=match.group(1).strip(),
                database=match.group(2).strip(),
                table=match.group(3).strip(),
            )
            return ClassifiedNode(
                node_id=node.id,
                description=description,
                role=NodeRole.SINK,
                platform=None,
                dataset_name=None,
                catalog_ref=ref,
            )

        # Kafka Sink V2 operator chaining: tableName[N]: Writer
        # This format does NOT include catalog/database info.
        match = self.SINK_WRITER.search(description)
        if match:
            ref = CatalogTableReference(
                catalog="",
                database="",
                table=match.group(1).strip(),
            )
            return ClassifiedNode(
                node_id=node.id,
                description=description,
                role=NodeRole.SINK,
                platform=None,
                dataset_name=None,
                catalog_ref=ref,
            )
        return None


@dataclass(frozen=True)
class CatalogInfo:
    """Cached catalog metadata from DESCRIBE CATALOG EXTENDED."""

    catalog_type: str
    properties: Dict[str, str]


_CATALOG_NOT_FOUND = CatalogInfo(catalog_type="__not_found__", properties={})


class PlatformResolver:
    """Resolves Flink catalog table references to DataHub platform + dataset name.

    Two-tier resolution strategy:

    Tier 1 — Catalog-level (1 call per catalog):
      - jdbc → parse base-url for postgres/mysql
      - iceberg → platform="iceberg"
      - paimon → platform="paimon"

    Tier 2 — Per-table (only for ambiguous catalog types):
      - hive / generic_in_memory → SHOW CREATE TABLE per referenced table
      - Only called for tables actually referenced in job plans

    Known limitation: Kafka Sink V2 in SQL/Table API mode produces
    "tableName[N]: Writer" in the plan description with no catalog path.
    This affects both persistent catalog tables (HiveCatalog) and TEMPORARY
    TABLEs. The actual Kafka topic name is only available via SHOW CREATE TABLE,
    which requires the catalog and database — information that Flink does not
    include in the plan for Sink V2. These sinks are reported as unresolved.
    DataStream API Kafka jobs are not affected (topic is in the operator name).
    """

    def __init__(
        self,
        sql_gateway_client: Optional[FlinkSQLGatewayClient] = None,
        catalog_platform_map: Optional[Dict[str, CatalogPlatformDetail]] = None,
    ) -> None:
        self._sql_client = sql_gateway_client
        # User-provided catalog overrides: platform and/or platform_instance per catalog.
        # Takes priority over SQL Gateway auto-detection.
        self._catalog_platform_map = catalog_platform_map or {}
        self._catalog_cache: Dict[str, CatalogInfo] = {}
        self._table_connector_cache: Dict[str, Dict[str, str]] = {}
        # Caches are populated from worker threads (ThreadPoolExecutor) for hive/generic_in_memory
        # catalogs. CPython GIL ensures dict operations are atomic. This follows the same pattern
        # as Snowflake and BigQuery connectors which share mutable state across threads without locks.

    def build_catalog_context(self) -> None:
        """Pre-fetch catalog info for all catalogs visible to SQL Gateway."""
        if not self._sql_client:
            return
        catalogs = self._sql_client.get_catalogs()
        for catalog_name in catalogs:
            self._get_catalog_info(catalog_name)

    def resolve(self, ref: CatalogTableReference) -> Optional[ClassifiedNode]:
        """Resolve a catalog table reference to a platform-correct ClassifiedNode.

        Returns None if platform cannot be determined (no SQL Gateway or unknown catalog).
        """
        # SINK_WRITER pattern: no catalog/database in plan — unresolvable.
        if not ref.catalog:
            return None

        # Recipe config takes priority over SQL Gateway auto-detection.
        # Needed when DESCRIBE CATALOG EXTENDED is unavailable (older Flink versions)
        # or to override incorrect auto-detection results.
        detail = self._catalog_platform_map.get(ref.catalog)
        if detail and detail.platform:
            return ClassifiedNode(
                node_id="",
                description="",
                role=NodeRole.SOURCE,
                platform=detail.platform,
                dataset_name=f"{ref.database}.{ref.table}",
                catalog_ref=ref,
            )

        if not self._sql_client:
            return None

        catalog_info = self._get_catalog_info(ref.catalog)

        if catalog_info:
            # Tier 1: catalog-level resolution for single-platform catalogs
            if catalog_info.catalog_type in _SINGLE_PLATFORM_CATALOG_TYPES:
                return self._resolve_from_catalog(catalog_info, ref)

            # Tier 2: per-table resolution for mixed-platform catalogs
            if catalog_info.catalog_type in _MIXED_PLATFORM_CATALOG_TYPES:
                return self._resolve_from_table_ddl(catalog_info, ref)

        # Fallback: DESCRIBE CATALOG EXTENDED unavailable (older Flink versions) or unknown catalog type.
        return self._resolve_from_table_ddl(
            catalog_info or CatalogInfo(catalog_type="unknown", properties={}),
            ref,
        )

    def _get_catalog_info(self, catalog_name: str) -> Optional[CatalogInfo]:
        if catalog_name in self._catalog_cache:
            cached = self._catalog_cache[catalog_name]
            return None if cached is _CATALOG_NOT_FOUND else cached
        if not self._sql_client:
            return None
        try:
            raw = self._sql_client.get_catalog_info(catalog_name)
            catalog_type = raw.get("type", "unknown")
            info = CatalogInfo(catalog_type=catalog_type, properties=raw)
            self._catalog_cache[catalog_name] = info
            return info
        except Exception as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status in (400, 404):
                logger.debug(
                    "DESCRIBE CATALOG not supported for '%s' (Flink < 1.20?)",
                    catalog_name,
                    exc_info=True,
                )
            else:
                logger.warning(
                    "Failed to describe catalog '%s'; lineage for its tables will be unresolved",
                    catalog_name,
                    exc_info=True,
                )
            self._catalog_cache[catalog_name] = _CATALOG_NOT_FOUND
            return None

    def _resolve_from_catalog(
        self, catalog_info: CatalogInfo, ref: CatalogTableReference
    ) -> Optional[ClassifiedNode]:
        """Resolve platform from catalog-level properties (no per-table calls)."""
        cat_type = catalog_info.catalog_type

        if cat_type == "jdbc":
            platform = self._jdbc_platform(catalog_info.properties)
            if not platform:
                return None
            # For JDBC, the table field may include schema prefix (e.g., "public.users")
            dataset_name = f"{ref.database}.{ref.table}"
            return ClassifiedNode(
                node_id="",
                description="",
                role=NodeRole.SOURCE,
                platform=platform,
                dataset_name=dataset_name,
                catalog_ref=ref,
            )

        if cat_type in ("iceberg", "paimon"):
            return ClassifiedNode(
                node_id="",
                description="",
                role=NodeRole.SOURCE,
                platform=cat_type,
                dataset_name=f"{ref.database}.{ref.table}",
                catalog_ref=ref,
            )

        return None

    def _resolve_from_table_ddl(
        self, catalog_info: CatalogInfo, ref: CatalogTableReference
    ) -> Optional[ClassifiedNode]:
        """Resolve platform by calling SHOW CREATE TABLE (for hive/generic_in_memory catalogs)."""
        cache_key = f"{ref.catalog}.{ref.database}.{ref.table}"
        if cache_key not in self._table_connector_cache:
            if not self._sql_client:
                return None
            props = self._sql_client.get_table_connector(
                ref.catalog, ref.database, ref.table
            )
            self._table_connector_cache[cache_key] = props

        props = self._table_connector_cache[cache_key]
        connector = props.get("connector", "")
        if not connector:
            return None

        platform = self._connector_to_platform(connector, props)
        dataset_name = self._build_dataset_name(connector, props, ref)

        return ClassifiedNode(
            node_id="",
            description="",
            role=NodeRole.SOURCE,
            platform=platform,
            dataset_name=dataset_name,
            catalog_ref=ref,
        )

    @staticmethod
    def _jdbc_platform(properties: Dict[str, str]) -> Optional[str]:
        """Parse JDBC base-url to determine database platform."""
        base_url = properties.get("option:base-url", "")
        for prefix, platform in _JDBC_PLATFORM_MAP.items():
            if base_url.startswith(prefix):
                return platform
        if base_url:
            logger.debug("Unknown JDBC base-url prefix: %s", base_url)
        return None

    @staticmethod
    def _connector_to_platform(
        connector: str, props: Optional[Dict[str, str]] = None
    ) -> str:
        """Map Flink connector type to DataHub platform name."""
        if connector == "jdbc" and props:
            url = props.get("url", "")
            for prefix, platform in _JDBC_PLATFORM_MAP.items():
                if url.startswith(prefix):
                    return platform

        return _CONNECTOR_PLATFORM_MAP.get(connector, connector)

    @staticmethod
    def _build_dataset_name(
        connector: str, props: Dict[str, str], ref: CatalogTableReference
    ) -> str:
        """Build the dataset name using connector-specific properties."""
        if connector == "kafka":
            topic = props.get("topic", "")
            if topic:
                return topic

        if connector == "jdbc":
            table_name = props.get("table-name", "")
            if table_name:
                return f"{ref.database}.{table_name}"

        return f"{ref.database}.{ref.table}"


class FlinkLineageOrchestrator:
    """Orchestrates lineage extraction using registered extractor plugins.

    Handles Flink's operator chaining where source, transform, and sink
    may be merged into a single plan node with <br/> separators.
    A single node can produce BOTH source and sink lineage.
    """

    def __init__(
        self,
        extractors: Optional[List[LineageExtractor]] = None,
        platform_resolver: Optional[PlatformResolver] = None,
    ) -> None:
        self.extractors: List[LineageExtractor] = extractors or [
            DataStreamKafkaExtractor(),
            SqlTableExtractor(),
        ]
        self.platform_resolver = platform_resolver

    def extract(self, plan_nodes: List[FlinkPlanNode]) -> LineageResult:
        result = LineageResult()
        for node in plan_nodes:
            roles = self._classify_node_roles(node)
            for role in roles:
                if role == NodeRole.TRANSFORM:
                    continue
                classified = self._try_extractors(node, role, result)
                if classified:
                    if classified.catalog_ref and not classified.platform:
                        original = classified
                        classified = self._resolve_platform(classified, role)
                        if not classified:
                            ref = original.catalog_ref
                            assert ref is not None  # guaranteed by the if above
                            if not ref.catalog:
                                # Kafka Sink V2 in SQL/Table API mode: plan description
                                # carries only the Flink table name, no catalog path.
                                result.unclassified.append(
                                    f"kafka-sink-v2-unresolved:{ref.table}"
                                )
                            else:
                                result.unclassified.append(
                                    f"{ref.catalog}.{ref.database}.{ref.table}"
                                )
                            continue
                    if classified.platform and classified.dataset_name:
                        target = (
                            result.sources if role == NodeRole.SOURCE else result.sinks
                        )
                        target.append(classified)
                    else:
                        result.unclassified.append(node.description[:200])
                else:
                    result.unclassified.append(node.description[:200])
        return result

    def _resolve_platform(
        self,
        node: ClassifiedNode,
        role: NodeRole,
    ) -> Optional[ClassifiedNode]:
        """Resolve platform for a node with catalog_ref via PlatformResolver."""
        if not self.platform_resolver or not node.catalog_ref:
            return None
        resolved = self.platform_resolver.resolve(node.catalog_ref)
        if not resolved:
            return None
        # Preserve original node_id and description from the plan node
        return ClassifiedNode(
            node_id=node.node_id,
            description=node.description,
            role=role,
            platform=resolved.platform,
            dataset_name=resolved.dataset_name,
            catalog_ref=node.catalog_ref,
        )

    def _classify_node_roles(self, node: FlinkPlanNode) -> Set[NodeRole]:
        """Classify a node into one or more roles.

        Flink may merge source + transform + sink into a single plan node
        via operator chaining. This is common with streaming connectors
        (e.g., Kafka) where the source and sink are chained into one
        operator. The description uses <br/> to separate sub-operations.
        A node can be both SOURCE and SINK simultaneously.
        """
        desc = node.description
        roles: Set[NodeRole] = set()
        if any(ind in desc for ind in _SOURCE_INDICATORS):
            roles.add(NodeRole.SOURCE)
        if any(ind in desc for ind in _SINK_INDICATORS):
            roles.add(NodeRole.SINK)
        return roles or {NodeRole.TRANSFORM}

    def _try_extractors(
        self,
        node: FlinkPlanNode,
        role: NodeRole,
        lineage_result: LineageResult,
    ) -> Optional[ClassifiedNode]:
        for extractor in self.extractors:
            try:
                if extractor.can_extract(node.description, node):
                    result = extractor.extract_dataset(node.description, node, role)
                    if result:
                        return result
            except Exception as e:
                lineage_result.extractor_warnings.append(
                    ExtractorWarning(
                        extractor_name=extractor.__class__.__name__,
                        node_id=node.id,
                        exc=e,
                    )
                )
        return None
