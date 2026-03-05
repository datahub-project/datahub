from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, List, Optional, Set

from datahub.ingestion.source.flink.client import FlinkPlanNode

if TYPE_CHECKING:
    from datahub.ingestion.source.flink.report import FlinkSourceReport

logger = logging.getLogger(__name__)

# Source indicators in Flink plan node descriptions.
_SOURCE_INDICATORS = ("Source:", "TableSourceScan", "KafkaSource")

# Sink indicators. Operator-chained nodes use "tableName[N]: Writer" format.
# Legacy and DataStream API nodes use "Sink:" prefix.
_SINK_INDICATORS = ("Sink:", "Sink(", "TableSinkFunction", "KafkaSink", ": Writer")


class NodeRole(Enum):
    SOURCE = "SOURCE"
    SINK = "SINK"
    TRANSFORM = "TRANSFORM"


@dataclass(frozen=True)
class ClassifiedNode:
    node_id: str
    description: str
    role: NodeRole
    platform: Optional[str]
    dataset_name: Optional[str]


@dataclass
class LineageResult:
    sources: List[ClassifiedNode] = field(default_factory=list)
    sinks: List[ClassifiedNode] = field(default_factory=list)
    unclassified: List[str] = field(default_factory=list)


class LineageExtractor(ABC):
    @abstractmethod
    def can_extract(self, description: str, node: FlinkPlanNode) -> bool: ...

    @abstractmethod
    def extract_dataset(
        self, description: str, node: FlinkPlanNode, role: NodeRole
    ) -> Optional[ClassifiedNode]: ...


class KafkaLineageExtractor(LineageExtractor):
    """Extract Kafka topic lineage from DataStream and SQL/Table API operators.

    Handles three plan node description formats:

    1. DataStream API:
       - Source: "KafkaSource-{topic} -> ..."
       - Sink:   "KafkaSink-{topic} -> ..."

    2. SQL/Table API (legacy format):
       - Source: "TableSourceScan(table=[[catalog, db, table]])"
       - Sink:   "Sink(table=[[catalog, db, table]])"

    3. SQL/Table API (operator chaining, verified on Flink 1.19):
       - Source: "[N]:TableSourceScan(table=[[catalog, db, table]])"
       - Sink:   "tableName[N]: Writer"
       Source and sink may be in the SAME node, separated by <br/>.
    """

    # DataStream API: "KafkaSource-<topic>" / "KafkaSink-<topic>"
    KAFKA_DATASTREAM_SOURCE = re.compile(r"KafkaSource-([^\s>]+?)(?:\s*->|$)")
    KAFKA_DATASTREAM_SINK = re.compile(r"KafkaSink-([^\s>]+?)(?:\s*->|$)")

    # SQL/Table API source (all versions): TableSourceScan(table=[[catalog, db, table]])
    TABLE_SOURCE_SCAN = re.compile(
        r"(?:Source:\s*)?(?:\[\d+\]:)?TableSourceScan\("
        r"table=\[\[([^,]+),\s*([^,]+),\s*([^\]]+)\]\]"
    )

    # SQL/Table API sink (legacy format): Sink(table=[[catalog, db, table]])
    TABLE_SINK_LEGACY = re.compile(
        r"(?:Sink:\s*)?(?:\[\d+\]:)?(?:TableSinkFunction|Sink)\("
        r"table=\[\[([^,]+),\s*([^,]+),\s*([^\]]+)\]\]"
    )

    # SQL/Table API sink (operator chaining): "tableName[N]: Writer"
    # The table name is the Flink SQL table name used in CREATE TABLE.
    # Verified on Flink 1.19 — may also apply to earlier versions.
    SINK_WRITER = re.compile(r"\b(\w+)\[\d+\]:\s*Writer\b")

    _INDICATORS = (
        "KafkaSource",
        "KafkaSink",
        "TableSourceScan",
        "TableSinkFunction",
        ": Writer",
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
        # DataStream API: KafkaSource-{topic}
        match = self.KAFKA_DATASTREAM_SOURCE.search(description)
        if match:
            return ClassifiedNode(
                node_id=node.id,
                description=description,
                role=NodeRole.SOURCE,
                platform="kafka",
                dataset_name=match.group(1),
            )

        # SQL/Table API: TableSourceScan(table=[[catalog, db, table]])
        match = self.TABLE_SOURCE_SCAN.search(description)
        if match:
            return ClassifiedNode(
                node_id=node.id,
                description=description,
                role=NodeRole.SOURCE,
                platform="kafka",
                dataset_name=match.group(3),
            )

        return None

    def _extract_sink(
        self, description: str, node: FlinkPlanNode
    ) -> Optional[ClassifiedNode]:
        # DataStream API: KafkaSink-{topic}
        match = self.KAFKA_DATASTREAM_SINK.search(description)
        if match:
            return ClassifiedNode(
                node_id=node.id,
                description=description,
                role=NodeRole.SINK,
                platform="kafka",
                dataset_name=match.group(1),
            )

        # SQL/Table API legacy: Sink(table=[[catalog, db, table]])
        match = self.TABLE_SINK_LEGACY.search(description)
        if match:
            return ClassifiedNode(
                node_id=node.id,
                description=description,
                role=NodeRole.SINK,
                platform="kafka",
                dataset_name=match.group(3),
            )

        # SQL/Table API operator chaining: tableName[N]: Writer
        match = self.SINK_WRITER.search(description)
        if match:
            return ClassifiedNode(
                node_id=node.id,
                description=description,
                role=NodeRole.SINK,
                platform="kafka",
                dataset_name=match.group(1),
            )

        return None


class FlinkLineageOrchestrator:
    """Orchestrates lineage extraction using registered extractor plugins.

    Handles Flink's operator chaining where source, transform, and sink
    may be merged into a single plan node with <br/> separators.
    A single node can produce BOTH source and sink lineage.
    """

    def __init__(
        self,
        extractors: Optional[List[LineageExtractor]] = None,
        report: Optional["FlinkSourceReport"] = None,
    ) -> None:
        self.extractors: List[LineageExtractor] = extractors or [
            KafkaLineageExtractor(),
        ]
        self.report = report

    def extract(self, plan_nodes: List[FlinkPlanNode]) -> LineageResult:
        result = LineageResult()
        for node in plan_nodes:
            roles = self._classify_node_roles(node)
            for role in roles:
                if role == NodeRole.TRANSFORM:
                    continue
                classified = self._try_extractors(node, role)
                if classified:
                    target = result.sources if role == NodeRole.SOURCE else result.sinks
                    target.append(classified)
                else:
                    result.unclassified.append(node.description[:200])
        return result

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
        self, node: FlinkPlanNode, role: NodeRole
    ) -> Optional[ClassifiedNode]:
        for extractor in self.extractors:
            try:
                if extractor.can_extract(node.description, node):
                    result = extractor.extract_dataset(node.description, node, role)
                    if result:
                        return result
            except Exception as e:
                logger.warning(
                    "Extractor %s failed for node %s",
                    extractor.__class__.__name__,
                    node.id,
                    exc_info=True,
                )
                if self.report:
                    self.report.warning(
                        title="Lineage extractor failed",
                        message="A lineage extractor threw an exception while processing a plan node.",
                        context=f"extractor={extractor.__class__.__name__}, node_id={node.id}",
                        exc=e,
                    )
        return None
