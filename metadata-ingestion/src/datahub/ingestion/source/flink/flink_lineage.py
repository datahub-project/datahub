import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from datahub.ingestion.source.flink.flink_client import FlinkPlanNode

logger = logging.getLogger(__name__)


class NodeRole(Enum):
    SOURCE = "SOURCE"
    SINK = "SINK"
    TRANSFORM = "TRANSFORM"


@dataclass(frozen=True)
class ClassifiedNode:
    """A plan node with its classified role."""

    node_id: str
    description: str
    role: NodeRole
    platform: Optional[str]
    dataset_name: Optional[str]


@dataclass
class LineageResult:
    """Extracted lineage from a Flink job execution plan."""

    sources: List[ClassifiedNode] = field(default_factory=list)
    sinks: List[ClassifiedNode] = field(default_factory=list)
    unclassified: List[str] = field(default_factory=list)


class LineageExtractor(ABC):
    """Base class for per-connector lineage extraction plugins.

    Each plugin knows how to extract dataset names from a specific
    Flink connector type's operator descriptions.
    """

    @abstractmethod
    def can_extract(self, description: str, node: FlinkPlanNode) -> bool:
        """Return True if this extractor can handle the given operator description."""
        ...

    @abstractmethod
    def extract_dataset(
        self, description: str, node: FlinkPlanNode, role: NodeRole
    ) -> Optional[ClassifiedNode]:
        """Extract dataset info from the operator description. Returns None on failure."""
        ...


class KafkaLineageExtractor(LineageExtractor):
    """Extract Kafka topic lineage from KafkaSource/KafkaSink and TableSourceScan.

    Handles both DataStream API and SQL/Table API operator formats.
    """

    # DataStream API patterns
    KAFKA_DATASTREAM_SOURCE = re.compile(r"KafkaSource-([^\s>]+?)(?:\s*->|$)")
    KAFKA_DATASTREAM_SINK = re.compile(r"KafkaSink-([^\s>]+?)(?:\s*->|$)")

    # SQL/Table API: pre-FLIP-195 and post-FLIP-195 formats
    TABLE_SOURCE_SCAN = re.compile(
        r"(?:Source:\s*)?(?:\[\d+\]:)?TableSourceScan\("
        r"table=\[\[([^,]+),\s*([^,]+),\s*([^\]]+)\]\]"
    )
    TABLE_SINK = re.compile(
        r"(?:Sink:\s*)?(?:\[\d+\]:)?(?:TableSinkFunction|Sink)\("
        r"table=\[\[([^,]+),\s*([^,]+),\s*([^\]]+)\]\]"
    )

    INDICATORS = ("KafkaSource", "KafkaSink", "TableSourceScan", "TableSinkFunction")

    def can_extract(self, description: str, node: FlinkPlanNode) -> bool:
        return any(ind in description for ind in self.INDICATORS)

    def extract_dataset(
        self, description: str, node: FlinkPlanNode, role: NodeRole
    ) -> Optional[ClassifiedNode]:
        # DataStream API patterns
        if role == NodeRole.SOURCE:
            match = self.KAFKA_DATASTREAM_SOURCE.search(description)
            if match:
                return ClassifiedNode(
                    node_id=node.id,
                    description=description,
                    role=role,
                    platform="kafka",
                    dataset_name=match.group(1),
                )
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

        # SQL/Table API patterns
        pattern = self.TABLE_SOURCE_SCAN if role == NodeRole.SOURCE else self.TABLE_SINK
        match = pattern.search(description)
        if match:
            return ClassifiedNode(
                node_id=node.id,
                description=description,
                role=role,
                platform="kafka",
                dataset_name=match.group(3),
            )

        return None


# Role classification heuristics (connector-agnostic)
SOURCE_INDICATORS = ("Source:", "TableSourceScan")
SINK_INDICATORS = ("Sink:", "Sink(", "TableSinkFunction")


class FlinkLineageOrchestrator:
    """Orchestrates lineage extraction using registered extractor plugins.

    Plugin architecture: MVP ships with KafkaLineageExtractor only.
    Post-MVP extractors (JDBC, S3, Hive, etc.) are added to the registry
    without changing this class.
    """

    def __init__(self, extractors: Optional[List[LineageExtractor]] = None) -> None:
        self.extractors: List[LineageExtractor] = extractors or [
            KafkaLineageExtractor(),
        ]

    def extract(self, plan_nodes: List[FlinkPlanNode]) -> LineageResult:
        """Classify plan nodes and extract dataset references via registered plugins."""
        result = LineageResult()

        for node in plan_nodes:
            role = self._classify_node(node)

            if role in (NodeRole.SOURCE, NodeRole.SINK):
                classified = self._try_extractors(node, role)
                if classified:
                    target = result.sources if role == NodeRole.SOURCE else result.sinks
                    target.append(classified)
                else:
                    result.unclassified.append(node.description)

        return result

    def _classify_node(self, node: FlinkPlanNode) -> NodeRole:
        """Classify a plan node as SOURCE, SINK, or TRANSFORM."""
        desc = node.description
        if any(ind in desc for ind in SINK_INDICATORS):
            return NodeRole.SINK
        if any(ind in desc for ind in SOURCE_INDICATORS):
            return NodeRole.SOURCE
        return NodeRole.TRANSFORM

    def _try_extractors(
        self, node: FlinkPlanNode, role: NodeRole
    ) -> Optional[ClassifiedNode]:
        """Try each registered extractor until one succeeds."""
        for extractor in self.extractors:
            if extractor.can_extract(node.description, node):
                result = extractor.extract_dataset(node.description, node, role)
                if result:
                    return result
        return None
