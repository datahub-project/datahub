from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.file import FileSourceConfig, GenericFileSource
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.com.linkedin.pegasus2avro.usage import UsageAggregation
from datahub.metadata.schema_classes import ASPECT_NAME_MAP, DomainPropertiesClass


class MockDataHubGraph(DataHubGraph):
    def __init__(self, entity_graph: Dict[str, Dict[str, Any]] = {}) -> None:
        self.emitted: List[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
                UsageAggregation,
            ]
        ] = []
        self.entity_graph = entity_graph

    def import_file(self, file: Path) -> None:
        file_source: GenericFileSource = GenericFileSource(
            ctx=PipelineContext(run_id="test"), config=FileSourceConfig(path=str(file))
        )
        for wu in file_source.get_workunits():
            if isinstance(wu, MetadataWorkUnit):
                metadata = wu.get_metadata().get("metadata")
                if isinstance(
                    metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
                ):
                    assert metadata.entityUrn
                    assert metadata.aspectName
                    assert metadata.aspect
                    if metadata.entityUrn not in self.entity_graph:
                        self.entity_graph[metadata.entityUrn] = {}
                    self.entity_graph[metadata.entityUrn][
                        metadata.aspectName
                    ] = metadata.aspect

    def get_aspect(
        self, entity_urn: str, aspect_type: Type[Aspect], version: int = 0
    ) -> Optional[Aspect]:
        aspect_name = [v for v in ASPECT_NAME_MAP if ASPECT_NAME_MAP[v] == aspect_type][
            0
        ]
        result = self.entity_graph.get(entity_urn, {}).get(aspect_name, None)
        if result is not None and isinstance(result, dict):
            return aspect_type.from_obj(result)
        else:
            return result

    def get_domain_urn_by_name(self, domain_name: str) -> Optional[str]:
        domain_metadata = {
            urn: metadata
            for urn, metadata in self.entity_graph.items()
            if urn.startswith("urn:li:domain:")
        }
        domain_properties_metadata = {
            urn: metadata["domainProperties"].name
            for urn, metadata in domain_metadata.items()
            if "domainProperties" in metadata
            and isinstance(metadata["domainProperties"], DomainPropertiesClass)
        }
        urn_match = [
            urn
            for urn, name in domain_properties_metadata.items()
            if name == domain_name
        ]
        if urn_match:
            return urn_match[0]
        else:
            return None

    def emit(
        self,
        item: Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
            UsageAggregation,
        ],
        callback: Union[Callable[[Exception, str], None], None] = None,
    ) -> Tuple[datetime, datetime]:
        self.emitted.append(item)
        return (datetime.now(), datetime.now())

    def emit_mce(self, mce: MetadataChangeEvent) -> None:
        self.emitted.append(mce)

    def emit_mcp(
        self, mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper]
    ) -> None:
        self.emitted.append(mcp)

    def emit_usage(self, usageStats: UsageAggregation) -> None:
        self.emitted.append(usageStats)

    def get_emitted(
        self,
    ) -> List[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
            UsageAggregation,
        ]
    ]:
        return self.emitted

    def sink_to_file(self, file: Path) -> None:
        write_metadata_file(file, self.emitted)
