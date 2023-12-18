from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Type, Union

from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import mcps_from_mce
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.file import FileSourceConfig, GenericFileSource
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.schema_classes import (
    ASPECT_NAME_MAP,
    DomainPropertiesClass,
    UsageAggregationClass,
)


class MockDataHubGraph(DataHubGraph):
    def __init__(
        self, entity_graph: Optional[Dict[str, Dict[str, Any]]] = None
    ) -> None:
        self.emitted: List[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ] = []
        self.entity_graph = entity_graph or {}

    def import_file(self, file: Path) -> None:
        """Imports metadata from any MCE/MCP file. Does not clear prior loaded data.
        This function can be called repeatedly on the same
        Mock instance to load up metadata from multiple files."""
        file_source: GenericFileSource = GenericFileSource(
            ctx=PipelineContext(run_id="test"), config=FileSourceConfig(path=str(file))
        )
        for wu in file_source.get_workunits():
            if isinstance(wu, MetadataWorkUnit):
                metadata = wu.get_metadata().get("metadata")
                mcps: Iterable[
                    Union[
                        MetadataChangeProposal,
                        MetadataChangeProposalWrapper,
                    ]
                ]
                if isinstance(metadata, MetadataChangeEvent):
                    mcps = mcps_from_mce(metadata)
                elif isinstance(
                    metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
                ):
                    mcps = [metadata]
                else:
                    raise Exception(
                        f"Unexpected metadata type {type(metadata)}. Was expecting MCE, MCP or MCPW"
                    )

                for mcp in mcps:
                    assert mcp.entityUrn
                    assert mcp.aspectName
                    assert mcp.aspect
                    if mcp.entityUrn not in self.entity_graph:
                        self.entity_graph[mcp.entityUrn] = {}
                    self.entity_graph[mcp.entityUrn][mcp.aspectName] = mcp.aspect

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
            UsageAggregationClass,
        ],
        callback: Union[Callable[[Exception, str], None], None] = None,
    ) -> None:
        self.emitted.append(item)  # type: ignore

    def emit_mce(self, mce: MetadataChangeEvent) -> None:
        self.emitted.append(mce)

    def emit_mcp(
        self, mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper]
    ) -> None:
        self.emitted.append(mcp)

    def get_emitted(
        self,
    ) -> List[
        Union[
            MetadataChangeEvent, MetadataChangeProposal, MetadataChangeProposalWrapper
        ]
    ]:
        return self.emitted

    def sink_to_file(self, file: Path) -> None:
        write_metadata_file(file, self.emitted)
