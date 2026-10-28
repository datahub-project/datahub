from typing import Optional, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    ApiSignatureClass as ApiSignature,
    EdgeClass as Edge,
    KafkaAuditHeaderClass,
    SystemMetadataClass,
)
from datahub.metadata.urns import DatasetUrn, Urn


class ApiPatchBuilder(MetadataPatchProposal):
    """Patch builder for the API entity.

    Adds or removes individual dataset references on ``apiSignature`` without rewriting the
    rest of the aspect, so a lineage-only writer does not clobber inline ``inputFields`` /
    ``outputFields`` ingested by another producer (and vice versa).
    """

    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            urn, system_metadata=system_metadata, audit_header=audit_header
        )

    @staticmethod
    def _dataset_edge(dataset: Union[Edge, Urn, str]) -> Edge:
        if isinstance(dataset, Edge):
            DatasetUrn.from_string(dataset.destinationUrn)
            return dataset
        return Edge(destinationUrn=str(DatasetUrn.from_string(str(dataset))))

    def add_input_dataset(self, dataset: Union[Edge, Urn, str]) -> "ApiPatchBuilder":
        """Add a dataset whose schema defines the API's input (a Consumes lineage edge)."""
        edge = self._dataset_edge(dataset)
        self._add_patch(
            ApiSignature.ASPECT_NAME,
            "add",
            path=("inputDatasetEdges", edge.destinationUrn),
            value=edge,
        )
        return self

    def remove_input_dataset(self, dataset: Union[Urn, str]) -> "ApiPatchBuilder":
        self._add_patch(
            ApiSignature.ASPECT_NAME,
            "remove",
            path=("inputDatasetEdges", str(dataset)),
            value={},
        )
        return self

    def add_output_dataset(self, dataset: Union[Edge, Urn, str]) -> "ApiPatchBuilder":
        """Add a dataset whose schema defines the API's output (a Produces lineage edge)."""
        edge = self._dataset_edge(dataset)
        self._add_patch(
            ApiSignature.ASPECT_NAME,
            "add",
            path=("outputDatasetEdges", edge.destinationUrn),
            value=edge,
        )
        return self

    def remove_output_dataset(self, dataset: Union[Urn, str]) -> "ApiPatchBuilder":
        self._add_patch(
            ApiSignature.ASPECT_NAME,
            "remove",
            path=("outputDatasetEdges", str(dataset)),
            value={},
        )
        return self
