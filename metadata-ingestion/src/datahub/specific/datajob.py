import time
from typing import Dict, List, Optional, TypeVar, Union
from urllib.parse import quote

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataJobInfoClass as DataJobInfo,
    DataJobInputOutputClass as DataJobInputOutput,
    EdgeClass as Edge,
    GlobalTagsClass as GlobalTags,
    GlossaryTermAssociationClass as Term,
    GlossaryTermsClass as GlossaryTerms,
    KafkaAuditHeaderClass,
    OwnerClass as Owner,
    OwnershipTypeClass,
    SystemMetadataClass,
    TagAssociationClass as Tag,
)
from datahub.specific.custom_properties import CustomPropertiesPatchHelper
from datahub.specific.ownership import OwnershipPatchHelper
from datahub.utilities.urns.tag_urn import TagUrn
from datahub.utilities.urns.urn import Urn

T = TypeVar("T", bound=MetadataPatchProposal)


class DataJobPatchBuilder(MetadataPatchProposal):
    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            urn, "datajob", system_metadata=system_metadata, audit_header=audit_header
        )
        self.custom_properties_patch_helper = CustomPropertiesPatchHelper(
            self, DataJobInfo.ASPECT_NAME
        )
        self.ownership_patch_helper = OwnershipPatchHelper(self)

    def _mint_auditstamp(self, message: Optional[str] = None) -> AuditStampClass:
        return AuditStampClass(
            time=int(time.time() * 1000.0),
            actor="urn:li:corpuser:datahub",
            message=message,
        )

    def _ensure_urn_type(
        self, entity_type: str, edges: List[Edge], context: str
    ) -> None:
        for e in edges:
            urn = Urn.create_from_string(e.destinationUrn)
            if not urn.get_type() == entity_type:
                raise ValueError(
                    f"{context}: {e.destinationUrn} is not of type {entity_type}"
                )

    def add_owner(self, owner: Owner) -> "DataJobPatchBuilder":
        self.ownership_patch_helper.add_owner(owner)
        return self

    def remove_owner(
        self, owner: str, owner_type: Optional[OwnershipTypeClass] = None
    ) -> "DataJobPatchBuilder":
        """
        param: owner_type is optional
        """
        self.ownership_patch_helper.remove_owner(owner, owner_type)
        return self

    def set_owners(self, owners: List[Owner]) -> "DataJobPatchBuilder":
        self.ownership_patch_helper.set_owners(owners)
        return self

    def add_input_datajob(self, input: Union[Edge, Urn, str]) -> "DataJobPatchBuilder":
        if isinstance(input, Edge):
            input_urn: str = input.destinationUrn
            input_edge: Edge = input
        elif isinstance(input, (Urn, str)):
            input_urn = str(input)
            if not input_urn.startswith("urn:li:dataJob:"):
                raise ValueError(f"Input {input} is not a DataJob urn")

            input_edge = Edge(
                destinationUrn=input_urn,
                created=self._mint_auditstamp(),
                lastModified=self._mint_auditstamp(),
            )

        self._ensure_urn_type("dataJob", [input_edge], "add_input_datajob")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path=f"/inputDatajobEdges/{quote(input_urn, safe='')}",
            value=input_edge,
        )
        return self

    def remove_input_datajob(self, input: Union[str, Urn]) -> "DataJobPatchBuilder":
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "remove",
            path=f"/inputDatajobEdges/{input}",
            value={},
        )
        return self

    def set_input_datajobs(self, inputs: List[Edge]) -> "DataJobPatchBuilder":
        self._ensure_urn_type("dataJob", inputs, "input datajobs")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "replace",
            path="/inputDatajobEdges",
            value=inputs,
        )
        return self

    def add_input_dataset(self, input: Union[Edge, Urn, str]) -> "DataJobPatchBuilder":
        if isinstance(input, Edge):
            input_urn: str = input.destinationUrn
            input_edge: Edge = input
        elif isinstance(input, (Urn, str)):
            input_urn = str(input)
            if not input_urn.startswith("urn:li:dataset:"):
                raise ValueError(f"Input {input} is not a Dataset urn")

            input_edge = Edge(
                destinationUrn=input_urn,
                created=self._mint_auditstamp(),
                lastModified=self._mint_auditstamp(),
            )

        self._ensure_urn_type("dataset", [input_edge], "add_input_dataset")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path=f"/inputDatasetEdges/{quote(input_urn, safe='')}",
            value=input_edge,
        )
        return self

    def remove_input_dataset(self, input: Union[str, Urn]) -> "DataJobPatchBuilder":
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "remove",
            path=f"/inputDatasetEdges/{input}",
            value={},
        )
        return self

    def set_input_datasets(self, inputs: List[Edge]) -> "DataJobPatchBuilder":
        self._ensure_urn_type("dataset", inputs, "set_input_datasets")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "replace",
            path="/inputDatasetEdges",
            value=inputs,
        )
        return self

    def add_output_dataset(
        self, output: Union[Edge, Urn, str]
    ) -> "DataJobPatchBuilder":
        if isinstance(output, Edge):
            output_urn: str = output.destinationUrn
            output_edge: Edge = output
        elif isinstance(output, (Urn, str)):
            output_urn = str(output)
            if not output_urn.startswith("urn:li:dataset:"):
                raise ValueError(f"Input {input} is not a Dataset urn")

            output_edge = Edge(
                destinationUrn=output_urn,
                created=self._mint_auditstamp(),
                lastModified=self._mint_auditstamp(),
            )

        self._ensure_urn_type("dataset", [output_edge], "add_output_dataset")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path=f"/outputDatasetEdges/{quote(output_urn, safe='')}",
            value=output_edge,
        )
        return self

    def remove_output_dataset(self, output: Union[str, Urn]) -> "DataJobPatchBuilder":
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "remove",
            path=f"/outputDatasetEdges/{output}",
            value={},
        )
        return self

    def set_output_datasets(self, outputs: List[Edge]) -> "DataJobPatchBuilder":
        self._ensure_urn_type("dataset", outputs, "set_output_datasets")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "replace",
            path="/outputDatasetEdges",
            value=outputs,
        )
        return self

    def add_input_dataset_field(
        self, input: Union[Edge, Urn, str]
    ) -> "DataJobPatchBuilder":
        if isinstance(input, Edge):
            input_urn: str = input.destinationUrn
            input_edge: Edge = input
        elif isinstance(input, (Urn, str)):
            input_urn = str(input)
            if not input_urn.startswith("urn:li:schemaField:"):
                raise ValueError(f"Input {input} is not a Schema Field urn")

            input_edge = Edge(
                destinationUrn=input_urn,
                created=self._mint_auditstamp(),
                lastModified=self._mint_auditstamp(),
            )

        self._ensure_urn_type("schemaField", [input_edge], "add_input_dataset_field")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path=f"/inputDatasetFields/{quote(input_urn, safe='')}",
            value=input_edge,
        )
        return self

    def remove_input_dataset_field(
        self, input: Union[str, Urn]
    ) -> "DataJobPatchBuilder":
        input_urn = str(input)
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "remove",
            path=f"/inputDatasetFields/{quote(input_urn, safe='')}",
            value={},
        )
        return self

    def set_input_dataset_fields(self, inputs: List[Edge]) -> "DataJobPatchBuilder":
        self._ensure_urn_type("schemaField", inputs, "set_input_dataset_fields")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "replace",
            path="/inputDatasetFields",
            value=inputs,
        )
        return self

    def add_output_dataset_field(
        self, output: Union[Edge, Urn, str]
    ) -> "DataJobPatchBuilder":
        if isinstance(output, Edge):
            output_urn: str = output.destinationUrn
            output_edge: Edge = output
        elif isinstance(output, (Urn, str)):
            output_urn = str(output)
            if not output_urn.startswith("urn:li:schemaField:"):
                raise ValueError(f"Input {input} is not a Schema Field urn")

            output_edge = Edge(
                destinationUrn=output_urn,
                created=self._mint_auditstamp(),
                lastModified=self._mint_auditstamp(),
            )

        self._ensure_urn_type("schemaField", [output_edge], "add_output_dataset_field")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path=f"/outputDatasetFields/{quote(output_urn, safe='')}",
            value=output_edge,
        )
        return self

    def remove_output_dataset_field(
        self, output: Union[str, Urn]
    ) -> "DataJobPatchBuilder":
        output_urn = str(output)
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "remove",
            path=f"/outputDatasetFields/{quote(output_urn, safe='')}",
            value={},
        )
        return self

    def set_output_dataset_fields(self, outputs: List[Edge]) -> "DataJobPatchBuilder":
        self._ensure_urn_type("schemaField", outputs, "set_output_dataset_fields")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "replace",
            path="/outputDatasetFields",
            value=outputs,
        )
        return self

    def add_tag(self, tag: Tag) -> "DataJobPatchBuilder":
        self._add_patch(
            GlobalTags.ASPECT_NAME, "add", path=f"/tags/{tag.tag}", value=tag
        )
        return self

    def remove_tag(self, tag: Union[str, Urn]) -> "DataJobPatchBuilder":
        if isinstance(tag, str) and not tag.startswith("urn:li:tag:"):
            tag = TagUrn.create_from_id(tag)
        self._add_patch(GlobalTags.ASPECT_NAME, "remove", path=f"/tags/{tag}", value={})
        return self

    def add_term(self, term: Term) -> "DataJobPatchBuilder":
        self._add_patch(
            GlossaryTerms.ASPECT_NAME, "add", path=f"/terms/{term.urn}", value=term
        )
        return self

    def remove_term(self, term: Union[str, Urn]) -> "DataJobPatchBuilder":
        if isinstance(term, str) and not term.startswith("urn:li:glossaryTerm:"):
            term = "urn:li:glossaryTerm:" + term
        self._add_patch(
            GlossaryTerms.ASPECT_NAME, "remove", path=f"/terms/{term}", value={}
        )
        return self

    def set_custom_properties(
        self, custom_properties: Dict[str, str]
    ) -> "DataJobPatchBuilder":
        self._add_patch(
            DataJobInfo.ASPECT_NAME,
            "replace",
            path="/customProperties",
            value=custom_properties,
        )
        return self

    def add_custom_property(self, key: str, value: str) -> "DataJobPatchBuilder":
        self.custom_properties_patch_helper.add_property(key, value)
        return self

    def remove_custom_property(self, key: str) -> "DataJobPatchBuilder":
        self.custom_properties_patch_helper.remove_property(key)
        return self
