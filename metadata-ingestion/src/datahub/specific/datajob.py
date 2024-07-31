import time
from typing import Dict, List, Optional, Union

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


class DataJobPatchBuilder(MetadataPatchProposal):
    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        """
        Initializes a DataJobPatchBuilder instance.

        Args:
            urn: The URN of the data job.
            system_metadata: The system metadata of the data job (optional).
            audit_header: The Kafka audit header of the data job (optional).
        """
        super().__init__(
            urn, system_metadata=system_metadata, audit_header=audit_header
        )
        self.custom_properties_patch_helper = CustomPropertiesPatchHelper(
            self, DataJobInfo.ASPECT_NAME
        )
        self.ownership_patch_helper = OwnershipPatchHelper(self)

    def _mint_auditstamp(self, message: Optional[str] = None) -> AuditStampClass:
        """
        Creates an AuditStampClass instance with the current timestamp and other default values.

        Args:
            message: The message associated with the audit stamp (optional).

        Returns:
            An instance of AuditStampClass.
        """
        return AuditStampClass(
            time=int(time.time() * 1000.0),
            actor="urn:li:corpuser:datahub",
            message=message,
        )

    def _ensure_urn_type(
        self, entity_type: str, edges: List[Edge], context: str
    ) -> None:
        """
        Ensures that the destination URNs in the given edges have the specified entity type.

        Args:
            entity_type: The entity type to check against.
            edges: A list of Edge objects.
            context: The context or description of the operation.

        Raises:
            ValueError: If any of the destination URNs is not of the specified entity type.
        """
        for e in edges:
            urn = Urn.create_from_string(e.destinationUrn)
            if not urn.get_type() == entity_type:
                raise ValueError(
                    f"{context}: {e.destinationUrn} is not of type {entity_type}"
                )

    def add_owner(self, owner: Owner) -> "DataJobPatchBuilder":
        """
        Adds an owner to the DataJobPatchBuilder.

        Args:
            owner: The Owner object to add.

        Returns:
            The DataJobPatchBuilder instance.
        """
        self.ownership_patch_helper.add_owner(owner)
        return self

    def remove_owner(
        self, owner: str, owner_type: Optional[OwnershipTypeClass] = None
    ) -> "DataJobPatchBuilder":
        """
        Removes an owner from the DataJobPatchBuilder.

        Args:
            owner: The owner to remove.
            owner_type: The ownership type of the owner (optional).

        Returns:
            The DataJobPatchBuilder instance.

        Notes:
            `owner_type` is optional.
        """
        self.ownership_patch_helper.remove_owner(owner, owner_type)
        return self

    def set_owners(self, owners: List[Owner]) -> "DataJobPatchBuilder":
        """
        Sets the owners of the DataJobPatchBuilder.

        Args:
            owners: A list of Owner objects.

        Returns:
            The DataJobPatchBuilder instance.
        """
        self.ownership_patch_helper.set_owners(owners)
        return self

    def add_input_datajob(self, input: Union[Edge, Urn, str]) -> "DataJobPatchBuilder":
        """
        Adds an input data job to the DataJobPatchBuilder.

        Args:
            input: The input data job, which can be an Edge object, Urn object, or a string.

        Returns:
            The DataJobPatchBuilder instance.

        Raises:
            ValueError: If the input is not a DataJob urn.

        Notes:
            If `input` is an Edge object, it is used directly. If `input` is a Urn object or string,
            it is converted to an Edge object and added with default audit stamps.
        """
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
            path=f"/inputDatajobEdges/{self.quote(input_urn)}",
            value=input_edge,
        )
        return self

    def remove_input_datajob(self, input: Union[str, Urn]) -> "DataJobPatchBuilder":
        """
        Removes an input data job from the DataJobPatchBuilder.

        Args:
            input: The input data job to remove, specified as a string or Urn object.

        Returns:
            The DataJobPatchBuilder instance.
        """
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "remove",
            path=f"/inputDatajobEdges/{input}",
            value={},
        )
        return self

    def set_input_datajobs(self, inputs: List[Edge]) -> "DataJobPatchBuilder":
        """
        Sets the input data jobs for the DataJobPatchBuilder.

        Args:
            inputs: A list of Edge objects representing the input data jobs.

        Returns:
            The DataJobPatchBuilder instance.

        Raises:
            ValueError: If any of the input edges are not of type 'dataJob'.

        Notes:
            This method replaces all existing input data jobs with the given inputs.
        """
        self._ensure_urn_type("dataJob", inputs, "input datajobs")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path="/inputDatajobEdges",
            value=inputs,
        )
        return self

    def add_input_dataset(self, input: Union[Edge, Urn, str]) -> "DataJobPatchBuilder":
        """
        Adds an input dataset to the DataJobPatchBuilder.

        Args:
            input: The input dataset, which can be an Edge object, Urn object, or a string.

        Returns:
            The DataJobPatchBuilder instance.

        Raises:
            ValueError: If the input is not a Dataset urn.

        Notes:
            If `input` is an Edge object, it is used directly. If `input` is a Urn object or string,
            it is converted to an Edge object and added with default audit stamps.
        """
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
            path=f"/inputDatasetEdges/{self.quote(input_urn)}",
            value=input_edge,
        )
        return self

    def remove_input_dataset(self, input: Union[str, Urn]) -> "DataJobPatchBuilder":
        """
        Removes an input dataset from the DataJobPatchBuilder.

        Args:
            input: The input dataset to remove, specified as a string or Urn object.

        Returns:
            The DataJobPatchBuilder instance.
        """
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "remove",
            path=f"/inputDatasetEdges/{self.quote(str(input))}",
            value={},
        )
        return self

    def set_input_datasets(self, inputs: List[Edge]) -> "DataJobPatchBuilder":
        """
        Sets the input datasets for the DataJobPatchBuilder.

        Args:
            inputs: A list of Edge objects representing the input datasets.

        Returns:
            The DataJobPatchBuilder instance.

        Raises:
            ValueError: If any of the input edges are not of type 'dataset'.

        Notes:
            This method replaces all existing input datasets with the given inputs.
        """
        self._ensure_urn_type("dataset", inputs, "set_input_datasets")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path="/inputDatasetEdges",
            value=inputs,
        )
        return self

    def add_output_dataset(
        self, output: Union[Edge, Urn, str]
    ) -> "DataJobPatchBuilder":
        """
        Adds an output dataset to the DataJobPatchBuilder.

        Args:
            output: The output dataset, which can be an Edge object, Urn object, or a string.

        Returns:
            The DataJobPatchBuilder instance.

        Raises:
            ValueError: If the output is not a Dataset urn.

        Notes:
            If `output` is an Edge object, it is used directly. If `output` is a Urn object or string,
            it is converted to an Edge object and added with default audit stamps.
        """
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
            path=f"/outputDatasetEdges/{self.quote(str(input))}",
            value=output_edge,
        )
        return self

    def remove_output_dataset(self, output: Union[str, Urn]) -> "DataJobPatchBuilder":
        """
        Removes an output dataset from the DataJobPatchBuilder.

        Args:
            output: The output dataset to remove, specified as a string or Urn object.

        Returns:
            The DataJobPatchBuilder instance.
        """
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "remove",
            path=f"/outputDatasetEdges/{self.quote(str(output))}",
            value={},
        )
        return self

    def set_output_datasets(self, outputs: List[Edge]) -> "DataJobPatchBuilder":
        """
        Sets the output datasets for the DataJobPatchBuilder.

        Args:
            outputs: A list of Edge objects representing the output datasets.

        Returns:
            The DataJobPatchBuilder instance.

        Raises:
            ValueError: If any of the output edges are not of type 'dataset'.

        Notes:
            This method replaces all existing output datasets with the given outputs.
        """
        self._ensure_urn_type("dataset", outputs, "set_output_datasets")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path="/outputDatasetEdges",
            value=outputs,
        )
        return self

    def add_input_dataset_field(self, input: Union[Urn, str]) -> "DataJobPatchBuilder":
        """
        Adds an input dataset field to the DataJobPatchBuilder.

        Args:
            input: The input dataset field, which can be an Urn object, or a string.

        Returns:
            The DataJobPatchBuilder instance.

        Raises:
            ValueError: If the input is not a Schema Field urn.
        """
        input_urn = str(input)
        urn = Urn.create_from_string(input_urn)
        if not urn.get_type() == "schemaField":
            raise ValueError(f"Input {input} is not a Schema Field urn")

        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path=f"/inputDatasetFields/{self.quote(input_urn)}",
            value={},
        )
        return self

    def remove_input_dataset_field(
        self, input: Union[str, Urn]
    ) -> "DataJobPatchBuilder":
        """
        Removes an input dataset field from the DataJobPatchBuilder.

        Args:
            input: The input dataset field to remove, specified as a string or Urn object.

        Returns:
            The DataJobPatchBuilder instance.
        """
        input_urn = str(input)
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "remove",
            path=f"/inputDatasetFields/{self.quote(input_urn)}",
            value={},
        )
        return self

    def set_input_dataset_fields(self, inputs: List[Edge]) -> "DataJobPatchBuilder":
        """
        Sets the input dataset fields for the DataJobPatchBuilder.

        Args:
            inputs: A list of Edge objects representing the input dataset fields.

        Returns:
            The DataJobPatchBuilder instance.

        Raises:
            ValueError: If any of the input edges are not of type 'schemaField'.

        Notes:
            This method replaces all existing input dataset fields with the given inputs.
        """
        self._ensure_urn_type("schemaField", inputs, "set_input_dataset_fields")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path="/inputDatasetFields",
            value=inputs,
        )
        return self

    def add_output_dataset_field(
        self, output: Union[Urn, str]
    ) -> "DataJobPatchBuilder":
        """
        Adds an output dataset field to the DataJobPatchBuilder.

        Args:
            output: The output dataset field, which can be an Urn object, or a string.

        Returns:
            The DataJobPatchBuilder instance.

        Raises:
            ValueError: If the output is not a Schema Field urn.
        """
        output_urn = str(output)
        urn = Urn.create_from_string(output_urn)
        if not urn.get_type() == "schemaField":
            raise ValueError(f"Input {output} is not a Schema Field urn")

        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path=f"/outputDatasetFields/{self.quote(output_urn)}",
            value={},
        )
        return self

    def remove_output_dataset_field(
        self, output: Union[str, Urn]
    ) -> "DataJobPatchBuilder":
        """
        Removes an output dataset field from the DataJobPatchBuilder.

        Args:
            output: The output dataset field to remove, specified as a string or Urn object.

        Returns:
            The DataJobPatchBuilder instance.
        """
        output_urn = str(output)
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "remove",
            path=f"/outputDatasetFields/{self.quote(output_urn)}",
            value={},
        )
        return self

    def set_output_dataset_fields(self, outputs: List[Edge]) -> "DataJobPatchBuilder":
        """
        Sets the output dataset fields for the DataJobPatchBuilder.

        Args:
            outputs: A list of Edge objects representing the output dataset fields.

        Returns:
            The DataJobPatchBuilder instance.

        Raises:
            ValueError: If any of the output edges are not of type 'schemaField'.

        Notes:
            This method replaces all existing output dataset fields with the given outputs.
        """
        self._ensure_urn_type("schemaField", outputs, "set_output_dataset_fields")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path="/outputDatasetFields",
            value=outputs,
        )
        return self

    def add_tag(self, tag: Tag) -> "DataJobPatchBuilder":
        """
        Adds a tag to the DataJobPatchBuilder.

        Args:
            tag: The Tag object representing the tag to be added.

        Returns:
            The DataJobPatchBuilder instance.
        """
        self._add_patch(
            GlobalTags.ASPECT_NAME, "add", path=f"/tags/{tag.tag}", value=tag
        )
        return self

    def remove_tag(self, tag: Union[str, Urn]) -> "DataJobPatchBuilder":
        """
        Removes a tag from the DataJobPatchBuilder.

        Args:
            tag: The tag to remove, specified as a string or Urn object.

        Returns:
            The DataJobPatchBuilder instance.
        """
        if isinstance(tag, str) and not tag.startswith("urn:li:tag:"):
            tag = TagUrn.create_from_id(tag)
        self._add_patch(GlobalTags.ASPECT_NAME, "remove", path=f"/tags/{tag}", value={})
        return self

    def add_term(self, term: Term) -> "DataJobPatchBuilder":
        """
        Adds a glossary term to the DataJobPatchBuilder.

        Args:
            term: The Term object representing the glossary term to be added.

        Returns:
            The DataJobPatchBuilder instance.
        """
        self._add_patch(
            GlossaryTerms.ASPECT_NAME, "add", path=f"/terms/{term.urn}", value=term
        )
        return self

    def remove_term(self, term: Union[str, Urn]) -> "DataJobPatchBuilder":
        """
        Removes a glossary term from the DataJobPatchBuilder.

        Args:
            term: The term to remove, specified as a string or Urn object.

        Returns:
            The DataJobPatchBuilder instance.
        """
        if isinstance(term, str) and not term.startswith("urn:li:glossaryTerm:"):
            term = "urn:li:glossaryTerm:" + term
        self._add_patch(
            GlossaryTerms.ASPECT_NAME, "remove", path=f"/terms/{term}", value={}
        )
        return self

    def set_custom_properties(
        self, custom_properties: Dict[str, str]
    ) -> "DataJobPatchBuilder":
        """
        Sets the custom properties for the DataJobPatchBuilder.

        Args:
            custom_properties: A dictionary containing the custom properties to be set.

        Returns:
            The DataJobPatchBuilder instance.

        Notes:
            This method replaces all existing custom properties with the given dictionary.
        """
        self._add_patch(
            DataJobInfo.ASPECT_NAME,
            "add",
            path="/customProperties",
            value=custom_properties,
        )
        return self

    def add_custom_property(self, key: str, value: str) -> "DataJobPatchBuilder":
        """
        Adds a custom property to the DataJobPatchBuilder.

        Args:
            key: The key of the custom property.
            value: The value of the custom property.

        Returns:
            The DataJobPatchBuilder instance.
        """
        self.custom_properties_patch_helper.add_property(key, value)
        return self

    def remove_custom_property(self, key: str) -> "DataJobPatchBuilder":
        """
        Removes a custom property from the DataJobPatchBuilder.

        Args:
            key: The key of the custom property to remove.

        Returns:
            The DataJobPatchBuilder instance.
        """
        self.custom_properties_patch_helper.remove_property(key)
        return self
