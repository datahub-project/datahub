from typing import List, Optional, Tuple, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal, PatchPath
from datahub.metadata.schema_classes import (
    DataJobInfoClass as DataJobInfo,
    DataJobInputOutputClass as DataJobInputOutput,
    EdgeClass as Edge,
    KafkaAuditHeaderClass,
    SystemMetadataClass,
)
from datahub.metadata.urns import SchemaFieldUrn, Urn
from datahub.specific.aspect_helpers.custom_properties import HasCustomPropertiesPatch
from datahub.specific.aspect_helpers.ownership import HasOwnershipPatch
from datahub.specific.aspect_helpers.tags import HasTagsPatch
from datahub.specific.aspect_helpers.terms import HasTermsPatch


class DataJobPatchBuilder(
    HasOwnershipPatch,
    HasCustomPropertiesPatch,
    HasTagsPatch,
    HasTermsPatch,
    MetadataPatchProposal,
):
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

    @classmethod
    def _custom_properties_location(cls) -> Tuple[str, PatchPath]:
        return DataJobInfo.ASPECT_NAME, ("customProperties",)

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
            it is converted to an Edge object and added without any audit stamps.
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
            )

        self._ensure_urn_type("dataJob", [input_edge], "add_input_datajob")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path=("inputDatajobEdges", input_urn),
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
            path=("inputDatajobEdges", input),
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
            path=("inputDatajobEdges",),
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
            it is converted to an Edge object and added without any audit stamps.
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
            )

        self._ensure_urn_type("dataset", [input_edge], "add_input_dataset")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path=("inputDatasetEdges", input_urn),
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
            path=("inputDatasetEdges", input),
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
            path=("inputDatasetEdges",),
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
            it is converted to an Edge object and added without any audit stamps.
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
            )

        self._ensure_urn_type("dataset", [output_edge], "add_output_dataset")
        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path=("outputDatasetEdges", output_urn),
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
            path=("outputDatasetEdges", output),
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
            path=("outputDatasetEdges",),
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
        assert SchemaFieldUrn.from_string(input_urn)

        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path=("inputDatasetFields", input_urn),
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
            path=("inputDatasetFields", input_urn),
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
            path=("inputDatasetFields",),
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
        assert SchemaFieldUrn.from_string(output_urn)

        self._add_patch(
            DataJobInputOutput.ASPECT_NAME,
            "add",
            path=("outputDatasetFields", output_urn),
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
            path=("outputDatasetFields", output_urn),
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
            path=("outputDatasetFields",),
            value=outputs,
        )
        return self
