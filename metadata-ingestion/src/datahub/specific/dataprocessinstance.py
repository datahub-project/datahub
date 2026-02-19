# ABOUTME: Patch builder for DataProcessInstance entity.
# ABOUTME: Supports adding/removing inputs, outputs, and their edges.
from typing import List, Optional, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    DataProcessInstanceInputClass,
    DataProcessInstanceOutputClass,
    EdgeClass as Edge,
    KafkaAuditHeaderClass,
    SystemMetadataClass,
)
from datahub.metadata.urns import Urn


class DataProcessInstancePatchBuilder(
    MetadataPatchProposal,
):
    """
    Patch builder for DataProcessInstance entities.

    Supports patching:
    - inputs (URN array in dataProcessInstanceInput aspect)
    - inputEdges (Edge array in dataProcessInstanceInput aspect)
    - outputs (URN array in dataProcessInstanceOutput aspect)
    - outputEdges (Edge array in dataProcessInstanceOutput aspect)

    Example usage:
        builder = DataProcessInstancePatchBuilder("urn:li:dataProcessInstance:my-instance")
        builder.add_input_edge("urn:li:dataset:(urn:li:dataPlatform:hive,input_table,PROD)")
        builder.add_output_edge("urn:li:dataset:(urn:li:dataPlatform:hive,output_table,PROD)")

        for mcp in builder.build():
            emitter.emit(mcp)
    """

    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        """
        Initializes a DataProcessInstancePatchBuilder instance.

        Args:
            urn: The URN of the data process instance.
            system_metadata: The system metadata (optional).
            audit_header: The Kafka audit header (optional).
        """
        super().__init__(
            urn, system_metadata=system_metadata, audit_header=audit_header
        )

    # ==================== Input Operations ====================

    def add_input(
        self, input_urn: Union[Urn, str]
    ) -> "DataProcessInstancePatchBuilder":
        """
        Adds an input URN to the inputs array.

        Args:
            input_urn: The input URN (dataset, mlModel, etc.)

        Returns:
            The DataProcessInstancePatchBuilder instance.
        """
        urn_str = str(input_urn)
        self._add_patch(
            DataProcessInstanceInputClass.ASPECT_NAME,
            "add",
            path=("inputs", urn_str),
            value=urn_str,
        )
        return self

    def remove_input(
        self, input_urn: Union[Urn, str]
    ) -> "DataProcessInstancePatchBuilder":
        """
        Removes an input URN from the inputs array.

        Args:
            input_urn: The input URN to remove.

        Returns:
            The DataProcessInstancePatchBuilder instance.
        """
        urn_str = str(input_urn)
        self._add_patch(
            DataProcessInstanceInputClass.ASPECT_NAME,
            "remove",
            path=("inputs", urn_str),
            value={},
        )
        return self

    def set_inputs(self, inputs: List[str]) -> "DataProcessInstancePatchBuilder":
        """
        Sets the inputs array, replacing any existing inputs.

        Args:
            inputs: A list of input URN strings.

        Returns:
            The DataProcessInstancePatchBuilder instance.
        """
        self._add_patch(
            DataProcessInstanceInputClass.ASPECT_NAME,
            "add",
            path=("inputs",),
            value=inputs,
        )
        return self

    # ==================== Input Edge Operations ====================

    def add_input_edge(
        self, input_edge: Union[Edge, Urn, str]
    ) -> "DataProcessInstancePatchBuilder":
        """
        Adds an input edge to the inputEdges array.

        Args:
            input_edge: The input edge, which can be an Edge object, Urn, or string.

        Returns:
            The DataProcessInstancePatchBuilder instance.
        """
        if isinstance(input_edge, Edge):
            input_urn: str = input_edge.destinationUrn
            edge: Edge = input_edge
        elif isinstance(input_edge, (Urn, str)):
            input_urn = str(input_edge)
            edge = Edge(destinationUrn=input_urn)

        self._add_patch(
            DataProcessInstanceInputClass.ASPECT_NAME,
            "add",
            path=("inputEdges", input_urn),
            value=edge,
        )
        return self

    def remove_input_edge(
        self, input_urn: Union[Urn, str]
    ) -> "DataProcessInstancePatchBuilder":
        """
        Removes an input edge from the inputEdges array.

        Args:
            input_urn: The destination URN of the edge to remove.

        Returns:
            The DataProcessInstancePatchBuilder instance.
        """
        urn_str = str(input_urn)
        self._add_patch(
            DataProcessInstanceInputClass.ASPECT_NAME,
            "remove",
            path=("inputEdges", urn_str),
            value={},
        )
        return self

    def set_input_edges(self, input_edges: List[Edge]) -> "DataProcessInstancePatchBuilder":
        """
        Sets the inputEdges array, replacing any existing input edges.

        Args:
            inputs: A list of Edge objects representing the input edges.

        Returns:
            The DataProcessInstancePatchBuilder instance.
        """
        self._add_patch(
            DataProcessInstanceInputClass.ASPECT_NAME,
            "add",
            path=("inputEdges",),
            value=input_edges,
        )
        return self

    # ==================== Output Operations ====================

    def add_output(
        self, output_urn: Union[Urn, str]
    ) -> "DataProcessInstancePatchBuilder":
        """
        Adds an output URN to the outputs array.

        Args:
            output_urn: The output URN (dataset, mlModel, etc.)

        Returns:
            The DataProcessInstancePatchBuilder instance.
        """
        urn_str = str(output_urn)
        self._add_patch(
            DataProcessInstanceOutputClass.ASPECT_NAME,
            "add",
            path=("outputs", urn_str),
            value=urn_str,
        )
        return self

    def remove_output(
        self, output_urn: Union[Urn, str]
    ) -> "DataProcessInstancePatchBuilder":
        """
        Removes an output URN from the outputs array.

        Args:
            output_urn: The output URN to remove.

        Returns:
            The DataProcessInstancePatchBuilder instance.
        """
        urn_str = str(output_urn)
        self._add_patch(
            DataProcessInstanceOutputClass.ASPECT_NAME,
            "remove",
            path=("outputs", urn_str),
            value={},
        )
        return self

    def set_outputs(self, outputs: List[str]) -> "DataProcessInstancePatchBuilder":
        """
        Sets the outputs array, replacing any existing outputs.

        Args:
            outputs: A list of output URN strings.

        Returns:
            The DataProcessInstancePatchBuilder instance.
        """
        self._add_patch(
            DataProcessInstanceOutputClass.ASPECT_NAME,
            "add",
            path=("outputs",),
            value=outputs,
        )
        return self

    # ==================== Output Edge Operations ====================

    def add_output_edge(
        self, output_edge: Union[Edge, Urn, str]
    ) -> "DataProcessInstancePatchBuilder":
        """
        Adds an output edge to the outputEdges array.

        Args:
            output_edge: The output edge, which can be an Edge object, Urn, or string.

        Returns:
            The DataProcessInstancePatchBuilder instance.
        """
        if isinstance(output_edge, Edge):
            output_urn: str = output_edge.destinationUrn
            edge: Edge = output_edge
        elif isinstance(output_edge, (Urn, str)):
            output_urn = str(output_edge)
            edge = Edge(destinationUrn=output_urn)

        self._add_patch(
            DataProcessInstanceOutputClass.ASPECT_NAME,
            "add",
            path=("outputEdges", output_urn),
            value=edge,
        )
        return self

    def remove_output_edge(
        self, output_urn: Union[Urn, str]
    ) -> "DataProcessInstancePatchBuilder":
        """
        Removes an output edge from the outputEdges array.

        Args:
            output_urn: The destination URN of the edge to remove.

        Returns:
            The DataProcessInstancePatchBuilder instance.
        """
        urn_str = str(output_urn)
        self._add_patch(
            DataProcessInstanceOutputClass.ASPECT_NAME,
            "remove",
            path=("outputEdges", urn_str),
            value={},
        )
        return self

    def set_output_edges(
        self, outputs: List[Edge]
    ) -> "DataProcessInstancePatchBuilder":
        """
        Sets the outputEdges array, replacing any existing output edges.

        Args:
            outputs: A list of Edge objects representing the output edges.

        Returns:
            The DataProcessInstancePatchBuilder instance.
        """
        self._add_patch(
            DataProcessInstanceOutputClass.ASPECT_NAME,
            "add",
            path=("outputEdges",),
            value=outputs,
        )
        return self
