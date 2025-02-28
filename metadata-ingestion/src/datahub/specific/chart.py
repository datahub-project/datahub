from typing import List, Optional, Tuple, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal, PatchPath
from datahub.metadata.schema_classes import (
    AccessLevelClass,
    ChangeAuditStampsClass,
    ChartInfoClass as ChartInfo,
    ChartTypeClass,
    EdgeClass as Edge,
    KafkaAuditHeaderClass,
    SystemMetadataClass,
)
from datahub.specific.aspect_helpers.custom_properties import HasCustomPropertiesPatch
from datahub.specific.aspect_helpers.ownership import HasOwnershipPatch
from datahub.specific.aspect_helpers.tags import HasTagsPatch
from datahub.specific.aspect_helpers.terms import HasTermsPatch
from datahub.utilities.urns.urn import Urn


class ChartPatchBuilder(
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
        Initializes a ChartPatchBuilder instance.

        Args:
            urn: The URN of the chart
            system_metadata: The system metadata of the chart (optional).
            audit_header: The Kafka audit header of the chart (optional).
        """
        super().__init__(
            urn, system_metadata=system_metadata, audit_header=audit_header
        )

    @classmethod
    def _custom_properties_location(cls) -> Tuple[str, PatchPath]:
        return ChartInfo.ASPECT_NAME, ("customProperties",)

    def add_input_edge(self, input: Union[Edge, Urn, str]) -> "ChartPatchBuilder":
        """
        Adds an input to the ChartPatchBuilder.

        Args:
            input: The input, which can be an Edge object, Urn object, or a string.

        Returns:
            The ChartPatchBuilder instance.

        Notes:
            If `input` is an Edge object, it is used directly. If `input` is a Urn object or string,
            it is converted to an Edge object and added with default audit stamps.
        """
        if isinstance(input, Edge):
            input_urn: str = input.destinationUrn
            input_edge: Edge = input
        elif isinstance(input, (Urn, str)):
            input_urn = str(input)

            input_edge = Edge(
                destinationUrn=input_urn,
                created=self._mint_auditstamp(),
                lastModified=self._mint_auditstamp(),
            )

        self._ensure_urn_type("dataset", [input_edge], "add_dataset")
        self._add_patch(
            ChartInfo.ASPECT_NAME,
            "add",
            path=("inputEdges", input_urn),
            value=input_urn,
        )
        return self

    def remove_input_edge(self, input: Union[str, Urn]) -> "ChartPatchBuilder":
        """
        Removes an input from the ChartPatchBuilder.

        Args:
            input: The input to remove, specified as a string or Urn object.

        Returns:
            The ChartPatchBuilder instance.
        """
        self._add_patch(
            ChartInfo.ASPECT_NAME,
            "remove",
            path=("inputEdges", str(input)),
            value={},
        )
        return self

    def set_input_edges(self, inputs: List[Edge]) -> "ChartPatchBuilder":
        """
        Sets the input edges for the ChartPatchBuilder.

        Args:
            inputs: A list of Edge objects representing the input edges.

        Returns:
            The ChartPatchBuilder instance.

        Notes:
            This method replaces all existing inputs with the given inputs.
        """
        self._add_patch(
            ChartInfo.ASPECT_NAME,
            "add",
            path=("inputEdges",),
            value=inputs,
        )
        return self

    def set_title(self, title: str) -> "ChartPatchBuilder":
        assert title, "ChartInfo title should not be None"
        self._add_patch(
            ChartInfo.ASPECT_NAME,
            "add",
            path=("title",),
            value=title,
        )

        return self

    def set_description(self, description: str) -> "ChartPatchBuilder":
        assert description, "DashboardInfo description should not be None"
        self._add_patch(
            ChartInfo.ASPECT_NAME,
            "add",
            path=("description",),
            value=description,
        )

        return self

    def set_last_refreshed(self, last_refreshed: Optional[int]) -> "ChartPatchBuilder":
        if last_refreshed:
            self._add_patch(
                ChartInfo.ASPECT_NAME,
                "add",
                path=("lastRefreshed",),
                value=last_refreshed,
            )

        return self

    def set_last_modified(
        self, last_modified: "ChangeAuditStampsClass"
    ) -> "ChartPatchBuilder":
        if last_modified:
            self._add_patch(
                ChartInfo.ASPECT_NAME,
                "add",
                path=("lastModified",),
                value=last_modified,
            )

        return self

    def set_external_url(self, external_url: Optional[str]) -> "ChartPatchBuilder":
        if external_url:
            self._add_patch(
                ChartInfo.ASPECT_NAME,
                "add",
                path=("externalUrl",),
                value=external_url,
            )
        return self

    def set_chart_url(self, dashboard_url: Optional[str]) -> "ChartPatchBuilder":
        if dashboard_url:
            self._add_patch(
                ChartInfo.ASPECT_NAME,
                "add",
                path=("chartUrl",),
                value=dashboard_url,
            )

        return self

    def set_type(
        self, type: Union[None, Union[str, "ChartTypeClass"]] = None
    ) -> "ChartPatchBuilder":
        if type:
            self._add_patch(
                ChartInfo.ASPECT_NAME,
                "add",
                path=("type",),
                value=type,
            )

        return self

    def set_access(
        self, access: Union[None, Union[str, "AccessLevelClass"]] = None
    ) -> "ChartPatchBuilder":
        if access:
            self._add_patch(
                ChartInfo.ASPECT_NAME,
                "add",
                path=("access",),
                value=access,
            )

        return self

    def add_inputs(self, input_urns: Optional[List[str]]) -> "ChartPatchBuilder":
        if input_urns:
            for urn in input_urns:
                self._add_patch(
                    aspect_name=ChartInfo.ASPECT_NAME,
                    op="add",
                    path=("inputs", urn),
                    value=urn,
                )

        return self
