import time
from typing import Dict, List, Optional, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    AccessLevelClass,
    AuditStampClass,
    ChangeAuditStampsClass,
    DashboardInfoClass as DashboardInfo,
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


class DashboardPatchBuilder(MetadataPatchProposal):
    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        """
        Initializes a DashboardPatchBuilder instance.

        Args:
            urn: The URN of the dashboard
            system_metadata: The system metadata of the dashboard (optional).
            audit_header: The Kafka audit header of the dashboard (optional).
        """
        super().__init__(
            urn, system_metadata=system_metadata, audit_header=audit_header
        )
        self.custom_properties_patch_helper = CustomPropertiesPatchHelper(
            self, DashboardInfo.ASPECT_NAME
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

    def add_owner(self, owner: Owner) -> "DashboardPatchBuilder":
        """
        Adds an owner to the DashboardPatchBuilder.

        Args:
            owner: The Owner object to add.

        Returns:
            The DashboardPatchBuilder instance.
        """
        self.ownership_patch_helper.add_owner(owner)
        return self

    def remove_owner(
        self, owner: str, owner_type: Optional[OwnershipTypeClass] = None
    ) -> "DashboardPatchBuilder":
        """
        Removes an owner from the DashboardPatchBuilder.

        Args:
            owner: The owner to remove.
            owner_type: The ownership type of the owner (optional).

        Returns:
            The DashboardPatchBuilder instance.

        Notes:
            `owner_type` is optional.
        """
        self.ownership_patch_helper.remove_owner(owner, owner_type)
        return self

    def set_owners(self, owners: List[Owner]) -> "DashboardPatchBuilder":
        """
        Sets the owners of the DashboardPatchBuilder.

        Args:
            owners: A list of Owner objects.

        Returns:
            The DashboardPatchBuilder instance.
        """
        self.ownership_patch_helper.set_owners(owners)
        return self

    def add_dataset_edge(
        self, dataset: Union[Edge, Urn, str]
    ) -> "DashboardPatchBuilder":
        """
        Adds an dataset to the DashboardPatchBuilder.

        Args:
            dataset: The dataset, which can be an Edge object, Urn object, or a string.

        Returns:
            The DashboardPatchBuilder instance.

        Raises:
            ValueError: If the dataset is not a Dataset urn.

        Notes:
            If `dataset` is an Edge object, it is used directly. If `dataset` is a Urn object or string,
            it is converted to an Edge object and added with default audit stamps.
        """
        if isinstance(dataset, Edge):
            dataset_urn: str = dataset.destinationUrn
            dataset_edge: Edge = dataset
        elif isinstance(dataset, (Urn, str)):
            dataset_urn = str(dataset)
            if not dataset_urn.startswith("urn:li:dataset:"):
                raise ValueError(f"Input {dataset} is not a Dataset urn")

            dataset_edge = Edge(
                destinationUrn=dataset_urn,
                created=self._mint_auditstamp(),
                lastModified=self._mint_auditstamp(),
            )

        self._ensure_urn_type("dataset", [dataset_edge], "add_dataset")
        self._add_patch(
            DashboardInfo.ASPECT_NAME,
            "add",
            path=f"/datasetEdges/{self.quote(dataset_urn)}",
            value=dataset_edge,
        )
        return self

    def remove_dataset_edge(self, dataset: Union[str, Urn]) -> "DashboardPatchBuilder":
        """
        Removes a dataset edge from the DashboardPatchBuilder.

        Args:
            dataset: The dataset to remove, specified as a string or Urn object.

        Returns:
            The DashboardPatchBuilder instance.
        """
        self._add_patch(
            DashboardInfo.ASPECT_NAME,
            "remove",
            path=f"/datasetEdges/{dataset}",
            value={},
        )
        return self

    def set_dataset_edges(self, datasets: List[Edge]) -> "DashboardPatchBuilder":
        """
        Sets the dataset edges for the DashboardPatchBuilder.

        Args:
            datasets: A list of Edge objects representing the dataset edges.

        Returns:
            The DashboardPatchBuilder instance.

        Raises:
            ValueError: If any of the input edges are not of type 'Datset'.

        Notes:
            This method replaces all existing datasets with the given inputs.
        """
        self._ensure_urn_type("dataset", datasets, "dataset edges")
        self._add_patch(
            DashboardInfo.ASPECT_NAME,
            "add",
            path="/datasetEdges",
            value=datasets,
        )
        return self

    def add_chart_edge(self, chart: Union[Edge, Urn, str]) -> "DashboardPatchBuilder":
        """
        Adds a chart edge to the DashboardPatchBuilder.

        Args:
            chart: The dataset, which can be an Edge object, Urn object, or a string.

        Returns:
            The DashboardPatchBuilder instance.

        Raises:
            ValueError: If the edge is not a Chart urn.

        Notes:
            If `chart` is an Edge object, it is used directly. If `chart` is a Urn object or string,
            it is converted to an Edge object and added with default audit stamps.
        """
        if isinstance(chart, Edge):
            chart_urn: str = chart.destinationUrn
            chart_edge: Edge = chart
        elif isinstance(chart, (Urn, str)):
            chart_urn = str(chart)
            if not chart_urn.startswith("urn:li:chart:"):
                raise ValueError(f"Input {chart} is not a Chart urn")

            chart_edge = Edge(
                destinationUrn=chart_urn,
                created=self._mint_auditstamp(),
                lastModified=self._mint_auditstamp(),
            )

        self._ensure_urn_type("dataset", [chart_edge], "add_chart_edge")
        self._add_patch(
            DashboardInfo.ASPECT_NAME,
            "add",
            path=f"/chartEdges/{self.quote(chart_urn)}",
            value=chart_edge,
        )
        return self

    def remove_chart_edge(self, chart: Union[str, Urn]) -> "DashboardPatchBuilder":
        """
        Removes an chart edge from the DashboardPatchBuilder.

        Args:
            chart: The chart to remove, specified as a string or Urn object.

        Returns:
            The DashboardPatchBuilder instance.
        """
        self._add_patch(
            DashboardInfo.ASPECT_NAME,
            "remove",
            path=f"/chartEdges/{chart}",
            value={},
        )
        return self

    def set_chart_edges(self, charts: List[Edge]) -> "DashboardPatchBuilder":
        """
        Sets the chart edges for the DashboardPatchBuilder.

        Args:
            charts: A list of Edge objects representing the chart edges.

        Returns:
            The DashboardPatchBuilder instance.

        Raises:
            ValueError: If any of the edges are not of type 'chart'.

        Notes:
            This method replaces all existing charts with the given charts.
        """
        self._ensure_urn_type("chart", charts, "set_charts")
        self._add_patch(
            DashboardInfo.ASPECT_NAME,
            "add",
            path="/chartEdges",
            value=charts,
        )
        return self

    def add_tag(self, tag: Tag) -> "DashboardPatchBuilder":
        """
        Adds a tag to the DashboardPatchBuilder.

        Args:
            tag: The Tag object representing the tag to be added.

        Returns:
            The DashboardPatchBuilder instance.
        """
        self._add_patch(
            GlobalTags.ASPECT_NAME, "add", path=f"/tags/{tag.tag}", value=tag
        )
        return self

    def remove_tag(self, tag: Union[str, Urn]) -> "DashboardPatchBuilder":
        """
        Removes a tag from the DashboardPatchBuilder.

        Args:
            tag: The tag to remove, specified as a string or Urn object.

        Returns:
            The DashboardPatchBuilder instance.
        """
        if isinstance(tag, str) and not tag.startswith("urn:li:tag:"):
            tag = TagUrn.create_from_id(tag)
        self._add_patch(GlobalTags.ASPECT_NAME, "remove", path=f"/tags/{tag}", value={})
        return self

    def add_term(self, term: Term) -> "DashboardPatchBuilder":
        """
        Adds a glossary term to the DashboardPatchBuilder.

        Args:
            term: The Term object representing the glossary term to be added.

        Returns:
            The DashboardPatchBuilder instance.
        """
        self._add_patch(
            GlossaryTerms.ASPECT_NAME, "add", path=f"/terms/{term.urn}", value=term
        )
        return self

    def remove_term(self, term: Union[str, Urn]) -> "DashboardPatchBuilder":
        """
        Removes a glossary term from the DashboardPatchBuilder.

        Args:
            term: The term to remove, specified as a string or Urn object.

        Returns:
            The DashboardPatchBuilder instance.
        """
        if isinstance(term, str) and not term.startswith("urn:li:glossaryTerm:"):
            term = "urn:li:glossaryTerm:" + term
        self._add_patch(
            GlossaryTerms.ASPECT_NAME, "remove", path=f"/terms/{term}", value={}
        )
        return self

    def set_custom_properties(
        self, custom_properties: Dict[str, str]
    ) -> "DashboardPatchBuilder":
        """
        Sets the custom properties for the DashboardPatchBuilder.

        Args:
            custom_properties: A dictionary containing the custom properties to be set.

        Returns:
            The DashboardPatchBuilder instance.

        Notes:
            This method replaces all existing custom properties with the given dictionary.
        """
        self._add_patch(
            DashboardInfo.ASPECT_NAME,
            "add",
            path="/customProperties",
            value=custom_properties,
        )
        return self

    def add_custom_property(self, key: str, value: str) -> "DashboardPatchBuilder":
        """
        Adds a custom property to the DashboardPatchBuilder.

        Args:
            key: The key of the custom property.
            value: The value of the custom property.

        Returns:
            The DashboardPatchBuilder instance.
        """
        self.custom_properties_patch_helper.add_property(key, value)
        return self

    def remove_custom_property(self, key: str) -> "DashboardPatchBuilder":
        """
        Removes a custom property from the DashboardPatchBuilder.

        Args:
            key: The key of the custom property to remove.

        Returns:
            The DashboardPatchBuilder instance.
        """
        self.custom_properties_patch_helper.remove_property(key)
        return self

    def set_title(self, title: str) -> "DashboardPatchBuilder":
        assert title, "DashboardInfo title should not be None"
        self._add_patch(
            DashboardInfo.ASPECT_NAME,
            "add",
            path="/title",
            value=title,
        )

        return self

    def set_description(self, description: str) -> "DashboardPatchBuilder":
        assert description, "DashboardInfo description should not be None"
        self._add_patch(
            DashboardInfo.ASPECT_NAME,
            "add",
            path="/description",
            value=description,
        )

        return self

    def add_custom_properties(
        self, custom_properties: Optional[Dict[str, str]] = None
    ) -> "DashboardPatchBuilder":

        if custom_properties:
            for key, value in custom_properties.items():
                self.custom_properties_patch_helper.add_property(key, value)

        return self

    def set_external_url(self, external_url: Optional[str]) -> "DashboardPatchBuilder":
        if external_url:
            self._add_patch(
                DashboardInfo.ASPECT_NAME,
                "add",
                path="/externalUrl",
                value=external_url,
            )
        return self

    def add_charts(self, chart_urns: Optional[List[str]]) -> "DashboardPatchBuilder":
        if chart_urns:
            for urn in chart_urns:
                self._add_patch(
                    aspect_name=DashboardInfo.ASPECT_NAME,
                    op="add",
                    path=f"/charts/{urn}",
                    value=urn,
                )

        return self

    def add_datasets(
        self, dataset_urns: Optional[List[str]]
    ) -> "DashboardPatchBuilder":
        if dataset_urns:
            for urn in dataset_urns:
                self._add_patch(
                    aspect_name=DashboardInfo.ASPECT_NAME,
                    op="add",
                    path=f"/datasets/{urn}",
                    value=urn,
                )

        return self

    def set_dashboard_url(
        self, dashboard_url: Optional[str]
    ) -> "DashboardPatchBuilder":
        if dashboard_url:
            self._add_patch(
                DashboardInfo.ASPECT_NAME,
                "add",
                path="/dashboardUrl",
                value=dashboard_url,
            )

        return self

    def set_access(
        self, access: Union[None, Union[str, "AccessLevelClass"]] = None
    ) -> "DashboardPatchBuilder":
        if access:
            self._add_patch(
                DashboardInfo.ASPECT_NAME,
                "add",
                path="/access",
                value=access,
            )

        return self

    def set_last_refreshed(
        self, last_refreshed: Optional[int]
    ) -> "DashboardPatchBuilder":
        if last_refreshed:
            self._add_patch(
                DashboardInfo.ASPECT_NAME,
                "add",
                path="/lastRefreshed",
                value=last_refreshed,
            )

        return self

    def set_last_modified(
        self, last_modified: "ChangeAuditStampsClass"
    ) -> "DashboardPatchBuilder":
        if last_modified:
            self._add_patch(
                DashboardInfo.ASPECT_NAME,
                "add",
                path="/lastModified",
                value=last_modified,
            )

        return self
