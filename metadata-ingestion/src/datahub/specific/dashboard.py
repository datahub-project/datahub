from typing import List, Optional, Tuple, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal, PatchPath
from datahub.metadata.schema_classes import (
    AccessLevelClass,
    ChangeAuditStampsClass,
    DashboardInfoClass as DashboardInfo,
    EdgeClass as Edge,
    KafkaAuditHeaderClass,
    SystemMetadataClass,
)
from datahub.specific.aspect_helpers.custom_properties import HasCustomPropertiesPatch
from datahub.specific.aspect_helpers.ownership import HasOwnershipPatch
from datahub.specific.aspect_helpers.tags import HasTagsPatch
from datahub.specific.aspect_helpers.terms import HasTermsPatch
from datahub.utilities.urns.urn import Urn


class DashboardPatchBuilder(
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
        Initializes a DashboardPatchBuilder instance.

        Args:
            urn: The URN of the dashboard
            system_metadata: The system metadata of the dashboard (optional).
            audit_header: The Kafka audit header of the dashboard (optional).
        """
        super().__init__(
            urn, system_metadata=system_metadata, audit_header=audit_header
        )

    @classmethod
    def _custom_properties_location(cls) -> Tuple[str, PatchPath]:
        return DashboardInfo.ASPECT_NAME, ("customProperties",)

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
            path=("datasetEdges", dataset_urn),
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
            path=("datasetEdges", dataset),
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
            path=("datasetEdges",),
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

        self._ensure_urn_type("chart", [chart_edge], "add_chart_edge")
        self._add_patch(
            DashboardInfo.ASPECT_NAME,
            "add",
            path=("chartEdges", chart_urn),
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
            path=("chartEdges", chart),
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
            path=("chartEdges",),
            value=charts,
        )
        return self

    def set_title(self, title: str) -> "DashboardPatchBuilder":
        assert title, "DashboardInfo title should not be None"
        self._add_patch(
            DashboardInfo.ASPECT_NAME,
            "add",
            path=("title",),
            value=title,
        )

        return self

    def set_description(self, description: str) -> "DashboardPatchBuilder":
        assert description, "DashboardInfo description should not be None"
        self._add_patch(
            DashboardInfo.ASPECT_NAME,
            "add",
            path=("description",),
            value=description,
        )

        return self

    def set_external_url(self, external_url: Optional[str]) -> "DashboardPatchBuilder":
        if external_url:
            self._add_patch(
                DashboardInfo.ASPECT_NAME,
                "add",
                path=("externalUrl",),
                value=external_url,
            )
        return self

    def add_charts(self, chart_urns: Optional[List[str]]) -> "DashboardPatchBuilder":
        if chart_urns:
            for urn in chart_urns:
                self._add_patch(
                    aspect_name=DashboardInfo.ASPECT_NAME,
                    op="add",
                    path=("charts", urn),
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
                    path=("datasets", urn),
                    value=urn,
                )

        return self

    def add_dashboard(
        self, dashboard: Union[Edge, Urn, str]
    ) -> "DashboardPatchBuilder":
        """
        Adds an dashboard to the DashboardPatchBuilder.

        Args:
            dashboard: The dashboard, which can be an Edge object, Urn object, or a string.

        Returns:
            The DashboardPatchBuilder instance.

        Raises:
            ValueError: If the dashboard is not a Dashboard urn.

        Notes:
            If `dashboard` is an Edge object, it is used directly. If `dashboard` is a Urn object or string,
            it is converted to an Edge object and added with default audit stamps.
        """
        if isinstance(dashboard, Edge):
            dashboard_urn: str = dashboard.destinationUrn
            dashboard_edge: Edge = dashboard
        elif isinstance(dashboard, (Urn, str)):
            dashboard_urn = str(dashboard)
            if not dashboard_urn.startswith("urn:li:dashboard:"):
                raise ValueError(f"Input {dashboard} is not a Dashboard urn")

            dashboard_edge = Edge(
                destinationUrn=dashboard_urn,
                created=self._mint_auditstamp(),
                lastModified=self._mint_auditstamp(),
            )

        self._ensure_urn_type("dashboard", [dashboard_edge], "add_dashboard")
        self._add_patch(
            DashboardInfo.ASPECT_NAME,
            "add",
            path=("dashboards", dashboard_urn),
            value=dashboard_edge,
        )
        return self

    def set_dashboard_url(
        self, dashboard_url: Optional[str]
    ) -> "DashboardPatchBuilder":
        if dashboard_url:
            self._add_patch(
                DashboardInfo.ASPECT_NAME,
                "add",
                path=("dashboardUrl",),
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
                path=("access",),
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
                path=("lastRefreshed",),
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
                path=("lastModified",),
                value=last_modified,
            )

        return self
