from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Type, Union

from typing_extensions import Self

import datahub.metadata.schema_classes as models
from datahub.metadata.urns import ChartUrn, DashboardUrn, DatasetUrn, Urn
from datahub.sdk._shared import (
    ChartUrnOrStr,
    DashboardUrnOrStr,
    DataPlatformInstanceUrnOrStr,
    DataPlatformUrnOrStr,
    DatasetUrnOrStr,
    DomainInputType,
    HasContainer,
    HasDomain,
    HasInstitutionalMemory,
    HasOwnership,
    HasPlatformInstance,
    HasSubtype,
    HasTags,
    HasTerms,
    LinksInputType,
    OwnersInputType,
    TagsInputType,
    TermsInputType,
)
from datahub.sdk.chart import Chart
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity, ExtraAspectsType


class Dashboard(
    HasPlatformInstance,
    HasSubtype,
    HasOwnership,
    HasContainer,
    HasInstitutionalMemory,
    HasTags,
    HasTerms,
    HasDomain,
    Entity,
):
    """Represents a dashboard in DataHub."""

    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[DashboardUrn]:
        """Get the URN type for dashboards.
        Returns:
            The DashboardUrn class.
        """
        return DashboardUrn

    def __init__(
        self,
        *,
        # Identity.
        name: str,
        platform: DataPlatformUrnOrStr,
        display_name: Optional[str] = None,
        platform_instance: Optional[DataPlatformInstanceUrnOrStr] = None,
        # Dashboard properties.
        description: str = "",
        external_url: Optional[str] = None,
        dashboard_url: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        last_modified: Optional[datetime] = None,
        last_refreshed: Optional[datetime] = None,
        input_datasets: Optional[List[Union[DatasetUrnOrStr, Dataset]]] = None,
        charts: Optional[List[Union[ChartUrnOrStr, Chart]]] = None,
        dashboards: Optional[List[Union[DashboardUrnOrStr, Dashboard]]] = None,
        # Standard aspects.
        subtype: Optional[str] = None,
        owners: Optional[OwnersInputType] = None,
        links: Optional[LinksInputType] = None,
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        domain: Optional[DomainInputType] = None,
        extra_aspects: ExtraAspectsType = None,
    ):
        """Initialize a new Dashboard instance."""
        urn = DashboardUrn.create_from_ids(
            platform=str(platform),
            name=name,
            platform_instance=str(platform_instance) if platform_instance else None,
        )
        super().__init__(urn)
        self._set_extra_aspects(extra_aspects)

        self._set_platform_instance(platform, platform_instance)

        # Initialize DashboardInfoClass with default values
        dashboard_info = models.DashboardInfoClass(
            title=display_name or name,
            description=description or "",
            lastModified=models.ChangeAuditStampsClass(
                lastModified=None,
            ),
            customProperties={},
            chartEdges=[],
            datasetEdges=[],
            dashboards=[],
        )

        if last_modified:
            dashboard_info.lastModified = models.ChangeAuditStampsClass(
                lastModified=models.AuditStampClass(
                    time=int(last_modified.timestamp()),
                    actor="urn:li:corpuser:datahub",
                ),
            )

        # Set additional properties
        if description is not None:
            self.set_description(description)
        if display_name is not None:
            self.set_display_name(display_name)
        if external_url is not None:
            self.set_external_url(external_url)
        if dashboard_url is not None:
            self.set_dashboard_url(dashboard_url)
        if custom_properties is not None:
            self.set_custom_properties(custom_properties)
        if last_modified is not None:
            self.set_last_modified(last_modified)
        if last_refreshed is not None:
            self.set_last_refreshed(last_refreshed)
        if subtype is not None:
            self.set_subtype(subtype)
        if owners is not None:
            self.set_owners(owners)
        if links is not None:
            self.set_links(links)
        if tags is not None:
            self.set_tags(tags)
        if terms is not None:
            self.set_terms(terms)
        if domain is not None:
            self.set_domain(domain)
        if input_datasets is not None:
            self.set_input_datasets(input_datasets)
        if charts is not None:
            self.set_charts(charts)
        if dashboards is not None:
            self.set_dashboards(dashboards)

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: models.AspectBag) -> Self:
        assert isinstance(urn, DashboardUrn)
        entity = cls(
            platform=urn.dashboard_tool,
            name=urn.dashboard_id,
        )
        return entity._init_from_graph(current_aspects)

    @property
    def urn(self) -> DashboardUrn:
        assert isinstance(self._urn, DashboardUrn)
        return self._urn

    def _ensure_dashboard_props(self) -> models.DashboardInfoClass:
        """Get the dashboard properties safely."""
        return self._setdefault_aspect(
            models.DashboardInfoClass(
                title=self.urn.dashboard_id,
                description="",
                lastModified=models.ChangeAuditStampsClass(
                    lastModified=models.AuditStampClass(
                        time=0, actor="urn:li:corpuser:unknown"
                    )
                ),
                customProperties={},
                chartEdges=[],
                datasetEdges=[],
                dashboards=[],
            )
        )

    @property
    def name(self) -> str:
        """Get the name of the dashboard."""
        return self.urn.dashboard_id

    @property
    def title(self) -> str:
        """Get the title of the dashboard."""
        return self._ensure_dashboard_props().title

    def set_title(self, title: str) -> None:
        """Set the title of the dashboard."""
        props = self._ensure_dashboard_props()
        props.title = title
        self._set_aspect(props)

    @property
    def description(self) -> Optional[str]:
        """Get the description of the dashboard."""
        props = self._ensure_dashboard_props()
        return props.description

    def set_description(self, description: str) -> None:
        """Set the description of the dashboard."""
        props = self._ensure_dashboard_props()
        props.description = description
        self._set_aspect(props)

    @property
    def display_name(self) -> Optional[str]:
        """Get the display name of the dashboard."""
        return self.title

    def set_display_name(self, display_name: str) -> None:
        """Set the display name of the dashboard."""
        self.set_title(display_name)

    @property
    def external_url(self) -> Optional[str]:
        """Get the external URL of the dashboard."""
        props = self._ensure_dashboard_props()
        return props.externalUrl

    def set_external_url(self, external_url: str) -> None:
        """Set the external URL of the dashboard."""
        props = self._ensure_dashboard_props()
        props.externalUrl = external_url
        self._set_aspect(props)

    @property
    def dashboard_url(self) -> Optional[str]:
        """Get the dashboard URL."""
        props = self._ensure_dashboard_props()
        return props.dashboardUrl

    def set_dashboard_url(self, dashboard_url: str) -> None:
        """Set the dashboard URL."""
        props = self._ensure_dashboard_props()
        props.dashboardUrl = dashboard_url
        self._set_aspect(props)

    @property
    def custom_properties(self) -> Dict[str, str]:
        """Get the custom properties of the dashboard."""
        props = self._ensure_dashboard_props()
        return props.customProperties or {}

    def set_custom_properties(self, custom_properties: Dict[str, str]) -> None:
        """Set the custom properties of the dashboard."""
        props = self._ensure_dashboard_props()
        props.customProperties = custom_properties
        self._set_aspect(props)

    @property
    def last_modified(self) -> Optional[datetime]:
        """Get the last modification timestamp of the dashboard."""
        props = self._ensure_dashboard_props()
        if props.lastModified.lastModified.time == 0:
            return None
        return datetime.fromtimestamp(props.lastModified.lastModified.time)

    def set_last_modified(self, last_modified: datetime) -> None:
        """Set the last modification timestamp of the dashboard."""
        props = self._ensure_dashboard_props()
        props.lastModified = models.ChangeAuditStampsClass(
            lastModified=models.AuditStampClass(
                time=int(last_modified.timestamp()),
                actor="urn:li:corpuser:datahub",
            ),
        )
        self._set_aspect(props)

    @property
    def last_refreshed(self) -> Optional[datetime]:
        """Get the last refresh timestamp of the dashboard."""
        props = self._ensure_dashboard_props()
        return (
            datetime.fromtimestamp(props.lastRefreshed)
            if props.lastRefreshed is not None
            else None
        )

    def set_last_refreshed(self, last_refreshed: datetime) -> None:
        """Set the last refresh timestamp of the dashboard."""
        props = self._ensure_dashboard_props()
        props.lastRefreshed = int(last_refreshed.timestamp())
        self._set_aspect(props)

    @property
    def input_datasets(self) -> List[DatasetUrn]:
        """Get the input datasets of the dashboard."""
        props = self._ensure_dashboard_props()
        return [
            DatasetUrn.from_string(edge.destinationUrn)
            for edge in (props.datasetEdges or [])
        ]

    def set_input_datasets(
        self, input_datasets: List[Union[DatasetUrnOrStr, Dataset]]
    ) -> None:
        """Set the input datasets of the dashboard."""
        props = self._ensure_dashboard_props()
        dataset_edges = props.datasetEdges or []
        for dataset in input_datasets:
            if isinstance(dataset, Dataset):
                dataset_urn = dataset.urn
            else:
                dataset_urn = DatasetUrn.from_string(dataset)
            dataset_edges.append(models.EdgeClass(destinationUrn=str(dataset_urn)))
        props.datasetEdges = dataset_edges
        self._set_aspect(props)

    def add_input_dataset(self, input_dataset: Union[DatasetUrnOrStr, Dataset]) -> None:
        """Add an input dataset to the dashboard."""
        if isinstance(input_dataset, Dataset):
            input_dataset_urn = input_dataset.urn
        else:
            input_dataset_urn = DatasetUrn.from_string(input_dataset)
        props = self._ensure_dashboard_props()
        dataset_edges = props.datasetEdges or []
        existing_urns = [edge.destinationUrn for edge in dataset_edges]
        if str(input_dataset_urn) not in existing_urns:
            dataset_edges.append(
                models.EdgeClass(destinationUrn=str(input_dataset_urn))
            )
        props.datasetEdges = dataset_edges
        self._set_aspect(props)

    def remove_input_dataset(
        self, input_dataset: Union[DatasetUrnOrStr, Dataset]
    ) -> None:
        """Remove an input dataset from the dashboard."""
        if isinstance(input_dataset, Dataset):
            input_dataset_urn = input_dataset.urn
        else:
            input_dataset_urn = DatasetUrn.from_string(input_dataset)
        props = self._ensure_dashboard_props()
        props.datasetEdges = [
            edge
            for edge in (props.datasetEdges or [])
            if edge.destinationUrn != str(input_dataset_urn)
        ]
        self._set_aspect(props)

    @property
    def charts(self) -> List[ChartUrn]:
        """Get the charts of the dashboard."""
        chart_edges = self._ensure_dashboard_props().chartEdges
        if chart_edges is None:
            return []
        return [ChartUrn.from_string(edge.destinationUrn) for edge in chart_edges]

    def set_charts(self, charts: List[Union[ChartUrnOrStr, Chart]]) -> None:
        """Set the charts of the dashboard."""
        props = self._ensure_dashboard_props()
        chart_edges = props.chartEdges or []
        for chart in charts:
            if isinstance(chart, Chart):
                chart_urn = chart.urn
            else:
                chart_urn = ChartUrn.from_string(chart)
            chart_edges.append(models.EdgeClass(destinationUrn=str(chart_urn)))
        props.chartEdges = chart_edges
        self._set_aspect(props)

    def add_chart(self, chart: Union[ChartUrnOrStr, Chart]) -> None:
        """Add a chart to the dashboard."""
        if isinstance(chart, Chart):
            chart_urn = chart.urn
        else:
            chart_urn = ChartUrn.from_string(chart)
        props = self._ensure_dashboard_props()
        chart_edges = props.chartEdges or []
        existing_urns = [
            edge.destinationUrn
            for edge in chart_edges
            if edge.destinationUrn is not None
        ]
        if str(chart_urn) not in existing_urns:
            chart_edges.append(models.EdgeClass(destinationUrn=str(chart_urn)))
        props.chartEdges = chart_edges
        self._set_aspect(props)

    def remove_chart(self, chart: Union[ChartUrnOrStr, Chart]) -> None:
        """Remove a chart from the dashboard."""
        if isinstance(chart, Chart):
            chart_urn = chart.urn
        else:
            chart_urn = ChartUrn.from_string(chart)
        props = self._ensure_dashboard_props()
        props.chartEdges = [
            edge
            for edge in (props.chartEdges or [])
            if edge.destinationUrn != str(chart_urn)
        ]
        self._set_aspect(props)

    @property
    def dashboards(self) -> List[DashboardUrn]:
        """Get the dashboards of the dashboard."""
        props = self._ensure_dashboard_props()
        return [
            DashboardUrn.from_string(dashboard.destinationUrn)
            for dashboard in (props.dashboards or [])
        ]

    def set_dashboards(
        self, dashboards: List[Union[DashboardUrnOrStr, Dashboard]]
    ) -> None:
        """Set the dashboards of the dashboard."""
        props = self._ensure_dashboard_props()
        for dashboard in dashboards:
            if isinstance(dashboard, Dashboard):
                dashboard_urn = dashboard.urn
            else:
                dashboard_urn = DashboardUrn.from_string(dashboard)
            props.dashboards.append(models.EdgeClass(destinationUrn=str(dashboard_urn)))
        self._set_aspect(props)

    def add_dashboard(self, dashboard: Union[DashboardUrnOrStr, Dashboard]) -> None:
        """Add a dashboard to the dashboard."""
        if isinstance(dashboard, Dashboard):
            dashboard_urn = dashboard.urn
        else:
            dashboard_urn = DashboardUrn.from_string(dashboard)
        props = self._ensure_dashboard_props()
        dashboards = props.dashboards or []
        existing_urns = [dashboard.destinationUrn for dashboard in dashboards]
        if str(dashboard_urn) not in existing_urns:
            dashboards.append(models.EdgeClass(destinationUrn=str(dashboard_urn)))
