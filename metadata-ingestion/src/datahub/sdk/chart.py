from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Type, Union

from typing_extensions import Self

import datahub.metadata.schema_classes as models
from datahub.emitter.enum_helpers import get_enum_options
from datahub.metadata.urns import ChartUrn, DatasetUrn, Urn
from datahub.sdk._shared import (
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
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity, ExtraAspectsType


class Chart(
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
    """Represents a chart in DataHub."""

    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[ChartUrn]:
        """Get the URN type for charts.
        Returns:
            The ChartUrn class.
        """
        return ChartUrn

    def __init__(
        self,
        *,
        # Identity.
        name: str,
        platform: str,
        display_name: Optional[str] = None,
        platform_instance: Optional[str] = None,
        # Chart properties.
        description: Optional[str] = "",
        external_url: Optional[str] = None,
        chart_url: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        last_modified: Optional[datetime] = None,
        last_refreshed: Optional[datetime] = None,
        chart_type: Optional[str] = None,
        access: Optional[str] = None,
        # Standard aspects.
        subtype: Optional[str] = None,
        owners: Optional[OwnersInputType] = None,
        links: Optional[LinksInputType] = None,
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        domain: Optional[DomainInputType] = None,
        input_datasets: Optional[List[Union[DatasetUrnOrStr, Dataset]]] = None,
        extra_aspects: ExtraAspectsType = None,
    ):
        """Initialize a new Chart instance."""
        urn = ChartUrn.create_from_ids(
            platform=platform,
            name=name,
            platform_instance=platform_instance,
        )
        super().__init__(urn)
        self._set_extra_aspects(extra_aspects)

        self._set_platform_instance(platform, platform_instance)

        # Initialize ChartInfoClass with all required arguments
        chart_info = models.ChartInfoClass(
            title=display_name or name,
            description=description or "",
            lastModified=models.ChangeAuditStampsClass(),  # TODO: this is required.
        )

        self._setdefault_aspect(chart_info)

        # Set additional properties
        if external_url is not None:
            self.set_external_url(external_url)
        if chart_url is not None:
            self.set_chart_url(chart_url)
        if custom_properties is not None:
            self.set_custom_properties(custom_properties)
        if last_refreshed is not None:
            self.set_last_refreshed(last_refreshed)
        if chart_type is not None:
            self.set_chart_type(chart_type)
        if access is not None:
            self.set_access(access)
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
        if last_modified is not None:
            self.set_last_modified(last_modified)
        if input_datasets is not None:
            self.set_input_datasets(input_datasets)

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: models.AspectBag) -> Self:
        assert isinstance(urn, ChartUrn)
        entity = cls(
            platform=urn.dashboard_tool,
            name=urn.chart_id,
        )
        return entity._init_from_graph(current_aspects)

    @property
    def urn(self) -> ChartUrn:
        return self._urn  # type: ignore

    def _ensure_chart_props(self) -> models.ChartInfoClass:
        """Ensure chart properties exist, using a safer approach."""
        props = self._get_aspect(models.ChartInfoClass)
        if props is None:
            # Use name from URN as fallback
            props = models.ChartInfoClass(
                title=self.urn.chart_id,
                description="",
                lastModified=models.ChangeAuditStampsClass(),
            )
            self._set_aspect(props)
        return props

    @property
    def name(self) -> str:
        """Get the name of the chart."""
        return self.urn.chart_id

    @property
    def title(self) -> str:
        """Get the title of the chart."""
        return self._ensure_chart_props().title

    def set_title(self, title: str) -> None:
        """Set the title of the chart."""
        self._ensure_chart_props().title = title

    @property
    def description(self) -> Optional[str]:
        """Get the description of the chart."""
        props = self._get_aspect(models.ChartInfoClass)
        return props.description if props is not None else None

    def set_description(self, description: str) -> None:
        """Set the description of the chart."""
        self._ensure_chart_props().description = description

    @property
    def display_name(self) -> Optional[str]:
        """Get the display name of the chart."""
        return self.title

    def set_display_name(self, display_name: str) -> None:
        """Set the display name of the chart."""
        self.set_title(display_name)

    @property
    def external_url(self) -> Optional[str]:
        """Get the external URL of the chart."""
        props = self._get_aspect(models.ChartInfoClass)
        return props.externalUrl if props is not None else None

    def set_external_url(self, external_url: str) -> None:
        """Set the external URL of the chart."""
        self._ensure_chart_props().externalUrl = external_url

    @property
    def chart_url(self) -> Optional[str]:
        """Get the chart URL."""
        props = self._get_aspect(models.ChartInfoClass)
        return props.chartUrl if props is not None else None

    def set_chart_url(self, chart_url: str) -> None:
        """Set the chart URL."""
        self._ensure_chart_props().chartUrl = chart_url

    @property
    def custom_properties(self) -> Dict[str, str]:
        """Get the custom properties of the chart."""
        props = self._get_aspect(models.ChartInfoClass)
        return props.customProperties if props is not None else {}

    def set_custom_properties(self, custom_properties: Dict[str, str]) -> None:
        """Set the custom properties of the chart."""
        self._ensure_chart_props().customProperties = custom_properties

    @property
    def last_modified(self) -> Optional[datetime]:
        """Get the last modification timestamp of the chart."""
        props = self._get_aspect(models.ChartInfoClass)

        # Convert timestamp to datetime
        if props is None or props.lastModified.lastModified.time == 0:  # Todo: hack
            return None
        return datetime.fromtimestamp(props.lastModified.lastModified.time)

    def set_last_modified(self, last_modified: datetime) -> None:
        """Set the last modification timestamp of the chart."""
        chart_props = self._ensure_chart_props()
        chart_props.lastModified = models.ChangeAuditStampsClass(
            lastModified=models.AuditStampClass(
                time=int(last_modified.timestamp()),
                actor="urn:li:corpuser:datahub",
            )
        )

    @property
    def last_refreshed(self) -> Optional[datetime]:
        """Get the last refresh timestamp of the chart."""
        props = self._get_aspect(models.ChartInfoClass)
        if props is None or props.lastRefreshed is None:
            return None
        return datetime.fromtimestamp(props.lastRefreshed)

    def set_last_refreshed(self, last_refreshed: datetime) -> None:
        """Set the last refresh timestamp of the chart."""
        chart_props = self._ensure_chart_props()
        chart_props.lastRefreshed = int(last_refreshed.timestamp())

    @property
    def chart_type(self) -> Optional[str]:
        """Get the type of the chart."""
        props = self._get_aspect(models.ChartInfoClass)
        type_val = props.type if props is not None else None
        # Convert ChartTypeClass to str if necessary
        return str(type_val) if type_val is not None else None

    def set_chart_type(self, chart_type: str) -> None:
        """Set the type of the chart."""
        # validate chart_type
        assert chart_type in get_enum_options(models.ChartTypeClass)
        self._ensure_chart_props().type = chart_type

    @property
    def access(self) -> Optional[str]:
        """Get the access level of the chart."""
        props = self._get_aspect(models.ChartInfoClass)
        access_val = props.access if props is not None else None
        # Convert AccessLevelClass to str if necessary
        return str(access_val) if access_val is not None else None

    def set_access(self, access: str) -> None:
        """Set the access level of the chart."""
        self._ensure_chart_props().access = access

    @property
    def input_datasets(self) -> List[DatasetUrn]:
        """Get the input datasets of the chart."""
        props = self._ensure_chart_props()
        # Convert all inputs to DatasetUrn
        return [DatasetUrn.from_string(input_urn) for input_urn in (props.inputs or [])]

    def set_input_datasets(
        self, input_datasets: List[Union[DatasetUrnOrStr, Dataset]]
    ) -> None:
        """Set the input datasets of the chart."""
        # Convert all inputs to strings
        inputs = []
        for input_dataset in input_datasets:
            if isinstance(input_dataset, Dataset):
                inputs.append(str(input_dataset.urn))
            else:
                inputs.append(str(input_dataset))
        self._ensure_chart_props().inputs = inputs

    def add_input_dataset(self, input_dataset: Union[DatasetUrnOrStr, Dataset]) -> None:
        """Add an input to the chart."""
        if isinstance(input_dataset, Dataset):
            input_dataset_urn = input_dataset.urn
        else:
            input_dataset_urn = DatasetUrn.from_string(input_dataset)

        chart_props = self._ensure_chart_props()
        inputs = chart_props.inputs or []
        if input_dataset_urn not in inputs:
            inputs.append(str(input_dataset_urn))
        chart_props.inputs = inputs

    def remove_input_dataset(
        self, input_dataset: Union[DatasetUrnOrStr, Dataset]
    ) -> None:
        """Remove an input from the chart."""
        chart_props = self._ensure_chart_props()
        inputs = chart_props.inputs or []
        if input_dataset in inputs:
            inputs.remove(str(input_dataset))
            chart_props.inputs = inputs
