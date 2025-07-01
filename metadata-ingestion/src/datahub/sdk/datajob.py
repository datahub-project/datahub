from __future__ import annotations

import warnings
from datetime import datetime
from typing import Dict, List, Optional, Type

from typing_extensions import Self

import datahub.metadata.schema_classes as models
from datahub.cli.cli_utils import first_non_null
from datahub.errors import IngestionAttributionWarning
from datahub.metadata.urns import (
    DataFlowUrn,
    DataJobUrn,
    DatasetUrn,
    Urn,
)
from datahub.sdk._attribution import is_ingestion_attribution
from datahub.sdk._shared import (
    DataflowUrnOrStr,
    DatasetUrnOrStr,
    DomainInputType,
    HasContainer,
    HasDomain,
    HasInstitutionalMemory,
    HasOwnership,
    HasPlatformInstance,
    HasStructuredProperties,
    HasSubtype,
    HasTags,
    HasTerms,
    LinksInputType,
    OwnersInputType,
    StructuredPropertyInputType,
    TagsInputType,
    TermsInputType,
    make_time_stamp,
    parse_time_stamp,
)
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.entity import Entity, ExtraAspectsType


class DataJob(
    HasPlatformInstance,
    HasSubtype,
    HasContainer,
    HasOwnership,
    HasInstitutionalMemory,
    HasTags,
    HasTerms,
    HasDomain,
    HasStructuredProperties,
    Entity,
):
    """Represents a data job in DataHub.
    A data job is an executable unit of a data pipeline, such as an Airflow task or a Spark job.
    """

    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[DataJobUrn]:
        """Get the URN type for data jobs."""
        return DataJobUrn

    def __init__(
        self,
        *,
        name: str,
        flow: Optional[DataFlow] = None,
        flow_urn: Optional[DataflowUrnOrStr] = None,
        platform_instance: Optional[str] = None,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        external_url: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        created: Optional[datetime] = None,
        last_modified: Optional[datetime] = None,
        # Standard aspects
        subtype: Optional[str] = None,
        owners: Optional[OwnersInputType] = None,
        links: Optional[LinksInputType] = None,
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        domain: Optional[DomainInputType] = None,
        inlets: Optional[List[DatasetUrnOrStr]] = None,
        outlets: Optional[List[DatasetUrnOrStr]] = None,
        structured_properties: Optional[StructuredPropertyInputType] = None,
        extra_aspects: ExtraAspectsType = None,
    ):
        """
        Initialize a DataJob with either a DataFlow or a DataFlowUrn with platform instance.

        Args:
            name: Name of the data job (required)
            flow: A DataFlow object (optional)
            flow_urn: A DataFlowUrn object (optional)
            platform_instance: Platform instance name (optional, required if flow_urn is provided)
            ... (other optional parameters)

        Raises:
            ValueError: If neither flow nor (flow_urn and platform_instance) are provided
        """
        if flow is None:
            if flow_urn is None or platform_instance is None:
                raise ValueError(
                    "You must provide either: 1. a DataFlow object, or 2. a DataFlowUrn (and a platform_instance config if required)"
                )
            flow_urn = DataFlowUrn.from_string(flow_urn)
            if flow_urn.flow_id.startswith(f"{platform_instance}."):
                flow_name = flow_urn.flow_id[len(platform_instance) + 1 :]
            else:
                flow_name = flow_urn.flow_id
            flow = DataFlow(
                platform=flow_urn.orchestrator,
                name=flow_name,
                platform_instance=platform_instance,
            )
        urn = DataJobUrn.create_from_ids(
            job_id=name,
            data_flow_urn=str(flow.urn),
        )
        super().__init__(urn)
        self._set_extra_aspects(extra_aspects)
        self._set_platform_instance(flow.urn.orchestrator, flow.platform_instance)
        self._set_browse_path_from_flow(flow)

        # Initialize DataJobInfoClass with default type
        job_info = models.DataJobInfoClass(
            name=display_name or name,
            type=models.AzkabanJobTypeClass.COMMAND,  # Default type
        )
        self._setdefault_aspect(job_info)
        self._ensure_datajob_props().flowUrn = str(flow.urn)

        # Set properties if provided
        if description is not None:
            self.set_description(description)
        if external_url is not None:
            self.set_external_url(external_url)
        if custom_properties is not None:
            self.set_custom_properties(custom_properties)
        if created is not None:
            self.set_created(created)
        if last_modified is not None:
            self.set_last_modified(last_modified)

        # Set standard aspects
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
        if inlets is not None:
            self.set_inlets(inlets)
        if outlets is not None:
            self.set_outlets(outlets)
        if structured_properties is not None:
            for key, value in structured_properties.items():
                self.set_structured_property(property_urn=key, values=value)

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: models.AspectBag) -> Self:
        assert isinstance(urn, DataJobUrn)
        # Extracting platform from the DataFlowUrn inside the DataJobUrn
        data_flow_urn = urn.get_data_flow_urn()

        entity = cls(
            flow=DataFlow(
                platform=data_flow_urn.orchestrator,
                name=data_flow_urn.flow_id,
            ),
            name=urn.job_id,
        )
        return entity._init_from_graph(current_aspects)

    @property
    def urn(self) -> DataJobUrn:
        return self._urn  # type: ignore

    def _ensure_datajob_props(self) -> models.DataJobInfoClass:
        props = self._get_aspect(models.DataJobInfoClass)
        if props is None:
            # Use name from URN as fallback with default type
            props = models.DataJobInfoClass(
                name=self.urn.job_id, type=models.AzkabanJobTypeClass.COMMAND
            )
            self._set_aspect(props)
        return props

    def _get_datajob_inputoutput_props(
        self,
    ) -> Optional[models.DataJobInputOutputClass]:
        return self._get_aspect(models.DataJobInputOutputClass)

    def _ensure_datajob_inputoutput_props(
        self,
    ) -> models.DataJobInputOutputClass:
        return self._setdefault_aspect(
            models.DataJobInputOutputClass(inputDatasets=[], outputDatasets=[])
        )

    def _get_editable_props(self) -> Optional[models.EditableDataJobPropertiesClass]:
        return self._get_aspect(models.EditableDataJobPropertiesClass)

    def _ensure_editable_props(self) -> models.EditableDataJobPropertiesClass:
        return self._setdefault_aspect(models.EditableDataJobPropertiesClass())

    @property
    def description(self) -> Optional[str]:
        """Get the description of the data job."""
        editable_props = self._get_editable_props()
        return first_non_null(
            [
                editable_props.description if editable_props is not None else None,
                self._ensure_datajob_props().description,
            ]
        )

    def set_description(self, description: str) -> None:
        """Set the description of the data job."""
        if is_ingestion_attribution():
            editable_props = self._get_editable_props()
            if editable_props is not None and editable_props.description is not None:
                warnings.warn(
                    "Overwriting non-ingestion description from ingestion is an anti-pattern.",
                    category=IngestionAttributionWarning,
                    stacklevel=2,
                )
                # Force the ingestion description to show up.
                editable_props.description = None

            self._ensure_datajob_props().description = description
        else:
            self._ensure_editable_props().description = description

    @property
    def name(self) -> str:
        """Get the name of the data job."""
        return self.urn.job_id

    @property
    def display_name(self) -> Optional[str]:
        """Get the display name of the data job."""
        return self._ensure_datajob_props().name

    def set_display_name(self, display_name: str) -> None:
        """Set the display name of the data job."""
        self._ensure_datajob_props().name = display_name

    @property
    def external_url(self) -> Optional[str]:
        """Get the external URL of the data job."""
        return self._ensure_datajob_props().externalUrl

    def set_external_url(self, external_url: str) -> None:
        """Set the external URL of the data job."""
        self._ensure_datajob_props().externalUrl = external_url

    @property
    def custom_properties(self) -> Dict[str, str]:
        """Get the custom properties of the data job."""
        return self._ensure_datajob_props().customProperties

    def set_custom_properties(self, custom_properties: Dict[str, str]) -> None:
        """Set the custom properties of the data job."""
        self._ensure_datajob_props().customProperties = custom_properties

    @property
    def created(self) -> Optional[datetime]:
        """Get the creation timestamp of the data job."""
        return parse_time_stamp(self._ensure_datajob_props().created)

    def set_created(self, created: datetime) -> None:
        """Set the creation timestamp of the data job."""
        self._ensure_datajob_props().created = make_time_stamp(created)

    @property
    def last_modified(self) -> Optional[datetime]:
        """Get the last modification timestamp of the data job."""
        return parse_time_stamp(self._ensure_datajob_props().lastModified)

    def set_last_modified(self, last_modified: datetime) -> None:
        """Set the last modification timestamp of the data job."""
        self._ensure_datajob_props().lastModified = make_time_stamp(last_modified)

    @property
    def flow_urn(self) -> DataFlowUrn:
        """Get the data flow associated with the data job."""
        return self.urn.get_data_flow_urn()

    def _set_browse_path_from_flow(self, flow: DataFlow) -> None:
        flow_browse_path = flow._get_aspect(models.BrowsePathsV2Class)

        # extend the flow's browse path with this job
        browse_path = []
        if flow_browse_path is not None:
            for entry in flow_browse_path.path:
                browse_path.append(
                    models.BrowsePathEntryClass(id=entry.id, urn=entry.urn)
                )

        # Add the job itself to the path
        browse_path.append(models.BrowsePathEntryClass(id=flow.name, urn=str(flow.urn)))
        # Set the browse path aspect
        self._set_aspect(models.BrowsePathsV2Class(path=browse_path))

    # TODO: support datajob input/output
    @property
    def inlets(self) -> List[DatasetUrn]:
        """Get the inlets of the data job."""
        inlets = self._ensure_datajob_inputoutput_props().inputDatasets
        return [DatasetUrn.from_string(inlet) for inlet in inlets]

    def set_inlets(self, inlets: List[DatasetUrnOrStr]) -> None:
        """Set the inlets of the data job."""
        for inlet in inlets:
            inlet_urn = DatasetUrn.from_string(inlet)  # type checking
            self._ensure_datajob_inputoutput_props().inputDatasets.append(
                str(inlet_urn)
            )

    @property
    def outlets(self) -> List[DatasetUrn]:
        """Get the outlets of the data job."""
        outlets = self._ensure_datajob_inputoutput_props().outputDatasets
        return [DatasetUrn.from_string(outlet) for outlet in outlets]

    def set_outlets(self, outlets: List[DatasetUrnOrStr]) -> None:
        """Set the outlets of the data job."""
        for outlet in outlets:
            outlet_urn = DatasetUrn.from_string(outlet)  # type checking
            self._ensure_datajob_inputoutput_props().outputDatasets.append(
                str(outlet_urn)
            )
