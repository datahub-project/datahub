from __future__ import annotations

import warnings
from datetime import datetime
from typing import Dict, Optional, Type

from typing_extensions import Self

from datahub.cli.cli_utils import first_non_null
from datahub.errors import IngestionAttributionWarning
from datahub.metadata.schema_classes import (
    AspectBag,
    AzkabanJobTypeClass,
    DataJobInfoClass,
    EditableDataJobPropertiesClass,
)
from datahub.metadata.urns import DataJobUrn, Urn
from datahub.sdk._attribution import is_ingestion_attribution
from datahub.sdk._shared import (
    DataflowUrnOrStr,
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
    make_time_stamp,
    parse_time_stamp,
)
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
        # Identity
        id: str,
        flow_urn: DataflowUrnOrStr,
        name: Optional[str] = None,
        platform: Optional[str] = None,
        platform_instance: Optional[str] = None,
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
        extra_aspects: ExtraAspectsType = None,
    ):
        urn = DataJobUrn.create_from_ids(
            job_id=id,
            data_flow_urn=str(flow_urn),
        )
        super().__init__(urn)
        self._set_extra_aspects(extra_aspects)

        if platform is None:
            platform = self.urn.get_data_flow_urn().orchestrator
        self._set_platform_instance(platform, platform_instance)

        # Initialize DataJobInfoClass with default type
        job_info = DataJobInfoClass(
            name=name or id,
            type=AzkabanJobTypeClass.COMMAND,  # Default type
        )
        self._setdefault_aspect(job_info)
        self._ensure_datajob_props().flowUrn = str(flow_urn)

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

    @classmethod
    def _new_from_graph(cls, urn: Urn, current_aspects: AspectBag) -> Self:
        assert isinstance(urn, DataJobUrn)
        # Extracting platform from the DataFlowUrn inside the DataJobUrn
        data_flow_urn = urn.get_data_flow_urn()

        entity = cls(
            platform=data_flow_urn.orchestrator,
            id=urn.job_id,
            flow_urn=str(data_flow_urn),
        )
        return entity._init_from_graph(current_aspects)

    @property
    def urn(self) -> DataJobUrn:
        return self._urn  # type: ignore

    def _ensure_datajob_props(self) -> DataJobInfoClass:
        props = self._get_aspect(DataJobInfoClass)
        if props is None:
            # Use name from URN as fallback with default type
            props = DataJobInfoClass(
                name=self.urn.job_id, type=AzkabanJobTypeClass.COMMAND
            )
            self._set_aspect(props)
        return props

    def _get_editable_props(self) -> Optional[EditableDataJobPropertiesClass]:
        return self._get_aspect(EditableDataJobPropertiesClass)

    def _ensure_editable_props(self) -> EditableDataJobPropertiesClass:
        return self._setdefault_aspect(EditableDataJobPropertiesClass())

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
        return self._ensure_datajob_props().name

    def set_name(self, name: str) -> None:
        """Set the name of the data job."""
        self._ensure_datajob_props().name = name

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
    def flow_urn(self) -> Optional[str]:
        """Get the URN of the data flow associated with the data job."""
        return str(self._ensure_datajob_props().flowUrn)

    def set_flow_urn(self, flow_urn: str) -> None:
        """Set the URN of the data flow associated with the data job."""
        self._ensure_datajob_props().flowUrn = flow_urn
