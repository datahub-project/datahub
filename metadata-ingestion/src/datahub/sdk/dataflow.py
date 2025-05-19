from __future__ import annotations

import warnings
from datetime import datetime
from typing import Dict, Optional, Type

from typing_extensions import Self

from datahub.cli.cli_utils import first_non_null
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.errors import (
    IngestionAttributionWarning,
)
from datahub.metadata.schema_classes import (
    AspectBag,
    DataFlowInfoClass,
    EditableDataFlowPropertiesClass,
)
from datahub.metadata.urns import DataFlowUrn, Urn
from datahub.sdk._attribution import is_ingestion_attribution
from datahub.sdk._shared import (
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


class DataFlow(
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
    """Represents a dataflow in DataHub.
    A dataflow represents a collection of data, such as a table, view, or file.
    This class provides methods for managing dataflow metadata including schema,
    lineage, and various aspects like ownership, tags, and terms.
    """

    __slots__ = ()

    @classmethod
    def get_urn_type(cls) -> Type[DataFlowUrn]:
        """Get the URN type for dataflows.
        Returns:
            The DataflowUrn class.
        """
        return DataFlowUrn

    def __init__(
        self,
        *,
        # Identity.
        id: str,
        platform: str,
        name: Optional[str] = None,
        platform_instance: Optional[str] = None,
        env: str = DEFAULT_ENV,  # TODO: is this reasonable?
        # Dataflow properties.
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        external_url: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        created: Optional[datetime] = None,
        last_modified: Optional[datetime] = None,
        # Standard aspects.
        subtype: Optional[str] = None,
        owners: Optional[OwnersInputType] = None,
        links: Optional[LinksInputType] = None,
        tags: Optional[TagsInputType] = None,
        terms: Optional[TermsInputType] = None,
        domain: Optional[DomainInputType] = None,
        extra_aspects: ExtraAspectsType = None,
    ):
        """Initialize a new Dataflow instance.
        Args:
            platform: The platform this dataflow belongs to (e.g. "mysql", "snowflake").
            name: The name of the dataflow.
            platform_instance: Optional platform instance identifier.
            env: The environment this dataflow belongs to (default: DEFAULT_ENV).
            description: Optional description of the dataflow.
            display_name: Optional display name for the dataflow.
            external_url: Optional URL to external documentation or source.
            custom_properties: Optional dictionary of custom properties.
            created: Optional creation timestamp.
            last_modified: Optional last modification timestamp.
            subtype: Optional subtype of the dataflow.
            owners: Optional list of owners.
            links: Optional list of links.
            tags: Optional list of tags.
            terms: Optional list of glossary terms.
            domain: Optional domain this dataflow belongs to.
            extra_aspects: Optional list of additional aspects.
            upstreams: Optional upstream lineage information.
        """
        urn = DataFlowUrn.create_from_ids(
            orchestrator=platform,
            flow_id=id,
            env=env,
            platform_instance=platform_instance,
        )
        super().__init__(urn)
        self._set_extra_aspects(extra_aspects)

        self._set_platform_instance(urn.orchestrator, platform_instance)

        # Initialize DataFlowInfoClass directly with name
        self._setdefault_aspect(DataFlowInfoClass(name=name or id))

        if description is not None:
            self.set_description(description)
        if display_name is not None:
            self.set_display_name(display_name)
        if external_url is not None:
            self.set_external_url(external_url)
        if custom_properties is not None:
            self.set_custom_properties(custom_properties)
        if created is not None:
            self.set_created(created)
        if last_modified is not None:
            self.set_last_modified(last_modified)
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
        assert isinstance(urn, DataFlowUrn)
        entity = cls(
            platform=urn.orchestrator,
            id=urn.flow_id,
        )
        return entity._init_from_graph(current_aspects)

    @property
    def urn(self) -> DataFlowUrn:
        return self._urn  # type: ignore

    def _ensure_dataflow_props(self) -> DataFlowInfoClass:
        props = self._get_aspect(DataFlowInfoClass)
        if props is None:
            # Use name from URN as fallback
            props = DataFlowInfoClass(name=self.urn.flow_id)
            self._set_aspect(props)
        return props

    def _get_editable_props(self) -> Optional[EditableDataFlowPropertiesClass]:
        return self._get_aspect(EditableDataFlowPropertiesClass)

    def _ensure_editable_props(self) -> EditableDataFlowPropertiesClass:
        # Note that most of the fields in this aspect are not used.
        # The only one that's relevant for us is the description.
        return self._setdefault_aspect(EditableDataFlowPropertiesClass())

    @property
    def description(self) -> Optional[str]:
        """Get the description of the dataflow.
        Returns:
            The description if set, None otherwise.
        """
        editable_props = self._get_editable_props()
        return first_non_null(
            [
                editable_props.description if editable_props is not None else None,
                self._ensure_dataflow_props().description,
            ]
        )

    def set_description(self, description: str) -> None:
        """Set the description of the dataflow.
        Args:
            description: The description to set.
        Note:
            If called during ingestion, this will warn if overwriting
            a non-ingestion description.
        """
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

            self._ensure_dataflow_props().description = description
        else:
            self._ensure_editable_props().description = description

    @property
    def name(self) -> str:
        """Get the name of the dataflow.
        Returns:
            The name of the dataflow.
        """
        return self._ensure_dataflow_props().name

    def set_name(self, name: str) -> None:
        """Set the name of the dataflow.
        Args:
            name: The name to set.
        """
        self._ensure_dataflow_props().name = name

    @property
    def display_name(self) -> Optional[str]:
        """Get the display name of the dataflow.
        Returns:
            The display name if set, None otherwise.
        """
        return self._ensure_dataflow_props().name

    def set_display_name(self, display_name: str) -> None:
        """Set the display name of the dataflow.
        Args:
            display_name: The display name to set.
        """
        self._ensure_dataflow_props().name = display_name

    @property
    def external_url(self) -> Optional[str]:
        """Get the external URL of the dataflow.
        Returns:
            The external URL if set, None otherwise.
        """
        return self._ensure_dataflow_props().externalUrl

    def set_external_url(self, external_url: str) -> None:
        """Set the external URL of the dataflow.
        Args:
            external_url: The external URL to set.
        """
        self._ensure_dataflow_props().externalUrl = external_url

    @property
    def custom_properties(self) -> Dict[str, str]:
        """Get the custom properties of the dataflow.
        Returns:
            Dictionary of custom properties.
        """
        return self._ensure_dataflow_props().customProperties

    def set_custom_properties(self, custom_properties: Dict[str, str]) -> None:
        """Set the custom properties of the dataflow.
        Args:
            custom_properties: Dictionary of custom properties to set.
        """
        self._ensure_dataflow_props().customProperties = custom_properties

    @property
    def created(self) -> Optional[datetime]:
        """Get the creation timestamp of the dataflow.
        Returns:
            The creation timestamp if set, None otherwise.
        """
        return parse_time_stamp(self._ensure_dataflow_props().created)

    def set_created(self, created: datetime) -> None:
        """Set the creation timestamp of the dataflow.
        Args:
            created: The creation timestamp to set.
        """
        self._ensure_dataflow_props().created = make_time_stamp(created)

    @property
    def last_modified(self) -> Optional[datetime]:
        """Get the last modification timestamp of the dataflow.
        Returns:
            The last modification timestamp if set, None otherwise.
        """
        return parse_time_stamp(self._ensure_dataflow_props().lastModified)

    def set_last_modified(self, last_modified: datetime) -> None:
        self._ensure_dataflow_props().lastModified = make_time_stamp(last_modified)
