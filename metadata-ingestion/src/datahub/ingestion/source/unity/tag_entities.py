import logging
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from datahub.api.entities.external.external_entities import (
    ExternalEntity,
    ExternalEntityId,
    LinkedResourceSet,
)
from datahub.api.entities.external.unity_catalog_external_entites import UnityCatalogTag
from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import TagUrn
from datahub.utilities.urns.urn import Urn


class UnityCatalogTagSyncContext(BaseModel):
    # it is intentionally empty
    platform_instance: Optional[str] = None


logger = logging.getLogger(__name__)


def get_unity_catalog_tag_cache_info(
    platform_resource_repository: Any,  # UnityCatalogPlatformResourceRepository - avoiding circular import
) -> Dict[str, Dict[str, int]]:
    """Get cache statistics for Unity Catalog tag operations."""
    return platform_resource_repository.get_entity_cache_info()


class UnityCatalogTagPlatformResourceId(BaseModel, ExternalEntityId):
    """
    A Unity Catalog tag platform resource ID.
    """

    tag_key: str
    tag_value: Optional[str] = None
    platform_instance: Optional[str] = None
    exists_in_unity_catalog: bool = False
    persisted: bool = False

    # this is a hack to make sure the property is a string and not private pydantic field
    @staticmethod
    def _RESOURCE_TYPE() -> str:
        return "UnityCatalogTagPlatformResource"

    def to_platform_resource_key(self) -> PlatformResourceKey:
        return PlatformResourceKey(
            platform="databricks",
            resource_type=str(UnityCatalogTagPlatformResourceId._RESOURCE_TYPE()),
            primary_key=f"{self.tag_key}:{self.tag_value}",
            platform_instance=self.platform_instance,
        )

    @classmethod
    def from_tag(
        cls,
        tag: UnityCatalogTag,
        platform_instance: Optional[str],
        platform_resource_repository: Any,  # UnityCatalogPlatformResourceRepository - avoiding circular import
        exists_in_unity_catalog: bool = False,
    ) -> "UnityCatalogTagPlatformResourceId":
        """
        Creates a UnityCatalogTagPlatformResourceId from a UnityCatalogTag.
        """

        existing_platform_resource = platform_resource_repository.search_entity_by_urn(
            tag.to_datahub_tag_urn().urn(),
            sync_context=UnityCatalogTagSyncContext(
                platform_instance=platform_instance
            ),
        )
        if existing_platform_resource:
            logger.debug(
                f"Found existing UnityCatalogTagPlatformResourceId for tag {tag.key.raw_text}: {existing_platform_resource}"
            )
            return existing_platform_resource

        return UnityCatalogTagPlatformResourceId(
            tag_key=tag.key.raw_text,
            tag_value=tag.value.raw_text if tag.value is not None else None,
            platform_instance=platform_instance,
            exists_in_unity_catalog=exists_in_unity_catalog,
            persisted=False,
        )

    @classmethod
    def from_datahub_urn(
        cls,
        urn: str,
        tag_sync_context: UnityCatalogTagSyncContext,
        platform_resource_repository: Any,  # UnityCatalogPlatformResourceRepository - avoiding circular import
        graph: DataHubGraph,
    ) -> "UnityCatalogTagPlatformResourceId":
        """
        Creates a UnityCatalogTagPlatformResourceId from a DataHub URN.
        """
        existing_platform_resource_id = (
            platform_resource_repository.search_entity_by_urn(urn, tag_sync_context)
        )
        if existing_platform_resource_id:
            return existing_platform_resource_id

        new_unity_catalog_tag_id = cls.generate_tag_id(graph, tag_sync_context, urn)
        if new_unity_catalog_tag_id:
            resource_key = platform_resource_repository.get(
                new_unity_catalog_tag_id.to_platform_resource_key()
            )
            if resource_key:
                # Mark that this tag exists in Unity Catalog
                new_unity_catalog_tag_id.exists_in_unity_catalog = True
            return new_unity_catalog_tag_id
        raise ValueError(
            f"Unable to create Unity Catalog tag ID from DataHub URN: {urn}"
        )

    @classmethod
    def generate_tag_id(
        cls, graph: DataHubGraph, tag_sync_context: UnityCatalogTagSyncContext, urn: str
    ) -> "UnityCatalogTagPlatformResourceId":
        parsed_urn = Urn.from_string(urn)
        entity_type = parsed_urn.entity_type
        if entity_type == "tag":
            return UnityCatalogTagPlatformResourceId.from_datahub_tag(
                TagUrn.from_string(urn), tag_sync_context
            )
        else:
            raise ValueError(f"Unsupported entity type {entity_type} for URN {urn}")

    @classmethod
    def from_datahub_tag(
        cls, tag_urn: TagUrn, tag_sync_context: UnityCatalogTagSyncContext
    ) -> "UnityCatalogTagPlatformResourceId":
        uc_tag = UnityCatalogTag.from_urn(tag_urn)

        return UnityCatalogTagPlatformResourceId(
            tag_key=str(uc_tag.key),
            tag_value=str(uc_tag.value) if uc_tag.value is not None else None,
            platform_instance=tag_sync_context.platform_instance,
            exists_in_unity_catalog=False,
        )


class UnityCatalogTagPlatformResource(BaseModel, ExternalEntity):
    datahub_urns: LinkedResourceSet
    managed_by_datahub: bool
    id: UnityCatalogTagPlatformResourceId
    allowed_values: Optional[List[str]] = None

    def get_id(self) -> ExternalEntityId:
        return self.id

    def is_managed_by_datahub(self) -> bool:
        return self.managed_by_datahub

    def datahub_linked_resources(self) -> LinkedResourceSet:
        return self.datahub_urns

    def as_platform_resource(self) -> PlatformResource:
        return PlatformResource.create(
            key=self.id.to_platform_resource_key(),
            secondary_keys=[u for u in self.datahub_urns.urns],
            value=self,
        )

    @classmethod
    def get_from_datahub(
        cls,
        unity_catalog_tag_id: UnityCatalogTagPlatformResourceId,
        platform_resource_repository: Any,  # UnityCatalogPlatformResourceRepository - avoiding circular import
        managed_by_datahub: bool = False,
    ) -> "UnityCatalogTagPlatformResource":
        """Get Unity Catalog tag platform resource from DataHub with caching."""
        return platform_resource_repository.get_entity_from_datahub(
            unity_catalog_tag_id, managed_by_datahub
        )
