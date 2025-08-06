import logging
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

from pydantic import BaseModel

from datahub.api.entities.external.external_entities import (
    ExternalEntity,
    ExternalEntityId,
    LinkedResourceSet,
    PlatformResourceRepository,
)
from datahub.api.entities.external.unity_catalog_external_entites import UnityCatalogTag
from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
    PlatformResourceSearchFields,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import TagUrn
from datahub.utilities.search_utils import ElasticDocumentQuery
from datahub.utilities.urns.urn import Urn


class UnityCatalogTagSyncContext(BaseModel):
    # it is intentionally empty
    platform_instance: Optional[str] = None


logger = logging.getLogger(__name__)


# Simple in-memory cache for tag operations
_urn_search_cache: Dict[str, Optional[Tuple[str, Optional[str], Optional[str], bool, bool]]] = {}
_platform_resource_cache: Dict[str, Optional[Dict]] = {}


def clear_unity_catalog_tag_cache() -> None:
    """Clear the Unity Catalog tag caches. Should be called at the start of ingestion runs."""
    global _urn_search_cache, _platform_resource_cache
    _urn_search_cache.clear()
    _platform_resource_cache.clear()
    logger.info("Unity Catalog tag cache cleared")


def get_cache_stats() -> Dict[str, int]:
    """Get cache statistics for debugging."""
    return {
        "urn_search_cache_size": len(_urn_search_cache),
        "platform_resource_cache_size": len(_platform_resource_cache),
    }


class UnityCatalogTagPlatformResourceId(BaseModel, ExternalEntityId):
    """
    A Unity Catalog tag platform resource ID.
    """

    tag_key: str
    tag_value: Optional[str] = None
    platform_instance: Optional[str] = None
    exists_in_unity_catalog: bool = False
    persisted: bool = False

    def __hash__(self) -> int:
        return hash(self.to_platform_resource_key().id)

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
        platform_resource_repository: PlatformResourceRepository,
        exists_in_unity_catalog: bool = False,
    ) -> "UnityCatalogTagPlatformResourceId":
        """
        Creates a UnityCatalogTagPlatformResourceId from a UnityCatalogTag.
        """

        existing_platform_resource = cls.search_by_urn(
            tag.to_datahub_tag_urn().urn(),
            platform_resource_repository=platform_resource_repository,
            tag_sync_context=UnityCatalogTagSyncContext(
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
    def search_by_urn(
        cls,
        urn: str,
        platform_resource_repository: PlatformResourceRepository,
        tag_sync_context: UnityCatalogTagSyncContext,
    ) -> Optional["UnityCatalogTagPlatformResourceId"]:
        """Search for existing platform resource by URN with simple caching."""
        
        # Create cache key
        cache_key = f"{urn}:{tag_sync_context.platform_instance}"
        
        # Check cache first
        if cache_key in _urn_search_cache:
            cached_result = _urn_search_cache[cache_key]
            if cached_result is not None:
                logger.debug(f"Cache hit for URN search: {urn}")
                tag_key, tag_value, platform_instance, exists, persisted = cached_result
                return cls(
                    tag_key=tag_key,
                    tag_value=tag_value,
                    platform_instance=platform_instance,
                    exists_in_unity_catalog=exists,
                    persisted=persisted
                )
            else:
                logger.debug(f"Cache hit (None) for URN search: {urn}")
                return None

        logger.debug(f"Cache miss for URN {urn}, querying ElasticSearch")
        
        # Perform actual ElasticSearch query
        mapped_tags = [
            t
            for t in platform_resource_repository.search_by_filter(
                ElasticDocumentQuery.create_from(
                    (
                        PlatformResourceSearchFields.RESOURCE_TYPE,
                        str(UnityCatalogTagPlatformResourceId._RESOURCE_TYPE()),
                    ),
                    (PlatformResourceSearchFields.SECONDARY_KEYS, urn),
                )
            )
        ]
        
        result = None
        if len(mapped_tags) > 0:
            for platform_resource in mapped_tags:
                if (
                    platform_resource.resource_info
                    and platform_resource.resource_info.value
                ):
                    unity_catalog_tag = UnityCatalogTagPlatformResource(
                        **platform_resource.resource_info.value.as_pydantic_object(
                            UnityCatalogTagPlatformResource
                        ).dict()
                    )
                    if (
                        unity_catalog_tag.id.platform_instance
                        == tag_sync_context.platform_instance
                    ):
                        unity_catalog_tag_id = unity_catalog_tag.id
                        unity_catalog_tag_id.exists_in_unity_catalog = True
                        unity_catalog_tag_id.persisted = True
                        result = unity_catalog_tag_id
                        break
        
        # Cache the result
        if result:
            cached_tuple = (result.tag_key, result.tag_value, result.platform_instance, 
                          result.exists_in_unity_catalog, result.persisted)
            _urn_search_cache[cache_key] = cached_tuple
        else:
            _urn_search_cache[cache_key] = None
        
        return result

    @classmethod
    def from_datahub_urn(
        cls,
        urn: str,
        platform_resource_repository: PlatformResourceRepository,
        tag_sync_context: UnityCatalogTagSyncContext,
        graph: DataHubGraph,
    ) -> "UnityCatalogTagPlatformResourceId":
        """
        Creates a UnityCatalogTagPlatformResourceId from a DataHub URN.
        """
        existing_platform_resource_id = cls.search_by_urn(
            urn, platform_resource_repository, tag_sync_context
        )
        if existing_platform_resource_id:
            return existing_platform_resource_id

        new_unity_catalog_tag_id = cls.generate_tag_id(graph, tag_sync_context, urn)
        if new_unity_catalog_tag_id:
            resource_key = platform_resource_repository.get(
                new_unity_catalog_tag_id.to_platform_resource_key()
            )
            if resource_key:
                new_unity_catalog_tag_id.exists_in_unity_catalog = True
            return new_unity_catalog_tag_id
        raise ValueError(f"Unable to create Unity Catalog tag ID from DataHub URN: {urn}")

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
        platform_resource_repository: PlatformResourceRepository,
        managed_by_datahub: bool = False,
    ) -> "UnityCatalogTagPlatformResource":
        """Get platform resource from DataHub with simple caching."""
        
        # Create cache key
        cache_key = f"{unity_catalog_tag_id.tag_key}:{unity_catalog_tag_id.tag_value}:{unity_catalog_tag_id.platform_instance}"
        
        # Check cache first
        if cache_key in _platform_resource_cache:
            cached_dict = _platform_resource_cache[cache_key]
            if cached_dict is not None:
                logger.debug(f"Cache hit for platform resource: {cache_key}")
                return cls(**cached_dict)
            else:
                logger.debug(f"Cache hit (default) for platform resource: {cache_key}")
                return cls(
                    id=unity_catalog_tag_id,
                    datahub_urns=LinkedResourceSet(urns=[]),
                    managed_by_datahub=managed_by_datahub,
                    allowed_values=None,
                )
        
        logger.debug(f"Cache miss for platform resource {cache_key}, querying ElasticSearch")
        
        platform_resources = [
            r
            for r in platform_resource_repository.search_by_filter(
                ElasticDocumentQuery.create_from(
                    (
                        PlatformResourceSearchFields.RESOURCE_TYPE,
                        str(UnityCatalogTagPlatformResourceId._RESOURCE_TYPE()),
                    ),
                    (
                        PlatformResourceSearchFields.PRIMARY_KEY,
                        f"{unity_catalog_tag_id.tag_key}/{unity_catalog_tag_id.tag_value}",
                    ),
                )
            )
        ]
        
        result = None
        if len(platform_resources) == 1:
            platform_resource: PlatformResource = platform_resources[0]
            if platform_resource.resource_info and platform_resource.resource_info.value:
                result = UnityCatalogTagPlatformResource(
                    **platform_resource.resource_info.value.as_pydantic_object(
                        UnityCatalogTagPlatformResource
                    ).dict()
                )
        else:
            for platform_resource in platform_resources:
                if platform_resource.resource_info and platform_resource.resource_info.value:
                    unity_catalog_tag = UnityCatalogTagPlatformResource(
                        **platform_resource.resource_info.value.as_pydantic_object(
                            UnityCatalogTagPlatformResource
                        ).dict()
                    )
                    if unity_catalog_tag.id.platform_instance == unity_catalog_tag_id.platform_instance:
                        result = unity_catalog_tag
                        break
        
        if result is None:
            result = cls(
                id=unity_catalog_tag_id,
                datahub_urns=LinkedResourceSet(urns=[]),
                managed_by_datahub=managed_by_datahub,
                allowed_values=None,
            )
            # Cache the default result as None to indicate no actual platform resource found
            _platform_resource_cache[cache_key] = None
        else:
            # Cache the actual result
            _platform_resource_cache[cache_key] = result.dict()
        
        return result