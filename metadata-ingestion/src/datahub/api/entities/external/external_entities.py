import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Protocol, Union

import cachetools
from pydantic import BaseModel

from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
    PlatformResourceSearchFields,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import PlatformResourceUrn, Urn
from datahub.utilities.search_utils import ElasticDocumentQuery

logger = logging.getLogger(__name__)


class SyncContext(Protocol):
    """Protocol defining the interface for platform-specific sync context objects.

    All sync context objects must have a platform_instance attribute that can be None.
    """

    platform_instance: Optional[str]


class PlatformResourceRepository(ABC):
    CACHE_SIZE = 1000

    def __init__(self, graph: DataHubGraph):
        self.graph = graph
        # Separate caches for different operations with their own statistics
        self.search_urn_cache = cachetools.LRUCache(maxsize=PlatformResourceRepository.CACHE_SIZE)
        self.get_entity_cache = cachetools.LRUCache(maxsize=PlatformResourceRepository.CACHE_SIZE)
        
        # Statistics tracking for search_entity_by_urn cache
        self.search_urn_cache_hits = 0
        self.search_urn_cache_misses = 0
        
        # Statistics tracking for get_entity_from_datahub cache
        self.get_entity_cache_hits = 0
        self.get_entity_cache_misses = 0

    def search_by_filter(
        self, query: ElasticDocumentQuery, add_to_cache: bool = True
    ) -> Iterable[PlatformResource]:
        results = PlatformResource.search_by_filters(self.graph, query)
        for platform_resource in results:
            if add_to_cache:
                # Add to get_entity_cache for general platform resource caching
                self.get_entity_cache[platform_resource.id] = platform_resource
            yield platform_resource

    def create(self, platform_resource: PlatformResource) -> None:
        platform_resource.to_datahub(self.graph)
        self.get_entity_cache[platform_resource.id] = platform_resource

    def get(self, key: PlatformResourceKey) -> Optional[PlatformResource]:
        return self.get_entity_cache.get(key.id)

    def delete(self, key: PlatformResourceKey) -> None:
        self.graph.delete_entity(urn=PlatformResourceUrn(key.id).urn(), hard=True)
        if key.id in self.get_entity_cache:
            del self.get_entity_cache[key.id]

    @abstractmethod
    def get_resource_type(self) -> str:
        """Get the platform-specific resource type for filtering.

        Returns:
            Resource type string (e.g., 'UnityCatalogTagPlatformResource')
        """
        pass

    @abstractmethod
    def get_entity_class(self) -> type:
        """Get the platform-specific entity class for deserialization.

        Returns:
            Entity class type (e.g., UnityCatalogTagPlatformResource)
        """
        pass

    @abstractmethod
    def build_primary_key(self, entity_id: "ExternalEntityId") -> str:
        """Build the primary key for platform resource search.

        Args:
            entity_id: The external entity ID

        Returns:
            Primary key string for search
        """
        pass

    @abstractmethod
    def create_default_entity(
        self, entity_id: "ExternalEntityId", managed_by_datahub: bool
    ) -> Any:
        """Create a default entity when none found in DataHub.

        Args:
            entity_id: The external entity ID
            managed_by_datahub: Whether the entity is managed by DataHub

        Returns:
            Default entity instance
        """
        pass

    @abstractmethod
    def extract_platform_instance(self, sync_context: SyncContext) -> Optional[str]:
        """Extract platform instance from sync context.

        Args:
            sync_context: Platform-specific sync context implementing SyncContext protocol

        Returns:
            Platform instance string or None
        """
        pass

    @abstractmethod
    def configure_entity_for_return(
        self, entity_id: "ExternalEntityId"
    ) -> "ExternalEntityId":
        """Configure entity ID for return (set flags, etc.).

        Args:
            entity_id: The entity ID to configure

        Returns:
            Configured entity ID
        """
        pass

    def search_entity_by_urn(
        self, urn: str, sync_context: SyncContext
    ) -> Optional["ExternalEntityId"]:
        """Search for existing external entity by URN with caching.

        Args:
            urn: The URN to search for
            sync_context: Platform-specific sync context implementing SyncContext protocol

        Returns:
            External entity ID if found, None otherwise
        """
        platform_instance = self.extract_platform_instance(sync_context)
        cache_key = f"search_urn:{self.get_resource_type()}:{urn}:{platform_instance}"

        # Check cache first
        if cache_key in self.search_urn_cache:
            cached_result = self.search_urn_cache[cache_key]
            self.search_urn_cache_hits += 1
            logger.debug(f"Cache hit for URN search: {cache_key}")
            return cached_result

        self.search_urn_cache_misses += 1
        logger.debug(
            f"Cache miss for URN {urn} with platform instance {platform_instance}"
        )

        mapped_entities = [
            t
            for t in self.search_by_filter(
                ElasticDocumentQuery.create_from(
                    (
                        PlatformResourceSearchFields.RESOURCE_TYPE,
                        self.get_resource_type(),
                    ),
                    (PlatformResourceSearchFields.SECONDARY_KEYS, urn),
                ),
                add_to_cache=False,  # We'll cache the result ourselves
            )
        ]

        result = None
        if len(mapped_entities) > 0:
            entity_class = self.get_entity_class()

            for platform_resource in mapped_entities:
                if (
                    platform_resource.resource_info
                    and platform_resource.resource_info.value
                ):
                    entity = entity_class(
                        **platform_resource.resource_info.value.as_pydantic_object(
                            entity_class
                        ).dict()  # type: ignore[attr-defined]
                    )
                    # Check if platform instance matches
                    entity_platform_instance = entity.id.platform_instance
                    if entity_platform_instance == platform_instance:
                        entity_id = entity.id
                        result = self.configure_entity_for_return(entity_id)
                        break

        # Cache the result (even if None)
        self.search_urn_cache[cache_key] = result
        return result

    def get_entity_from_datahub(
        self, entity_id: "ExternalEntityId", managed_by_datahub: bool = False
    ) -> Any:
        """Get external entity from DataHub with caching.

        Args:
            entity_id: The external entity ID to retrieve
            managed_by_datahub: Whether the entity is managed by DataHub

        Returns:
            External entity if found or created
        """
        primary_key = self.build_primary_key(entity_id)
        platform_instance = entity_id.platform_instance
        cache_key = (
            f"get_entity:{self.get_resource_type()}:{primary_key}:{platform_instance}"
        )

        # Check cache first
        cached_result = self.get_entity_cache.get(cache_key)
        if cached_result is not None:
            self.get_entity_cache_hits += 1
            logger.debug(f"Cache hit for entity get: {cache_key}")
            return cached_result

        self.get_entity_cache_misses += 1
        logger.debug(f"Cache miss for entity {entity_id}")

        platform_resources = [
            r
            for r in self.search_by_filter(
                ElasticDocumentQuery.create_from(
                    (
                        PlatformResourceSearchFields.RESOURCE_TYPE,
                        self.get_resource_type(),
                    ),
                    (
                        PlatformResourceSearchFields.PRIMARY_KEY,
                        primary_key,
                    ),
                ),
                add_to_cache=False,  # We'll cache the result ourselves
            )
        ]

        entity_class = self.get_entity_class()
        result = None

        if len(platform_resources) == 1:
            platform_resource = platform_resources[0]
            if (
                platform_resource.resource_info
                and platform_resource.resource_info.value
            ):
                result = entity_class(
                    **platform_resource.resource_info.value.as_pydantic_object(
                        entity_class
                    ).dict()  # type: ignore[attr-defined]
                )
        else:
            # Handle multiple matches - find the one with matching platform instance
            target_platform_instance = entity_id.platform_instance
            for platform_resource in platform_resources:
                if (
                    platform_resource.resource_info
                    and platform_resource.resource_info.value
                ):
                    entity = entity_class(
                        **platform_resource.resource_info.value.as_pydantic_object(
                            entity_class
                        ).dict()  # type: ignore[attr-defined]
                    )
                    if entity.id.platform_instance == target_platform_instance:
                        result = entity
                        break

        if result is None:
            result = self.create_default_entity(entity_id, managed_by_datahub)

        # Cache the result
        self.get_entity_cache[cache_key] = result
        return result

    def get_entity_cache_info(self) -> Dict[str, Dict[str, int]]:
        """Get cache statistics for entity operations.

        Returns:
            Dictionary containing cache statistics with structure:
            {
                "search_by_urn_cache": {"hits": int, "misses": int, "current_size": int, "max_size": int},
                "get_from_datahub_cache": {"hits": int, "misses": int, "current_size": int, "max_size": int}
            }
        """
        # Return separate statistics for each cache
        return {
            "search_by_urn_cache": {
                "hits": self.search_urn_cache_hits,
                "misses": self.search_urn_cache_misses,
                "current_size": len(self.search_urn_cache),
                "max_size": int(self.search_urn_cache.maxsize),
            },
            "get_from_datahub_cache": {
                "hits": self.get_entity_cache_hits,
                "misses": self.get_entity_cache_misses,
                "current_size": len(self.get_entity_cache),
                "max_size": int(self.get_entity_cache.maxsize),
            },
        }


class GenericPlatformResourceRepository(PlatformResourceRepository):
    """
    Generic implementation of PlatformResourceRepository that provides basic functionality.
    
    This implementation should only be used when no specific platform implementation is available
    and basic repository functionality is needed. It uses default implementations that may not
    be optimal for specific platforms.
    """
    
    def __init__(self, graph: DataHubGraph, resource_type: str = "GenericPlatformResource"):
        super().__init__(graph)
        self._resource_type = resource_type
        
    def get_resource_type(self) -> str:
        return self._resource_type
        
    def get_entity_class(self) -> type:
        # Return a generic Pydantic model class
        return GenericPlatformResource
        
    def build_primary_key(self, entity_id: "ExternalEntityId") -> str:
        # Basic implementation - use the platform resource key's ID
        return entity_id.to_platform_resource_key().id
        
    def create_default_entity(self, entity_id: "ExternalEntityId", managed_by_datahub: bool) -> Any:
        # Return a GenericPlatformResource instance  
        return GenericPlatformResource(
            id=entity_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=managed_by_datahub
        )
        
    def extract_platform_instance(self, sync_context: SyncContext) -> Optional[str]:
        return sync_context.platform_instance
        
    def configure_entity_for_return(self, entity_id: "ExternalEntityId") -> "ExternalEntityId":
        # No specific configuration needed for generic implementation
        return entity_id


class ExternalEntityId:
    """
    ExternalEntityId is a unique
    identifier for an ExternalEntity.
    """
    
    platform_instance: Optional[str] = None

    @abstractmethod
    def to_platform_resource_key(self) -> PlatformResourceKey:
        """
        Converts the ExternalEntityId to a PlatformResourceKey.
        """
        pass


class CaseSensitivity(Enum):
    UPPER = "upper"
    LOWER = "lower"
    MIXED = "mixed"

    @staticmethod
    def detect_case_sensitivity(value: str) -> "CaseSensitivity":
        if value.isupper():
            return CaseSensitivity.UPPER
        elif value.islower():
            return CaseSensitivity.LOWER
        return CaseSensitivity.MIXED

    @staticmethod
    def detect_for_many(values: List[str]) -> "CaseSensitivity":
        """
        Detects the case sensitivity for a list of strings.
        Returns CaseSensitivity.MIXED if the case sensitivity is mixed.
        """
        if len(values) == 0:
            return CaseSensitivity.MIXED

        if all(
            CaseSensitivity.detect_case_sensitivity(value) == CaseSensitivity.UPPER
            for value in values
        ):
            return CaseSensitivity.UPPER
        elif all(
            CaseSensitivity.detect_case_sensitivity(value) == CaseSensitivity.LOWER
            for value in values
        ):
            return CaseSensitivity.LOWER
        return CaseSensitivity.MIXED


class LinkedResourceSet(BaseModel):
    """
    A LinkedResourceSet is a set of DataHub URNs that are linked to an ExternalEntity.
    """

    urns: List[str]

    def _has_conflict(self, urn: Urn) -> bool:
        """
        Detects if the urn is safe to add into the set
        This is used to detect conflicts between DataHub URNs that are linked to
        the same ExternalEntity.
        e.g. Case sensitivity of URNs
        Mixing tags and terms in the same set etc.
        Return True if the urn is not safe to add into the set, else False.
        If the urn is already in the set, we don't need to add it again, but
        that is not a conflict.
        """
        if urn.urn() in self.urns:
            return False

        # Detect the entity_type of the urns in the existing set
        detected_entity_type = None
        for existing_urn in self.urns:
            try:
                parsed_urn = Urn.from_string(existing_urn)
                entity_type = parsed_urn.entity_type
                if detected_entity_type is None:
                    detected_entity_type = entity_type
                elif detected_entity_type != entity_type:
                    logger.warning(
                        f"Detected entity_type {detected_entity_type} is not equals to {entity_type}"
                    )
                    return True
            except ValueError:
                # Not a valid URN
                logger.warning(f"Invalid URN {existing_urn} in LinkedResourceSet")
                return True
        try:
            parsed_urn = urn
            if (
                detected_entity_type is not None
                and parsed_urn.entity_type != detected_entity_type
            ):
                logger.warning(
                    f"Detected entity_type {detected_entity_type} is not equals to parsed_urn's entity_type: {parsed_urn.entity_type}"
                )
                return True
        except ValueError:
            # Not a valid URN
            logger.warning(f"Invalid URN: {urn} in LinkedResourceSet")
            return True
        return False

    def add(self, urn: Union[str, Urn]) -> bool:
        """
        Adds a URN to the set.
        Returns True if the URN was added, False if it was already in the set.
        Raises a ValueError if the URN is in conflict with the existing set.
        """
        # Deduplicate the URNs if we have somehow duplicate items from concurrent runs
        self.urns = list(set(self.urns))
        if isinstance(urn, str):
            urn = Urn.from_string(urn)
        if self._has_conflict(urn):
            raise ValueError(f"Conflict detected when adding URN {urn} to the set")
        if urn.urn() not in self.urns:
            self.urns.append(urn.urn())
            return True
        return False


class ExternalEntity:
    """
    An ExternalEntity is a representation of an entity that external to DataHub
    but could be linked to one or more DataHub entities.
    """

    @abstractmethod
    def is_managed_by_datahub(self) -> bool:
        """
        Returns whether the entity is managed by DataHub.
        """
        pass

    @abstractmethod
    def datahub_linked_resources(self) -> LinkedResourceSet:
        """
        Returns the URNs of the DataHub entities linked to the external entity.
        Empty list if no linked entities.
        """
        pass

    @abstractmethod
    def as_platform_resource(self) -> PlatformResource:
        """
        Converts the ExternalEntity to a PlatformResource.
        """
        pass

    @abstractmethod
    def get_id(self) -> ExternalEntityId:
        """
        Returns the ExternalEntityId for the ExternalEntity.
        """
        pass


@dataclass
class MissingExternalEntity(ExternalEntity):
    id: ExternalEntityId

    def is_managed_by_datahub(self) -> bool:
        return False

    def datahub_linked_resources(self) -> LinkedResourceSet:
        return LinkedResourceSet(urns=[])

    def as_platform_resource(self) -> Optional[PlatformResource]:  # type: ignore[override]
        return None

    def get_id(self) -> ExternalEntityId:
        return self.id


class ExternalSystem:
    @abstractmethod
    def exists(self, external_entity_id: ExternalEntityId) -> bool:
        """
        Returns whether the ExternalEntityId exists in the external system.
        """
        pass

    @abstractmethod
    def get(
        self,
        external_entity_id: ExternalEntityId,
        platform_resource_repository: PlatformResourceRepository,
    ) -> Optional[ExternalEntity]:
        """
        Returns the ExternalEntity for the ExternalEntityId.
        Uses the platform resource repository to enrich the ExternalEntity with DataHub URNs.
        """
        pass


class GenericPlatformResource(BaseModel, ExternalEntity):
    """Generic platform resource that works with any platform when no specific implementation exists."""
    
    id: ExternalEntityId
    datahub_urns: LinkedResourceSet = LinkedResourceSet(urns=[])
    managed_by_datahub: bool = False
    
    class Config:
        arbitrary_types_allowed = True
    
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
