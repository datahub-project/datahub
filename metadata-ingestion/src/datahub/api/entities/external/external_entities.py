import logging
import typing
from abc import ABC, abstractmethod
from enum import Enum
from typing import (
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Protocol,
    Type,
    TypeVar,
    Union,
    cast,
)

import cachetools
from pydantic import BaseModel
from typing_extensions import get_original_bases

from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
    PlatformResourceSearchFields,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import PlatformResourceUrn, Urn
from datahub.utilities.search_utils import ElasticDocumentQuery

logger = logging.getLogger(__name__)

# Type variables for generic repository
TExternalEntityId = TypeVar("TExternalEntityId", bound="ExternalEntityId")
TExternalEntity = TypeVar("TExternalEntity", bound="ExternalEntity")


class SyncContext(Protocol):
    """Protocol defining the interface for platform-specific sync context objects.

    All sync context objects must have a platform_instance attribute that can be None.
    """

    platform_instance: Optional[str]


class PlatformResourceRepository(ABC, Generic[TExternalEntityId, TExternalEntity]):
    CACHE_SIZE = 1000

    # Subclasses should override this with their specific entity class
    entity_class: Type[TExternalEntity]

    def __init__(self, graph: DataHubGraph):
        self.graph = graph

        # Extract the entity class from generic type parameters
        # self.entity_class = typing.get_args(self.__class__.__orig_bases__[0])[1]
        self.entity_class = typing.get_args(get_original_bases(self.__class__)[0])[1]

        # Two main caches for different data types
        self.entity_id_cache: cachetools.LRUCache = cachetools.LRUCache(
            maxsize=PlatformResourceRepository.CACHE_SIZE
        )
        self.entity_object_cache: cachetools.LRUCache = cachetools.LRUCache(
            maxsize=PlatformResourceRepository.CACHE_SIZE
        )

        # Statistics tracking for entity ID cache (URN searches)
        self.entity_id_cache_hits = 0
        self.entity_id_cache_misses = 0

        # Statistics tracking for entity object cache (full entities)
        self.entity_object_cache_hits = 0
        self.entity_object_cache_misses = 0

    def search_by_filter(
        self, query: ElasticDocumentQuery, add_to_cache: bool = True
    ) -> Iterable[PlatformResource]:
        results = PlatformResource.search_by_filters(self.graph, query)
        for platform_resource in results:
            if add_to_cache:
                # Add to entity object cache (can store any type of object)
                self.entity_object_cache[platform_resource.id] = platform_resource
            yield platform_resource

    def create(self, platform_resource: PlatformResource) -> None:
        platform_resource.to_datahub(self.graph)

        # Update cache with an entity that has correct flags after ingestion
        if platform_resource.resource_info and platform_resource.resource_info.value:
            # Extract the original entity from the serialized resource value
            try:
                entity_obj = platform_resource.resource_info.value.as_pydantic_object(
                    self.entity_class
                )
                entity = self.entity_class(**entity_obj.dict())

                # Create updated entity ID with persisted=True
                entity_id = entity.get_id()
                if hasattr(entity_id, "dict"):
                    entity_id_data = entity_id.dict()
                    entity_id_data["persisted"] = True

                    # Create new entity ID with updated flags
                    updated_entity_id = type(entity_id)(**entity_id_data)

                    # Update the entity with the new ID (immutable update)
                    entity_data = entity.dict()  # type: ignore[attr-defined]
                    entity_data["id"] = updated_entity_id
                    updated_entity = type(entity)(**entity_data)

                    # Cache the updated entity for the key that get_entity_from_datahub uses
                    primary_key = (
                        updated_entity_id.to_platform_resource_key().primary_key
                    )
                    platform_instance = updated_entity_id.platform_instance
                    cache_key = (
                        f"{self.get_resource_type()}:{primary_key}:{platform_instance}"
                    )
                    self.entity_object_cache[cache_key] = updated_entity
            except Exception:
                # If we can't update the entity, just cache the platform resource as before
                pass

        # Always cache the platform resource as well
        self.entity_object_cache[platform_resource.id] = platform_resource

    def get(self, key: PlatformResourceKey) -> Optional[PlatformResource]:
        return self.entity_object_cache.get(key.id)

    def delete(self, key: PlatformResourceKey) -> None:
        self.graph.delete_entity(urn=PlatformResourceUrn(key.id).urn(), hard=True)
        if key.id in self.entity_object_cache:
            del self.entity_object_cache[key.id]

    def get_resource_type(self) -> str:
        """Get the platform-specific resource type for filtering.

        Returns the entity class name, which matches the resource type.

        Returns:
            Resource type string (e.g., 'UnityCatalogTagPlatformResource')
        """
        return self.entity_class.__name__

    def create_default_entity(
        self, entity_id: TExternalEntityId, managed_by_datahub: bool
    ) -> TExternalEntity:
        """Create a default entity when none found in DataHub.

        This method delegates to the entity class's create_default class method
        to avoid circular dependencies and ensure entity creation logic stays
        with the entity class.

        Args:
            entity_id: The external entity ID
            managed_by_datahub: Whether the entity is managed by DataHub

        Returns:
            Default entity instance
        """
        # Call the abstract create_default method on the entity class
        return cast(
            TExternalEntity,
            self.entity_class.create_default(entity_id, managed_by_datahub),
        )

    @abstractmethod
    def extract_platform_instance(self, sync_context: SyncContext) -> Optional[str]:
        """Extract platform instance from sync context.

        Args:
            sync_context: Platform-specific sync context implementing SyncContext protocol

        Returns:
            Platform instance string or None
        """
        pass

    def search_entity_by_urn(
        self, urn: str, sync_context: SyncContext
    ) -> Optional[TExternalEntityId]:
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
        if cache_key in self.entity_id_cache:
            cached_result = self.entity_id_cache[cache_key]
            self.entity_id_cache_hits += 1
            logger.debug(f"Cache hit for URN search: {cache_key}")
            return cached_result

        self.entity_id_cache_misses += 1
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
            for platform_resource in mapped_entities:
                if (
                    platform_resource.resource_info
                    and platform_resource.resource_info.value
                ):
                    entity_obj = (
                        platform_resource.resource_info.value.as_pydantic_object(
                            self.entity_class
                        )
                    )
                    entity = self.entity_class(**entity_obj.dict())
                    # Check if platform instance matches
                    entity_id = entity.get_id()
                    if entity_id.platform_instance == platform_instance:
                        # Create a new entity ID with the correct state instead of mutating
                        # All our entity IDs are Pydantic models, so we can use dict() method
                        entity_data = entity_id.dict()
                        entity_data["persisted"] = (
                            True  # This entity was found in DataHub
                        )
                        result = cast(TExternalEntityId, type(entity_id)(**entity_data))
                        break

        # Cache the result (even if None)
        self.entity_id_cache[cache_key] = result
        return result

    def get_entity_from_datahub(
        self, entity_id: TExternalEntityId, managed_by_datahub: bool = False
    ) -> TExternalEntity:
        """Get external entity from DataHub with caching.

        Args:
            entity_id: The external entity ID to retrieve
            managed_by_datahub: Whether the entity is managed by DataHub

        Returns:
            External entity if found or created
        """
        primary_key = entity_id.to_platform_resource_key().primary_key
        platform_instance = entity_id.platform_instance
        cache_key = f"{self.get_resource_type()}:{primary_key}:{platform_instance}"

        # Check cache first
        cached_result = self.entity_object_cache.get(cache_key)
        if cached_result is not None:
            self.entity_object_cache_hits += 1
            logger.debug(f"Cache hit for get_entity_from_datahub: {cache_key}")
            return cached_result

        self.entity_object_cache_misses += 1
        logger.debug(f"Cache miss for get_entity_from_datahub {entity_id}")

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

        result = None

        if len(platform_resources) == 1:
            platform_resource = platform_resources[0]
            if (
                platform_resource.resource_info
                and platform_resource.resource_info.value
            ):
                entity_obj = platform_resource.resource_info.value.as_pydantic_object(
                    self.entity_class
                )
                result = self.entity_class(**entity_obj.dict())
        elif len(platform_resources) > 1:
            # Handle multiple matches - find the one with matching platform instance
            target_platform_instance = entity_id.platform_instance
            for platform_resource in platform_resources:
                if (
                    platform_resource.resource_info
                    and platform_resource.resource_info.value
                ):
                    entity_obj = (
                        platform_resource.resource_info.value.as_pydantic_object(
                            self.entity_class
                        )
                    )
                    entity = self.entity_class(**entity_obj.dict())
                    if entity.get_id().platform_instance == target_platform_instance:
                        result = entity
                        break

        if result is None:
            result = self.create_default_entity(entity_id, managed_by_datahub)

        # Cache the result
        self.entity_object_cache[cache_key] = result
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
                "hits": self.entity_id_cache_hits,
                "misses": self.entity_id_cache_misses,
                "current_size": len(self.entity_id_cache),
                "max_size": int(self.entity_id_cache.maxsize),
            },
            "get_from_datahub_cache": {
                "hits": self.entity_object_cache_hits,
                "misses": self.entity_object_cache_misses,
                "current_size": len(self.entity_object_cache),
                "max_size": int(self.entity_object_cache.maxsize),
            },
        }


class ExternalEntityId(BaseModel):
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


class ExternalEntity(BaseModel):
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

    @classmethod
    @abstractmethod
    def create_default(
        cls, entity_id: ExternalEntityId, managed_by_datahub: bool
    ) -> "ExternalEntity":
        """
        Create a default entity instance when none found in DataHub.

        Args:
            entity_id: The external entity ID (concrete implementations use specific types)
            managed_by_datahub: Whether the entity is managed by DataHub

        Returns:
            Default entity instance
        """
        pass


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

    @classmethod
    def create_default(
        cls, entity_id: ExternalEntityId, managed_by_datahub: bool
    ) -> "MissingExternalEntity":
        """Create a missing external entity."""
        return cls(id=entity_id)


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
