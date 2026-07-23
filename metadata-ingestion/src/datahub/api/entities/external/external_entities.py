import logging
import threading
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import (
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
from datahub.ingestion.api.report import SupportsAsObj
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import PlatformResourceUrn, Urn
from datahub.utilities.search_utils import ElasticDocumentQuery

logger = logging.getLogger(__name__)

# Type variables for generic repository
TExternalEntityId = TypeVar("TExternalEntityId", bound="ExternalEntityId")
TExternalEntity = TypeVar("TExternalEntity", bound="ExternalEntity")


@dataclass(frozen=True)
class UrnCacheKey:
    """Typed compound key for URN search cache.

    This eliminates fragile string parsing and provides type safety for cache operations.
    Using dataclass with frozen=True makes it immutable and hashable.
    """

    urn: str
    platform_instance: Optional[str]

    def __str__(self) -> str:
        """String representation for debugging purposes only."""
        return (
            f"UrnCacheKey(urn={self.urn}, platform_instance={self.platform_instance})"
        )


class SyncContext(Protocol):
    """Protocol defining the interface for platform-specific sync context objects.

    All sync context objects must have a platform_instance attribute that can be None.
    """

    platform_instance: Optional[str]


class PlatformResourceRepository(
    SupportsAsObj, ABC, Generic[TExternalEntityId, TExternalEntity]
):
    CACHE_SIZE = 1000

    # Subclasses should override this with their specific entity class
    entity_class: Type[TExternalEntity]

    def __init__(self, graph: DataHubGraph, platform_instance: Optional[str] = None):
        self.graph = graph
        self.platform_instance = platform_instance

        # Extract the entity class from generic type parameters
        # self.entity_class = typing.get_args(self.__class__.__orig_bases__[0])[1]
        self.entity_class = typing.get_args(get_original_bases(self.__class__)[0])[1]

        # Two-tier cache architecture for efficient external entity management
        # URN search cache: maps UrnCacheKey -> ExternalEntityId
        self.urn_search_cache: cachetools.LRUCache[
            UrnCacheKey, Optional[TExternalEntityId]
        ] = cachetools.LRUCache(maxsize=PlatformResourceRepository.CACHE_SIZE)
        # External entity cache: maps platform_resource_key.id -> ExternalEntity
        self.external_entity_cache: cachetools.LRUCache[str, TExternalEntity] = (
            cachetools.LRUCache(maxsize=PlatformResourceRepository.CACHE_SIZE)
        )

        # Statistics tracking - simple integers following DataHub report patterns
        self.urn_search_cache_hits = 0
        self.urn_search_cache_misses = 0
        self.external_entity_cache_hits = 0
        self.external_entity_cache_misses = 0

        # Error tracking for cache operations
        self.cache_update_errors = 0
        self.cache_invalidation_errors = 0
        self.entity_creation_errors = 0
        self.cache_key_parsing_errors = 0

        # Thread safety infrastructure
        # Use RLock to allow recursive acquisition within the same thread
        self._cache_lock = threading.RLock()

    def search_by_filter(
        self, query: ElasticDocumentQuery, add_to_cache: bool = True
    ) -> Iterable[PlatformResource]:
        results = PlatformResource.search_by_filters(self.graph, query)
        # Note: add_to_cache parameter is kept for API compatibility but ignored
        # since we no longer cache raw PlatformResource objects
        for platform_resource in results:
            yield platform_resource

    def create(self, platform_resource: PlatformResource) -> None:
        """Create platform resource in DataHub with atomic cache operations.

        This method ensures thread-safe, atomic updates across both caches.
        """
        # First, perform the DataHub ingestion outside the cache lock
        platform_resource.to_datahub(self.graph)

        # Now perform atomic cache operations
        with self._cache_lock:
            # Cache the transformed entity with correct flags after ingestion and update related caches
            if (
                platform_resource.resource_info
                and platform_resource.resource_info.value
            ):
                try:
                    # Extract the original entity from the serialized resource value
                    entity_obj = (
                        platform_resource.resource_info.value.as_pydantic_object(
                            self.entity_class
                        )
                    )
                    entity = self.entity_class(**entity_obj.model_dump())

                    # Create updated entity ID with persisted=True
                    entity_id = entity.get_id()
                    if hasattr(entity_id, "model_dump"):
                        entity_id_data = entity_id.model_dump()
                        entity_id_data["persisted"] = True

                        # Create new entity ID with updated flags
                        updated_entity_id = type(entity_id)(**entity_id_data)

                        # Update the entity with the new ID (immutable update)
                        entity_data = entity.model_dump()  # type: ignore[attr-defined]
                        entity_data["id"] = updated_entity_id
                        updated_entity = type(entity)(**entity_data)

                        # Cache the updated entity in the external entity cache
                        # Use the same cache key that get_entity_from_datahub uses
                        updated_platform_resource_key = (
                            updated_entity_id.to_platform_resource_key()
                        )
                        self.external_entity_cache[updated_platform_resource_key.id] = (
                            updated_entity
                        )

                        # Update URN search cache for any URNs associated with this entity
                        # This ensures that future URN searches will find the newly created entity
                        if (
                            platform_resource.resource_info
                            and platform_resource.resource_info.secondary_keys
                        ):
                            for (
                                secondary_key
                            ) in platform_resource.resource_info.secondary_keys:
                                # Create typed compound cache key
                                urn_cache_key = UrnCacheKey(
                                    urn=secondary_key,
                                    platform_instance=self.platform_instance,
                                )
                                # Cache the updated entity ID so URN searches will find it
                                self.urn_search_cache[urn_cache_key] = cast(
                                    TExternalEntityId, updated_entity_id
                                )

                        # Also check if there are any None cache entries that should be invalidated
                        # Look for cache entries that were previously searched but not found
                        stale_cache_keys = []
                        if (
                            platform_resource.resource_info
                            and platform_resource.resource_info.secondary_keys
                        ):
                            for cache_key, cached_value in list(
                                self.urn_search_cache.items()
                            ):
                                if cached_value is None:
                                    # Direct attribute access on typed key - no parsing needed!
                                    try:
                                        # Check if this cache key refers to this entity
                                        if (
                                            cache_key.platform_instance
                                            == self.platform_instance
                                            and cache_key.urn
                                            in platform_resource.resource_info.secondary_keys
                                        ):
                                            stale_cache_keys.append(cache_key)
                                    except Exception as cache_key_error:
                                        # Track cache key processing errors and log them
                                        self.cache_key_parsing_errors += 1
                                        logger.warning(
                                            f"Failed to process cache key '{cache_key}' during stale cache invalidation: {cache_key_error}"
                                        )
                                        continue

                        # Remove stale None cache entries and replace with the actual entity ID
                        for stale_key in stale_cache_keys:
                            try:
                                del self.urn_search_cache[stale_key]
                                self.urn_search_cache[stale_key] = cast(
                                    TExternalEntityId, updated_entity_id
                                )
                            except Exception as invalidation_error:
                                # Track cache invalidation errors
                                self.cache_invalidation_errors += 1
                                logger.warning(
                                    f"Failed to invalidate stale cache entry '{stale_key}': {invalidation_error}"
                                )

                except Exception as cache_error:
                    # Track cache update errors and log them
                    self.cache_update_errors += 1
                    logger.error(
                        f"Failed to update caches after entity creation for resource {platform_resource.id}: {cache_error}"
                    )

    def get(self, key: PlatformResourceKey) -> Optional[PlatformResource]:
        """Retrieve platform resource by performing a direct DataHub query.

        Note: This method no longer uses caching since we eliminated the
        platform_resource_cache in favor of the more useful external_entity_cache.
        """
        # Query DataHub directly for the platform resource
        platform_resources = list(
            self.search_by_filter(
                ElasticDocumentQuery.create_from(
                    (
                        PlatformResourceSearchFields.RESOURCE_TYPE,
                        self.get_resource_type(),
                    ),
                    (PlatformResourceSearchFields.PRIMARY_KEY, key.primary_key),
                ),
                add_to_cache=False,
            )
        )

        # Find matching resource by ID
        for platform_resource in platform_resources:
            if platform_resource.id == key.id:
                return platform_resource
        return None

    def delete(self, key: PlatformResourceKey) -> None:
        """Thread-safe atomic deletion from DataHub and all caches."""
        # First, perform the DataHub deletion outside the cache lock
        self.graph.delete_entity(urn=PlatformResourceUrn(key.id).urn(), hard=True)

        # Now perform atomic cache cleanup
        with self._cache_lock:
            # Clear external entity cache
            if key.id in self.external_entity_cache:
                del self.external_entity_cache[key.id]

            # Note: We intentionally do not clear URN search cache entries here
            # The URN cache will naturally expire via LRU eviction, and clearing
            # stale entries would require expensive O(n) iteration over all cache keys.
            # Stale cache entries pointing to deleted entities will return None on
            # subsequent lookups, which is the correct behavior.

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

    def search_entity_by_urn(self, urn: str) -> Optional[TExternalEntityId]:
        """Search for existing external entity by URN with thread-safe caching.

        Args:
            urn: The URN to search for

        Returns:
            External entity ID if found, None otherwise
        """
        # Create typed compound cache key
        cache_key = UrnCacheKey(urn=urn, platform_instance=self.platform_instance)

        # Thread-safe cache check
        with self._cache_lock:
            if cache_key in self.urn_search_cache:
                cached_result = self.urn_search_cache[cache_key]
                # Update statistics within cache lock following DataHub patterns
                self.urn_search_cache_hits += 1
                logger.debug(f"Cache hit for URN search: {cache_key}")
                return cached_result

            self.urn_search_cache_misses += 1

        logger.debug(
            f"Cache miss for URN {urn} with platform instance {self.platform_instance}"
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
                    entity = self.entity_class(**entity_obj.model_dump())
                    # Check if platform instance matches
                    entity_id = entity.get_id()
                    if entity_id.platform_instance == self.platform_instance:
                        # Create a new entity ID with the correct state instead of mutating
                        # All our entity IDs are Pydantic models, so we can use model_dump() method
                        entity_data = entity_id.model_dump()
                        entity_data["persisted"] = (
                            True  # This entity was found in DataHub
                        )
                        result = cast(TExternalEntityId, type(entity_id)(**entity_data))
                        break

        # Thread-safe cache update of the result (even if None)
        with self._cache_lock:
            self.urn_search_cache[cache_key] = result
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
        platform_resource_key = entity_id.to_platform_resource_key()
        cache_key = platform_resource_key.id

        # Thread-safe cache check
        with self._cache_lock:
            cached_result = self.external_entity_cache.get(cache_key)
            if cached_result is not None:
                # Update statistics within cache lock following DataHub patterns
                self.external_entity_cache_hits += 1
                logger.debug(f"Cache hit for get_entity_from_datahub: {cache_key}")
                return cached_result

            # Cache miss - update statistics within cache lock
            self.external_entity_cache_misses += 1
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
                        platform_resource_key.primary_key,
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
                result = self.entity_class(**entity_obj.model_dump())
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
                    entity = self.entity_class(**entity_obj.model_dump())
                    if entity.get_id().platform_instance == target_platform_instance:
                        result = entity
                        break

        if result is None:
            try:
                result = self.create_default_entity(entity_id, managed_by_datahub)
            except Exception as create_error:
                # Track entity creation errors
                self.entity_creation_errors += 1
                logger.error(
                    f"Failed to create default entity for {entity_id}: {create_error}"
                )
                raise

        # Thread-safe cache update
        with self._cache_lock:
            self.external_entity_cache[cache_key] = result
        return result

    def as_obj(self) -> dict:
        """Implementation of SupportsAsObj protocol for automatic report serialization.

        Returns cache statistics and error metrics on demand when the repository is included in a report.
        This eliminates the need for manual cache statistics collection.

        Returns:
            Dictionary containing cache statistics and error metrics with structure:
            {
                "search_by_urn_cache": {"hits": int, "misses": int, "current_size": int, "max_size": int},
                "external_entity_cache": {"hits": int, "misses": int, "current_size": int, "max_size": int},
                "errors": {"cache_updates": int, "cache_invalidations": int, "entity_creations": int, "cache_key_parsing": int}
            }
        """
        return {
            "search_by_urn_cache": {
                "hits": self.urn_search_cache_hits,
                "misses": self.urn_search_cache_misses,
                "current_size": len(self.urn_search_cache),
                "max_size": int(self.urn_search_cache.maxsize),
            },
            "external_entity_cache": {
                "hits": self.external_entity_cache_hits,
                "misses": self.external_entity_cache_misses,
                "current_size": len(self.external_entity_cache),
                "max_size": int(self.external_entity_cache.maxsize),
            },
            "errors": {
                "cache_updates": self.cache_update_errors,
                "cache_invalidations": self.cache_invalidation_errors,
                "entity_creations": self.entity_creation_errors,
                "cache_key_parsing": self.cache_key_parsing_errors,
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
        cls, entity_id: "ExternalEntityId", managed_by_datahub: bool
    ) -> "ExternalEntity":
        """
        Create a default entity instance when none found in DataHub.

        Args:
            entity_id: The external entity ID (concrete implementations can expect their specific types)
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
