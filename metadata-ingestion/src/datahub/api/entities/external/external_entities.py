import logging
from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Iterable, List, Optional, Union

import cachetools
from pydantic import BaseModel

from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import PlatformResourceUrn, Urn
from datahub.utilities.search_utils import ElasticDocumentQuery

logger = logging.getLogger(__name__)


class PlatformResourceRepository:
    def __init__(self, graph: DataHubGraph):
        self.graph = graph
        self.cache: cachetools.TTLCache = cachetools.TTLCache(maxsize=1000, ttl=60 * 5)

    def search_by_filter(
        self, filter: ElasticDocumentQuery, add_to_cache: bool = True
    ) -> Iterable[PlatformResource]:
        results = PlatformResource.search_by_filters(self.graph, filter)
        for platform_resource in results:
            if add_to_cache:
                self.cache[platform_resource.id] = platform_resource
            yield platform_resource

    def create(self, platform_resource: PlatformResource) -> None:
        platform_resource.to_datahub(self.graph)
        self.cache[platform_resource.id] = platform_resource

    def get(self, key: PlatformResourceKey) -> Optional[PlatformResource]:
        return self.cache.get(key.id)

    def delete(self, key: PlatformResourceKey) -> None:
        self.graph.delete_entity(urn=PlatformResourceUrn(key.id).urn(), hard=True)
        del self.cache[key.id]


class ExternalEntityId:
    """
    ExternalEntityId is a unique
    identifier for an ExternalEntity.
    """

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


class LinkedResourceSet(BaseModel):
    """
    A LinkedResourceSet is a set of DataHub URNs that are linked to an ExternalEntity.
    """

    urns: List[str]

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def detect_case_sensitivity(urn: Urn) -> CaseSensitivity:
        if urn.get_entity_id_as_string().isupper():
            return CaseSensitivity.UPPER
        elif urn.get_entity_id_as_string().islower():
            return CaseSensitivity.LOWER
        return CaseSensitivity.MIXED

    def _detect_case_sensitivity(self) -> Optional[CaseSensitivity]:
        """
        Detects the case sensitivity of the URNs in the set.
        """
        if len(self.urns) == 0:
            return CaseSensitivity.MIXED

        if all(
            LinkedResourceSet.detect_case_sensitivity(Urn.from_string(urn))
            == CaseSensitivity.UPPER
            for urn in self.urns
        ):
            return CaseSensitivity.UPPER
        elif all(
            LinkedResourceSet.detect_case_sensitivity(Urn.from_string(urn))
            == CaseSensitivity.LOWER
            for urn in self.urns
        ):
            return CaseSensitivity.LOWER
        return CaseSensitivity.MIXED

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

        # Detect the family of the urns in the existing set
        detected_family = None
        for existing_urn in self.urns:
            try:
                parsed_urn = Urn.from_string(existing_urn)
                family = parsed_urn.entity_type
                if detected_family is None:
                    detected_family = family
                elif detected_family != family:
                    logger.warning(
                        f"Detected family {detected_family} is not equals to {family}"
                    )
                    return True
            except ValueError:
                # Not a valid URN
                logger.warning(f"Invalid URN {existing_urn} in LinkedResourceSet")
                return True
        try:
            parsed_urn = urn
            if (
                detected_family is not None
                and parsed_urn.entity_type != detected_family
            ):
                logger.warning(
                    f"Detected family {detected_family} is not equals to parsed_urn's family: {parsed_urn.entity_type}"
                )
                return True
        except ValueError:
            # Not a valid URN
            logger.warning(f"Invalid URN: {urn} in LinkedResourceSet")
            return True
        detected_case_sensitivity = self._detect_case_sensitivity()
        if (
            LinkedResourceSet.detect_case_sensitivity(urn) != detected_case_sensitivity
            and detected_case_sensitivity != CaseSensitivity.MIXED
        ):
            logger.warning(
                f"Detected case sensitivity {detected_case_sensitivity} is not equals to {LinkedResourceSet.detect_case_sensitivity(urn)}"
            )
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

    def as_platform_resource(self) -> PlatformResource:
        return None  # type: ignore

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
