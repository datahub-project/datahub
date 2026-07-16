import logging
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from datahub.ingestion.source.aws.platform_resource_repository import (
        GluePlatformResourceRepository,
    )

from pydantic import BaseModel

from datahub.api.entities.external.external_entities import (
    ExternalEntity,
    ExternalEntityId,
    LinkedResourceSet,
)
from datahub.api.entities.external.lake_formation_external_entites import (
    LakeFormationTag,
)
from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
)
from datahub.metadata.urns import TagUrn
from datahub.utilities.urns.urn import Urn

logger = logging.getLogger(__name__)


class LakeFormationTagSyncContext(BaseModel):
    # it is intentionally empty
    platform_instance: Optional[str] = None
    catalog: Optional[str] = None

    # Making it compatible with SyncContext interface
    def get_platform_instance(self) -> Optional[str]:
        return self.platform_instance


class LakeFormationTagPlatformResourceId(ExternalEntityId):
    """
    A LakeFormationTag is a unique identifier for a Lakeformation tag.
    """

    tag_key: str
    tag_value: Optional[str] = None
    platform_instance: Optional[str] = None
    catalog: Optional[str] = None
    exists_in_lake_formation: bool = False
    persisted: bool = False

    # this is a hack to make sure the property is a string and not private pydantic field
    @staticmethod
    def _RESOURCE_TYPE() -> str:
        return "LakeFormationTagPlatformResource"

    def to_platform_resource_key(self) -> PlatformResourceKey:
        return PlatformResourceKey(
            platform="glue",
            resource_type=str(LakeFormationTagPlatformResourceId._RESOURCE_TYPE()),
            primary_key=f"{self.catalog}.{self.tag_key}:{self.tag_value}"
            if self.catalog
            else f"{self.tag_key}:{self.tag_value}",
            platform_instance=self.platform_instance,
        )

    @classmethod
    def get_or_create_from_tag(
        cls,
        tag: LakeFormationTag,
        platform_resource_repository: "GluePlatformResourceRepository",
        exists_in_lake_formation: bool = False,
        catalog_id: Optional[str] = None,
    ) -> "LakeFormationTagPlatformResourceId":
        """
        Creates a LakeFormationTagPlatformResourceId from a LakeFormationTag.
        """

        # Use catalog_id if provided, otherwise fall back to repository catalog
        effective_catalog = catalog_id or platform_resource_repository.catalog

        existing_platform_resource = cls.search_by_urn(
            tag.to_datahub_tag_urn().urn(),
            platform_resource_repository=platform_resource_repository,
            tag_sync_context=LakeFormationTagSyncContext(
                platform_instance=platform_resource_repository.platform_instance,
                catalog=effective_catalog,
            ),
        )
        if existing_platform_resource:
            logger.info(
                f"Found existing LakeFormationTagPlatformResourceId for tag {tag.key}: {existing_platform_resource}"
            )
            return existing_platform_resource

        return LakeFormationTagPlatformResourceId(
            tag_key=str(tag.key),
            tag_value=str(tag.value) if tag.value is not None else None,
            platform_instance=platform_resource_repository.platform_instance,
            catalog=effective_catalog,
            exists_in_lake_formation=exists_in_lake_formation,
            persisted=False,
        )

    @classmethod
    def search_by_urn(
        cls,
        urn: str,
        platform_resource_repository: "GluePlatformResourceRepository",
        tag_sync_context: LakeFormationTagSyncContext,
    ) -> Optional["LakeFormationTagPlatformResourceId"]:
        """
        Search for existing Lake Formation tag entity by URN using repository caching.

        This method now delegates to the repository's search_entity_by_urn method to ensure
        consistent caching behavior across all platform implementations.
        """
        # Use repository's cached search method instead of duplicating search logic
        existing_entity_id = platform_resource_repository.search_entity_by_urn(urn)

        if existing_entity_id:
            # Verify platform instance and catalog match
            if (
                existing_entity_id.platform_instance
                == tag_sync_context.platform_instance
                and existing_entity_id.catalog == tag_sync_context.catalog
            ):
                logger.info(
                    f"Found existing LakeFormationTagPlatformResourceId for URN {urn}: {existing_entity_id}"
                )
                # Create a new ID with the correct state instead of mutating
                return LakeFormationTagPlatformResourceId(
                    tag_key=existing_entity_id.tag_key,
                    tag_value=existing_entity_id.tag_value,
                    platform_instance=existing_entity_id.platform_instance,
                    catalog=existing_entity_id.catalog,
                    exists_in_lake_formation=True,  # This tag exists in Lake Formation
                    persisted=True,  # And it's persisted in DataHub
                )

        logger.info(
            f"No mapped tag found for URN {urn} with platform instance {tag_sync_context.platform_instance}. Creating a new LakeFormationTagPlatformResourceId."
        )
        return None

    @classmethod
    def from_datahub_urn(
        cls,
        urn: str,
        platform_resource_repository: "GluePlatformResourceRepository",
        tag_sync_context: LakeFormationTagSyncContext,
    ) -> "LakeFormationTagPlatformResourceId":
        """
        Creates a UnityCatalogTagPlatformResourceId from a DataHub URN.
        """
        # First we check if we already have a mapped platform resource for this
        # urn that is of the type UnityCatalogTagPlatformResource
        # If we do, we can use it to create the UnityCatalogTagPlatformResourceId
        # Else, we need to generate a new UnityCatalogTagPlatformResourceId
        existing_platform_resource_id = cls.search_by_urn(
            urn, platform_resource_repository, tag_sync_context
        )
        if existing_platform_resource_id:
            logger.info(
                f"Found existing LakeFormationTagPlatformResourceId for URN {urn}: {existing_platform_resource_id}"
            )
            return existing_platform_resource_id

        # Otherwise, we need to create a new UnityCatalogTagPlatformResourceId
        new_tag_id = cls.generate_tag_id(tag_sync_context, urn)
        if new_tag_id:
            # we then check if this tag has already been ingested as a platform
            # resource in the platform resource repository
            resource_key = platform_resource_repository.get(
                new_tag_id.to_platform_resource_key()
            )
            if resource_key:
                logger.info(
                    f"Tag {new_tag_id} already exists in platform resource repository with {resource_key}"
                )
                # Create a new ID with the correct state instead of mutating
                return LakeFormationTagPlatformResourceId(
                    tag_key=new_tag_id.tag_key,
                    tag_value=new_tag_id.tag_value,
                    platform_instance=new_tag_id.platform_instance,
                    catalog=new_tag_id.catalog,
                    exists_in_lake_formation=True,  # This tag exists in Lake Formation
                    persisted=new_tag_id.persisted,
                )
            return new_tag_id
        raise ValueError(f"Unable to create LakeFormationTagId from DataHub URN: {urn}")

    @classmethod
    def generate_tag_id(
        cls, tag_sync_context: LakeFormationTagSyncContext, urn: str
    ) -> "LakeFormationTagPlatformResourceId":
        parsed_urn = Urn.from_string(urn)
        entity_type = parsed_urn.entity_type
        if entity_type == "tag":
            new_tag_id = LakeFormationTagPlatformResourceId.from_datahub_tag(
                TagUrn.from_string(urn), tag_sync_context
            )
        else:
            raise ValueError(f"Unsupported entity type {entity_type} for URN {urn}")
        return new_tag_id

    @classmethod
    def from_datahub_tag(
        cls, tag_urn: TagUrn, tag_sync_context: LakeFormationTagSyncContext
    ) -> "LakeFormationTagPlatformResourceId":
        tag = LakeFormationTag.from_urn(tag_urn)

        return LakeFormationTagPlatformResourceId(
            tag_key=str(tag.key),
            tag_value=str(tag.value),
            platform_instance=tag_sync_context.platform_instance,
            catalog=tag_sync_context.catalog,
            exists_in_lake_formation=False,
        )


class LakeFormationTagPlatformResource(ExternalEntity):
    datahub_urns: LinkedResourceSet
    managed_by_datahub: bool
    id: LakeFormationTagPlatformResourceId
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
    def create_default(
        cls,
        entity_id: ExternalEntityId,
        managed_by_datahub: bool,
    ) -> "LakeFormationTagPlatformResource":
        """Create a default Lake Formation tag entity when none found in DataHub."""
        # Type narrowing: we know this will be a LakeFormationTagPlatformResourceId
        assert isinstance(entity_id, LakeFormationTagPlatformResourceId), (
            f"Expected LakeFormationTagPlatformResourceId, got {type(entity_id)}"
        )

        # Create a new entity ID with correct default state instead of mutating
        default_entity_id = LakeFormationTagPlatformResourceId(
            tag_key=entity_id.tag_key,
            tag_value=entity_id.tag_value,
            platform_instance=entity_id.platform_instance,
            catalog=entity_id.catalog,
            exists_in_lake_formation=False,  # New entities don't exist in Lake Formation yet
            persisted=False,  # New entities are not persisted yet
        )

        return cls(
            id=default_entity_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=managed_by_datahub,
            allowed_values=None,
        )
