import logging
from typing import List, Optional

from pydantic import BaseModel

from datahub.api.entities.external.external_entities import (
    ExternalEntity,
    ExternalEntityId,
    LinkedResourceSet,
    PlatformResourceRepository,
)
from datahub.api.entities.external.lake_formation_external_entites import (
    LakeFormationTag,
)
from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
    PlatformResourceSearchFields,
)
from datahub.metadata.urns import TagUrn
from datahub.utilities.search_utils import ElasticDocumentQuery
from datahub.utilities.urns.urn import Urn

logger = logging.getLogger(__name__)


class LakeFormationTagSyncContext(BaseModel):
    # it is intentionally empty
    platform_instance: Optional[str] = None
    catalog: Optional[str] = None


class LakeFormationTagPlatformResourceId(BaseModel, ExternalEntityId):
    """
    A LakeFormationTag is a unique identifier for a Lakeformation tag.
    """

    tag_key: str
    tag_value: Optional[str] = None
    platform_instance: Optional[str]
    catalog: Optional[str] = None
    exists_in_lake_formation: bool = False
    persisted: bool = False

    def __hash__(self) -> int:
        return hash(self.to_platform_resource_key().id)

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
    def from_tag(
        cls,
        tag: LakeFormationTag,
        platform_instance: Optional[str],
        platform_resource_repository: PlatformResourceRepository,
        catalog: Optional[str] = None,
        exists_in_lake_formation: bool = False,
    ) -> "LakeFormationTagPlatformResourceId":
        """
        Creates a LakeFormationTagPlatformResourceId from a LakeFormationTag.
        """

        existing_platform_resource = cls.search_by_urn(
            tag.to_datahub_tag_urn().urn(),
            platform_resource_repository=platform_resource_repository,
            tag_sync_context=LakeFormationTagSyncContext(
                platform_instance=platform_instance,
                catalog=catalog,
            ),
        )
        if existing_platform_resource:
            logger.info(
                f"Found existing LakeFormationTagPlatformResourceId for tag {tag.key}: {existing_platform_resource}"
            )
            return existing_platform_resource

        return LakeFormationTagPlatformResourceId(
            tag_key=tag.key,
            tag_value=tag.value if tag.value is not None else None,
            platform_instance=platform_instance,
            exists_in_lake_formation=exists_in_lake_formation,
            catalog=catalog,
            persisted=False,
        )

    @classmethod
    def search_by_urn(
        cls,
        urn: str,
        platform_resource_repository: PlatformResourceRepository,
        tag_sync_context: LakeFormationTagSyncContext,
    ) -> Optional["LakeFormationTagPlatformResourceId"]:
        mapped_tags = [
            t
            for t in platform_resource_repository.search_by_filter(
                ElasticDocumentQuery.create_from(
                    (
                        PlatformResourceSearchFields.RESOURCE_TYPE,
                        str(LakeFormationTagPlatformResourceId._RESOURCE_TYPE()),
                    ),
                    (PlatformResourceSearchFields.SECONDARY_KEYS, urn),
                )
            )
        ]
        logger.info(
            f"Found {len(mapped_tags)} mapped tags for URN {urn}. {mapped_tags}"
        )
        if len(mapped_tags) > 0:
            for platform_resource in mapped_tags:
                if (
                    platform_resource.resource_info
                    and platform_resource.resource_info.value
                ):
                    lake_formation_tag_platform_resource = (
                        LakeFormationTagPlatformResource(
                            **platform_resource.resource_info.value.as_pydantic_object(
                                LakeFormationTagPlatformResource
                            ).dict()
                        )
                    )
                    if (
                        lake_formation_tag_platform_resource.id.platform_instance
                        == tag_sync_context.platform_instance
                        and lake_formation_tag_platform_resource.id.catalog
                        == tag_sync_context.catalog
                    ):
                        lake_formation_tag_id = lake_formation_tag_platform_resource.id
                        lake_formation_tag_id.exists_in_lake_formation = True
                        lake_formation_tag_id.persisted = True
                        return lake_formation_tag_id
                else:
                    logger.warning(
                        f"Platform resource {platform_resource} does not have a resource_info value"
                    )
                    continue

            # If we reach here, it means we did not find a mapped tag for the URN
            logger.info(
                f"No mapped tag found for URN {urn} with platform instance {tag_sync_context.platform_instance}. Creating a new LakeFormationTagPlatformResourceId."
            )
        return None

    @classmethod
    def from_datahub_urn(
        cls,
        urn: str,
        platform_resource_repository: PlatformResourceRepository,
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
                new_tag_id.exists_in_lake_formation = (
                    True  # TODO: Check if this is a safe assumption
                )
            return new_tag_id
        raise ValueError(f"Unable to create SnowflakeTagId from DataHub URN: {urn}")

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


class LakeFormationTagPlatformResource(BaseModel, ExternalEntity):
    datahub_urns: LinkedResourceSet
    managed_by_datahub: bool
    id: LakeFormationTagPlatformResourceId
    allowed_values: Optional[List[str]]

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
        lake_formation_tag_id: LakeFormationTagPlatformResourceId,
        platform_resource_repository: PlatformResourceRepository,
        managed_by_datahub: bool = False,
    ) -> "LakeFormationTagPlatformResource":
        # Search for linked DataHub URNs
        platform_resources = [
            r
            for r in platform_resource_repository.search_by_filter(
                ElasticDocumentQuery.create_from(
                    (
                        PlatformResourceSearchFields.RESOURCE_TYPE,
                        str(LakeFormationTagPlatformResourceId._RESOURCE_TYPE()),
                    ),
                    (
                        PlatformResourceSearchFields.PRIMARY_KEY,
                        f"{lake_formation_tag_id.tag_key}/{lake_formation_tag_id.tag_value}",
                    ),
                )
            )
        ]
        for platform_resource in platform_resources:
            if (
                platform_resource.resource_info
                and platform_resource.resource_info.value
            ):
                lf_tag = LakeFormationTagPlatformResource(
                    **platform_resource.resource_info.value.as_pydantic_object(
                        LakeFormationTagPlatformResource
                    ).dict()
                )
                if (
                    lf_tag.id.platform_instance
                    == lake_formation_tag_id.platform_instance
                    and lf_tag.id.catalog == lake_formation_tag_id.catalog
                ):
                    return lf_tag
        return cls(
            id=lake_formation_tag_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=managed_by_datahub,
            allowed_values=None,
        )
