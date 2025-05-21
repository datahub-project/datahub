import logging
from collections import deque
from datetime import datetime
from typing import List, Optional, Tuple, Union, Any

import cachetools
from pydantic import BaseModel

from datahub.api.entities.external.external_entities import (
    ExternalEntity,
    ExternalEntityId,
    ExternalSystem,
    LinkedResourceSet,
    MissingExternalEntity,
    PlatformResourceRepository,
)
from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
    PlatformResourceSearchFields,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.metadata.urns import GlossaryTermUrn, TagUrn
from datahub.utilities.search_utils import ElasticDocumentQuery
from datahub.utilities.urns.urn import Urn


class LakeFormationTagSyncContext(BaseModel):
    catalog_id: str


logger = logging.getLogger(__name__)

class LakeFormationTagId(BaseModel, ExternalEntityId):
    """
    A SnowflakeTagId is a unique identifier for a Snowflake tag.
    """

    _RESOURCE_TYPE = "LakeFormationTag"
    tag_name: str
    platform_instance: str
    exists_in_lake_formation: bool = False
    persisted: bool = False

    def __hash__(self) -> int:
        return hash(self.to_platform_resource_key().id)

    def to_platform_resource_key(self) -> PlatformResourceKey:
        return PlatformResourceKey(
            platform="lake_formation",
            resource_type=self._RESOURCE_TYPE,
            primary_key=self.tag_name,
            platform_instance=self.platform_instance,
        )

    @classmethod
    def from_datahub_urn(
            cls,
            urn: str,
            platform_resource_repository: PlatformResourceRepository,
            tag_sync_context: LakeFormationTagSyncContext,
            graph: DataHubGraph,
    ) -> Optional["LakeFormationTagId"]:
        """
        Creates a LakeFormationTagId from a DataHub URN.
        """
        # First we check if we already have a mapped platform resource for this
        # urn that is of the type LakeFormationTag
        # If we do, we can use it to create the LakeFormationTagId
        # Else, we need to generate a new LakeFormationTagId
        mapped_tags = [
            t
            for t in platform_resource_repository.search_by_filter(
                ElasticDocumentQuery.create_from(
                    (PlatformResourceSearchFields.RESOURCE_TYPE, cls._RESOURCE_TYPE),
                    (PlatformResourceSearchFields.SECONDARY_KEYS, urn),
                )
            )
        ]
        logger.info(
            f"Found {len(mapped_tags)} mapped tags for URN {urn}. {mapped_tags}"
        )
        if len(mapped_tags) > 0:
            if len(mapped_tags) > 1:
                logger.warning(f"Multiple mapped tags found for URN {urn}")
            platform_resource: PlatformResource = mapped_tags[0]
            if (
                    platform_resource.resource_info
                    and platform_resource.resource_info.value
            ):
                lake_formation_tag = LakeFormationTag(
                    **platform_resource.resource_info.value.as_pydantic_object(
                        LakeFormationTag
                    ).dict()
                )
                lake_formation_tag_id = lake_formation_tag.id
                lake_formation_tag_id.exists_in_lake_formation = True
                lake_formation_tag_id.persisted = True
                return lake_formation_tag_id

        # Otherwise, we need to create a new SnowflakeTagId
        lake_formation_tag_id = cls.generate_tag_id(tag_sync_context, urn)
        if lake_formation_tag_id:
            # we then check if this tag has already been ingested as a platform
            # resource in the platform resource repository
            resource_key = platform_resource_repository.get(
                lake_formation_tag_id.to_platform_resource_key()
            )
            if resource_key:
                logger.info(
                    f"Tag {lake_formation_tag_id} already exists in platform resource repository with {resource_key}"
                )
                lake_formation_tag_id.exists_in_lake_formation = (
                    True  # TODO: Check if this is a safe assumption
                )
            return lake_formation_tag_id
        raise ValueError(f"Unable to create LakeFormationTagId from DataHub URN: {urn}")

    @classmethod
    def generate_tag_id(
            cls, tag_sync_context: LakeFormationTagSyncContext, urn: str
    ) -> "LakeFormationTagId":
        parsed_urn = Urn.from_string(urn)
        entity_type = parsed_urn.entity_type
        if entity_type == "tag":
            new_lake_formation_tag_id = LakeFormationTagId.from_datahub_tag(
                TagUrn.from_string(urn), tag_sync_context
            )
        else:
            raise ValueError(f"Unsupported entity type {entity_type} for URN {urn}")
        return new_lake_formation_tag_id

    @classmethod
    def get_key_value_from_datahub_tag(
            cls, urn: Union[TagUrn, GlossaryTermUrn]
    ) -> Tuple[str, str]:
        tag_name = urn.name
        if ":" in tag_name:
            tag_name, value = tag_name.split(":", 1)
            return tag_name, value
        else:
            tag_name = tag_name
            return tag_name, ""

    @classmethod
    def from_datahub_tag(
            cls, tag_urn: TagUrn, tag_sync_context: LakeFormationTagSyncContext
    ) -> "LakeFormationTagId":
        tag_name, _ = cls.get_key_value_from_datahub_tag(tag_urn)

        return LakeFormationTagId(
            tag_name=tag_name,
            platform_instance=tag_sync_context.catalog_id,
            exists_in_lake_formation=False,
        )

class LakeFormationTag(BaseModel, ExternalEntity):
    datahub_urns: LinkedResourceSet
    managed_by_datahub: bool
    id: LakeFormationTagId
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
    def get_from_lake_formation(
            cls,
            lakeformation_tag_id: LakeFormationTagId,
            lakeformation_client: Any,
            platform_resource_repository: PlatformResourceRepository,
    ) -> Optional["LakeFormationTag"]:
        try:
            result = lakeformation_client.get_lf_tag(CatalogId=lakeformation_tag_id.platform_instance, TagKey=lakeformation_tag_id.tag_name)
            if not result or "TagKey" not in result:
                return None

            # Search for linked DataHub URNs
            platform_resources = [
                r
                for r in platform_resource_repository.search_by_filter(
                    ElasticDocumentQuery.create_from(
                        (
                            PlatformResourceSearchFields.RESOURCE_TYPE,
                            LakeFormationTagId._RESOURCE_TYPE,
                        ),
                        (
                            PlatformResourceSearchFields.PRIMARY_KEY,
                            lakeformation_tag_id.tag_name,
                        ),
                    )
                )
            ]

            if len(platform_resources) == 1:
                platform_resource: PlatformResource = platform_resources[0]
                if (
                        platform_resource.resource_info
                        and platform_resource.resource_info.value
                ):
                    lakeformation_tag = LakeFormationTag(
                        **platform_resource.resource_info.value.as_pydantic_object(
                            LakeFormationTag
                        ).dict()
                    )
                    return lakeformation_tag
            if len(platform_resources) > 1:
                lakeformation_tag: Optional[LakeFormationTag] = None
                for platform_resource in platform_resources:
                    existing_tag = LakeFormationTag(
                        **platform_resource.resource_info.value.as_pydantic_object(
                            LakeFormationTag
                        ).dict()
                    )
                    if existing_tag.id.platform_instance == lakeformation_tag_id.platform_instance:
                        if not lakeformation_tag:
                            lakeformation_tag = existing_tag
                        else:
                            logger.warning(
                                f"Multiple platform resources found for LakeFormation tag {lakeformation_tag_id}"
                            )
                            return None
                if lakeformation_tag:
                    return lakeformation_tag
            return cls(
                id=lakeformation_tag_id,
                datahub_urns=LinkedResourceSet(urns=[]),
                managed_by_datahub=False,  # Assuming it's not managed by DataHub if it exists in Snowflake
                allowed_values=None,
            )
        except Exception as e:
            logger.error(f"Error fetching LakeFormation Tag {lakeformation_tag_id}: {e}")
            return None


class LakeFormationSystem(ExternalSystem):
    def __init__(self, aws_config: AwsConnectionConfig) -> None:
        super().__init__()
        self.lf_client = aws_config.get_lakeformation_client()
        self.cached_entities: cachetools.TTLCache = cachetools.TTLCache(
            maxsize=1000, ttl=60 * 5
        )

    def exists(self, external_entity_id: ExternalEntityId) -> bool:
        return external_entity_id in self.cached_entities

    def get(
            self,
            external_entity_id: ExternalEntityId,
            platform_resource_repository: PlatformResourceRepository,
    ) -> Optional[ExternalEntity]:
        try:
            cached_entity = self.cached_entities[external_entity_id]
            if isinstance(cached_entity, MissingExternalEntity):
                return None
            return cached_entity
        except KeyError:
            external_entity = self._get_external_entity(
                external_entity_id, platform_resource_repository
            )
            if external_entity:
                self.cached_entities[external_entity_id] = external_entity
            else:
                # store a sentinel value to indicate that the entity does not
                # exist
                self.cached_entities[external_entity_id] = MissingExternalEntity(
                    external_entity_id
                )
            return external_entity

    def _get_external_entity(
            self,
            external_entity_id: ExternalEntityId,
            platform_resource_repository: PlatformResourceRepository,
    ) -> Optional[ExternalEntity]:
        if isinstance(external_entity_id, LakeFormationTagId):
            return LakeFormationTag.get_from_lake_formation(
                external_entity_id,
                self.lf_client,
                platform_resource_repository,
            )
        raise ValueError(
            f"Unsupported external entity id type: {type(external_entity_id)}"
        )
