import logging
from typing import List, Optional, Tuple, Union

from pydantic import BaseModel

from datahub.api.entities.external.external_entities import (
    ExternalEntity,
    ExternalEntityId,
    LinkedResourceSet,
    PlatformResourceRepository,
)
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


class UnityCatalogTagId(BaseModel, ExternalEntityId):
    """
    A SnowflakeTagId is a unique identifier for a Snowflake tag.
    """

    tag_key: str
    tag_value: Optional[str] = None
    platform_instance: Optional[str]
    exists_in_unity_catalog: bool = False
    persisted: bool = False

    def __hash__(self) -> int:
        return hash(self.to_platform_resource_key().id)

    # this is a hack to make sure the property is a string and not private pydantic field
    @staticmethod
    def _RESOURCE_TYPE() -> str:
        return "UnityCatalogTag"

    def to_platform_resource_key(self) -> PlatformResourceKey:
        return PlatformResourceKey(
            platform="databricks",
            resource_type=str(UnityCatalogTagId._RESOURCE_TYPE()),
            primary_key=f"{self.tag_key}/{self.tag_value}",
            platform_instance=self.platform_instance,
        )

    @classmethod
    def from_datahub_urn(
        cls,
        urn: str,
        platform_resource_repository: PlatformResourceRepository,
        tag_sync_context: UnityCatalogTagSyncContext,
        graph: DataHubGraph,
    ) -> "UnityCatalogTagId":
        """
        Creates a UnityCatalogTagId from a DataHub URN.
        """
        # First we check if we already have a mapped platform resource for this
        # urn that is of the type UnityCatalogTag
        # If we do, we can use it to create the UnityCatalogTagId
        # Else, we need to generate a new UnityCatalogTagId
        mapped_tags = [
            t
            for t in platform_resource_repository.search_by_filter(
                ElasticDocumentQuery.create_from(
                    (
                        PlatformResourceSearchFields.RESOURCE_TYPE,
                        str(UnityCatalogTagId._RESOURCE_TYPE()),
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
                    unity_catalog_tag = UnityCatalogTag(
                        **platform_resource.resource_info.value.as_pydantic_object(
                            UnityCatalogTag
                        ).dict()
                    )
                    if (
                        unity_catalog_tag.id.platform_instance
                        == tag_sync_context.platform_instance
                    ):
                        unity_catalog_tag_id = unity_catalog_tag.id
                        unity_catalog_tag_id.exists_in_unity_catalog = True
                        unity_catalog_tag_id.persisted = True
                        return unity_catalog_tag_id
                else:
                    logger.warning(
                        f"Platform resource {platform_resource} does not have a resource_info value"
                    )
                    continue

            # If we reach here, it means we did not find a mapped tag for the URN
            logger.info(
                f"No mapped tag found for URN {urn} with platform instance {tag_sync_context.platform_instance}. Creating a new UnityCatalogTagId."
            )

        # Otherwise, we need to create a new UnityCatalogTagId
        new_unity_catalog_tag_id = cls.generate_tag_id(graph, tag_sync_context, urn)
        if new_unity_catalog_tag_id:
            # we then check if this tag has already been ingested as a platform
            # resource in the platform resource repository
            resource_key = platform_resource_repository.get(
                new_unity_catalog_tag_id.to_platform_resource_key()
            )
            if resource_key:
                logger.info(
                    f"Tag {new_unity_catalog_tag_id} already exists in platform resource repository with {resource_key}"
                )
                new_unity_catalog_tag_id.exists_in_unity_catalog = (
                    True  # TODO: Check if this is a safe assumption
                )
            return new_unity_catalog_tag_id
        raise ValueError(f"Unable to create SnowflakeTagId from DataHub URN: {urn}")

    @classmethod
    def generate_tag_id(
        cls, graph: DataHubGraph, tag_sync_context: UnityCatalogTagSyncContext, urn: str
    ) -> "UnityCatalogTagId":
        parsed_urn = Urn.from_string(urn)
        entity_type = parsed_urn.entity_type
        if entity_type == "tag":
            new_snowflake_tag_id = UnityCatalogTagId.from_datahub_tag(
                TagUrn.from_string(urn), tag_sync_context
            )
        else:
            raise ValueError(f"Unsupported entity type {entity_type} for URN {urn}")
        return new_snowflake_tag_id

    @classmethod
    def get_key_value_from_datahub_tag(cls, urn: Union[TagUrn]) -> Tuple[str, str]:
        tag_name = urn.name
        if ":" in tag_name:
            tag_name, value = tag_name.split(":", 1)
            return tag_name, value
        else:
            tag_name = tag_name
            return tag_name, ""

    @classmethod
    def from_datahub_tag(
        cls, tag_urn: TagUrn, tag_sync_context: UnityCatalogTagSyncContext
    ) -> "UnityCatalogTagId":
        tag_key, tag_value = cls.get_key_value_from_datahub_tag(tag_urn)

        return UnityCatalogTagId(
            tag_key=tag_key,
            tag_value=tag_value,
            platform_instance=tag_sync_context.platform_instance,
            exists_in_unity_catalog=False,
        )


class UnityCatalogTag(BaseModel, ExternalEntity):
    datahub_urns: LinkedResourceSet
    managed_by_datahub: bool
    id: UnityCatalogTagId
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
        unity_catalog_tag_id: UnityCatalogTagId,
        platform_resource_repository: PlatformResourceRepository,
        managed_by_datahub: bool = False,
    ) -> "UnityCatalogTag":
        # Search for linked DataHub URNs
        platform_resources = [
            r
            for r in platform_resource_repository.search_by_filter(
                ElasticDocumentQuery.create_from(
                    (
                        PlatformResourceSearchFields.RESOURCE_TYPE,
                        str(UnityCatalogTagId._RESOURCE_TYPE()),
                    ),
                    (
                        PlatformResourceSearchFields.PRIMARY_KEY,
                        f"{unity_catalog_tag_id.tag_key}/{unity_catalog_tag_id.tag_value}",
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
                unity_catalog_tag = UnityCatalogTag(
                    **platform_resource.resource_info.value.as_pydantic_object(
                        UnityCatalogTag
                    ).dict()
                )
                return unity_catalog_tag
        else:
            for platform_resource in platform_resources:
                if (
                    platform_resource.resource_info
                    and platform_resource.resource_info.value
                ):
                    unity_catalog_tag = UnityCatalogTag(
                        **platform_resource.resource_info.value.as_pydantic_object(
                            UnityCatalogTag
                        ).dict()
                    )
                    if (
                        unity_catalog_tag.id.platform_instance
                        == unity_catalog_tag_id.platform_instance
                    ):
                        return unity_catalog_tag
        return cls(
            id=unity_catalog_tag_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=managed_by_datahub,
            allowed_values=None,
        )
