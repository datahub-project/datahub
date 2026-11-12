from typing import Optional

from datahub.api.entities.external.external_entities import (
    ExternalEntity,
    ExternalEntityId,
    LinkedResourceSet,
    PlatformResourceRepository,
)
from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
)
from datahub.ingestion.source.snowflake.snowflake_schema import SnowflakeTag

SNOWFLAKE_TAG_RESOURCE_TYPE = "SnowflakeTagPlatformResource"
SNOWFLAKE_PLATFORM = "snowflake"


class SnowflakeTagPlatformResourceId(ExternalEntityId):
    """Identifies a Snowflake tag, or one tag-and-value pair of it.

    Fields keep Snowflake's exact case so sync-back can address the tag; the
    PlatformResource key, platform instance included, is lower-cased so
    ingestion (which sees upper-case Snowflake names and the configured
    instance as typed) and sync-back (which starts from lower-case URNs) agree.
    With a value the record stands for a DataHub tag or glossary term; without
    one it stands for a structured property, whose value varies per asset.
    """

    database: str
    schema_name: str
    tag_name: str
    tag_value: Optional[str] = None
    platform_instance: Optional[str] = None
    persisted: bool = False

    def to_platform_resource_key(self) -> PlatformResourceKey:
        primary_key = f"{self.database}.{self.schema_name}.{self.tag_name}"
        if self.tag_value is not None:
            primary_key = f"{primary_key}:{self.tag_value}"
        return PlatformResourceKey(
            platform=SNOWFLAKE_PLATFORM,
            resource_type=SNOWFLAKE_TAG_RESOURCE_TYPE,
            primary_key=primary_key.lower(),
            platform_instance=(
                self.platform_instance.lower()
                if self.platform_instance is not None
                else None
            ),
        )

    @classmethod
    def from_snowflake_tag(
        cls,
        tag: SnowflakeTag,
        platform_instance: Optional[str],
        with_value: bool,
    ) -> "SnowflakeTagPlatformResourceId":
        return cls(
            database=tag.database,
            schema_name=tag.schema,
            tag_name=tag.name,
            tag_value=tag.value if with_value else None,
            platform_instance=platform_instance,
        )


class SnowflakeTagPlatformResource(ExternalEntity):
    id: SnowflakeTagPlatformResourceId
    datahub_urns: LinkedResourceSet
    managed_by_datahub: bool

    def get_id(self) -> ExternalEntityId:
        return self.id

    def is_managed_by_datahub(self) -> bool:
        return self.managed_by_datahub

    def datahub_linked_resources(self) -> LinkedResourceSet:
        return self.datahub_urns

    def as_platform_resource(self) -> PlatformResource:
        return PlatformResource.create(
            key=self.id.to_platform_resource_key(),
            secondary_keys=list(self.datahub_urns.urns),
            value=self,
        )

    @classmethod
    def create_default(
        cls, entity_id: ExternalEntityId, managed_by_datahub: bool
    ) -> "SnowflakeTagPlatformResource":
        assert isinstance(entity_id, SnowflakeTagPlatformResourceId)
        return cls(
            id=entity_id.model_copy(update={"persisted": False}),
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=managed_by_datahub,
        )


class SnowflakeTagPlatformResourceRepository(
    PlatformResourceRepository[
        SnowflakeTagPlatformResourceId, SnowflakeTagPlatformResource
    ]
):
    pass
