from typing import Dict, Iterable, List, Optional, TypeVar

from pydantic.fields import Field
from pydantic.main import BaseModel

from datahub.emitter.mce_builder import (
    datahub_guid,
    make_container_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    DataPlatformInstance,
    TimeStamp,
)
from datahub.metadata.com.linkedin.pegasus2avro.container import ContainerProperties
from datahub.metadata.schema_classes import (
    ContainerClass,
    DomainsClass,
    EmbedClass,
    GlobalTagsClass,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
)


class DatahubKey(BaseModel):
    def guid_dict(self) -> Dict[str, str]:
        return self.dict(by_alias=True, exclude_none=True)

    def guid(self) -> str:
        bag = self.guid_dict()
        return datahub_guid(bag)


class ContainerKey(DatahubKey):
    """Base class for container guid keys. Most users should use one of the subclasses instead."""

    platform: str
    instance: Optional[str] = None

    env: Optional[str] = None

    # BUG: In some of our sources, we incorrectly set the platform instance
    # to the env if the platform instance was not specified. Now, we have to maintain
    # backwards compatibility with this bug, which means generating our GUIDs
    # in the same way.
    backcompat_env_as_instance: bool = Field(default=False, exclude=True)

    def guid_dict(self) -> Dict[str, str]:
        bag = self.dict(by_alias=True, exclude_none=True, exclude={"env"})

        if (
            self.backcompat_env_as_instance
            and self.instance is None
            and self.env is not None
        ):
            bag["instance"] = self.env

        return bag

    def property_dict(self) -> Dict[str, str]:
        return self.dict(by_alias=True, exclude_none=True)

    def as_urn(self) -> str:
        return make_container_urn(guid=self.guid())


# DEPRECATION: Keeping the `PlatformKey` name around for backwards compatibility.
PlatformKey = ContainerKey


class DatabaseKey(ContainerKey):
    database: str


class SchemaKey(DatabaseKey):
    db_schema: str = Field(alias="schema")


class ProjectIdKey(ContainerKey):
    project_id: str


class MetastoreKey(ContainerKey):
    metastore: str


class CatalogKeyWithMetastore(MetastoreKey):
    catalog: str


class UnitySchemaKeyWithMetastore(CatalogKeyWithMetastore):
    unity_schema: str


class CatalogKey(ContainerKey):
    catalog: str


class UnitySchemaKey(CatalogKey):
    unity_schema: str


class BigQueryDatasetKey(ProjectIdKey):
    dataset_id: str


class FolderKey(ContainerKey):
    folder_abs_path: str


class BucketKey(ContainerKey):
    bucket_name: str


class NotebookKey(DatahubKey):
    notebook_id: int
    platform: str
    instance: Optional[str]

    def as_urn(self) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.platform, platform_instance=self.instance, name=self.guid()
        )


KeyType = TypeVar("KeyType", bound=ContainerKey)


def add_domain_to_entity_wu(
    entity_urn: str, domain_urn: str
) -> Iterable[MetadataWorkUnit]:
    yield MetadataChangeProposalWrapper(
        entityUrn=f"{entity_urn}",
        aspect=DomainsClass(domains=[domain_urn]),
    ).as_workunit()


def add_owner_to_entity_wu(
    entity_type: str, entity_urn: str, owner_urn: str
) -> Iterable[MetadataWorkUnit]:
    yield MetadataChangeProposalWrapper(
        entityUrn=f"{entity_urn}",
        aspect=OwnershipClass(
            owners=[
                OwnerClass(
                    owner=owner_urn,
                    type=OwnershipTypeClass.DATAOWNER,
                )
            ]
        ),
    ).as_workunit()


def add_tags_to_entity_wu(
    entity_type: str, entity_urn: str, tags: List[str]
) -> Iterable[MetadataWorkUnit]:
    yield MetadataChangeProposalWrapper(
        entityType=entity_type,
        entityUrn=f"{entity_urn}",
        aspect=GlobalTagsClass(
            tags=[TagAssociationClass(f"urn:li:tag:{tag}") for tag in tags]
        ),
    ).as_workunit()


def gen_containers(
    container_key: KeyType,
    name: str,
    sub_types: List[str],
    parent_container_key: Optional[ContainerKey] = None,
    extra_properties: Optional[Dict[str, str]] = None,
    domain_urn: Optional[str] = None,
    description: Optional[str] = None,
    owner_urn: Optional[str] = None,
    external_url: Optional[str] = None,
    tags: Optional[List[str]] = None,
    qualified_name: Optional[str] = None,
    created: Optional[int] = None,
    last_modified: Optional[int] = None,
) -> Iterable[MetadataWorkUnit]:
    container_urn = container_key.as_urn()
    yield MetadataChangeProposalWrapper(
        entityUrn=f"{container_urn}",
        # entityKeyAspect=ContainerKeyClass(guid=parent_container_key.guid()),
        aspect=ContainerProperties(
            name=name,
            description=description,
            customProperties={
                **container_key.property_dict(),
                **(extra_properties or {}),
            },
            externalUrl=external_url,
            qualifiedName=qualified_name,
            created=TimeStamp(time=created) if created is not None else None,
            lastModified=TimeStamp(time=last_modified)
            if last_modified is not None
            else None,
        ),
    ).as_workunit()

    # add status
    yield MetadataChangeProposalWrapper(
        entityUrn=f"{container_urn}",
        aspect=StatusClass(removed=False),
    ).as_workunit()

    yield MetadataChangeProposalWrapper(
        entityUrn=f"{container_urn}",
        aspect=DataPlatformInstance(
            platform=f"{make_data_platform_urn(container_key.platform)}",
            instance=f"{make_dataplatform_instance_urn(container_key.platform, container_key.instance)}"
            if container_key.instance
            else None,
        ),
    ).as_workunit()

    # Set subtype
    yield MetadataChangeProposalWrapper(
        entityUrn=f"{container_urn}",
        aspect=SubTypesClass(typeNames=sub_types),
    ).as_workunit()

    if domain_urn:
        yield from add_domain_to_entity_wu(
            entity_urn=container_urn,
            domain_urn=domain_urn,
        )

    if owner_urn:
        yield from add_owner_to_entity_wu(
            entity_type="container",
            entity_urn=container_urn,
            owner_urn=owner_urn,
        )

    if tags:
        yield from add_tags_to_entity_wu(
            entity_type="container",
            entity_urn=container_urn,
            tags=sorted(tags),
        )

    if parent_container_key:
        parent_container_urn = make_container_urn(
            guid=parent_container_key.guid(),
        )

        # Set database container
        parent_container_mcp = MetadataChangeProposalWrapper(
            entityUrn=f"{container_urn}",
            aspect=ContainerClass(container=parent_container_urn),
        )
        yield parent_container_mcp.as_workunit()


def add_dataset_to_container(
    container_key: KeyType, dataset_urn: str
) -> Iterable[MetadataWorkUnit]:
    container_urn = make_container_urn(
        guid=container_key.guid(),
    )

    yield MetadataChangeProposalWrapper(
        entityUrn=f"{dataset_urn}",
        aspect=ContainerClass(container=f"{container_urn}"),
    ).as_workunit()


def add_entity_to_container(
    container_key: KeyType, entity_type: str, entity_urn: str
) -> Iterable[MetadataWorkUnit]:
    container_urn = make_container_urn(
        guid=container_key.guid(),
    )
    yield MetadataChangeProposalWrapper(
        entityType=entity_type,
        entityUrn=entity_urn,
        aspect=ContainerClass(container=f"{container_urn}"),
    ).as_workunit()


def mcps_from_mce(
    mce: MetadataChangeEventClass,
) -> Iterable[MetadataChangeProposalWrapper]:
    for aspect in mce.proposedSnapshot.aspects:
        yield MetadataChangeProposalWrapper(
            entityUrn=mce.proposedSnapshot.urn,
            auditHeader=mce.auditHeader,
            aspect=aspect,
            systemMetadata=mce.systemMetadata,
        )


def create_embed_mcp(urn: str, embed_url: str) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=EmbedClass(renderUrl=embed_url),
    )
