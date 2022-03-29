import hashlib
import json
from typing import Any, Iterable, List, Optional, TypeVar, Union

from pydantic.fields import Field
from pydantic.main import BaseModel

from datahub.emitter.mce_builder import make_container_urn, make_data_platform_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance
from datahub.metadata.com.linkedin.pegasus2avro.container import ContainerProperties
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    ContainerClass,
    DomainsClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SubTypesClass,
    TagAssociationClass,
)


class DatahubKey(BaseModel):
    def guid(self) -> str:
        nonnull_dict = self.dict(by_alias=True, exclude_none=True)
        json_key = json.dumps(
            nonnull_dict,
            separators=(",", ":"),
            sort_keys=True,
            cls=DatahubKeyJSONEncoder,
        )
        md5_hash = hashlib.md5(json_key.encode("utf-8"))
        return str(md5_hash.hexdigest())


class PlatformKey(DatahubKey):
    platform: str
    instance: Optional[str] = None


class DatabaseKey(PlatformKey):
    database: str


class SchemaKey(DatabaseKey):
    db_schema: str = Field(alias="schema")


class ProjectIdKey(PlatformKey):
    project_id: str


class BigQueryDatasetKey(ProjectIdKey):
    dataset_id: str


class FolderKey(PlatformKey):
    folder_abs_path: str


class S3BucketKey(PlatformKey):
    bucket_name: str


class DatahubKeyJSONEncoder(json.JSONEncoder):

    # overload method default
    def default(self, obj: Any) -> Any:
        if hasattr(obj, "guid"):
            return obj.guid()
        # Call the default method for other types
        return json.JSONEncoder.default(self, obj)


KeyType = TypeVar("KeyType", bound=PlatformKey)


def add_domain_to_entity_wu(
    entity_type: str, entity_urn: str, domain_urn: str
) -> Iterable[MetadataWorkUnit]:
    mcp = MetadataChangeProposalWrapper(
        entityType=entity_type,
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=f"{entity_urn}",
        aspectName="domains",
        aspect=DomainsClass(domains=[domain_urn]),
    )
    wu = MetadataWorkUnit(id=f"{domain_urn}-to-{entity_urn}", mcp=mcp)
    yield wu


def add_owner_to_entity_wu(
    entity_type: str, entity_urn: str, owner_urn: str
) -> Iterable[MetadataWorkUnit]:
    mcp = MetadataChangeProposalWrapper(
        entityType=entity_type,
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=f"{entity_urn}",
        aspectName="ownership",
        aspect=OwnershipClass(
            owners=[
                OwnerClass(
                    owner=owner_urn,
                    type=OwnershipTypeClass.DATAOWNER,
                )
            ]
        ),
    )
    wu = MetadataWorkUnit(id=f"{owner_urn}-to-{entity_urn}", mcp=mcp)
    yield wu


def add_tags_to_entity_wu(
    entity_type: str, entity_urn: str, tags: List[str]
) -> Iterable[MetadataWorkUnit]:
    mcp = MetadataChangeProposalWrapper(
        entityType=entity_type,
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=f"{entity_urn}",
        aspectName="globalTags",
        aspect=GlobalTagsClass(
            tags=[TagAssociationClass(f"urn:li:tag:{tag}") for tag in tags]
        ),
    )
    wu = MetadataWorkUnit(id=f"tags-to-{entity_urn}", mcp=mcp)
    yield wu


def gen_containers(
    container_key: KeyType,
    name: str,
    sub_types: List[str],
    parent_container_key: Optional[PlatformKey] = None,
    domain_urn: Optional[str] = None,
    description: Optional[str] = None,
    owner_urn: Optional[str] = None,
    external_url: Optional[str] = None,
    tags: Optional[List[str]] = None,
) -> Iterable[MetadataWorkUnit]:
    container_urn = make_container_urn(
        guid=container_key.guid(),
    )
    mcp = MetadataChangeProposalWrapper(
        entityType="container",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=f"{container_urn}",
        # entityKeyAspect=ContainerKeyClass(guid=schema_container_key.guid()),
        aspectName="containerProperties",
        aspect=ContainerProperties(
            name=name,
            description=description,
            customProperties=container_key.dict(exclude_none=True, by_alias=True),
            externalUrl=external_url,
        ),
    )
    wu = MetadataWorkUnit(id=f"container-info-{name}-{container_urn}", mcp=mcp)
    yield wu

    mcp = MetadataChangeProposalWrapper(
        entityType="container",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=f"{container_urn}",
        # entityKeyAspect=ContainerKeyClass(guid=schema_container_key.guid()),
        aspectName="dataPlatformInstance",
        aspect=DataPlatformInstance(
            platform=f"{make_data_platform_urn(container_key.platform)}",
        ),
    )
    wu = MetadataWorkUnit(
        id=f"container-platforminstance-{name}-{container_urn}", mcp=mcp
    )
    yield wu

    # Set subtype
    subtype_mcp = MetadataChangeProposalWrapper(
        entityType="container",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=f"{container_urn}",
        # entityKeyAspect=ContainerKeyClass(guid=schema_container_key.guid()),
        aspectName="subTypes",
        aspect=SubTypesClass(typeNames=sub_types),
    )
    wu = MetadataWorkUnit(
        id=f"container-subtypes-{name}-{container_urn}", mcp=subtype_mcp
    )
    yield wu

    if domain_urn:
        yield from add_domain_to_entity_wu(
            entity_type="container",
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
            tags=tags,
        )

    if parent_container_key:
        parent_container_urn = make_container_urn(
            guid=parent_container_key.guid(),
        )

        # Set database container
        parent_container_mcp = MetadataChangeProposalWrapper(
            entityType="container",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=f"{container_urn}",
            # entityKeyAspect=ContainerKeyClass(guid=schema_container_key.guid()),
            aspectName="container",
            aspect=ContainerClass(container=parent_container_urn),
            # aspect=ContainerKeyClass(guid=database_container_key.guid())
        )
        wu = MetadataWorkUnit(
            id=f"container-parent-container-{name}-{container_urn}-{parent_container_urn}",
            mcp=parent_container_mcp,
        )

        yield wu


def add_dataset_to_container(
    container_key: KeyType, dataset_urn: str
) -> Iterable[Union[MetadataWorkUnit]]:
    container_urn = make_container_urn(
        guid=container_key.guid(),
    )

    mcp = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=f"{dataset_urn}",
        aspectName="container",
        aspect=ContainerClass(container=f"{container_urn}"),
        # aspect=ContainerKeyClass(guid=schema_container_key.guid())
    )
    wu = MetadataWorkUnit(id=f"container-{container_urn}-to-{dataset_urn}", mcp=mcp)
    yield wu


def add_entity_to_container(
    container_key: KeyType, entity_type: str, entity_urn: str
) -> Iterable[MetadataWorkUnit]:
    container_urn = make_container_urn(
        guid=container_key.guid(),
    )
    mcp = MetadataChangeProposalWrapper(
        entityType=entity_type,
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=entity_urn,
        aspectName="container",
        aspect=ContainerClass(container=f"{container_urn}"),
    )
    wu = MetadataWorkUnit(id=f"container-{container_urn}-to-{entity_urn}", mcp=mcp)
    yield wu
