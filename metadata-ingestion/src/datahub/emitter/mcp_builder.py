from typing import Dict, Iterable, List, Optional, Type, TypeVar

from pydantic.fields import Field
from pydantic.main import BaseModel

from datahub.cli.env_utils import get_boolean_env_variable
from datahub.emitter.mce_builder import (
    ALL_ENV_TYPES,
    Aspect,
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
    KEY_ASPECTS,
    ContainerClass,
    DomainsClass,
    EmbedClass,
    GlobalTagsClass,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
    SubTypesClass,
    TagAssociationClass,
)
from datahub.metadata.urns import ContainerUrn, StructuredPropertyUrn

# In https://github.com/datahub-project/datahub/pull/11214, we added a
# new env field to container properties. However, populating this field
# with servers older than 0.14.1 will cause errors. This environment
# variable is an escape hatch to avoid this compatibility issue.
# TODO: Once the model change has been deployed for a while, we can remove this.
#       Probably can do it at the beginning of 2025.
_INCLUDE_ENV_IN_CONTAINER_PROPERTIES = get_boolean_env_variable(
    "DATAHUB_INCLUDE_ENV_IN_CONTAINER_PROPERTIES", default=True
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

    def as_urn_typed(self) -> ContainerUrn:
        return ContainerUrn.from_string(self.as_urn())

    def as_urn(self) -> str:
        return make_container_urn(guid=self.guid())

    def parent_key(self) -> Optional["ContainerKey"]:
        # Find the immediate base class of self.
        # This is a bit of a hack, but it works.
        base_classes = self.__class__.__bases__
        if len(base_classes) != 1:
            # TODO: Raise a more specific error.
            raise ValueError(
                f"Unable to determine parent key for {self.__class__}: {self}"
            )
        base_class = base_classes[0]
        if base_class is DatahubKey or base_class is ContainerKey:
            return None

        # We need to use `__dict__` instead of `pydantic.BaseModel.dict()`
        # in order to include "excluded" fields e.g. `backcompat_env_as_instance`.
        # Tricky: this only works because DatahubKey is a BaseModel and hence
        # allows extra fields.
        return base_class(**self.__dict__)


# DEPRECATION: Keeping the `PlatformKey` name around for backwards compatibility.
PlatformKey = ContainerKey


class NamespaceKey(ContainerKey):
    """
    For Iceberg namespaces (databases/schemas)
    """

    namespace: str


class DatabaseKey(ContainerKey):
    database: str


class SchemaKey(DatabaseKey):
    db_schema: str = Field(alias="schema")


class ProjectIdKey(ContainerKey):
    project_id: str


class ExperimentKey(ContainerKey):
    id: str


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
    instance: Optional[str] = None

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


def add_structured_properties_to_entity_wu(
    entity_urn: str, structured_properties: Dict[StructuredPropertyUrn, str]
) -> Iterable[MetadataWorkUnit]:
    aspect = StructuredPropertiesClass(
        properties=[
            StructuredPropertyValueAssignmentClass(
                propertyUrn=urn.urn(),
                values=[value],
            )
            for urn, value in structured_properties.items()
        ]
    )
    yield MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        aspect=aspect,
    ).as_workunit()


def gen_containers(
    container_key: KeyType,
    name: str,
    sub_types: List[str],
    parent_container_key: Optional[ContainerKey] = None,
    extra_properties: Optional[Dict[str, str]] = None,
    structured_properties: Optional[Dict[StructuredPropertyUrn, str]] = None,
    domain_urn: Optional[str] = None,
    description: Optional[str] = None,
    owner_urn: Optional[str] = None,
    external_url: Optional[str] = None,
    tags: Optional[List[str]] = None,
    qualified_name: Optional[str] = None,
    created: Optional[int] = None,
    last_modified: Optional[int] = None,
) -> Iterable[MetadataWorkUnit]:
    # Extra validation on the env field.
    # In certain cases (mainly for backwards compatibility), the env field will actually
    # have a platform instance name.
    env = container_key.env if container_key.env in ALL_ENV_TYPES else None

    container_urn = container_key.as_urn()

    if parent_container_key:  # Yield Container aspect first for auto_browse_path_v2
        parent_container_urn = make_container_urn(guid=parent_container_key.guid())

        # Set database container
        parent_container_mcp = MetadataChangeProposalWrapper(
            entityUrn=f"{container_urn}",
            aspect=ContainerClass(container=parent_container_urn),
        )
        yield parent_container_mcp.as_workunit()

    yield MetadataChangeProposalWrapper(
        entityUrn=f"{container_urn}",
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
            lastModified=(
                TimeStamp(time=last_modified) if last_modified is not None else None
            ),
            env=env if _INCLUDE_ENV_IN_CONTAINER_PROPERTIES else None,
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
            instance=(
                f"{make_dataplatform_instance_urn(container_key.platform, container_key.instance)}"
                if container_key.instance
                else None
            ),
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

    if structured_properties:
        yield from add_structured_properties_to_entity_wu(
            entity_urn=container_urn, structured_properties=structured_properties
        )


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


def entity_supports_aspect(entity_type: str, aspect_type: Type[Aspect]) -> bool:
    entity_key_aspect = KEY_ASPECTS[entity_type]
    aspect_name = aspect_type.get_aspect_name()

    supported_aspects = entity_key_aspect.ASPECT_INFO["entityAspects"]

    return aspect_name in supported_aspects
