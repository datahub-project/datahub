from typing import Dict, Iterable, List, Optional, Tuple

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_domain_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
    DatabaseKey,
    SchemaKey,
    add_dataset_to_container,
    add_domain_to_entity_wu,
    gen_containers,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import DataPlatformInstanceClass
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.urns.dataset_urn import DatasetUrn

ARRAY_TOKEN = "[type=array]"
UNION_TOKEN = "[type=union]"
KEY_SCHEMA_PREFIX = "[key=True]."
VERSION_PREFIX = "[version=2.0]."


def gen_schema_key(
    db_name: str,
    schema: str,
    platform: str,
    platform_instance: Optional[str],
    env: Optional[str],
) -> SchemaKey:
    return SchemaKey(
        database=db_name,
        schema=schema,
        platform=platform,
        instance=platform_instance,
        env=env,
        backcompat_env_as_instance=True,
    )


def gen_database_key(
    database: str, platform: str, platform_instance: Optional[str], env: Optional[str]
) -> DatabaseKey:
    return DatabaseKey(
        database=database,
        platform=platform,
        instance=platform_instance,
        env=env,
        backcompat_env_as_instance=True,
    )


def gen_schema_container(
    schema: str,
    database: str,
    sub_types: List[str],
    database_container_key: ContainerKey,
    schema_container_key: ContainerKey,
    domain_registry: Optional[DomainRegistry] = None,
    domain_config: Optional[Dict[str, AllowDenyPattern]] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    owner_urn: Optional[str] = None,
    external_url: Optional[str] = None,
    tags: Optional[List[str]] = None,
    qualified_name: Optional[str] = None,
    created: Optional[int] = None,
    last_modified: Optional[int] = None,
    extra_properties: Optional[Dict[str, str]] = None,
) -> Iterable[MetadataWorkUnit]:
    domain_urn: Optional[str] = None
    if domain_registry:
        assert domain_config
        domain_urn = gen_domain_urn(
            f"{database}.{schema}",
            domain_config=domain_config,
            domain_registry=domain_registry,
        )

    yield from gen_containers(
        container_key=schema_container_key,
        name=name if name else schema,
        sub_types=sub_types,
        parent_container_key=database_container_key,
        domain_urn=domain_urn,
        external_url=external_url,
        description=description,
        created=created,
        last_modified=last_modified,
        tags=tags,
        owner_urn=owner_urn,
        qualified_name=qualified_name,
        extra_properties=extra_properties,
    )


def gen_domain_urn(
    dataset_name: str,
    domain_config: Dict[str, AllowDenyPattern],
    domain_registry: DomainRegistry,
) -> Optional[str]:
    domain_urn: Optional[str] = None

    domain: str
    pattern: AllowDenyPattern
    for domain, pattern in domain_config.items():
        if pattern.allowed(dataset_name):
            domain_urn = make_domain_urn(domain_registry.get_domain_urn(domain))

    return domain_urn


def gen_database_container(
    database: str,
    database_container_key: ContainerKey,
    sub_types: List[str],
    domain_config: Optional[Dict[str, AllowDenyPattern]] = None,
    domain_registry: Optional[DomainRegistry] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    owner_urn: Optional[str] = None,
    external_url: Optional[str] = None,
    tags: Optional[List[str]] = None,
    qualified_name: Optional[str] = None,
    created: Optional[int] = None,
    last_modified: Optional[int] = None,
    extra_properties: Optional[Dict[str, str]] = None,
) -> Iterable[MetadataWorkUnit]:
    domain_urn: Optional[str] = None
    if domain_registry:
        assert domain_config
        domain_urn = gen_domain_urn(
            database, domain_config=domain_config, domain_registry=domain_registry
        )

    yield from gen_containers(
        container_key=database_container_key,
        name=name if name else database,
        sub_types=sub_types,
        domain_urn=domain_urn,
        external_url=external_url,
        description=description,
        created=created,
        last_modified=last_modified,
        tags=tags,
        owner_urn=owner_urn,
        qualified_name=qualified_name,
        extra_properties=extra_properties,
    )


def add_table_to_schema_container(
    dataset_urn: str,
    parent_container_key: ContainerKey,
) -> Iterable[MetadataWorkUnit]:
    yield from add_dataset_to_container(
        container_key=parent_container_key,
        dataset_urn=dataset_urn,
    )


def get_domain_wu(
    dataset_name: str,
    entity_urn: str,
    domain_config: Dict[str, AllowDenyPattern],
    domain_registry: DomainRegistry,
) -> Iterable[MetadataWorkUnit]:
    domain_urn = gen_domain_urn(dataset_name, domain_config, domain_registry)
    if domain_urn:
        yield from add_domain_to_entity_wu(
            entity_urn=entity_urn,
            domain_urn=domain_urn,
        )


def get_dataplatform_instance_aspect(
    dataset_urn: str, platform: str, platform_instance: Optional[str]
) -> Optional[MetadataWorkUnit]:
    # If we are a platform instance based source, emit the instance aspect
    if platform_instance:
        aspect = DataPlatformInstanceClass(
            platform=make_data_platform_urn(platform),
            instance=make_dataplatform_instance_urn(platform, platform_instance),
        )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=aspect,
        ).as_workunit()
    else:
        return None


def gen_lineage(
    dataset_urn: str,
    lineage_info: Optional[Tuple[UpstreamLineage, Dict[str, str]]] = None,
    incremental_lineage: bool = True,
) -> Iterable[MetadataWorkUnit]:
    if lineage_info is None:
        return

    upstream_lineage, upstream_column_props = lineage_info
    if upstream_lineage is not None:
        if incremental_lineage:
            patch_builder: DatasetPatchBuilder = DatasetPatchBuilder(urn=dataset_urn)
            for upstream in upstream_lineage.upstreams:
                patch_builder.add_upstream_lineage(upstream)

            lineage_workunits = [
                MetadataWorkUnit(
                    id=f"upstreamLineage-for-{dataset_urn}",
                    mcp_raw=mcp,
                )
                for mcp in patch_builder.build()
            ]
        else:
            lineage_workunits = [
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=upstream_lineage
                ).as_workunit()
            ]

        for wu in lineage_workunits:
            yield wu


# downgrade a schema field
def downgrade_schema_field_from_v2(field: SchemaField) -> SchemaField:
    field.fieldPath = DatasetUrn.get_simple_field_path_from_v2_field_path(
        field.fieldPath
    )
    return field


# downgrade a list of schema fields
def downgrade_schema_from_v2(
    canonical_schema: List[SchemaField],
) -> List[SchemaField]:
    return [downgrade_schema_field_from_v2(field) for field in canonical_schema]


# v2 is only required in case UNION or ARRAY types are present- all other types can be represented in v1 paths
def schema_requires_v2(canonical_schema: List[SchemaField]) -> bool:
    for field in canonical_schema:
        field_name = field.fieldPath
        if ARRAY_TOKEN in field_name or UNION_TOKEN in field_name:
            return True
    return False
