# TODO: Remove to common
from typing import Dict, Iterable, List, Optional

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_domain_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    DatabaseKey,
    PlatformKey,
    SchemaKey,
    add_dataset_to_container,
    add_domain_to_entity_wu,
    gen_containers,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import DataPlatformInstanceClass
from datahub.utilities.registries.domain_registry import DomainRegistry


def gen_schema_key(
    db_name: str,
    schema: str,
    platform: str,
    platform_instance: Optional[str],
    env: Optional[str],
) -> PlatformKey:
    return SchemaKey(
        database=db_name,
        schema=schema,
        platform=platform,
        instance=platform_instance,
        backcompat_instance_for_guid=env,
    )


def gen_database_key(
    database: str, platform: str, platform_instance: Optional[str], env: Optional[str]
) -> PlatformKey:
    return DatabaseKey(
        database=database,
        platform=platform,
        instance=platform_instance,
        backcompat_instance_for_guid=env,
    )


def gen_schema_container(
    schema: str,
    database: str,
    sub_types: List[str],
    database_container_key: PlatformKey,
    schema_container_key: PlatformKey,
    domain_registry: Optional[DomainRegistry] = None,
    domain_config: Optional[Dict[str, AllowDenyPattern]] = None,
    report: Optional[SourceReport] = None,
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

    container_workunits = gen_containers(
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

    for wu in container_workunits:
        if report:
            report.report_workunit(wu)
        yield wu


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
    database_container_key: PlatformKey,
    sub_types: List[str],
    domain_config: Optional[Dict[str, AllowDenyPattern]] = None,
    domain_registry: Optional[DomainRegistry] = None,
    report: Optional[SourceReport] = None,
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

    container_workunits = gen_containers(
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

    for wu in container_workunits:
        if report:
            report.report_workunit(wu)
        yield wu


def add_table_to_schema_container(
    dataset_urn: str,
    parent_container_key: PlatformKey,
    report: Optional[SourceReport] = None,
) -> Iterable[MetadataWorkUnit]:

    container_workunits = add_dataset_to_container(
        container_key=parent_container_key,
        dataset_urn=dataset_urn,
    )
    for wu in container_workunits:
        if report:
            report.report_workunit(wu)
        yield wu


def get_domain_wu(
    dataset_name: str,
    entity_urn: str,
    domain_config: Dict[str, AllowDenyPattern],
    domain_registry: DomainRegistry,
    report: SourceReport,
) -> Iterable[MetadataWorkUnit]:
    domain_urn = gen_domain_urn(dataset_name, domain_config, domain_registry)
    if domain_urn:
        wus = add_domain_to_entity_wu(
            entity_urn=entity_urn,
            domain_urn=domain_urn,
        )
        for wu in wus:
            report.report_workunit(wu)
            yield wu


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
