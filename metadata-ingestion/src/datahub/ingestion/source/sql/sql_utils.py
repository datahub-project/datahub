# TODO: Remove to common
from typing import Dict, Iterable, List, Optional

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_domain_urn,
)
from datahub.emitter.mcp_builder import (
    DatabaseKey,
    PlatformKey,
    SchemaKey,
    add_dataset_to_container,
    add_domain_to_entity_wu,
    gen_containers,
    wrap_aspect_as_workunit,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConfig
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


def gen_schema_containers(
    config: "SQLAlchemyConfig",
    schema: str,
    database: str,
    sub_types: List[str],
    platform: Optional[str] = None,
    domain_registry: Optional[DomainRegistry] = None,
    platform_instance: Optional[str] = None,
    env: Optional[str] = None,
    report: Optional[SourceReport] = None,
    database_container_key: Optional[PlatformKey] = None,
    schema_container_key: Optional[PlatformKey] = None,
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

    if not platform:
        platform = config.platform

    assert platform

    if not platform_instance:
        platform_instance = config.platform_instance

    if not env:
        env = config.env

    database_container_key = (
        gen_database_key(
            database, platform=platform, platform_instance=platform_instance, env=env
        )
        if not database_container_key
        else database_container_key
    )

    schema_container_key = (
        gen_schema_key(
            db_name=database,
            schema=schema,
            platform=platform,
            platform_instance=platform_instance,
            env=env,
        )
        if not schema_container_key
        else schema_container_key
    )

    domain_urn: Optional[str] = None
    if domain_registry:
        domain_urn = gen_domain_urn(
            f"{database}.{schema}",
            domain_config=config.domain,
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


def gen_database_containers(
    config: SQLAlchemyConfig,
    database: str,
    sub_types: List[str],
    platform: Optional[str] = None,
    domain_registry: Optional[DomainRegistry] = None,
    platform_instance: Optional[str] = None,
    env: Optional[str] = None,
    report: Optional[SourceReport] = None,
    database_container_key: Optional[PlatformKey] = None,
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
        domain_urn = gen_domain_urn(
            database, domain_config=config.domain, domain_registry=domain_registry
        )

    if not platform:
        platform = config.platform

    assert platform

    if not platform_instance:
        platform_instance = config.platform_instance

    if not env:
        env = config.env

    database_container_key = (
        gen_database_key(
            database, platform=platform, platform_instance=platform_instance, env=env
        )
        if not database_container_key
        else database_container_key
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
    config: SQLAlchemyConfig,
    dataset_urn: str,
    db_name: str,
    schema: str,
    platform: Optional[str] = None,
    platform_instance: Optional[str] = None,
    env: Optional[str] = None,
    report: Optional[SourceReport] = None,
    schema_container_key: Optional[PlatformKey] = None,
) -> Iterable[MetadataWorkUnit]:

    if not platform:
        platform = config.platform

    assert platform

    if not platform_instance:
        platform_instance = config.platform_instance

    if not env:
        env = config.env

    schema_container_key = (
        gen_schema_key(
            db_name=db_name,
            schema=schema,
            platform=platform,
            platform_instance=platform_instance,
            env=env,
        )
        if not schema_container_key
        else schema_container_key
    )

    container_workunits = add_dataset_to_container(
        container_key=schema_container_key,
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

        return wrap_aspect_as_workunit(
            "dataset", dataset_urn, "dataPlatformInstance", aspect
        )
    else:
        return None
