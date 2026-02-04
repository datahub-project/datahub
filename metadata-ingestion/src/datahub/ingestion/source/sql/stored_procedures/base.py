import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    datahub_guid,
    make_data_flow_urn,
    make_data_job_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    FlowContainerSubTypes,
    JobContainerSubTypes,
)
from datahub.ingestion.source.sql.stored_procedures.lineage import parse_procedure_code
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataPlatformInstanceClass,
    DataTransformClass,
    DataTransformLogicClass,
    QueryLanguageClass,
    QueryStatementClass,
    SubTypesClass,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver

logger = logging.getLogger(__name__)


@dataclass
class BaseProcedure:
    name: str
    procedure_definition: Optional[str]
    created: Optional[datetime]
    last_altered: Optional[datetime]
    comment: Optional[str]
    argument_signature: Optional[str]
    return_type: Optional[str]
    language: str
    extra_properties: Optional[Dict[str, str]]
    default_db: Optional[str] = None
    default_schema: Optional[str] = None
    subtype: str = JobContainerSubTypes.STORED_PROCEDURE

    def get_procedure_identifier(
        self,
    ) -> str:
        if self.argument_signature:
            argument_signature_hash = datahub_guid(
                dict(argument_signature=self.argument_signature)
            )
            return f"{self.name}_{argument_signature_hash}"

        return self.name

    def to_urn(self, database_key: DatabaseKey, schema_key: Optional[SchemaKey]) -> str:
        return make_data_job_urn(
            orchestrator=database_key.platform,
            flow_id=_get_procedure_flow_name(database_key, schema_key, self.subtype),
            job_id=self.get_procedure_identifier(),
            cluster=database_key.env or DEFAULT_ENV,
            platform_instance=database_key.instance,
        )


def _generate_flow_workunits(
    database_key: DatabaseKey, schema_key: Optional[SchemaKey], subtype: str
) -> Iterable[MetadataWorkUnit]:
    """Generate flow workunits for database and schema with specific subtype"""

    procedure_flow_name = _get_procedure_flow_name(database_key, schema_key, subtype)

    flow_urn = make_data_flow_urn(
        orchestrator=database_key.platform,
        flow_id=procedure_flow_name,
        cluster=database_key.env or DEFAULT_ENV,
        platform_instance=database_key.instance,
    )

    yield MetadataChangeProposalWrapper(
        entityUrn=flow_urn,
        aspect=DataFlowInfoClass(
            name=procedure_flow_name,
        ),
    ).as_workunit()

    yield MetadataChangeProposalWrapper(
        entityUrn=flow_urn,
        aspect=SubTypesClass(
            typeNames=[FlowContainerSubTypes.MSSQL_PROCEDURE_CONTAINER],
        ),
    ).as_workunit()

    if database_key.instance:
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataPlatformInstanceClass(
                platform=make_data_platform_urn(database_key.platform),
                instance=make_dataplatform_instance_urn(
                    platform=database_key.platform,
                    instance=database_key.instance,
                ),
            ),
        ).as_workunit()

    # Only set parent container if database name exists
    # For two-tier sources without database names, flow is top-level
    if database_key.database:
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=ContainerClass(container=database_key.as_urn()),
        ).as_workunit()


def _get_procedure_flow_name(
    database_key: DatabaseKey, schema_key: Optional[SchemaKey], subtype: str
) -> str:
    # Determine container suffix based on subtype
    container_suffix = (
        "functions" if subtype == JobContainerSubTypes.FUNCTION else "stored_procedures"
    )

    if schema_key:
        # For two-tier sources without database names, don't include empty database prefix
        if schema_key.database:
            procedure_flow_name = (
                f"{schema_key.database}.{schema_key.db_schema}.{container_suffix}"
            )
        else:
            procedure_flow_name = f"{schema_key.db_schema}.{container_suffix}"
    else:
        # Handle case where database_key.database might be empty
        if database_key.database:
            procedure_flow_name = f"{database_key.database}.{container_suffix}"
        else:
            procedure_flow_name = container_suffix
    return procedure_flow_name


def _generate_job_workunits(
    database_key: DatabaseKey,
    schema_key: Optional[SchemaKey],
    procedure: BaseProcedure,
) -> Iterable[MetadataWorkUnit]:
    """Generate job workunits for database, schema and procedure"""

    job_urn = procedure.to_urn(database_key, schema_key)

    yield MetadataChangeProposalWrapper(
        entityUrn=job_urn,
        aspect=DataJobInfoClass(
            name=procedure.name,
            type=procedure.subtype,
            description=procedure.comment,
            customProperties=procedure.extra_properties,
        ),
    ).as_workunit()

    yield MetadataChangeProposalWrapper(
        entityUrn=job_urn,
        aspect=SubTypesClass(
            typeNames=[procedure.subtype],
        ),
    ).as_workunit()

    if database_key.instance:
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataPlatformInstanceClass(
                platform=make_data_platform_urn(database_key.platform),
                instance=make_dataplatform_instance_urn(
                    platform=database_key.platform,
                    instance=database_key.instance,
                ),
            ),
        ).as_workunit()

    container_key = schema_key or database_key  # database_key for 2-tier
    yield MetadataChangeProposalWrapper(
        entityUrn=job_urn,
        aspect=ContainerClass(container=container_key.as_urn()),
    ).as_workunit()

    # TODO: Config whether to ingest procedure code
    if procedure.procedure_definition:
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataTransformLogicClass(
                transforms=[
                    DataTransformClass(
                        queryStatement=QueryStatementClass(
                            value=procedure.procedure_definition,
                            language=(
                                QueryLanguageClass.SQL
                                if procedure.language == "SQL"
                                # The language field uses a pretty limited enum.
                                # The "UNKNOWN" enum value is pretty new, so we don't want to
                                # emit it until it has broader server-side support. As a
                                # short-term solution, we map all languages to "SQL".
                                # TODO: Once we've released server 1.1.0, we should change
                                # this to be "UNKNOWN" for all languages except "SQL".
                                else QueryLanguageClass.SQL
                            ),
                        ),
                    )
                ]
            ),
        ).as_workunit()


def _parse_procedure_dependencies(
    dependencies_str: str,
    database_key: DatabaseKey,
    schema_key: Optional[SchemaKey],
    procedure_registry: Optional[Dict[str, str]] = None,
) -> List[str]:
    """
    Parse Oracle/SQL Server dependencies string and convert to DataJob URNs.

    Args:
        dependencies_str: Comma-separated string like "SCHEMA.PROC_NAME (PROCEDURE), SCHEMA.FUNC_NAME (FUNCTION)"
        database_key: Database key for platform/instance/env information
        schema_key: Schema key for schema context
        procedure_registry: Optional mapping of "schema.procedure_name" -> full_job_id (with hash if overloaded)

    Returns:
        List of DataJob URNs for the referenced procedures/functions

    Note:
        For overloaded procedures (multiple signatures), the URN will only match if a procedure_registry
        is provided. Otherwise, only the procedure name is used, which may not match the actual URN
        if the procedure has an argument signature hash.
    """
    input_jobs = []

    # Split by comma
    for dep in dependencies_str.split(","):
        dep = dep.strip()

        # Parse "SCHEMA.NAME (TYPE)" format - match PROCEDURE, FUNCTION, or PACKAGE
        match = re.match(r"^([^(]+)\s*\((PROCEDURE|FUNCTION|PACKAGE)\)$", dep)
        if not match:
            continue

        full_name = match.group(1).strip()
        dep_type = match.group(2).strip()
        parts = full_name.split(".")

        if len(parts) != 2:
            continue

        dep_schema, dep_name = parts

        # Try to look up the full identifier (with hash) from the registry
        registry_key = f"{dep_schema.lower()}.{dep_name.lower()}"
        job_id = dep_name.lower()

        if procedure_registry and registry_key in procedure_registry:
            job_id = procedure_registry[registry_key]

        # Determine subtype based on Oracle object_type
        dep_subtype = (
            JobContainerSubTypes.FUNCTION
            if dep_type == "FUNCTION"
            else JobContainerSubTypes.STORED_PROCEDURE
        )

        # Create DataJob URN for the referenced procedure/function
        dep_job_urn = make_data_job_urn(
            orchestrator=database_key.platform,
            flow_id=_get_procedure_flow_name(
                database_key,
                SchemaKey(
                    database=database_key.database,
                    schema=dep_schema.lower(),
                    platform=database_key.platform,
                    instance=database_key.instance,
                    env=database_key.env,
                    backcompat_env_as_instance=database_key.backcompat_env_as_instance,
                ),
                dep_subtype,
            ),
            job_id=job_id,
            cluster=database_key.env or DEFAULT_ENV,
            platform_instance=database_key.instance,
        )

        input_jobs.append(dep_job_urn)

    return input_jobs


def generate_procedure_lineage(
    *,
    schema_resolver: SchemaResolver,
    procedure: BaseProcedure,
    procedure_job_urn: str,
    default_db: Optional[str] = None,
    default_schema: Optional[str] = None,
    is_temp_table: Callable[[str], bool] = lambda _: False,
    raise_: bool = False,
    report_failure: Optional[Callable[[str], None]] = None,
    database_key: Optional[DatabaseKey] = None,
    schema_key: Optional[SchemaKey] = None,
    procedure_registry: Optional[Dict[str, str]] = None,
) -> Iterable[MetadataChangeProposalWrapper]:
    if procedure.procedure_definition and procedure.language == "SQL":
        datajob_input_output = parse_procedure_code(
            schema_resolver=schema_resolver,
            default_db=default_db,
            default_schema=default_schema,
            code=procedure.procedure_definition,
            is_temp_table=is_temp_table,
            raise_=raise_,
            procedure_name=procedure.name,
        )

        # Add procedure-to-procedure lineage from Oracle/SQL Server dependencies
        if datajob_input_output and procedure.extra_properties and database_key:
            upstream_deps = procedure.extra_properties.get("upstream_dependencies", "")
            if upstream_deps:
                input_datajobs = _parse_procedure_dependencies(
                    upstream_deps, database_key, schema_key, procedure_registry
                )
                if input_datajobs:
                    # Add to existing inputDatajobs or create new list
                    if datajob_input_output.inputDatajobs:
                        datajob_input_output.inputDatajobs.extend(input_datajobs)
                    else:
                        datajob_input_output.inputDatajobs = input_datajobs

        if datajob_input_output:
            yield MetadataChangeProposalWrapper(
                entityUrn=procedure_job_urn,
                aspect=datajob_input_output,
            )
        else:
            logger.warning(
                f"Failed to extract lineage for stored procedure: {procedure.name}. "
                f"URN: {procedure_job_urn}."
            )
            if report_failure:
                report_failure(procedure.name)


def generate_procedure_container_workunits(
    database_key: DatabaseKey,
    schema_key: Optional[SchemaKey],
    subtype: str,
) -> Iterable[MetadataWorkUnit]:
    """Generate container workunits for database and schema with specific subtype"""

    yield from _generate_flow_workunits(database_key, schema_key, subtype)


def generate_procedure_workunits(
    procedure: BaseProcedure,
    database_key: DatabaseKey,
    schema_key: Optional[SchemaKey],
    schema_resolver: Optional[SchemaResolver],
    procedure_registry: Optional[Dict[str, str]] = None,
) -> Iterable[MetadataWorkUnit]:
    yield from _generate_job_workunits(database_key, schema_key, procedure)

    if schema_resolver:
        job_urn = procedure.to_urn(database_key, schema_key)

        yield from auto_workunit(
            generate_procedure_lineage(
                schema_resolver=schema_resolver,
                procedure=procedure,
                procedure_job_urn=job_urn,
                default_db=procedure.default_db or database_key.database,
                default_schema=procedure.default_schema
                or (schema_key.db_schema if schema_key else None),
                database_key=database_key,
                schema_key=schema_key,
                procedure_registry=procedure_registry,
            )
        )
