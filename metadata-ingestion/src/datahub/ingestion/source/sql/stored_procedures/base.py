import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Set

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.engine.reflection import Inspector

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
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sql.stored_procedures.lineage import parse_procedure_code
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataPlatformInstanceClass,
    DataTransformClass,
    DataTransformLogicClass,
    QueryLanguageClass,
    QueryStatementClass,
    SubTypesClass,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver

logger = logging.getLogger(__name__)


def extract_temp_tables_from_sql(sql: str, temp_table_pattern: re.Pattern) -> Set[str]:
    """Extract temporary table names from SQL using platform-specific regex pattern."""
    temp_tables = set()
    for match in temp_table_pattern.finditer(sql):
        table_name = match.group(2) if len(match.groups()) >= 2 else match.group(1)
        if table_name:
            temp_tables.add(table_name)
    return temp_tables


def make_temp_table_checker(
    procedure_definition: Optional[str],
    temp_table_pattern: re.Pattern,
    case_normalizer: Callable[[str], str] = str.lower,
) -> Optional[Callable[[str], bool]]:
    """
    Create a temp table checker function for a stored procedure.

    Extracts temp table names from the procedure SQL and returns a function
    that checks if a given table name matches any of them.

    Args:
        procedure_definition: SQL code of the stored procedure
        temp_table_pattern: Regex pattern to match temp table creation statements
        case_normalizer: Function to normalize case (str.lower for MySQL/PostgreSQL, str.upper for Oracle)

    Returns:
        A function that checks if a table name is a temp table, or None if no temp tables found
    """
    if not procedure_definition:
        return None

    temp_tables = extract_temp_tables_from_sql(procedure_definition, temp_table_pattern)
    temp_tables_normalized = {case_normalizer(t) for t in temp_tables}

    if not temp_tables_normalized:
        return None

    def is_temp_table(table_name: str) -> bool:
        table_name_only = table_name.split(".")[-1]
        return case_normalizer(table_name_only) in temp_tables_normalized

    return is_temp_table


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
            flow_id=_get_procedure_flow_name(database_key, schema_key),
            job_id=self.get_procedure_identifier(),
            cluster=database_key.env or DEFAULT_ENV,
            platform_instance=database_key.instance,
        )


def _generate_flow_workunits(
    database_key: DatabaseKey, schema_key: Optional[SchemaKey]
) -> Iterable[MetadataWorkUnit]:
    procedure_flow_name = _get_procedure_flow_name(database_key, schema_key)

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

    yield MetadataChangeProposalWrapper(
        entityUrn=flow_urn,
        aspect=ContainerClass(container=database_key.as_urn()),
    ).as_workunit()


def _get_procedure_flow_name(
    database_key: DatabaseKey, schema_key: Optional[SchemaKey]
) -> str:
    if schema_key:
        procedure_flow_name = (
            f"{schema_key.database}.{schema_key.db_schema}.stored_procedures"
        )
    else:
        procedure_flow_name = f"{database_key.database}.stored_procedures"
    return procedure_flow_name


def _generate_job_workunits(
    database_key: DatabaseKey,
    schema_key: Optional[SchemaKey],
    procedure: BaseProcedure,
    include_stored_procedures_code: bool = True,
) -> Iterable[MetadataWorkUnit]:
    job_urn = procedure.to_urn(database_key, schema_key)

    yield MetadataChangeProposalWrapper(
        entityUrn=job_urn,
        aspect=DataJobInfoClass(
            name=procedure.name,
            type=JobContainerSubTypes.STORED_PROCEDURE,
            description=procedure.comment,
            customProperties=procedure.extra_properties,
        ),
    ).as_workunit()

    yield MetadataChangeProposalWrapper(
        entityUrn=job_urn,
        aspect=SubTypesClass(
            typeNames=[JobContainerSubTypes.STORED_PROCEDURE],
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

    if include_stored_procedures_code and procedure.procedure_definition:
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


def _is_temp_table_field(
    schema_field_urn: str,
    is_temp_table: Callable[[str], bool],
    dataset_urn_to_key: Callable[[str], Any],
) -> bool:
    """Check if a schemaField URN references a temp table."""
    try:
        if schema_field_urn.startswith("urn:li:schemaField:("):
            # Format: urn:li:schemaField:(<DATASET_URN>,<FIELD_NAME>)
            # Find the last comma to split dataset URN from field name
            inner_content = schema_field_urn[len("urn:li:schemaField:(") : -1]
            last_comma_idx = inner_content.rfind(",")
            if last_comma_idx == -1:
                return False
            dataset_urn = inner_content[:last_comma_idx]
            dataset_key = dataset_urn_to_key(dataset_urn)
            if dataset_key is None:
                return False
            return is_temp_table(dataset_key.name)
    except Exception as e:
        logger.warning(
            f"Failed to check if schemaField URN is temp table {schema_field_urn}: {e}",
            exc_info=True,
        )
    return False


def _filter_temp_tables_from_lineage(
    lineage: DataJobInputOutputClass,
    is_temp_table: Callable[[str], bool],
) -> DataJobInputOutputClass:
    """Filter out temporary tables from lineage input/output datasets and fine-grained lineage."""
    from datahub.emitter.mce_builder import dataset_urn_to_key

    def filter_datasets(urns: List[str]) -> List[str]:
        filtered = []
        for urn in urns:
            try:
                dataset_key = dataset_urn_to_key(urn)
                if dataset_key is None:
                    filtered.append(urn)
                    continue
                table_name = dataset_key.name
                if not is_temp_table(table_name):
                    filtered.append(urn)
                else:
                    logger.debug(f"Filtered out temp table from lineage: {table_name}")
            except Exception as e:
                logger.warning(f"Failed to parse dataset URN {urn}: {e}")
                filtered.append(urn)
        return filtered

    lineage.inputDatasets = filter_datasets(lineage.inputDatasets or [])
    lineage.outputDatasets = filter_datasets(lineage.outputDatasets or [])

    if lineage.fineGrainedLineages:
        filtered_fgl = []
        for fgl in lineage.fineGrainedLineages:
            if fgl.upstreams:
                # Filter upstreams that reference temp tables
                fgl.upstreams = [
                    upstream_urn
                    for upstream_urn in fgl.upstreams
                    if not _is_temp_table_field(
                        upstream_urn, is_temp_table, dataset_urn_to_key
                    )
                ]
            # Only keep fine-grained lineage if it still has upstreams after filtering
            if fgl.upstreams:
                filtered_fgl.append(fgl)
        lineage.fineGrainedLineages = filtered_fgl

    return lineage


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

        if datajob_input_output:
            # Filter out any temp table URNs that may have slipped through the SQL parser
            # The SQL parser already tracks lineage through temp tables (A→temp→B becomes A→B)
            # but we filter as a safety net for edge cases
            datajob_input_output = _filter_temp_tables_from_lineage(
                datajob_input_output, is_temp_table
            )

            if (
                datajob_input_output.inputDatasets
                or datajob_input_output.outputDatasets
            ):
                yield MetadataChangeProposalWrapper(
                    entityUrn=procedure_job_urn,
                    aspect=datajob_input_output,
                )
            else:
                logger.debug(
                    f"Skipping empty lineage for procedure {procedure.name} after temp table filtering"
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
) -> Iterable[MetadataWorkUnit]:
    yield from _generate_flow_workunits(database_key, schema_key)


def generate_procedure_workunits(
    procedure: BaseProcedure,
    database_key: DatabaseKey,
    schema_key: Optional[SchemaKey],
    schema_resolver: Optional[SchemaResolver],
    is_temp_table_fn: Optional[Callable[[str], bool]] = None,
    include_stored_procedures_code: bool = True,
) -> Iterable[MetadataWorkUnit]:
    yield from _generate_job_workunits(
        database_key, schema_key, procedure, include_stored_procedures_code
    )

    if schema_resolver:
        job_urn = procedure.to_urn(database_key, schema_key)

        if is_temp_table_fn is not None:
            yield from auto_workunit(
                generate_procedure_lineage(
                    schema_resolver=schema_resolver,
                    procedure=procedure,
                    procedure_job_urn=job_urn,
                    default_db=database_key.database,
                    default_schema=schema_key.db_schema if schema_key else None,
                    is_temp_table=is_temp_table_fn,
                )
            )
        else:
            yield from auto_workunit(
                generate_procedure_lineage(
                    schema_resolver=schema_resolver,
                    procedure=procedure,
                    procedure_job_urn=job_urn,
                    default_db=database_key.database,
                    default_schema=schema_key.db_schema if schema_key else None,
                )
            )


def _wrap_fetch_with_error_handling(
    fetch_fn: Callable[[], List[BaseProcedure]],
    source_name: str,
    schema: str,
    report: SQLSourceReport,
    permission_error_message: str,
    system_table: str,
) -> List[BaseProcedure]:
    """Wraps procedure fetching with error handling. Returns empty list on error."""
    try:
        return fetch_fn()
    except Exception as e:
        logger.warning(
            f"Failed to get stored procedures for {source_name} schema {schema}: {e}"
        )

        report.warning(
            title="Failed to Ingest Stored Procedures",
            message=permission_error_message,
            context=f"schema={schema}, table={system_table}",
            exc=e,
        )

        return []


def fetch_procedures_from_query(
    inspector: Inspector,
    query: str,
    params: Dict[str, Any],
    row_mapper: Callable[[Any], Optional[BaseProcedure]],
    source_name: str,
    schema: str,
    report: SQLSourceReport,
    permission_error_message: str,
    system_table: str,
    use_text_wrapper: bool = True,
) -> List[BaseProcedure]:
    """Fetch procedures using a single SQL query. Used by MySQL, PostgreSQL, etc."""

    def fetch_procedures() -> List[BaseProcedure]:
        base_procedures = []
        with inspector.engine.connect() as conn:
            if use_text_wrapper:
                result = conn.execute(text(query), params)
            else:
                result = conn.execute(query, params)

            for row in result:
                procedure = row_mapper(row)
                if procedure is not None:
                    base_procedures.append(procedure)

        return base_procedures

    return _wrap_fetch_with_error_handling(
        fetch_fn=fetch_procedures,
        source_name=source_name,
        schema=schema,
        report=report,
        permission_error_message=permission_error_message,
        system_table=system_table,
    )


def fetch_procedures_with_enrichment(
    inspector: Inspector,
    query: str,
    params: Dict[str, Any],
    row_mapper: Callable[[Connection, Any], Optional[BaseProcedure]],
    source_name: str,
    schema: str,
    report: SQLSourceReport,
    permission_error_message: str,
    system_table: str,
    use_text_wrapper: bool = True,
) -> List[BaseProcedure]:
    """Fetch procedures with per-row enrichment queries (Oracle pattern)."""

    def fetch_procedures() -> List[BaseProcedure]:
        base_procedures = []
        with inspector.engine.connect() as conn:
            if use_text_wrapper:
                result = conn.execute(text(query), params)
            else:
                result = conn.execute(query, params)

            for row in result:
                procedure = row_mapper(conn, row)
                if procedure is not None:
                    base_procedures.append(procedure)

        return base_procedures

    return _wrap_fetch_with_error_handling(
        fetch_fn=fetch_procedures,
        source_name=source_name,
        schema=schema,
        report=report,
        permission_error_message=permission_error_message,
        system_table=system_table,
    )
