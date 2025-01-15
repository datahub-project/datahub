import logging
from typing import Callable, Iterable, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.sql.mysql.job_models import MySQLStoredProcedure
from datahub.metadata.schema_classes import DataJobInputOutputClass
from datahub.sql_parsing.datajob import to_datajob_input_output
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.split_statements import split_statements
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)

logger = logging.getLogger(__name__)


def parse_procedure_code(
    *,
    schema_resolver: SchemaResolver,
    default_db: Optional[str],
    default_schema: Optional[str],
    code: str,
    is_temp_table: Callable[[str], bool],
    raise_: bool = False,
) -> Optional[DataJobInputOutputClass]:
    """
    Parse MySQL stored procedure code to extract lineage information.
    Args:
        schema_resolver: Resolver for database schemas
        default_db: Default database context
        default_schema: Default schema context
        code: The stored procedure code to parse
        is_temp_table: Function to determine if a table reference is temporary
        raise_: Whether to raise exceptions on parsing failures
    Returns:
        DataJobInputOutputClass containing lineage information if successful
    """
    aggregator = SqlParsingAggregator(
        platform=schema_resolver.platform,
        env=schema_resolver.env,
        schema_resolver=schema_resolver,
        generate_lineage=True,
        generate_queries=False,
        generate_usage_statistics=False,
        generate_operations=False,
        generate_query_subject_fields=False,
        generate_query_usage_statistics=False,
        is_temp_table=is_temp_table,
    )

    for query in split_statements(code):
        aggregator.add_observed_query(
            observed=ObservedQuery(
                default_db=default_db,
                default_schema=default_schema,
                query=query,
            )
        )

    if aggregator.report.num_observed_queries_failed and raise_:
        logger.info(aggregator.report.as_string())
        raise ValueError(
            f"Failed to parse {aggregator.report.num_observed_queries_failed} queries."
        )

    mcps = list(aggregator.gen_metadata())
    return to_datajob_input_output(
        mcps=mcps,
        ignore_extra_mcps=True,
    )


def generate_procedure_lineage(
    *,
    schema_resolver: SchemaResolver,
    procedure: MySQLStoredProcedure,
    procedure_job_urn: str,
    is_temp_table: Callable[[str], bool] = lambda _: False,
    raise_: bool = False,
) -> Iterable[MetadataChangeProposalWrapper]:
    """
    Generate lineage information for a MySQL stored procedure.
    Args:
        schema_resolver: Resolver for database schemas
        procedure: The stored procedure to analyze
        procedure_job_urn: URN for the procedure job
        is_temp_table: Function to determine if a table reference is temporary
        raise_: Whether to raise exceptions on parsing failures
    Yields:
        MetadataChangeProposalWrapper objects containing lineage information
    """
    if procedure.code:
        datajob_input_output = parse_procedure_code(
            schema_resolver=schema_resolver,
            default_db=procedure.db,
            default_schema=procedure.routine_schema,
            code=procedure.code,
            is_temp_table=is_temp_table,
            raise_=raise_,
        )

        if datajob_input_output:
            yield MetadataChangeProposalWrapper(
                entityUrn=procedure_job_urn,
                aspect=datajob_input_output,
            )
