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
    default_schema: Optional[str],
    code: str,
    is_temp_table: Callable[[str], bool],
    raise_: bool = False,
) -> Optional[DataJobInputOutputClass]:
    """
    Parse MySQL/MariaDB stored procedure code to extract lineage information.
    """
    import re

    logger.debug(f"Parsing code with default_schema={default_schema}")

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

    # Extract the procedure body between BEGIN and END
    try:
        body_match = re.search(r"BEGIN(.*?)END", code, re.DOTALL | re.IGNORECASE)
        if body_match:
            procedure_body = body_match.group(1)
            logger.debug("Successfully extracted procedure body between BEGIN/END")
        else:
            procedure_body = code
            logger.debug("No BEGIN/END found, using full code")
    except Exception as e:
        logger.debug(f"Error extracting procedure body: {e}")
        procedure_body = code

    queries = list(split_statements(procedure_body))
    logger.debug(f"Split code into {len(queries)} statements")

    # Filter for data manipulation statements
    dml_queries = []
    for query in queries:
        if any(
            keyword in query.upper()
            for keyword in [
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "MERGE",
                "CREATE TABLE",
                "ALTER TABLE",
                "DROP TABLE",
            ]
        ):
            dml_queries.append(query)

    logger.debug(f"Found {len(dml_queries)} DML statements to analyze")

    for i, query in enumerate(dml_queries):
        try:
            logger.debug(f"Processing DML query {i + 1}: {query[:100]}...")
            aggregator.add_observed_query(
                observed=ObservedQuery(
                    default_schema=default_schema,
                    query=query,
                )
            )
        except Exception as e:
            logger.debug(f"Failed to parse DML query {i + 1}: {e}")
            if raise_:
                raise

    if aggregator.report.num_observed_queries_failed:
        logger.debug(
            f"Failed to parse {aggregator.report.num_observed_queries_failed} queries"
        )
        if raise_:
            logger.info(aggregator.report.as_string())
            raise ValueError(
                f"Failed to parse {aggregator.report.num_observed_queries_failed} queries."
            )

    mcps = list(aggregator.gen_metadata())
    logger.debug(f"Generated {len(mcps)} metadata change proposals")

    result = to_datajob_input_output(mcps=mcps, ignore_extra_mcps=True)
    if result:
        logger.debug("Successfully created DataJobInputOutputClass")
    else:
        logger.debug("No DataJobInputOutputClass created")

    return result


def generate_procedure_lineage(
    *,
    schema_resolver: SchemaResolver,
    procedure: MySQLStoredProcedure,
    procedure_job_urn: str,
    is_temp_table: Callable[[str], bool] = lambda _: False,
    raise_: bool = False,
) -> Iterable[MetadataChangeProposalWrapper]:
    """
    Generate comprehensive lineage information for a MySQL/MariaDB stored procedure.
    """
    if not procedure.code:
        logger.debug(f"No code found for procedure {procedure.full_name}")
        return

    logger.debug(f"Processing lineage for procedure {procedure.full_name}")
    logger.debug(f"Procedure code:\n{procedure.code}")

    try:
        # Parse the procedure code to extract lineage
        datajob_input_output = parse_procedure_code(
            schema_resolver=schema_resolver,
            default_schema=procedure.routine_schema,
            code=procedure.code,
            is_temp_table=is_temp_table,
            raise_=raise_,
        )

        if datajob_input_output:
            logger.debug(f"Found lineage for {procedure.full_name}:")
            logger.debug(f"Input datasets: {datajob_input_output.inputDatasets}")
            logger.debug(f"Output datasets: {datajob_input_output.outputDatasets}")
            logger.debug(f"Input jobs: {datajob_input_output.inputDatajobs}")

            wrapper = MetadataChangeProposalWrapper(
                entityUrn=procedure_job_urn,
                aspect=datajob_input_output,
            )
            logger.debug(f"Created workunit with URN: {procedure_job_urn}")
            yield wrapper
        else:
            logger.debug(f"No lineage found for procedure {procedure.full_name}")

    except Exception as e:
        logger.error(
            f"Failed to generate lineage for procedure {procedure.full_name}: {e}",
            exc_info=True,
        )
        if raise_:
            raise
