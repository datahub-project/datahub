import logging
from typing import Callable, Iterable, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.sql.mssql.job_models import StoredProcedure
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
        # TODO: We should take into account `USE x` statements.
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


# Is procedure handling generic enough to be added to SqlParsingAggregator?
def generate_procedure_lineage(
    *,
    schema_resolver: SchemaResolver,
    procedure: StoredProcedure,
    procedure_job_urn: str,
    is_temp_table: Callable[[str], bool] = lambda _: False,
    raise_: bool = False,
) -> Iterable[MetadataChangeProposalWrapper]:
    if procedure.code:
        datajob_input_output = parse_procedure_code(
            schema_resolver=schema_resolver,
            default_db=procedure.db,
            default_schema=procedure.schema,
            code=procedure.code,
            is_temp_table=is_temp_table,
            raise_=raise_,
        )

        if datajob_input_output:
            yield MetadataChangeProposalWrapper(
                entityUrn=procedure_job_urn,
                aspect=datajob_input_output,
            )
