from typing import Iterable, Optional

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


def parse_procedure_code(
    *,
    schema_resolver: SchemaResolver,
    default_db: Optional[str],
    default_schema: Optional[str],
    code: str,
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

    mcps = list(aggregator.gen_metadata())
    return to_datajob_input_output(
        mcps=mcps,
        ignore_extra_mcps=True,
    )


# Is procedure handling generic enough to be added to SqlParsingAggregator?
def generate_procedure_lineage(
    *,
    aggregator: SqlParsingAggregator,
    procedure: StoredProcedure,
    procedure_job_urn: str,
) -> Iterable[MetadataChangeProposalWrapper]:
    if procedure.code:
        datajob_input_output = parse_procedure_code(
            schema_resolver=aggregator._schema_resolver,
            default_db=procedure.db,
            default_schema=procedure.schema,
            code=procedure.code,
        )

        if datajob_input_output:
            yield MetadataChangeProposalWrapper(
                entityUrn=procedure_job_urn,
                aspect=datajob_input_output,
            )
