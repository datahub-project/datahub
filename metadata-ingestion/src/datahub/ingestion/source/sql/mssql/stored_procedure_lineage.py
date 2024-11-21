from typing import Iterable

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.sql.mssql.job_models import StoredProcedure
from datahub.sql_parsing.datajob import to_job_lineage
from datahub.sql_parsing.split_statements import split_statements
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)


# Is procedure handling generic enough to be added to SqlParsingAggregator?
def generate_procedure_lineage(
    *,
    aggregator: SqlParsingAggregator,
    procedure: StoredProcedure,
    procedure_job_urn: str,
) -> Iterable[MetadataChangeProposalWrapper]:
    if procedure.code:
        for query in split_statements(procedure.code):
            aggregator.add_observed_query(
                observed=ObservedQuery(
                    default_db=procedure.db,
                    default_schema=procedure.schema,
                    query=query,
                )
            )
        mcps = list(aggregator.gen_metadata())
        yield from to_job_lineage(
            job_urn=procedure_job_urn,
            mcps=mcps,
            ignore_extra_mcps=True,
        )
