import logging
from typing import Callable, Optional

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
    procedure_name: Optional[str] = None,
) -> Optional[DataJobInputOutputClass]:
    # Don't split if code starts with CREATE PROCEDURE - treat the entire procedure as one unit
    # The fallback parser inside sqlglot_lineage will handle splitting if needed
    code_upper = code.strip().upper()
    if code_upper.startswith("CREATE PROCEDURE") or code_upper.startswith(
        "CREATE OR REPLACE PROCEDURE"
    ):
        logger.info(
            f"[SPLIT-SKIP] Detected CREATE PROCEDURE, passing entire code to parser ({len(code)} chars)"
        )
        statements = [code]
    else:
        statements = list(split_statements(code))

    aggregator = SqlParsingAggregator(
        platform=schema_resolver.platform,
        platform_instance=schema_resolver.platform_instance,
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

    for query in statements:
        aggregator.add_observed_query(
            observed=ObservedQuery(
                default_db=default_db,
                default_schema=default_schema,
                query=query,
            )
        )

    logger.info(
        f"[AGGREGATOR] Processed {aggregator.report.num_observed_queries} queries, "
        f"{aggregator.report.num_observed_queries_failed} failed"
    )

    if aggregator.report.num_observed_queries_failed and raise_:
        logger.info(aggregator.report.as_string())
        raise ValueError(
            f"Failed to parse {aggregator.report.num_observed_queries_failed} queries."
        )

    mcps = list(aggregator.gen_metadata())
    logger.info(f"[AGGREGATOR-MCPS] Generated {len(mcps)} MCPs")

    result = to_datajob_input_output(
        mcps=mcps,
        ignore_extra_mcps=True,
    )

    if result:
        logger.info(
            f"[DATAJOB-OUTPUT] Created DataJobInputOutput with "
            f"{len(result.inputDatasets or [])} inputs, "
            f"{len(result.outputDatasets or [])} outputs"
        )
    else:
        logger.warning("[DATAJOB-OUTPUT] to_datajob_input_output returned None")

    return result
