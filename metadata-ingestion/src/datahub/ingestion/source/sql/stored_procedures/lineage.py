import logging
import uuid
from typing import Callable, Optional

import sqlglot

from datahub.metadata.schema_classes import DataJobInputOutputClass
from datahub.sql_parsing.datajob import to_datajob_input_output
from datahub.sql_parsing.query_types import get_query_type_of_sql
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.split_statements import split_statements
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.sql_parsing.sqlglot_utils import parse_statement

logger = logging.getLogger(__name__)

# TSQL control flow keywords that don't produce lineage
# Imported from sqlglot_lineage.py to maintain consistency
TSQL_CONTROL_FLOW_KEYWORDS = {
    "BEGIN",
    "END",
    "BEGIN TRY",
    "END TRY",
    "BEGIN CATCH",
    "END CATCH",
    "BEGIN TRANSACTION",
    "BEGIN TRAN",
    "COMMIT",
    "ROLLBACK",
    "SAVE TRANSACTION",
    "SAVE TRAN",
    "DECLARE",
    "SET",
    "IF",
    "ELSE",
    "WHILE",
    "BREAK",
    "CONTINUE",
    "RETURN",
    "GOTO",
    "WAITFOR",
    "PRINT",
    "RAISERROR",
    "THROW",
}


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
    """
    Parse stored procedure code and extract lineage.

    Phase 2 Implementation: Split statements BEFORE aggregation to ensure each
    downstream table gets only its relevant upstreams (not aggregated from all statements).
    """
    # Always split statements using split_statements()
    # This respects the code review feedback about using split_statements.py properly
    statements = list(split_statements(code))

    logger.info(
        f"[PHASE2-SPLIT] Split procedure into {len(statements)} statements "
        f"(code length: {len(code)} chars)"
    )

    # Filter out non-DML statements using sqlglot parser
    # This approach is more robust than string matching as it properly parses SQL
    dml_statements = []
    for stmt in statements:
        stmt_stripped = stmt.strip()
        if not stmt_stripped:
            continue

        stmt_upper = stmt_stripped.upper()

        # Skip CREATE PROCEDURE wrapper (prevent recursion)
        if stmt_upper.startswith("CREATE PROCEDURE") or stmt_upper.startswith(
            "CREATE OR REPLACE PROCEDURE"
        ):
            logger.debug(
                f"[PHASE2-FILTER] Skipping CREATE PROCEDURE wrapper: {stmt_stripped[:50]}..."
            )
            continue

        # Skip control flow keywords that don't produce lineage
        # These are not valid SQL statements, so we filter by keyword matching
        is_control_flow = any(
            stmt_upper.startswith(kw) for kw in TSQL_CONTROL_FLOW_KEYWORDS
        )
        if is_control_flow:
            logger.debug(
                f"[PHASE2-FILTER] Skipping control flow statement: {stmt_stripped[:50]}..."
            )
            continue

        # Parse statement to determine its type using sqlglot
        try:
            parsed = parse_statement(stmt_stripped, dialect="tsql")
            query_type, _ = get_query_type_of_sql(parsed, dialect="tsql")

            # Skip UNKNOWN types (RAISERROR, unsupported SQL, etc.)
            # These don't produce lineage
            if query_type == QueryType.UNKNOWN:
                logger.debug(
                    f"[PHASE2-FILTER] Skipping UNKNOWN statement type: {stmt_stripped[:50]}..."
                )
                continue

            # Skip CREATE_DDL (table definitions without data operations)
            # Example: CREATE TABLE #temp (col1 INT)
            if query_type == QueryType.CREATE_DDL:
                logger.debug(
                    f"[PHASE2-FILTER] Skipping CREATE_DDL: {stmt_stripped[:50]}..."
                )
                continue

            # Skip SELECT without FROM clause (variable assignments)
            # Example: SELECT @ErrorMessage = ERROR_MESSAGE()
            # BUT keep: SELECT @Count = COUNT(0) FROM table (has FROM, produces lineage)
            if query_type == QueryType.SELECT:
                has_from = any(
                    isinstance(node, sqlglot.exp.From) for node in parsed.walk()
                )
                if not has_from:
                    logger.debug(
                        f"[PHASE2-FILTER] Skipping SELECT without FROM (variable assignment): {stmt_stripped[:50]}..."
                    )
                    continue

            # This is a DML statement that produces lineage
            dml_statements.append(stmt_stripped)
            logger.debug(
                f"[PHASE2-DML] Keeping {query_type.value} statement: {stmt_stripped[:100]}..."
            )

        except Exception as e:
            # Parse errors: comments, malformed SQL, etc.
            # These can't produce lineage, so skip them
            logger.debug(
                f"[PHASE2-FILTER] Skipping unparseable statement: {stmt_stripped[:50]}... Error: {type(e).__name__}"
            )
            continue

    logger.info(
        f"[PHASE2-DML] Filtered to {len(dml_statements)} DML statements from {len(statements)} total statements"
    )

    if not dml_statements:
        logger.warning(
            "[PHASE2-DML] No DML statements found after filtering - procedure may contain only control flow"
        )
        return None

    # Generate a shared session_id for all statements in this procedure
    # This is critical for temp table resolution across statements
    session_id = str(uuid.uuid4())
    logger.info(
        f"[PHASE2-SESSION] Generated session_id={session_id} for {len(dml_statements)} statements"
    )

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

    # Add each DML statement as a separate ObservedQuery
    # All share the same session_id to enable temp table resolution
    for i, query in enumerate(dml_statements, 1):
        logger.info(
            f"[PHASE2-ADD] Adding statement {i}/{len(dml_statements)} to aggregator: "
            f"{len(query)} chars, starts with: {query[:100] if len(query) > 100 else query}"
        )
        aggregator.add_observed_query(
            observed=ObservedQuery(
                default_db=default_db,
                default_schema=default_schema,
                query=query,
                session_id=session_id,  # Shared session for temp table resolution
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
