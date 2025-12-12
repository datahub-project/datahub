import logging
import uuid
from typing import Callable, List, Optional

import sqlglot
from sqlglot.dialects.dialect import Dialect

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
from datahub.sql_parsing.sqlglot_lineage import _is_tsql_control_flow_statement
from datahub.sql_parsing.sqlglot_utils import (
    get_dialect,
    is_dialect_instance,
    parse_statement,
)

logger = logging.getLogger(__name__)


def _filter_dml_statements(
    *,
    statements: List[str],
    dialect: Dialect,
    platform: str,
    procedure_name: Optional[str],
) -> List[str]:
    """Filter out non-DML statements that don't produce lineage."""
    dml_statements = []

    for i, stmt in enumerate(statements, 1):
        stmt_stripped = stmt.strip()
        if not stmt_stripped:
            logger.debug(
                f"[STMT-FILTER] {procedure_name or 'unknown'}: Statement #{i} empty, skipping"
            )
            continue

        stmt_upper = stmt_stripped.upper()
        stmt_preview = stmt_stripped[:80].replace("\n", " ")

        # Skip CREATE PROCEDURE wrapper (prevent recursion)
        if stmt_upper.startswith("CREATE PROCEDURE") or stmt_upper.startswith(
            "CREATE OR REPLACE PROCEDURE"
        ):
            logger.info(
                f"[STMT-FILTER] {procedure_name or 'unknown'}: "
                f"Statement #{i} SKIPPED (CREATE PROCEDURE): {stmt_preview}"
            )
            continue

        # Skip TSQL control flow keywords that don't produce lineage
        # Only apply for MSSQL/TSQL dialect
        if is_dialect_instance(dialect, "tsql") and _is_tsql_control_flow_statement(
            stmt_upper
        ):
            logger.info(
                f"[STMT-FILTER] {procedure_name or 'unknown'}: "
                f"Statement #{i} SKIPPED (control flow): {stmt_preview}"
            )
            continue

        # Parse statement to determine its type using sqlglot
        try:
            parsed = parse_statement(stmt_stripped, dialect=dialect)
            query_type, _ = get_query_type_of_sql(parsed, dialect=platform)

            # Skip non-DML statements that don't produce lineage
            # UNKNOWN: RAISERROR, unsupported SQL, etc.
            # CREATE_DDL: table definitions without data operations
            if query_type in (QueryType.UNKNOWN, QueryType.CREATE_DDL):
                logger.info(
                    f"[STMT-FILTER] {procedure_name or 'unknown'}: "
                    f"Statement #{i} SKIPPED (query_type={query_type}): {stmt_preview}"
                )
                continue

            # Skip SELECT without FROM clause (variable assignments)
            if query_type == QueryType.SELECT:
                has_from = any(
                    isinstance(node, sqlglot.exp.From) for node in parsed.walk()
                )
                if not has_from:
                    logger.info(
                        f"[STMT-FILTER] {procedure_name or 'unknown'}: "
                        f"Statement #{i} SKIPPED (SELECT without FROM): {stmt_preview}"
                    )
                    continue

            # This is a DML statement that produces lineage
            dml_statements.append(stmt_stripped)
            logger.info(
                f"[STMT-FILTER] {procedure_name or 'unknown'}: "
                f"Statement #{i} KEPT (query_type={query_type}): {stmt_preview}"
            )

        except Exception as e:
            # Parse errors: comments, malformed SQL, etc.
            logger.warning(
                f"[STMT-FILTER] {procedure_name or 'unknown'}: "
                f"Statement #{i} SKIPPED (parse exception: {type(e).__name__}): {stmt_preview}"
            )
            continue

    return dml_statements


def parse_procedure_code(
    *,
    schema_resolver: SchemaResolver,
    default_db: Optional[str],
    default_schema: Optional[str],
    code: str,
    is_temp_table: Callable[[str], bool],
    raise_: bool = False,
    procedure_name: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Optional[DataJobInputOutputClass]:
    """
    Parse stored procedure code and extract lineage.

    Splits statements BEFORE aggregation to ensure each downstream table gets
    only its relevant upstreams (prevents lineage pollution from statement aggregation).

    Args:
        schema_resolver: Schema resolver for table lookups
        default_db: Default database context
        default_schema: Default schema context
        code: Stored procedure SQL code
        is_temp_table: Callback to check if a table is temporary
        raise_: Whether to raise on parse failures
        procedure_name: Name of the procedure for logging
        session_id: Optional session ID for deterministic temp table resolution.
            If not provided, generates a random UUID. Useful for testing.
    """
    # Derive dialect from schema_resolver's platform to support multiple databases
    platform = schema_resolver.platform
    dialect = get_dialect(platform)

    # Split statements using split_statements()
    statements = list(split_statements(code))

    logger.info(
        f"[PARSE-START] {procedure_name or 'unknown'}: "
        f"Starting parse, code_length={len(code)}, split into {len(statements)} statements"
    )

    # Filter out non-DML statements
    dml_statements = _filter_dml_statements(
        statements=statements,
        dialect=dialect,
        platform=platform,
        procedure_name=procedure_name,
    )

    logger.info(
        f"[STMT-FILTER] {procedure_name or 'unknown'}: "
        f"{len(dml_statements)} DML statements from {len(statements)} total statements"
    )

    if not dml_statements:
        logger.warning(
            f"[PARSE-RESULT] {procedure_name or 'unknown'}: "
            f"Returning None - all {len(statements)} statements were filtered, no DML found"
        )
        return None

    # Generate a shared session_id for all statements in this procedure
    # This is critical for temp table resolution across statements
    if session_id is None:
        session_id = str(uuid.uuid4())

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
    logger.info(
        f"[AGGREGATOR-START] {procedure_name or 'unknown'}: "
        f"Adding {len(dml_statements)} DML statements to aggregator"
    )

    for i, query in enumerate(dml_statements, 1):
        query_preview = query[:100].replace("\n", " ")
        logger.info(
            f"[AGGREGATOR-ADD] {procedure_name or 'unknown'}: "
            f"Query #{i}: {query_preview}..."
        )

        aggregator.add_observed_query(
            observed=ObservedQuery(
                default_db=default_db,
                default_schema=default_schema,
                query=query,
                session_id=session_id,
            )
        )

    logger.info(
        f"[AGGREGATOR-STATS] {procedure_name or 'unknown'}: "
        f"observed_queries={aggregator.report.num_observed_queries}, "
        f"failed={aggregator.report.num_observed_queries_failed}"
    )

    if aggregator.report.num_observed_queries_failed and raise_:
        logger.info(aggregator.report.as_string())
        raise ValueError(
            f"Failed to parse {aggregator.report.num_observed_queries_failed} queries."
        )

    mcps = list(aggregator.gen_metadata())
    logger.info(
        f"[AGGREGATOR-RESULT] {procedure_name or 'unknown'}: Generated {len(mcps)} MCPs"
    )

    result = to_datajob_input_output(
        mcps=mcps,
        ignore_extra_mcps=True,
    )

    if result:
        logger.info(
            f"[PARSE-RESULT] {procedure_name or 'unknown'}: "
            f"Returning DataJobInputOutput - "
            f"inputs={len(result.inputDatasets or [])}, "
            f"outputs={len(result.outputDatasets or [])}, "
            f"column_lineages={len(result.fineGrainedLineages or [])}"
        )
    else:
        logger.warning(
            f"[PARSE-RESULT] {procedure_name or 'unknown'}: "
            f"Returning None from to_datajob_input_output"
        )

    return result
