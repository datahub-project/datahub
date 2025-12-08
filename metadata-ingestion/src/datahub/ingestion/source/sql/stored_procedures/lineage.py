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
from datahub.sql_parsing.sqlglot_lineage import _is_control_flow_statement
from datahub.sql_parsing.sqlglot_utils import (
    get_dialect,
    is_dialect_instance,
    parse_statement,
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

    # Filter out non-DML statements using sqlglot parser
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
            continue

        # Skip TSQL control flow keywords that don't produce lineage
        # Only apply for MSSQL/TSQL dialect
        if is_dialect_instance(dialect, "tsql") and _is_control_flow_statement(
            stmt_upper
        ):
            continue

        # Parse statement to determine its type using sqlglot
        try:
            parsed = parse_statement(stmt_stripped, dialect=dialect)
            query_type, _ = get_query_type_of_sql(parsed, dialect=platform)

            # Skip UNKNOWN types (RAISERROR, unsupported SQL, etc.)
            if query_type == QueryType.UNKNOWN:
                continue

            # Skip CREATE_DDL (table definitions without data operations)
            if query_type == QueryType.CREATE_DDL:
                continue

            # Skip SELECT without FROM clause (variable assignments)
            if query_type == QueryType.SELECT:
                has_from = any(
                    isinstance(node, sqlglot.exp.From) for node in parsed.walk()
                )
                if not has_from:
                    continue

            # This is a DML statement that produces lineage
            dml_statements.append(stmt_stripped)

        except Exception:
            # Parse errors: comments, malformed SQL, etc.
            continue

    logger.debug(
        f"Procedure {procedure_name or 'unknown'}: "
        f"{len(dml_statements)} DML statements from {len(statements)} total"
    )

    if not dml_statements:
        logger.debug(
            f"No DML statements found in procedure {procedure_name or 'unknown'}"
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
    for query in dml_statements:
        aggregator.add_observed_query(
            observed=ObservedQuery(
                default_db=default_db,
                default_schema=default_schema,
                query=query,
                session_id=session_id,
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
