"""
Shared SQL parsing logic for DataHub's Airflow plugin.

This module provides the core SQL parsing functions used by all SQL parsing
code paths (Airflow 3 patch, Airflow 2 patch, and Airflow 2 extractors).
"""

import logging
from typing import List, Optional, Union

from datahub.ingestion.graph.client import DataHubGraph
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_from_sql_statements,
    create_lineage_sql_parsed_result,
)

logger = logging.getLogger(__name__)


def parse_sql_with_datahub(
    sql: Union[str, List[str]],
    default_database: Optional[str],
    platform: str,
    env: str,
    default_schema: Optional[str],
    graph: Optional[DataHubGraph],
    enable_multi_statement: bool,
) -> SqlParsingResult:
    """Parse SQL using DataHub's SQL parser, with optional multi-statement support.

    When enable_multi_statement is True, uses create_lineage_from_sql_statements()
    which resolves temporary tables and merges lineage across all statements.
    When False, uses create_lineage_sql_parsed_result() on only the first statement.
    """
    if enable_multi_statement:
        result = create_lineage_from_sql_statements(
            queries=sql,
            default_db=default_database,
            platform=platform,
            platform_instance=None,
            env=env,
            default_schema=default_schema,
            graph=graph,
        )
    else:
        if isinstance(sql, list):
            logger.debug("Got list of SQL statements. Using first one for parsing.")
            sql = sql[0] if sql else ""

        result = create_lineage_sql_parsed_result(
            query=sql,
            default_db=default_database,
            platform=platform,
            platform_instance=None,
            env=env,
            default_schema=default_schema,
            graph=graph,
        )

    if result.debug_info.error:
        logger.warning(
            "Failed to extract lineage from SQL (platform=%s): %s\nSQL: %s",
            platform,
            result.debug_info.error,
            sql,
        )
    return result


def format_sql_for_job_facet(
    sql: Union[str, List[str]], enable_multi_statement: bool
) -> str:
    """Format SQL for OpenLineage SqlJobFacet.

    When multi-statement parsing is enabled, join all statements with semicolons.
    Otherwise, use only the first statement (for backward compatibility).
    """
    if isinstance(sql, list) and enable_multi_statement:
        return ";\n".join(str(s) for s in sql if str(s).strip())
    elif isinstance(sql, list):
        return str(sql[0]) if sql else ""
    else:
        return str(sql)
