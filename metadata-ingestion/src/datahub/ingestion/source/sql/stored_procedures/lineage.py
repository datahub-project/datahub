import logging
from typing import Callable, List, Optional

from datahub.metadata.schema_classes import DataJobInputOutputClass
from datahub.sql_parsing.datajob import to_datajob_input_output
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.split_statements import split_statements
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)

logger = logging.getLogger(__name__)


def _is_qualified_table_urn(urn: str, platform_instance: Optional[str] = None) -> bool:
    """Check if a table URN represents a fully qualified table name.

    A qualified table name should have at least 3 parts: database.schema.table.
    This helps identify real tables vs. unqualified aliases (e.g., 'dst' in TSQL UPDATE statements).

    Args:
        urn: Dataset URN
        platform_instance: Platform instance to strip from the name if present

    Returns:
        True if the table name is fully qualified, False otherwise
    """
    # Extract name from URN: urn:li:dataset:(urn:li:dataPlatform:PLATFORM,NAME,ENV)
    parts = urn.split(",")
    if len(parts) < 2:
        return False

    name = parts[1]

    # Strip platform_instance prefix if present
    if platform_instance and name.startswith(f"{platform_instance}."):
        name = name[len(platform_instance) + 1 :]

    # Check if name has at least 3 parts (database.schema.table)
    name_parts = name.split(".")
    return len(name_parts) >= 3


def _filter_qualified_table_urns(
    table_urns: List[str], platform_instance: Optional[str] = None
) -> List[str]:
    """Filter a list of table URNs to include only qualified tables.

    This removes unqualified aliases that don't represent real tables.
    Specifically handles TSQL UPDATE aliases like "UPDATE dst FROM table dst"
    where 'dst' is an alias, not a real table.

    Args:
        table_urns: List of table URNs
        platform_instance: Platform instance to consider when parsing names

    Returns:
        List of URNs for qualified tables only
    """
    return [
        urn for urn in table_urns if _is_qualified_table_urn(urn, platform_instance)
    ]


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

    if aggregator.report.num_observed_queries_failed and raise_:
        logger.info(aggregator.report.as_string())
        raise ValueError(
            f"Failed to parse {aggregator.report.num_observed_queries_failed} queries."
        )

    mcps = list(aggregator.gen_metadata())

    result = to_datajob_input_output(
        mcps=mcps,
        ignore_extra_mcps=True,
    )

    # Filter out unqualified table URNs (e.g., TSQL UPDATE aliases like 'dst')
    # This prevents invalid URNs from causing the sink to reject the entire aspect
    # Only apply this filtering for MSSQL, which uses 3-part naming (database.schema.table)
    # Other platforms like Oracle use 2-part naming (schema.table)
    if result and schema_resolver.platform == "mssql":
        platform_instance = schema_resolver.platform_instance
        original_input_count = len(result.inputDatasets)
        original_output_count = len(result.outputDatasets)

        result.inputDatasets = _filter_qualified_table_urns(
            result.inputDatasets, platform_instance
        )
        result.outputDatasets = _filter_qualified_table_urns(
            result.outputDatasets, platform_instance
        )

        # Log if any tables were filtered out
        if (
            len(result.inputDatasets) < original_input_count
            or len(result.outputDatasets) < original_output_count
        ):
            logger.debug(
                f"Filtered unqualified tables for {procedure_name or 'procedure'}: "
                f"inputs {original_input_count} -> {len(result.inputDatasets)}, "
                f"outputs {original_output_count} -> {len(result.outputDatasets)}"
            )

        # If all tables were filtered out, return None
        if not result.inputDatasets and not result.outputDatasets:
            return None

    return result
