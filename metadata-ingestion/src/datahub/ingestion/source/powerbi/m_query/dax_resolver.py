import logging
import warnings
from typing import Dict, List

from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
)

logger = logging.getLogger(__name__)

_PYDAXLEXER_WARNING_ISSUED = False


def _get_dax_expression(expression: str):  # type: ignore[return]
    """Import DAXExpression lazily, warn once if PyDAXLexer is not installed."""
    global _PYDAXLEXER_WARNING_ISSUED
    try:
        from PyDAX import DAXExpression

        return DAXExpression(expression)
    except ImportError:
        if not _PYDAXLEXER_WARNING_ISSUED:
            warnings.warn(
                "PyDAXLexer is not installed; DAX lineage will not be extracted. "
                "Install with: pip install 'acryl-datahub[powerbi]'",
                stacklevel=3,
            )
            _PYDAXLEXER_WARNING_ISSUED = True
        return None


def extract_dax_table_references(
    expression: str,
    sibling_names: frozenset,
) -> List[str]:
    """Extract table references from a DAX expression, filtered to known sibling tables.

    sibling_names: frozenset of original-cased sibling table names.
    Returns: matched sibling names in original casing.
    (powerbi.py performs case-insensitive lookup when building URNs.)
    """
    if not sibling_names:
        return []

    parsed = _get_dax_expression(expression)
    if parsed is None:
        return []

    try:
        referenced_lower = {
            ref.table_name.lower()
            for ref in parsed.table_column_references
            if ref.table_name
        }
        sibling_name_map = {n.lower(): n for n in sibling_names}
        return list(
            dict.fromkeys(
                sibling_name_map[lower]
                for lower in sibling_name_map
                if lower in referenced_lower
            )
        )
    except Exception as e:
        logger.debug(
            "DAX table reference extraction failed for expression %r: %s",
            expression[:200],
            e,
        )
        return []


def extract_dax_column_lineage(
    column_name: str,
    expression: str,
    table_urn: str,
    sibling_table_urns: Dict[str, str],
) -> List[ColumnLineageInfo]:
    """Extract column-level lineage from a DAX calculated column expression.

    Args:
        column_name: Name of the calculated column being defined.
        expression: DAX expression for the column (from Column.expression).
        table_urn: URN of the table that contains this column (the downstream).
        sibling_table_urns: Mapping of lowercase table name -> URN for all tables in
            the same dataset. References to unknown tables are silently skipped.

    Returns: List with one ColumnLineageInfo if cross-table references were found,
             empty list otherwise.

    Note: References with empty table_name (intra-table measure calls like [Total Sales])
    are ignored — they reference measures in the same table, not columns in other tables.
    """
    if not sibling_table_urns:
        return []

    parsed = _get_dax_expression(expression)
    if parsed is None:
        return []

    try:
        upstreams = [
            ColumnRef(
                table=sibling_table_urns[ref.table_name.lower()],
                column=ref.artifact_name,
            )
            for ref in parsed.table_column_references
            if ref.table_name
            and ref.table_name.lower() in sibling_table_urns
            and ref.artifact_name
        ]
        if not upstreams:
            return []
        return [
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=table_urn, column=column_name),
                upstreams=upstreams,
            )
        ]
    except Exception as e:
        logger.debug(
            "DAX column lineage extraction failed for column %r expression %r: %s",
            column_name,
            expression[:200],
            e,
        )
        return []
