import logging
from typing import List

from PyDAX import DAXExpression

logger = logging.getLogger(__name__)


def extract_dax_table_references(expression: str) -> List[str]:
    """Return distinct table names referenced by a DAX calculated-table expression.

    The names are candidates: the mapper validates them against the dataset's actual
    tables before emitting lineage. Best-effort — an unparseable expression yields [].
    """
    try:
        parsed = DAXExpression(expression)
        names = {
            ref.table_name for ref in parsed.table_column_references if ref.table_name
        }
    except Exception as e:
        logger.debug(
            "DAX table-reference extraction failed for expression %r: %s",
            expression[:200],
            e,
        )
        return []
    return sorted(names)
