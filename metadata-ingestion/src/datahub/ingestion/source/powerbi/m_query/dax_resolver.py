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
        # verify_best_practices runs a rule engine whose output we never read.
        parsed = DAXExpression(expression, verify_best_practices=False)
        # `Table[Column]` pairs land on table_column_references; standalone table
        # references (e.g. DISTINCT('T'), UNION('A','B'), a plain table copy) land
        # on table_references. Both are calculated-table sources. PyDAX is
        # unstubbed, so cast to str to keep the List[str] contract honest.
        names = {
            str(ref.table_name)
            for ref in parsed.table_column_references
            if ref.table_name
        }
        names |= {str(ref.name) for ref in parsed.table_references if ref.name}
    except Exception as e:
        logger.debug(
            "DAX table-reference extraction failed for expression %r: %s",
            expression[:200],
            e,
        )
        return []
    return sorted(names)
