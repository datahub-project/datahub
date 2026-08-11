import logging
from typing import List, Optional

from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceReport

logger = logging.getLogger(__name__)

# PyDAX token type for a single-quoted identifier. In DAX, single quotes are the
# table-name syntax, so a quoted name is provably a table; a bare identifier is
# ambiguous — it may equally be an unqualified column reference.
_QUOTED_TABLE_NAME_TOKEN = 400


def extract_dax_table_references(
    expression: str,
    reporter: Optional[PowerBiDashboardSourceReport] = None,
) -> List[str]:
    """Return distinct table names referenced by a DAX calculated-table expression.

    Only names that are provably tables are returned: those qualified as
    ``'Table'[Column]``, and standalone names written in the quoted ``'Table'``
    form. Bare identifiers are skipped because DAX allows an unqualified column
    reference in the same position (``SUMMARIZE(Sales, Region, ...)`` — ``Region``
    is a column), and Power BI models routinely name a dimension table and a
    column alike (``Date``, ``Region``, ``Product``), which would otherwise emit a
    wrong edge that name validation cannot catch.

    The names are candidates: the mapper validates them against the dataset's
    actual tables before emitting lineage.
    """
    try:
        from PyDAX import DAXExpression
    except ImportError as e:
        # Lazily imported so a missing/incompatible PyDAXLexer degrades DAX
        # lineage instead of breaking the whole PowerBI source at import time.
        _report_failure(
            reporter,
            "DAX lineage requires 'PyDAXLexer'. Install it with: "
            "pip install 'acryl-datahub[powerbi]'",
            expression,
            e,
        )
        return []

    try:
        # verify_best_practices runs a rule engine whose output we never read.
        parsed = DAXExpression(expression, verify_best_practices=False)
        names = {
            str(ref.table_name)
            for ref in parsed.table_column_references
            if ref.table_name
        }
        names |= {
            str(ref.name)
            for ref in parsed.table_references
            if ref.name and _is_quoted_table(ref)
        }
    except Exception as e:
        # PyDAX is lenient and does not raise on malformed DAX, so reaching here
        # almost certainly means its reference API changed — which the
        # PyDAXLexer>=0.3.0,<0.4.0 range permits. Surface it rather than letting
        # DAX lineage vanish silently across a run.
        _report_failure(
            reporter,
            "Could not extract sibling-table references from a DAX calculated "
            "table; lineage for this table will be missing. This usually means the "
            "PyDAXLexer reference API changed — check the installed version.",
            expression,
            e,
        )
        return []
    return sorted(names)


def _is_quoted_table(ref: object) -> bool:
    token = getattr(ref, "token", None)
    return getattr(token, "type", None) == _QUOTED_TABLE_NAME_TOKEN


def _report_failure(
    reporter: Optional[PowerBiDashboardSourceReport],
    message: str,
    expression: str,
    exc: Exception,
) -> None:
    if reporter is None:
        logger.debug("%s (expression=%r): %s", message, expression[:200], exc)
        return
    reporter.m_query_dax_extraction_errors += 1
    reporter.warning(
        title="DAX table-reference extraction failed",
        message=message,
        context=f"expression={expression[:200]}",
        exc=exc,
    )
