from typing import Final

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

# Activated calculation views in ``_SYS_REPO.ACTIVE_OBJECT``.
#
# Returns ``PACKAGE_ID`` (dot-separated package path), ``OBJECT_NAME``
# (leaf view name) and ``CDATA`` (raw XML, cast to ``VARCHAR`` because
# some pyhdb versions do not stream CLOB lob handles cleanly).
LIST_CALCULATION_VIEWS: Final[TextClause] = text(
    """
    SELECT PACKAGE_ID,
           OBJECT_NAME,
           TO_VARCHAR(CDATA) AS CDATA
    FROM _SYS_REPO.ACTIVE_OBJECT
    WHERE LOWER(OBJECT_SUFFIX) = 'calculationview'
    ORDER BY PACKAGE_ID, OBJECT_NAME
    """
)


# Columns of an activated calculation view in ``_SYS_BIC``.
#
# Bind parameter ``view_name`` is the qualified name HANA uses when
# activating a calc view: ``<package_id>/<view_name>``.
COLUMNS_FOR_CALCULATION_VIEW: Final[TextClause] = text(
    """
    SELECT COLUMN_NAME,
           COMMENTS,
           DATA_TYPE_NAME,
           IS_NULLABLE,
           POSITION,
           LENGTH,
           SCALE
    FROM SYS.VIEW_COLUMNS
    WHERE SCHEMA_NAME = '_SYS_BIC'
      AND VIEW_NAME = :view_name
    ORDER BY POSITION ASC
    """
)


# Stored procedures in a schema, with parenthesised argument signature.
#
# The subquery builds ``ARGUMENT_SIGNATURE`` by string-aggregating
# parameters ordered by ``POSITION``; the format
# ``(IN PARAM_A NVARCHAR(100), OUT PARAM_B INTEGER)`` matches HANA's own
# ``EXPLAIN PROCEDURE`` output and is stable across procedure overloads
# (used for procedure URN identity).
LIST_STORED_PROCEDURES: Final[TextClause] = text(
    """
    SELECT P.SCHEMA_NAME,
           P.PROCEDURE_NAME,
           P.DEFINITION,
           P.PROCEDURE_TYPE,
           P.CREATE_TIME,
           P.LANGUAGE,
           (
             SELECT '(' || STRING_AGG(
                      PARAMETER_TYPE || ' ' || PARAMETER_NAME ||
                      ' ' || DATA_TYPE_NAME,
                      ', '
                      ORDER BY POSITION
                    ) || ')'
             FROM SYS.PROCEDURE_PARAMETERS PP
             WHERE PP.SCHEMA_NAME = P.SCHEMA_NAME
               AND PP.PROCEDURE_NAME = P.PROCEDURE_NAME
           ) AS ARGUMENT_SIGNATURE
    FROM SYS.PROCEDURES P
    WHERE P.SCHEMA_NAME = :schema
    ORDER BY P.PROCEDURE_NAME
    """
)


def usage_query_from_host_sql_plan_cache(top_n: int) -> TextClause:
    """Distinct observed queries from ``_SYS_STATISTICS.HOST_SQL_PLAN_CACHE``.

    The statistics service snapshots ``M_SQL_PLAN_CACHE`` every few
    minutes; the same cached plan therefore appears in many rows even if
    it executed only once. We collapse to ``(STATEMENT_HASH,
    LAST_EXECUTION_TIMESTAMP)`` so each output row represents one
    distinct moment the plan was executed (within bind window). Per the
    HANA SQL Reference for ``M_SQL_PLAN_CACHE``, ``STATEMENT_HASH`` is
    the MD5 of ``STATEMENT_STRING``, so ``MAX(STATEMENT_STRING)`` over a
    fixed hash returns the canonical text. Across snapshots within one
    ``LAST_EXECUTION_TIMESTAMP``, ``USER_NAME`` / ``SESSION_USER_NAME``
    describe a single execution event and are stable.

    Filters:

    - System users (``SYS`` and ``_SYS_*``) are excluded — they never
      represent real user workload. ``LEFT(UPPER(USER_NAME), 5) != '_SYS_'``
      is used instead of LIKE because the underscore is a LIKE wildcard
      in HANA and the SQL standard ESCAPE clause is painful to round-trip
      cleanly through Python string literals.
    - ``STATEMENT_STRING`` is required (some internal entries have NULL).
    - The window is bound to ``LAST_EXECUTION_TIMESTAMP`` rather than the
      snapshot ``SERVER_TIMESTAMP`` so callers can reason about query
      execution time directly.

    No ``STATEMENT_STRING`` shape filtering is applied: previous versions
    excluded ``SELECT ... FROM SYS.*`` / ``FROM _SYS_*`` traffic via LIKE,
    but ``_`` is a single-char LIKE wildcard so ``_SYS_`` over-matched
    arbitrary 5-char tokens, and the filter only caught ``SELECT``s
    (missing ``CALL``, ``INSERT INTO ... SELECT``, etc.). The user-name
    exclusion above is sufficient to drop HANA's own internal traffic;
    any application that happens to read ``SYS`` views ends up with a
    ``SYS.*`` upstream URN that downstream consumers can filter on.

    ``top_n`` is interpolated into ``LIMIT`` because HANA's ``LIMIT``
    clause does not accept bind parameters (the SAP HANA SQL grammar
    requires a literal ``<unsigned_integer>``). To rule out SQL
    injection regardless of caller, we re-coerce to a Python ``int``
    and reject non-positive values *inside* this function — so even
    callers that bypass the Pydantic ``PositiveInt`` config boundary
    cannot smuggle a SQL string through the interpolation.
    """
    safe_top_n = int(top_n)
    if safe_top_n <= 0:
        raise ValueError(f"top_n must be a positive integer, got {top_n!r}")
    return text(
        f"""
        SELECT STATEMENT_HASH,
               MAX(STATEMENT_STRING) AS STATEMENT_STRING,
               MAX(COALESCE(NULLIF(SESSION_USER_NAME, ''), USER_NAME))
                   AS USER_NAME,
               MAX(SCHEMA_NAME) AS SCHEMA_NAME,
               MAX(APPLICATION_NAME) AS APPLICATION_NAME,
               LAST_EXECUTION_TIMESTAMP
        FROM _SYS_STATISTICS.HOST_SQL_PLAN_CACHE
        WHERE LAST_EXECUTION_TIMESTAMP >= :start_time
          AND LAST_EXECUTION_TIMESTAMP <= :end_time
          AND STATEMENT_STRING IS NOT NULL
          AND UPPER(USER_NAME) != 'SYS'
          AND LEFT(UPPER(USER_NAME), 5) != '_SYS_'
        GROUP BY STATEMENT_HASH, LAST_EXECUTION_TIMESTAMP
        ORDER BY LAST_EXECUTION_TIMESTAMP DESC
        LIMIT {safe_top_n}
        """
    )
