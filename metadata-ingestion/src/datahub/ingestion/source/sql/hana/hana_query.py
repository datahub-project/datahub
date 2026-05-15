"""SQL queries used by the SAP HANA ingestion source.

All queries are exposed as :class:`sqlalchemy.sql.elements.TextClause` instances
so they can be executed with bound parameters. Identifier values like schema
and view names are passed via ``:bindparam`` placeholders rather than
interpolated into the query text, which avoids the SQL-injection surface that
naive f-string construction would create.
"""

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause


def list_calculation_views() -> TextClause:
    """List every activated calculation view in ``_SYS_REPO.ACTIVE_OBJECT``.

    The result rows expose the columns:

    - ``PACKAGE_ID`` — dot-separated package path, e.g. ``acme.analytics``.
    - ``OBJECT_NAME`` — leaf view name.
    - ``CDATA`` — raw XML definition; cast to ``VARCHAR`` so the driver does
      not surface it as a CLOB lob handle that some pyhdb versions do not
      stream cleanly.
    """
    return text(
        """
        SELECT PACKAGE_ID,
               OBJECT_NAME,
               TO_VARCHAR(CDATA) AS CDATA
        FROM _SYS_REPO.ACTIVE_OBJECT
        WHERE LOWER(OBJECT_SUFFIX) = 'calculationview'
        ORDER BY PACKAGE_ID, OBJECT_NAME
        """
    )


def columns_for_calculation_view() -> TextClause:
    """Columns of an activated calculation view in the ``_SYS_BIC`` runtime schema.

    Bind parameter ``view_name`` matches the qualified name SAP HANA uses
    when activating a calc view: ``<package_id>/<view_name>`` (e.g.
    ``acme.analytics/SalesOverview``). The query returns column ordinal
    position, native HANA data type, and the precision / scale needed to
    rebuild the precise SQL type.
    """
    return text(
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
