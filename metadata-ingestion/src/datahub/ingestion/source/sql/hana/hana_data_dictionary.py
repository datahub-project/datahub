"""SAP HANA data dictionary for the calculation-view extraction path.

Regular tables and views are extracted via SQLAlchemy reflection in
:class:`SQLAlchemySource`, so this module deliberately stays narrow: it only
covers the calculation-view metadata that lives in ``_SYS_REPO`` and
``SYS.VIEW_COLUMNS``.
"""

import logging
from typing import Iterator, List

from sqlalchemy.engine import Connection

from datahub.ingestion.source.sql.hana import hana_query
from datahub.ingestion.source.sql.hana.hana_schema import (
    HanaCalculationView,
    HanaCalcViewColumn,
)
from datahub.ingestion.source.sql.sql_report import SQLSourceReport

logger = logging.getLogger(__name__)


class HanaDataDictionary:
    """Issue SQL against an existing connection to retrieve calc-view metadata.

    The dictionary takes a :class:`sqlalchemy.engine.Connection` (not an
    ``Engine``) so callers stay in control of the transaction lifecycle.
    Engine-level ``.execute`` was removed in SQLAlchemy 2.0; this design
    matches the modern style used elsewhere in the codebase.
    """

    def __init__(self, connection: Connection, report: SQLSourceReport):
        self.connection = connection
        self.report = report

    def get_calculation_views(self) -> Iterator[HanaCalculationView]:
        """Yield every activated calculation view in ``_SYS_REPO.ACTIVE_OBJECT``.

        Errors are logged as a warning and reported via
        :meth:`SQLSourceReport.report_warning` rather than raised, because
        ``_SYS_REPO`` is missing on some HANA deployments (it requires the
        XS classic repository) and the user may not have ``SELECT`` on it.
        Returning an empty iterator on failure lets ingestion continue with
        the regular table/view path.
        """
        try:
            rows = self.connection.execute(hana_query.list_calculation_views())
        except Exception as e:
            logger.warning("Failed to list calculation views: %s", e)
            self.report.report_warning(
                "calc-view-list",
                f"Could not query _SYS_REPO.ACTIVE_OBJECT: {e}",
            )
            return

        for row in rows:
            mapping = row._mapping
            definition = mapping["CDATA"]
            if definition is None:
                # CDATA can be NULL for objects that have been deleted from
                # the repository but never garbage-collected; skip silently.
                continue
            yield HanaCalculationView(
                package_id=mapping["PACKAGE_ID"],
                name=mapping["OBJECT_NAME"],
                definition=str(definition),
            )

    def get_columns_for_calculation_view(
        self, calc_view: HanaCalculationView
    ) -> List[HanaCalcViewColumn]:
        """Look up the column list for an activated calculation view."""
        try:
            rows = self.connection.execute(
                hana_query.columns_for_calculation_view(),
                {"view_name": calc_view.runtime_view_name},
            ).all()
        except Exception as e:
            logger.warning(
                "Failed to load columns for calculation view %s: %s",
                calc_view.runtime_view_name,
                e,
            )
            self.report.report_warning(
                "calc-view-columns",
                f"Could not read SYS.VIEW_COLUMNS for "
                f"{calc_view.runtime_view_name}: {e}",
            )
            return []

        columns: List[HanaCalcViewColumn] = []
        for row in rows:
            mapping = row._mapping
            columns.append(
                HanaCalcViewColumn(
                    name=mapping["COLUMN_NAME"],
                    data_type=mapping["DATA_TYPE_NAME"],
                    # SYS.VIEW_COLUMNS encodes nullability as 'TRUE'/'FALSE'.
                    nullable=str(mapping.get("IS_NULLABLE", "TRUE")).upper() == "TRUE",
                    ordinal_position=int(mapping["POSITION"])
                    if mapping.get("POSITION") is not None
                    else len(columns) + 1,
                    length=mapping.get("LENGTH"),
                    scale=mapping.get("SCALE"),
                    comment=mapping.get("COMMENTS"),
                )
            )
        return columns
