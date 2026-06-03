from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

from sqlalchemy.engine import Connection
from sqlalchemy.engine.row import RowMapping
from sqlalchemy.sql import ClauseElement
from typing_extensions import LiteralString

from datahub.ingestion.source.sql.hana import hana_query
from datahub.ingestion.source.sql.hana.hana_schema import (
    HanaCalculationView,
    HanaCalcViewColumn,
)
from datahub.ingestion.source.sql.hana.models import HanaObservedQueryRow
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sql.stored_procedures.base import BaseProcedure


class HanaDataDictionary:
    """Issue catalog queries on a HANA connection.

    Takes a :class:`Connection` (not an ``Engine``) so callers stay in
    control of the transaction lifecycle; engine-level ``.execute`` was
    removed in SQLAlchemy 2.0.
    """

    def __init__(self, connection: Connection, report: SQLSourceReport):
        self.connection = connection
        self.report = report

    def _execute_or_warn(
        self,
        query: ClauseElement,
        *,
        title: LiteralString,
        message: LiteralString,
        params: Optional[Dict[str, Any]] = None,
        context: Optional[str] = None,
    ) -> Iterator[RowMapping]:
        """Run ``query`` and yield each row's ``_mapping``.

        On failure, emit a single structured warning and yield nothing.
        """
        try:
            rows = self.connection.execute(query, params or {}).all()
        except Exception as e:
            self.report.warning(title=title, message=message, context=context, exc=e)
            return
        for row in rows:
            yield row._mapping

    def get_calculation_views(self) -> Iterator[HanaCalculationView]:
        """Yield every activated calculation view in ``_SYS_REPO.ACTIVE_OBJECT``.

        Returns an empty iterator on failure (e.g. ``_SYS_REPO`` missing on
        non-XS-classic deployments, or no SELECT grant) so ingestion can
        continue along the regular table/view path.
        """
        rows = self._execute_or_warn(
            hana_query.LIST_CALCULATION_VIEWS,
            title="Calculation view discovery failed",
            message=(
                "Could not query _SYS_REPO.ACTIVE_OBJECT. "
                "This is expected on HANA Cloud / HDI-only deployments; "
                "calculation-view ingestion will be skipped."
            ),
        )
        for mapping in rows:
            definition = mapping["CDATA"]
            # CDATA can be NULL for tombstoned-but-not-GC'd repository
            # objects; skip them silently.
            if definition is None:
                continue
            yield HanaCalculationView(
                package_id=mapping["PACKAGE_ID"],
                name=mapping["OBJECT_NAME"],
                definition=str(definition),
            )

    def get_columns_for_calculation_view(
        self, calc_view: HanaCalculationView
    ) -> List[HanaCalcViewColumn]:
        columns: List[HanaCalcViewColumn] = []
        for mapping in self._execute_or_warn(
            hana_query.COLUMNS_FOR_CALCULATION_VIEW,
            title="Calculation view column lookup failed",
            message=(
                "Could not read SYS.VIEW_COLUMNS for a calculation view; "
                "the dataset will be emitted without a column schema."
            ),
            params={"view_name": calc_view.runtime_view_name},
            context=calc_view.runtime_view_name,
        ):
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

    def get_stored_procedures(self, schema: str) -> Iterator[BaseProcedure]:
        """Yield ``BaseProcedure`` rows for every procedure in ``schema``.

        Reads ``SYS.PROCEDURES`` plus a left join to ``SYS.PROCEDURE_PARAMETERS``
        to build a parenthesised argument signature compatible with the
        stored-procedures common module. Failures degrade to an empty iterator
        so missing-grant errors don't tank the rest of ingestion.
        """
        for mapping in self._execute_or_warn(
            hana_query.LIST_STORED_PROCEDURES,
            title="Stored procedure discovery failed",
            message=(
                "Could not query SYS.PROCEDURES for a schema; "
                "stored procedures from this schema will not be ingested."
            ),
            params={"schema": schema},
            context=schema,
        ):
            yield BaseProcedure(
                name=mapping["PROCEDURE_NAME"],
                procedure_definition=mapping.get("DEFINITION"),
                created=mapping.get("CREATE_TIME"),
                last_altered=None,
                comment=None,
                argument_signature=mapping.get("ARGUMENT_SIGNATURE"),
                return_type=None,
                # HANA exposes both ``SQLSCRIPT`` (the dialect we want
                # sqlglot to ignore) and ``RUNTIME`` for L-language
                # procedures; we expose them as-is and let the lineage
                # parser short-circuit non-SQL languages.
                language=str(mapping.get("LANGUAGE") or "SQLSCRIPT"),
                extra_properties=None,
                default_schema=schema,
            )

    def iter_observed_queries(
        self,
        start_time: datetime,
        end_time: datetime,
        top_n: int,
    ) -> Iterator[HanaObservedQueryRow]:
        """Yield one ``HanaObservedQueryRow`` per distinct execution moment.

        Queries ``_SYS_STATISTICS.HOST_SQL_PLAN_CACHE``. Two prerequisites
        on the HANA side that we cannot enforce from here:

        - The statistics service must be running (it is by default; see
          SAP note 2147247 for diagnostics).
        - The ingestion user needs ``MONITORING`` (or ``CATALOG READ``
          system privilege).

        Failures degrade to an empty iterator with a single report
        warning so usage extraction never blocks metadata ingestion.
        """
        for mapping in self._execute_or_warn(
            hana_query.usage_query_from_host_sql_plan_cache(top_n),
            title="Query usage extraction failed",
            message=(
                "Could not query _SYS_STATISTICS.HOST_SQL_PLAN_CACHE. "
                "Verify that the statistics service is running and that the "
                "ingestion user holds MONITORING (or CATALOG READ). Usage "
                "extraction is disabled for this run."
            ),
            params={"start_time": start_time, "end_time": end_time},
        ):
            statement_string = mapping.get("STATEMENT_STRING")
            statement_hash = mapping.get("STATEMENT_HASH")
            if not statement_string or not statement_hash:
                continue
            yield HanaObservedQueryRow(
                statement_hash=str(statement_hash),
                statement_string=str(statement_string),
                user_name=(
                    str(mapping["USER_NAME"]) if mapping.get("USER_NAME") else None
                ),
                schema_name=(
                    str(mapping["SCHEMA_NAME"]) if mapping.get("SCHEMA_NAME") else None
                ),
                application_name=(
                    str(mapping["APPLICATION_NAME"])
                    if mapping.get("APPLICATION_NAME")
                    else None
                ),
                last_execution_timestamp=mapping.get("LAST_EXECUTION_TIMESTAMP"),
            )
