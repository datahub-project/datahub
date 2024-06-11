import logging
import re
from typing import Any, ClassVar, Dict, List, Optional, Tuple, cast

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.looker.looker_common import (
    LookerConnectionDefinition,
    ViewField,
    ViewFieldType,
)
from datahub.ingestion.source.looker.lookml_config import (
    DERIVED_VIEW_PATTERN,
    LookMLSourceReport,
)
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)

logger = logging.getLogger(__name__)


def _drop_hive_dot(urn: str) -> str:
    """
    This is special handling for hive platform where hive. is coming in urn id because of way SQL is written in lookml.

    Example: urn:li:dataset:(urn:li:dataPlatform:hive,hive.my_database.my_table,PROD)

    Here we need to transform hive.my_database.my_table to my_database.my_table
    """
    if urn.startswith("urn:li:dataset:(urn:li:dataPlatform:hive"):
        return re.sub(r"hive\.", "", urn)

    return urn


def _drop_hive_dot_from_upstream(upstreams: List[ColumnRef]) -> List[ColumnRef]:
    return [
        ColumnRef(table=_drop_hive_dot(column_ref.table), column=column_ref.column)
        for column_ref in upstreams
    ]


def _create_fields(spr: SqlParsingResult) -> List[ViewField]:
    fields: List[ViewField] = []

    column_lineages: List[ColumnLineageInfo] = (
        spr.column_lineage if spr.column_lineage is not None else []
    )

    for cll in column_lineages:
        fields.append(
            ViewField(
                name=cll.downstream.column,
                label="",
                type=cll.downstream.native_column_type
                if cll.downstream.native_column_type is not None
                else "unknown",
                description="",
                field_type=ViewFieldType.UNKNOWN,
                upstream_fields=_drop_hive_dot_from_upstream(cll.upstreams),
            )
        )

    return fields


def _update_upstream_fields_from_spr(
    fields: List[ViewField],
    upstream_table_urn_for_skip_field: str,
    spr: SqlParsingResult,
) -> List[ViewField]:
    column_lineages: List[ColumnLineageInfo] = (
        spr.column_lineage if spr.column_lineage is not None else []
    )

    view_field_map: Dict[str, ViewField] = {}
    # It is used to filter out fields which haven't updated.
    view_updated: Dict[str, bool] = {field.name: False for field in fields}

    for field in fields:
        view_field_map[field.name] = field

    for cll in column_lineages:
        if view_field_map.get(cll.downstream.column) is None:
            continue

        view_field_map[
            cll.downstream.column
        ].upstream_fields = _drop_hive_dot_from_upstream(cll.upstreams)
        view_updated[cll.downstream.column] = True

    # filter field skip in update.
    # field might get skip either because of Parser not able to identify the column from GMS
    # in-case of "select * from look_ml_view.SQL_TABLE_NAME or extra field are defined in the looker view which is
    # referring to upstream table
    skip_fields: List[ViewField] = [
        field for field in fields if view_updated[field.name] is False
    ]

    for field in skip_fields:
        # convert normal column name to ColumnRef
        field.upstream_fields = [
            ColumnRef(table=upstream_table_urn_for_skip_field, column=column)
            for column in field.upstream_fields
        ]

    return fields


def _update_fields(
    fields: List[ViewField],
    spr: SqlParsingResult,
    upstream_urns: List[str],
) -> List[ViewField]:
    return _update_upstream_fields_from_spr(
        fields=fields,
        upstream_table_urn_for_skip_field=upstream_urns[
            0
        ],  # The 0th index contains URN of table referred in from
        spr=spr,
    )


class SqlQuery:
    SELECT_STAR_PATTERN: ClassVar[str] = r"\bSELECT\s+\*\s+FROM\b"
    lookml_sql_query: str
    view_name: str
    liquid_context: Dict[Any, Any]

    def __init__(
        self, lookml_sql_query: str, view_name: str, liquid_variable: Dict[Any, Any]
    ):
        """
        lookml_sql_query: This is not pure sql query,
        It might contains liquid variable and might not have `from` clause.
        """
        self.lookml_sql_query = lookml_sql_query
        self.view_name = view_name
        self.liquid_variable = liquid_variable

    def sql_query(self):
        # Looker supports sql fragments that omit the SELECT and FROM parts of the query
        # Add those in if we detect that it is missing
        sql_query: str = self.lookml_sql_query
        if not re.search(r"SELECT\s", self.lookml_sql_query, flags=re.I):
            # add a SELECT clause at the beginning
            sql_query = f"SELECT {self.lookml_sql_query}"

        if not re.search(r"FROM\s", self.lookml_sql_query, flags=re.I):
            # add a FROM clause at the end
            sql_query = f"{sql_query} FROM {self.view_name}"
            # Get the list of tables in the query

        # Drop ${ and }
        sql_query = re.sub(DERIVED_VIEW_PATTERN, r"\1", sql_query)

        return sql_query

    def is_select_star_query(self) -> bool:
        return (
            re.search(
                SqlQuery.SELECT_STAR_PATTERN, self.lookml_sql_query, re.IGNORECASE
            )
            is not None
        )


class ViewFieldBuilder:
    fields: Optional[List[ViewField]]
    sql_query: SqlQuery
    reporter: LookMLSourceReport
    ctx: PipelineContext

    def __init__(
        self,
        fields: Optional[List[ViewField]],
        sql_query: SqlQuery,
        reporter: LookMLSourceReport,
        ctx: PipelineContext,
    ):
        self.fields = fields
        self.sql_query = sql_query
        self.reporter = reporter
        self.ctx = ctx

    def create_or_update_fields(
        self,
        connection: LookerConnectionDefinition,
        view_urn: str,
    ) -> Tuple[List[ViewField], List[str]]:
        """
        There are two syntax to define lookml view using sql.

        The syntax are:

        Syntax1:
            view: customer_order_summary {
              derived_table: {
                sql:
                  SELECT
                    customer_id,
                    MIN(DATE(time)) AS first_order,
                    SUM(amount) AS total_amount
                  FROM
                    orders
                  GROUP BY
                    customer_id ;;
              }

              dimension: customer_id {
                type: number
                primary_key: yes
                sql: ${TABLE}.customer_id ;;
              }

              dimension_group: first_order {
                type: time
                timeframes: [date, week, month]
                sql: ${TABLE}.first_order ;;
              }

              dimension: total_amount {
                type: number
                value_format: "0.00"
                sql: ${TABLE}.total_amount ;;
              }
            }

        Syntax2:
            view: customer_order_summary {
              derived_table: {
                sql:
                  SELECT
                    customer_id,
                    MIN(DATE(time)) AS first_order,
                    SUM(amount) AS total_amount
                  FROM
                    orders
                  GROUP BY
                    customer_id ;;
              }
            }

        view defined in Syntax1 is useful because measure/dimension are defined based on SQL and
        looker can generate the metadata required to define explore on top of view.

        view defined in Syntax2 is not useful as column information is missing and no use-able explore can be defined on
        top of such view.

        This function will parse both of the syntax to generate the column-level lineage.

        In-case of Syntax1 we will update the upstream attribute of ViewField instance available in `self.fields`
        argument.

        In-case of Syntax2 we will generate new list of ViewField.

        if `self.fields` is None that means view is defined as per Syntax2.
        """

        query: str = self.sql_query.sql_query()

        logger.debug(f"Processing query: {query} for view-urn {view_urn}")

        spr: SqlParsingResult = create_lineage_sql_parsed_result(
            query=query,
            default_schema=connection.default_schema,
            default_db=connection.default_db,
            platform=connection.platform,
            platform_instance=connection.platform_instance,
            env=cast(str, connection.platform_env),  # It's never going to be None
            graph=self.ctx.graph,
        )

        if (
            spr.debug_info.table_error is not None
            or spr.debug_info.column_error is not None
        ):
            # self.reporter.report_warning(
            #     view_urn,
            #     f"Failed to parsed the sql query. table_error={spr.debug_info.table_error} and column_error={spr.debug_info.column_error}",
            # )
            return [], []

        upstream_urns: List[str] = [_drop_hive_dot(urn) for urn in spr.in_tables]

        logger.debug(f"SqlParsingResult({view_urn}) : {spr}")

        if self.fields:  # It is syntax1
            return (
                _update_fields(
                    fields=self.fields,
                    spr=spr,
                    upstream_urns=upstream_urns,
                ),
                upstream_urns,
            )

        # It is syntax2
        return _create_fields(spr), upstream_urns
