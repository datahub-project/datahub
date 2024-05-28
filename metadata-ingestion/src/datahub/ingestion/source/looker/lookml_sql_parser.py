import logging
import re
from typing import Any, Dict, List, Optional

from liquid import Template, Undefined

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.looker.looker_common import (
    LookerConnectionDefinition,
    ViewField,
    ViewFieldType,
)
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)

logger = logging.getLogger(__name__)


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
                upstream_fields=cll.upstreams,
            )
        )

    return fields


def _update_fields(fields: List[ViewField], spr: SqlParsingResult) -> List[ViewField]:
    column_lineages: List[ColumnLineageInfo] = (
        spr.column_lineage if spr.column_lineage is not None else []
    )

    view_field_map: Dict[str, ViewField] = {}
    for field in fields:
        view_field_map[field.name] = field

    for cll in column_lineages:
        if view_field_map.get(cll.downstream.column) is None:
            logger.debug(
                f"column {cll.downstream.column} is not present in view defined using sql"
                f"measure/dimension/dimension_group"
            )
            continue

        view_field_map[cll.downstream.column].upstream_fields = cll.upstreams

    return fields


def get_qt_name(urn: str) -> str:
    return urn.split(",")[-2]


def get_qt_names_from_spr(spr: SqlParsingResult) -> List[str]:
    qualified_table_names: List[str] = []

    for in_table in spr.in_tables:
        qualified_table_names.append(get_qt_name(in_table))

    for out_table in spr.out_tables:
        qualified_table_names.append(get_qt_name(out_table))

    return qualified_table_names


class LookerFieldName:
    column_ref: ColumnRef

    def __init__(self, column_ref: ColumnRef):
        self.column_ref = column_ref

    def name(self):
        qualified_table_name: str = get_qt_name(self.column_ref.table)

        view_name: str = qualified_table_name.split(".")[-1]

        return f"{view_name}.{self.column_ref.column}"


class SqlQuery:
    lookml_sql_query: str
    view_name: str
    liquid_context: Dict[Any, Any]

    def __init__(
        self, lookml_sql_query: str, view_name: str, liquid_context: Dict[Any, Any]
    ):
        """
        lookml_sql_query: This is not pure sql query,
        It might contains liquid variable and might not have `from` clause.
        """
        self.lookml_sql_query = lookml_sql_query
        self.view_name = view_name
        self.liquid_context = liquid_context

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

        # set liquid variables value to NULL
        Undefined.__str__ = lambda instance: "NULL"  # type: ignore

        # Resolve liquid template
        return Template(sql_query).render(self.liquid_context)


class ViewFieldBuilder:
    fields: Optional[List[ViewField]]

    def __init__(self, fields: Optional[List[ViewField]]):
        self.fields = fields

    def create_or_update_fields(
        self,
        sql_query: SqlQuery,
        connection: LookerConnectionDefinition,
        env: str,
        ctx: PipelineContext,
    ) -> List[ViewField]:
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

        query: str = sql_query.sql_query()

        spr: SqlParsingResult = create_lineage_sql_parsed_result(
            query=query,
            default_schema=connection.default_schema,
            default_db=connection.default_db,
            platform=connection.platform,
            platform_instance=connection.platform_instance,
            env=env,
            graph=ctx.graph,
        )

        if self.fields:  # It is syntax1
            return _update_fields(self.fields, spr)

        # It is syntax2
        return _create_fields(spr)
