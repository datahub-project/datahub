import logging
import re
from functools import partial
from typing import Callable, Dict, List, Optional, Tuple

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.looker.looker_common import ViewField, ViewFieldType
from datahub.ingestion.source.looker.lookml_source import LookerConnectionDefinition
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
        upstream_fields = [
            (LookerFieldName(column_ref)).name() for column_ref in cll.upstreams
        ]

        fields.append(
            ViewField(
                name=cll.downstream.column,
                label="",
                type=cll.downstream.native_column_type
                if cll.downstream.native_column_type is not None
                else "unknown",
                description="",
                field_type=ViewFieldType.UNKNOWN,
                upstream_fields=upstream_fields,
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
        upstream_fields = [
            (LookerFieldName(column_ref)).name() for column_ref in cll.upstreams
        ]

        if view_field_map.get(cll.downstream.column) is None:
            logger.debug(
                f"column {cll.downstream.column} is not present in view defined "
                f"measure/dimension/dimension_group"
            )
            continue

        view_field_map[cll.downstream.column].upstream_fields = upstream_fields

    return fields


class LookerFieldName:
    column_ref: ColumnRef

    def __init__(self, column_ref: ColumnRef):
        self.column_ref = column_ref

    def name(self):
        qualified_table_name: str = self.column_ref.table.split(",")[-2]

        view_name: str = qualified_table_name.split(".")[-1]

        return f"{view_name}.{self.column_ref.column}"


class SqlQuery:
    lookml_sql_query: str
    view_name: str

    def __init__(self, lookml_sql_query: str, view_name: str):
        """
        lookml_sql_query: This is not pure sql query,
        It might contains liquid variable and might not have `from` clause.
        """
        self.lookml_sql_query = lookml_sql_query
        self.view_name = view_name

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

        return sql_query


class ViewFieldBuilder:
    fields: Optional[List[ViewField]]

    def __init__(self, fields: Optional[List[ViewField]]):
        self.fields = fields

    def _determine_syntax(self):
        if self.fields:
            return "syntax1"
        else:
            return "syntax2"

    def create_or_update_fields(
        self,
        sql_query: SqlQuery,
        connection: LookerConnectionDefinition,
        env: str,
        ctx: PipelineContext,
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

        syntax_func_map: Dict[str, Callable] = {
            "syntax1": partial(_update_fields, self.fields, spr),
            "syntax2": partial(_create_fields, spr),
        }

        syntax: str = self._determine_syntax()

        return syntax_func_map[syntax](), []  # TODO: determine second return argument
