import itertools
import logging
import re
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.looker.looker_common import (
    ViewFieldValue,
    find_view_from_resolved_includes,
)
from datahub.ingestion.source.looker.looker_config import LookerConnectionDefinition
from datahub.ingestion.source.looker.looker_dataclasses import LookerViewFile
from datahub.ingestion.source.looker.looker_file_loader import LookerViewFileLoader
from datahub.ingestion.source.looker.lookml_config import (
    DERIVED_VIEW_PATTERN,
    DERIVED_VIEW_SUFFIX,
    NAME,
    LookMLSourceReport,
)
from datahub.ingestion.source.looker.lookml_refinement import LookerRefinementResolver

logger = logging.getLogger(__name__)


class LookerFieldContext:
    raw_field: Dict[Any, Any]

    def __init__(self, raw_field: Dict[Any, Any]):
        self.raw_field = raw_field

    def name(self) -> str:
        return self.raw_field[NAME]

    def sql(self) -> Optional[str]:
        return self.raw_field.get("sql")

    def column_name_in_sql_attribute(self) -> List[str]:
        if self.sql() is None:
            # If no "sql" is specified, we assume this is referencing an upstream field
            # with the same name. This commonly happens for extends and derived tables.
            return [self.name()]

        column_names: List[str] = []

        sql: Optional[str] = self.sql()

        assert sql  # to silent lint false positive

        for upstream_field_match in re.finditer(r"\${TABLE}\.[\"]*([\.\w]+)", sql):
            matched_field = upstream_field_match.group(1)
            # Remove quotes from field names
            matched_field = matched_field.replace('"', "").replace("`", "").lower()
            column_names.append(matched_field)

        return column_names


class LookerViewContext:
    """
    There are six patterns to associate the view's fields with dataset

    Pattern1:
        view: view_name {
            ... measure and dimension definition i.e. fields of a view
        }

    In Pattern1 the fields' upstream dataset name is equivalent to view_name and this dataset should be present in
    the connection.

    Pattern2:
        view: view_name {
            sql_table_name: dataset-name

            ... measure and dimension definition i.e. fields of a view
        }

    In Pattern2 the fields' upstream dataset name is mentioned in "sql_table_name" attribute and this dataset
    should be present in the connection.

    Pattern3:
        view: view_name {
            sql_table_name: "<view-name>.SQL_TABLE_NAME"

            ... measure and dimension definition i.e. fields of a view
        }

    In Pattern3 the fields' upstream is another view in same looker project.

    Pattern4:
        view: view_name {
            derived_table:
                sql:
                    ... SQL select query

            ... measure and dimension definition i.e. fields of a view
        }

    In Pattern4 the fields' upstream dataset is the output of sql mentioned in derived_table.sql.

    Pattern5:
        view: view_name {
            derived_table:
                explore_source:
                    ... LookML native query

            ... measure and dimension definition i.e. fields of a view
        }

    In Pattern5 the fields' upstream dataset is the output of LookML native query mentioned in
    derived_table.explore_source.

    In all patterns the "sql_table_name" or "derived_table" field might present in parent view instead of current
    view (see "extends" doc https://cloud.google.com/looker/docs/reference/param-view-extends)

    In all the patterns the common thing is field definition and fields are defined as
        # Dimensions
        dimension: id {
            primary_key: yes
            type: number
            sql: ${TABLE}.id ;;
        }

        # Measures
        measure: total_revenue {
            type: sum
            sql: ${TABLE}.total_revenue ;;
        }

    Here "sql" attribute is referring to column present in upstream dataset.

    This sql can be complex sql, see below example

    dimension: profit_in_dollars { type: number sql: ${TABLE}.revenue_in_dollars - ${TABLE}.cost_in_dollars ;; }
    Here "profit_in_dollars" has two upstream columns from upstream dataset i.e. revenue_in_dollars and
    cost_in_dollars.

    There is one special case of view definition, which is actually not useful but still a valid lookml definition. We
    call it pattern 6. Refer below lookml

    view: customer_facts {
      derived_table: {
        sql:
              SELECT
                customer_id,
                SUM(sale_price) AS lifetime_spend
              FROM
                order
              WHERE
                {% if order.region == "ap-south-1" %}
                    region = "AWS_AP_SOUTH_1"
                {% else %}
                    region = "GCP_SOUTH_1"
                {% endif %}
              GROUP BY 1
            ;;
            }
    }

    The customer_facts view is not useful for looker as there is no field definition, but still such view appears in
    connector test-cases, and it might be present on customer side

    For all possible options of "sql" attribute please refer looker doc:
    https://cloud.google.com/looker/docs/reference/param-field-sql

    """

    raw_view: Dict
    view_file: LookerViewFile
    view_connection: LookerConnectionDefinition
    view_file_loader: LookerViewFileLoader
    looker_refinement_resolver: LookerRefinementResolver
    base_folder_path: str
    reporter: LookMLSourceReport

    def __init__(
        self,
        raw_view: Dict,
        view_file: LookerViewFile,
        view_connection: LookerConnectionDefinition,
        view_file_loader: LookerViewFileLoader,
        looker_refinement_resolver: LookerRefinementResolver,
        base_folder_path: str,
        reporter: LookMLSourceReport,
    ):
        self.raw_view = raw_view
        self.view_file = view_file
        self.view_connection = view_connection
        self.view_file_loader = view_file_loader
        self.looker_refinement_resolver = looker_refinement_resolver
        self.base_folder_path = base_folder_path
        self.reporter = reporter

    def resolve_extends_view_name(
        self,
        target_view_name: str,
    ) -> Optional[dict]:
        # The view could live in the same file.
        for raw_view in self.view_file.views:
            raw_view_name = raw_view["name"]
            if raw_view_name == target_view_name:
                return self.looker_refinement_resolver.apply_view_refinement(raw_view)

        # Or, it could live in one of the imports.
        view = find_view_from_resolved_includes(
            connection=self.view_connection,
            resolved_includes=self.view_file.resolved_includes,
            looker_viewfile_loader=self.view_file_loader,
            target_view_name=target_view_name,
            reporter=self.reporter,
        )

        if view:
            return self.looker_refinement_resolver.apply_view_refinement(view[1])
        else:
            logger.warning(
                f"failed to resolve view {target_view_name} included from {self.view_file.absolute_file_path}"
            )
            return None

    def get_including_extends(
        self,
        field: str,
    ) -> Optional[Any]:
        extends = list(
            itertools.chain.from_iterable(
                self.raw_view.get("extends", self.raw_view.get("extends__all", []))
            )
        )

        # First, check the current view.
        if field in self.raw_view:
            return self.raw_view[field]

        # The field might be defined in another view and this view is extending that view,
        # so we resolve this field while taking that into account.
        # following Looker's precedence rules.
        for extend in reversed(extends):
            assert extend != self.raw_view[NAME], "a view cannot extend itself"
            extend_view = self.resolve_extends_view_name(
                extend,
            )
            if not extend_view:
                raise NameError(
                    f"failed to resolve extends view {extend} in view {self.raw_view[NAME]} of"
                    f" file {self.view_file.absolute_file_path}"
                )
            if field in extend_view:
                return extend_view[field]

        return None

    def _get_sql_table_name_field(self) -> Optional[str]:
        return self.get_including_extends(field="sql_table_name")

    def _is_dot_sql_table_name_present(self) -> bool:
        sql_table_name: Optional[str] = self._get_sql_table_name_field()

        if sql_table_name is None:
            return False

        if DERIVED_VIEW_SUFFIX in sql_table_name.lower():
            return True

        return False

    def sql_table_name(self) -> str:
        sql_table_name: Optional[str] = self._get_sql_table_name_field()
        # if sql_table_name field is not set then the table name is equal to view-name
        if sql_table_name is None:
            return self.raw_view[NAME].lower()

        # sql_table_name is in the format "${view-name}.SQL_TABLE_NAME"
        # remove extra characters
        if self._is_dot_sql_table_name_present():
            sql_table_name = re.sub(DERIVED_VIEW_PATTERN, r"\1", sql_table_name)

        # Some sql_table_name fields contain quotes like: optimizely."group", just remove the quotes
        return sql_table_name.replace('"', "").replace("`", "").lower()

    def derived_table(self) -> Dict[Any, Any]:
        """
        This function should only be called if is_native_derived_case return true
        """
        derived_table = self.get_including_extends(field="derived_table")

        assert derived_table, "derived_table should not be None"

        return derived_table

    def explore_source(self) -> Dict[Any, Any]:
        """
        This function should only be called if is_native_derived_case return true
        """
        derived_table = self.derived_table()

        assert derived_table.get("explore_source"), "explore_source should not be None"

        return derived_table["explore_source"]

    def sql(self, transformed: bool = True) -> str:
        """
        This function should only be called if is_sql_based_derived_case return true
        """
        derived_table = self.derived_table()

        # Looker supports sql fragments that omit the SELECT and FROM parts of the query
        # Add those in if we detect that it is missing
        sql_query: str = derived_table["sql"]

        if transformed:  # update the original sql attribute only if transformed is true
            if not re.search(r"SELECT\s", sql_query, flags=re.I):
                # add a SELECT clause at the beginning
                sql_query = f"SELECT {sql_query}"

            if not re.search(r"FROM\s", sql_query, flags=re.I):
                # add a FROM clause at the end
                sql_query = f"{sql_query} FROM {self.name()}"
                # Get the list of tables in the query

            # Drop ${ and }
            sql_query = re.sub(DERIVED_VIEW_PATTERN, r"\1", sql_query)

        return sql_query

    def name(self) -> str:
        return self.raw_view[NAME]

    def view_file_name(self) -> str:
        splits: List[str] = self.view_file.absolute_file_path.split(
            self.base_folder_path, 1
        )
        if len(splits) != 2:
            logger.debug(
                f"base_folder_path({self.base_folder_path}) and absolute_file_path({self.view_file.absolute_file_path})"
                f" not matching"
            )
            return ViewFieldValue.NOT_AVAILABLE.value

        file_name: str = splits[1]
        logger.debug(f"file_path={file_name}")

        return file_name.strip(
            "/"
        )  # strip / from path to make it equivalent to source_file attribute of LookerModelExplore API

    def _get_list_dict(self, attribute_name: str) -> List[Dict]:
        ans: Optional[List[Dict]] = self.raw_view.get(attribute_name)
        if ans is not None:
            return ans
        return []

    def dimensions(self) -> List[Dict]:
        return self._get_list_dict("dimensions")

    def measures(self) -> List[Dict]:
        return self._get_list_dict("measures")

    def dimension_groups(self) -> List[Dict]:
        return self._get_list_dict("dimension_groups")

    def is_materialized_derived_view(self) -> bool:
        for k in self.derived_table():
            if k in ["datagroup_trigger", "sql_trigger_value", "persist_for"]:
                return True

        if "materialized_view" in self.derived_table():
            return self.derived_table()["materialized_view"] == "yes"

        return False

    def is_regular_case(self) -> bool:
        # regular-case is pattern1 and 2 where upstream table is either view-name or
        # table name mentioned in sql_table_name attribute
        if (
            self.is_sql_table_name_referring_to_view()
            or self.is_sql_based_derived_case()
            or self.is_native_derived_case()
        ):
            return False

        return True

    def is_sql_table_name_referring_to_view(self) -> bool:
        # It is pattern3
        return self._is_dot_sql_table_name_present()

    def is_sql_based_derived_case(self) -> bool:
        # It is pattern 4
        if "derived_table" in self.raw_view and "sql" in self.raw_view["derived_table"]:
            return True

        return False

    def is_native_derived_case(self) -> bool:
        # It is pattern 5
        if (
            "derived_table" in self.raw_view
            and "explore_source" in self.raw_view["derived_table"]
        ):
            return True

        return False

    def is_sql_based_derived_view_without_fields_case(self) -> bool:
        # Pattern 6
        fields: List[Dict] = []

        fields.extend(self.dimensions())
        fields.extend(self.measures())
        fields.extend(self.dimension_groups())

        if self.is_sql_based_derived_case() and len(fields) == 0:
            return True

        return False
