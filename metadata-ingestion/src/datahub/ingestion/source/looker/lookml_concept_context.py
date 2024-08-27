import itertools
import logging
import re
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.looker.looker_common import (
    ViewFieldValue,
    find_view_from_resolved_includes,
)
from datahub.ingestion.source.looker.looker_config import LookerConnectionDefinition
from datahub.ingestion.source.looker.looker_constant import (
    DIMENSION_GROUPS,
    DIMENSIONS,
    MEASURES,
)
from datahub.ingestion.source.looker.looker_dataclasses import LookerViewFile
from datahub.ingestion.source.looker.looker_file_loader import LookerViewFileLoader
from datahub.ingestion.source.looker.lookml_config import (
    DERIVED_VIEW_SUFFIX,
    NAME,
    LookMLSourceReport,
)
from datahub.ingestion.source.looker.lookml_refinement import LookerRefinementResolver
from datahub.ingestion.source.looker.str_functions import (
    remove_extra_spaces_and_newlines,
)

logger = logging.getLogger(__name__)


def merge_parent_and_child_fields(
    child_fields: List[dict], parent_fields: List[dict]
) -> List[Dict]:
    # Fetch the fields from the parent view, i.e., the view name mentioned in view.extends, and include those
    # fields in child_fields. This inclusion will resolve the fields according to the precedence rules mentioned
    # in the LookML documentation: https://cloud.google.com/looker/docs/reference/param-view-extends.

    # Create a map field-name vs field
    child_field_map: dict = {}
    for field in child_fields:
        assert (
            NAME in field
        ), "A lookml view must have a name field"  # name is required field of lookml field array

        child_field_map[field[NAME]] = field

    for field in parent_fields:
        assert (
            NAME in field
        ), "A lookml view must have a name field"  # name is required field of lookml field array

        if field[NAME] in child_field_map:
            # Fields defined in the child view take higher precedence.
            # This is an override case where the child has redefined the parent field.
            # There are some additive attributes; however, we are not consuming them in metadata ingestion
            # and hence not adding them to the child field.
            continue

        child_fields.append(field)

    return child_fields


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
    There are seven patterns to associate the view's fields with dataset

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

    For pattern 6 i.e. view.derived.sql, The looker creates a temporary table to store the sql result,
    However if we don't want to have a temporary table and want looker to always execute the sql to fetch the result then
    in that case pattern 7 is useful (mentioned below).

    Pattern7:
        view: customer_sales {
          sql_table_name: (
            SELECT
              customer_id,
              SUM(sales_amount) AS total_sales
            FROM
              sales
            GROUP BY
              customer_id
          ) ;;

          dimension: customer_id {
            sql: ${TABLE}.customer_id ;;
          }

          measure: total_sales {
            type: sum
            sql: ${TABLE}.total_sales ;;
          }
        }


    In Pattern7 the fields' upstream dataset is the output of sql mentioned in
    customer_sales.sql_table_name.

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

    def _get_parent_attribute(
        self,
        attribute_name: str,
    ) -> Optional[Any]:
        """
        Search for the attribute_name in the parent views of the current view and return its value.
        """
        extends = list(
            itertools.chain.from_iterable(
                self.raw_view.get("extends", self.raw_view.get("extends__all", []))
            )
        )

        # Following Looker's precedence rules.
        # reversed the view-names mentioned in `extends` attribute
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
            if attribute_name in extend_view:
                return extend_view[attribute_name]

        return None

    def get_including_extends(
        self,
        field: str,
    ) -> Optional[Any]:

        # According to Looker's inheritance rules, we need to merge the fields(i.e. dimensions, measures and
        # dimension_groups) from both the child and parent.
        if field in [DIMENSIONS, DIMENSION_GROUPS, MEASURES]:
            # Get the child fields
            child_fields = self._get_list_dict(field)
            # merge parent and child fields
            return merge_parent_and_child_fields(
                child_fields=child_fields,
                parent_fields=self._get_parent_attribute(attribute_name=field) or [],
            )
        else:
            # Return the field from the current view if it exists.
            if field in self.raw_view:
                return self.raw_view[field]

            # The field might be defined in another view, and this view is extending that view,
            return self._get_parent_attribute(field)

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
            sql_table_name = self.raw_view[NAME].lower()

        return sql_table_name.lower()

    def datahub_transformed_sql_table_name(self) -> str:
        table_name: Optional[str] = self.raw_view.get(
            "datahub_transformed_sql_table_name"
        )

        if not table_name:
            table_name = self.sql_table_name()

        # remove extra spaces and new lines from sql_table_name if it is not a sql
        if not self.is_direct_sql_query_case():
            # Some sql_table_name fields contain quotes like: optimizely."group", just remove the quotes
            table_name = table_name.replace('"', "").replace("`", "").lower()
            table_name = remove_extra_spaces_and_newlines(table_name).strip()

        return table_name

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

    def sql(self) -> str:
        """
        This function should only be called if is_sql_based_derived_case return true
        """
        derived_table = self.derived_table()

        return derived_table["sql"]

    def datahub_transformed_sql(self) -> str:
        """
        This function should only be called if is_sql_based_derived_case return true
        """
        derived_table = self.derived_table()

        return derived_table["datahub_transformed_sql"]

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
        return self.get_including_extends(field=DIMENSIONS) or []

    def measures(self) -> List[Dict]:
        return self.get_including_extends(field=MEASURES) or []

    def dimension_groups(self) -> List[Dict]:
        return self.get_including_extends(field=DIMENSION_GROUPS) or []

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

        # It should not be the sql query
        if self.is_direct_sql_query_case():
            return False

        if (
            self.is_sql_table_name_referring_to_view()
            or self.is_sql_based_derived_case()
            or self.is_native_derived_case()
        ):
            return False

        return True

    def is_sql_table_name_referring_to_view(self) -> bool:
        if self.is_direct_sql_query_case():
            return False

        # It is pattern3
        return self._is_dot_sql_table_name_present()

    def is_sql_based_derived_case(self) -> bool:
        # It is pattern 4
        if "derived_table" in self.raw_view and "sql" in self.raw_view["derived_table"]:
            return True

        return False

    def is_native_derived_case(self) -> bool:
        # It is pattern 5, mentioned in Class documentation
        if (
            "derived_table" in self.raw_view
            and "explore_source" in self.raw_view["derived_table"]
        ):
            return True

        return False

    def is_sql_based_derived_view_without_fields_case(self) -> bool:
        # Pattern 6, mentioned in Class documentation
        fields: List[Dict] = []

        fields.extend(self.dimensions())
        fields.extend(self.measures())
        fields.extend(self.dimension_groups())

        if self.is_sql_based_derived_case() and len(fields) == 0:
            return True

        return False

    def is_direct_sql_query_case(self) -> bool:
        # pattern 7
        # sqlglot doesn't have a function to validate whether text is valid SQL or not.
        # Applying a simple logic to check if sql_table_name contains a sql.
        # if sql_table_name contains sql then its value starts with "(" and checking if "select" is present in side the
        # text
        return (
            self.sql_table_name().strip().startswith("(")
            and "select" in self.sql_table_name()
        )
