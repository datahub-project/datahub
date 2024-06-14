import itertools
import logging
from typing import Any, Dict, Optional

from datahub.ingestion.source.looker.looker_common import (
    LookerConnectionDefinition,
    find_view_from_resolved_includes,
)
from datahub.ingestion.source.looker.looker_dataclasses import LookerViewFile
from datahub.ingestion.source.looker.looker_file_loader import LookerViewFileLoader
from datahub.ingestion.source.looker.lookml_config import NAME, LookMLSourceReport
from datahub.ingestion.source.looker.lookml_refinement import LookerRefinementResolver

logger = logging.getLogger(__name__)


class LookerViewContext:
    raw_view: Dict[Any, Any]
    view_file: LookerViewFile
    view_connection: LookerConnectionDefinition
    view_file_loader: LookerViewFileLoader
    looker_refinement_resolver: LookerRefinementResolver
    reporter: LookMLSourceReport

    def __init__(
        self,
        raw_view: Dict[Any, Any],
        view_file: LookerViewFile,
        view_connection: LookerConnectionDefinition,
        view_file_loader: LookerViewFileLoader,
        looker_refinement_resolver: LookerRefinementResolver,
        reporter: LookMLSourceReport,
    ):
        """
        There are three pattern to associate the view's fields with dataset
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
                derived_table:
                    sql:
                        ... SQL select query

                ... measure and dimension definition i.e. fields of a view
            }

        In Pattern3 the fields' upstream dataset is the output of sql mentioned in derived_table.sql.

        Pattern4:
            view: view_name {
                derived_table:
                    explore_source:
                        ... LookML native query

                ... measure and dimension definition i.e. fields of a view
            }

        In Pattern4 the fields' upstream dataset is the output of LookML native query mentioned in
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

        For all possible options of "sql" attribute please refer looker doc:
        https://cloud.google.com/looker/docs/reference/param-field-sql

        """

        self.raw_view = raw_view
        self.view_file = view_file
        self.view_connection = view_connection
        self.view_file_loader = view_file_loader
        self.looker_refinement_resolver = looker_refinement_resolver
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

    def sql_table_name(self) -> str:
        sql_table_name: Optional[str] = self.get_including_extends(
            field="sql_table_name"
        )

        # if sql_table_name field is not set then the table name is equal to view-name
        if sql_table_name is None:
            sql_table_name = self.raw_view[NAME]

        # Some sql_table_name fields contain quotes like: optimizely."group", just remove the quotes
        return sql_table_name.replace('"', "").replace("`", "")

    def derived_table(self) -> Optional[Dict[Any, Any]]:
        return self.get_including_extends(field="derived_table")

    def name(self) -> str:
        return self.raw_view[NAME]
