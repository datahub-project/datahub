import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime
from functools import lru_cache
from typing import Dict, List, Optional

from looker_sdk.sdk.api40.models import (
    LookmlModelExplore,
    LookmlModelExploreField,
    WriteQuery,
)

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.looker.looker_common import (
    LookerExplore,
    LookerViewId,
    ViewField,
    ViewFieldDimensionGroupType,
    ViewFieldType,
)
from datahub.ingestion.source.looker.looker_config import LookerConnectionDefinition
from datahub.ingestion.source.looker.looker_constant import (
    NAME,
    VIEW_FIELD_INTERVALS_ATTRIBUTE,
    VIEW_FIELD_TIMEFRAMES_ATTRIBUTE,
    VIEW_FIELD_TYPE_ATTRIBUTE,
)
from datahub.ingestion.source.looker.looker_lib_wrapper import (
    LookerAPI,
)
from datahub.ingestion.source.looker.looker_view_id_cache import LookerViewIdCache
from datahub.ingestion.source.looker.lookml_concept_context import (
    LookerFieldContext,
    LookerViewContext,
)
from datahub.ingestion.source.looker.lookml_config import (
    DERIVED_VIEW_SUFFIX,
    LookMLSourceConfig,
    LookMLSourceReport,
)
from datahub.ingestion.source.looker.urn_functions import get_qualified_table_name
from datahub.sql_parsing.schema_resolver import match_columns_to_schema
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    SqlParsingResult,
    Urn,
    create_and_cache_schema_resolver,
    create_lineage_sql_parsed_result,
)

logger = logging.getLogger(__name__)


def is_derived_view(view_name: str) -> bool:
    if DERIVED_VIEW_SUFFIX in view_name.lower():
        return True

    return False


def get_derived_looker_view_id(
    qualified_table_name: str,
    looker_view_id_cache: "LookerViewIdCache",
    base_folder_path: str,
) -> Optional[LookerViewId]:
    # qualified_table_name can be in either of below format
    # 1) db.schema.employee_income_source.sql_table_name
    # 2) db.employee_income_source.sql_table_name
    # 3) employee_income_source.sql_table_name
    # In any of the form we need the text coming before ".sql_table_name" and after last "."
    parts: List[str] = re.split(
        DERIVED_VIEW_SUFFIX, qualified_table_name, flags=re.IGNORECASE
    )
    view_name: str = parts[0].split(".")[-1]

    looker_view_id: Optional[LookerViewId] = looker_view_id_cache.get_looker_view_id(
        view_name=view_name,
        base_folder_path=base_folder_path,
    )

    return looker_view_id


def resolve_derived_view_urn_of_col_ref(
    column_refs: List[ColumnRef],
    looker_view_id_cache: "LookerViewIdCache",
    base_folder_path: str,
    config: LookMLSourceConfig,
) -> List[ColumnRef]:
    new_column_refs: List[ColumnRef] = []
    for col_ref in column_refs:
        if is_derived_view(col_ref.table.lower()):
            new_urns: List[str] = fix_derived_view_urn(
                urns=[col_ref.table],
                looker_view_id_cache=looker_view_id_cache,
                base_folder_path=base_folder_path,
                config=config,
            )
            if not new_urns:
                logger.warning(
                    f"Not able to resolve to derived view looker id for {col_ref.table}"
                )
                continue

            new_column_refs.append(ColumnRef(table=new_urns[0], column=col_ref.column))
        else:
            new_column_refs.append(col_ref)

    return new_column_refs


def fix_derived_view_urn(
    urns: List[str],
    looker_view_id_cache: "LookerViewIdCache",
    base_folder_path: str,
    config: LookMLSourceConfig,
) -> List[str]:
    # Regenerate view urn if .sql_table_name is present in urn
    new_urns: List[str] = []
    for urn in urns:
        if is_derived_view(urn):
            looker_view_id = get_derived_looker_view_id(
                qualified_table_name=get_qualified_table_name(urn),
                looker_view_id_cache=looker_view_id_cache,
                base_folder_path=base_folder_path,
            )

            if looker_view_id is None:
                logger.warning(
                    f"Not able to resolve to derived view looker id for {urn}"
                )
                continue

            new_urns.append(looker_view_id.get_urn(config=config))
        else:
            new_urns.append(urn)

    return new_urns


def _platform_names_have_2_parts(platform: str) -> bool:
    return platform in {"hive", "mysql", "athena"}


def _drop_hive_dot(urn: str) -> str:
    """
    This is special handling for hive platform where "hive." is coming in urn's id because of the way SQL
    is written in lookml.

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


def _generate_fully_qualified_name(
    sql_table_name: str,
    connection_def: LookerConnectionDefinition,
    reporter: LookMLSourceReport,
    view_name: str,
) -> str:
    """Returns a fully qualified dataset name, resolved through a connection definition.
    Input sql_table_name can be in three forms: table, db.table, db.schema.table"""
    # TODO: This function should be extracted out into a Platform specific naming class since name translations
    #  are required across all connectors

    # Bigquery has "project.db.table" which can be mapped to db.schema.table form
    # All other relational db's follow "db.schema.table"
    # With the exception of mysql, hive, athena which are "db.table"

    # first detect which one we have
    parts = len(sql_table_name.split("."))

    if parts == 3:
        # fully qualified, but if platform is of 2-part, we drop the first level
        if _platform_names_have_2_parts(connection_def.platform):
            sql_table_name = ".".join(sql_table_name.split(".")[1:])
        return sql_table_name.lower()

    if parts == 1:
        # Bare table form
        if _platform_names_have_2_parts(connection_def.platform):
            dataset_name = f"{connection_def.default_db}.{sql_table_name}"
        else:
            dataset_name = f"{connection_def.default_db}.{connection_def.default_schema}.{sql_table_name}"
        return dataset_name.lower()

    if parts == 2:
        # if this is a 2 part platform, we are fine
        if _platform_names_have_2_parts(connection_def.platform):
            return sql_table_name.lower()
        # otherwise we attach the default top-level container
        dataset_name = f"{connection_def.default_db}.{sql_table_name}"
        return dataset_name.lower()

    reporter.report_warning(
        title="Malformed Table Name",
        message="Table name has more than 3 parts.",
        context=f"view-name: {view_name}, table-name: {sql_table_name}",
    )
    return sql_table_name.lower()


class AbstractViewUpstream(ABC):
    """
    Implementation of this interface extracts the view upstream as per the way the view is bound to datasets.
    For detail explanation, please refer lookml_concept_context.LookerViewContext documentation.
    """

    view_context: LookerViewContext
    looker_view_id_cache: LookerViewIdCache
    config: LookMLSourceConfig
    reporter: LookMLSourceReport
    ctx: PipelineContext

    def __init__(
        self,
        view_context: LookerViewContext,
        looker_view_id_cache: LookerViewIdCache,
        config: LookMLSourceConfig,
        reporter: LookMLSourceReport,
        ctx: PipelineContext,
    ):
        self.view_context = view_context
        self.looker_view_id_cache = looker_view_id_cache
        self.config = config
        self.reporter = reporter
        self.ctx = ctx

    @abstractmethod
    def get_upstream_column_ref(
        self, field_context: LookerFieldContext
    ) -> List[ColumnRef]:
        pass

    @abstractmethod
    def get_upstream_dataset_urn(self) -> List[Urn]:
        pass

    def create_fields(self) -> List[ViewField]:
        return []  # it is for the special case

    def create_upstream_column_refs(
        self, upstream_urn: str, downstream_looker_columns: List[str]
    ) -> List[ColumnRef]:
        """
        - **`upstream_urn`**: The URN of the upstream dataset.

        - **`expected_columns`**: These are the columns identified by the Looker connector as belonging to the `upstream_urn` dataset. However, there is potential for human error in specifying the columns of the upstream dataset. For example, a user might declare a column in lowercase, while on the actual platform, it may exist in uppercase, or vice versa.

        - This function ensures consistency in column-level lineage by consulting GMS before creating the final `ColumnRef` instance, avoiding discrepancies.
        """
        schema_resolver = create_and_cache_schema_resolver(
            platform=self.view_context.view_connection.platform,
            platform_instance=self.view_context.view_connection.platform_instance,
            env=self.view_context.view_connection.platform_env or self.config.env,
            graph=self.ctx.graph,
        )

        urn, schema_info = schema_resolver.resolve_urn(urn=upstream_urn)

        if schema_info:
            actual_columns = match_columns_to_schema(
                schema_info, downstream_looker_columns
            )
        else:
            logger.info(
                f"schema_info not found for dataset {urn} in GMS. Using expected_columns to form ColumnRef"
            )
            actual_columns = [column.lower() for column in downstream_looker_columns]

        upstream_column_refs: List[ColumnRef] = []

        for column in actual_columns:
            upstream_column_refs.append(
                ColumnRef(
                    column=column,
                    table=upstream_urn,
                )
            )

        return upstream_column_refs

    @classmethod
    @lru_cache(maxsize=1000)
    def get_explore_fields_from_looker_api(
        cls, looker_client: "LookerAPI", model_name: str, explore_name: str
    ) -> LookmlModelExplore:
        """
        Get fields from Looker API for the given explore.
        Only queries for the fields we need (dimensions, measures, and dimension groups)
        to optimize API performance.

        Note: This is a cached method to optimize API performance since a single explore can have multiple views.
        When we associate a view with an explore, we try to associate as many views as possible with the same explore to reduce the number of API calls.

        Returns:
            LookmlModelExplore: The explore with the fields.
        Raises:
            Exception: If there is an error getting the explore from the Looker API.
        """
        try:
            fields_to_query = ["fields"]
            explore: LookmlModelExplore = looker_client.lookml_model_explore(
                model_name,
                explore_name,
                fields=fields_to_query,
            )
            return explore
        except Exception as e:
            logger.error(f"Error getting explore from Looker API: {e}")
            raise e


class LookerQueryAPIBasedViewUpstream(AbstractViewUpstream):
    """
    Implements Looker view upstream lineage extraction using the Looker Query API.

    This class leverages the Looker API to generate the fully resolved SQL for a Looker view by constructing a WriteQuery
    that includes all dimensions, dimension groups and measures. The SQL is then parsed to extract column-level lineage.
    The Looker client is required for this class, as it is used to execute the WriteQuery and retrieve the SQL.

    Other view upstream implementations use string parsing to extract lineage information from the SQL, which does not cover all the edge cases.
    Limitations of string based lineage extraction: Ref: https://cloud.google.com/looker/docs/reference/param-field-sql#sql_for_dimensions

    Key Features:
    - Requires a Looker client (`looker_client`) to execute queries and retrieve SQL for the view.
    - Requires a `view_to_explore_map` to map view names to their corresponding explore name
    - Field name translation is handled: Looker API field names are constructed as `<view_name>.<field_name>`, and helper
      methods are provided to convert between Looker API field names and raw field names.
    - SQL parsing is cached for efficiency, and the class is designed to gracefully fall back if the Looker Query API fails.
    - All lineage extraction is based on the SQL returned by the Looker API, ensuring accurate and up-to-date lineage.

    Why view_to_explore_map is required:
    The Looker Query API expects the explore name (not the view name) as the "view" parameter in the WriteQuery.
    In Looker, a view can be referenced by multiple explores, but the API needs any one of the
    explores to access the view's fields

    Example WriteQuery request (see `_execute_query` for details):
        {
            "model": "test_model",
            "view": "users_explore",  # This is the explore name, not the view name
            "fields": [
                "users.email", "users.lifetime_purchase_count"
            ],
            "limit": "1",
            "cache": true
        }
    The SQL response is then parsed to extract upstream tables and column-level lineage.

    For further details, see the method-level docstrings, especially:
      - `__get_spr`: SQL parsing and lineage extraction workflow
      - `_get_sql_write_query`: WriteQuery construction and field enumeration
      - `_execute_query`: Looker API invocation and SQL retrieval - this only generates the SQL query, does not execute it
      - Field name translation: `_get_looker_api_field_name` and `_get_field_name_from_looker_api_field_name`

    Note: This class is intended to be robust and raise exceptions if SQL parsing or API calls fail, and will fall back to
    other implementations - custom regex-based parsing if necessary.
    """

    def __init__(
        self,
        view_context: LookerViewContext,
        looker_view_id_cache: LookerViewIdCache,
        config: LookMLSourceConfig,
        reporter: LookMLSourceReport,
        ctx: PipelineContext,
        looker_client: LookerAPI,
        view_to_explore_map: Dict[str, str],
    ):
        super().__init__(view_context, looker_view_id_cache, config, reporter, ctx)
        self.looker_client = looker_client
        self.view_to_explore_map = view_to_explore_map
        # Cache the SQL parsing results
        # We use maxsize=1 because a new class instance is created for each view, Ref: view_upstream.create_view_upstream
        self._get_spr = lru_cache(maxsize=1)(self.__get_spr)
        self._get_upstream_dataset_urn = lru_cache(maxsize=1)(
            self.__get_upstream_dataset_urn
        )
        # Initialize the cache
        # Done to fallback to other implementations if the Looker Query API fails
        self._get_spr()

    def __get_spr(self) -> SqlParsingResult:
        """
        Retrieves the SQL parsing result for the current Looker view by:
        1. Building a WriteQuery for the view.
        2. Executing the query via the Looker API to get the SQL.
        3. Parsing the SQL to extract lineage information.

        Returns:
            SqlParsingResult if successful, otherwise None.
        Raises:
            ValueError: If no SQL is found in the response.
            ValueError: If no fields are found for the view.
            ValueError: If explore name is not found for the view.
            ValueError: If error in parsing SQL for upstream tables.
            ValueError: If error in parsing SQL for column lineage.
        """
        try:
            # Build the WriteQuery for the current view.
            sql_query: WriteQuery = self._get_sql_write_query()

            # Execute the query to get the SQL representation from Looker.
            sql_response = self._execute_query(sql_query)

            # Parse the SQL to extract lineage information.
            spr = create_lineage_sql_parsed_result(
                query=sql_response,
                default_schema=self.view_context.view_connection.default_schema,
                default_db=self.view_context.view_connection.default_db,
                platform=self.view_context.view_connection.platform,
                platform_instance=self.view_context.view_connection.platform_instance,
                env=self.view_context.view_connection.platform_env or self.config.env,
                graph=self.ctx.graph,
            )

            # Check for errors encountered during table extraction.
            table_error = spr.debug_info.table_error
            if table_error is not None:
                self.reporter.report_warning(
                    title="Table Level Lineage Extraction Failed",
                    message="Error in parsing derived sql",
                    context=f"View-name: {self.view_context.name()}",
                    exc=table_error,
                )
                raise ValueError(
                    f"Error in parsing SQL for upstream tables: {table_error}"
                )

            column_error = spr.debug_info.column_error
            if column_error is not None:
                self.reporter.report_warning(
                    title="Column Level Lineage Extraction Failed",
                    message="Error in parsing derived sql",
                    context=f"View-name: {self.view_context.name()}",
                    exc=column_error,
                )
                raise ValueError(
                    f"Error in parsing SQL for column lineage: {column_error}"
                )

            return spr
        except Exception:
            # Reraise the exception to allow higher-level handling.
            raise

    def _get_time_dim_group_field_name(self, dim_group: dict) -> str:
        """
        Time dimension groups must be referenced by their individual timeframes suffix.
        Example:
            dimension_group: created {
                type: time
                timeframes: [date, week, month]
                sql: ${TABLE}.created_at ;;
            }
        Used as: {view_name.date_created}

        created -> created_date, created_week, created_month
        # Ref: https://cloud.google.com/looker/docs/reference/param-field-dimension-group#dimension_groups_must_be_referenced_by_their_individual_dimensions
        """
        dim_group_name = dim_group.get(NAME)
        timeframes = dim_group.get(VIEW_FIELD_TIMEFRAMES_ATTRIBUTE)

        # If timeframes is not included (rare case), the dimension group will include all possible timeframes.
        # We will pick to use "raw"
        suffix = timeframes[0] if timeframes else "raw"
        return f"{dim_group_name}_{suffix}"

    def _get_duration_dim_group_field_name(self, dim_group: dict) -> str:
        """
        Duration dimension groups must be referenced by their plural version of the interval value as prefix
        Example:
            dimension_group: since_event {
                type: duration
                intervals: [hour, day, week, month, quarter, year]
                sql_start: ${faa_event_date_raw} ;;
                sql_end: CURRENT_TIMESTAMP();;
            }
        Used as: {view_name.hours_since_event}

        since_event -> hours_since_event, days_since_event, weeks_since_event, months_since_event, quarters_since_event, years_since_event
        # Ref: https://cloud.google.com/looker/docs/reference/param-field-dimension-group#referencing_intervals_from_another_lookml_field
        """
        dim_group_name = dim_group.get(NAME)
        intervals = dim_group.get(VIEW_FIELD_INTERVALS_ATTRIBUTE)

        # If intervals is not included (rare case), the dimension group will include all possible intervals.
        # We will pick to use "day" -> "days"
        prefix = f"{intervals[0]}s" if intervals else "days"
        return f"{prefix}_{dim_group_name}"

    def _is_field_from_current_view(
        self, field: LookmlModelExploreField, current_view_name: str
    ) -> bool:
        """
        Check if a field belongs to the current view based on the original_view attribute.

        Args:
            field: The field object from the explore
            current_view_name: The name of the current view we're processing

        Returns:
            True if the field belongs to the current view, False otherwise
        """
        # Check if the field has an original_view attribute and it matches our current view
        if hasattr(field, "view"):
            return field.view == current_view_name

        # If no view attribute, we can't determine the source view
        # In this case, we'll be conservative and include the field
        logger.debug(f"Field {field.name} has no view attribute, including it")
        return True

    def _get_fields_from_looker_api(self, explore_name: str) -> List[str]:
        """
        Get fields from Looker API for the given explore.
        Only queries for the fields we need (dimensions, measures, and dimension groups)
        to optimize API performance.

        Sample Response: [{
                "align": "left",
                "can_filter": true,
                "category": "dimension",
                "default_filter_value": null,
                "description": "",
                "enumerations": null,
                "field_group_label": null,
                "fill_style": null,
                "fiscal_month_offset": 0,
                "has_allowed_values": false,
                "hidden": false,
                "is_filter": false,
                "is_numeric": false,
                "label": "Customer Analysis User Purchase Status",
                "label_from_parameter": null,
                "label_short": "User Purchase Status",
                "map_layer": null,
                "name": "customer_analysis.user_purchase_status",
                "strict_value_format": false,
                "requires_refresh_on_sort": false,
                "sortable": true,
                "suggestions": null,
                "synonyms": [],
                "tags": [],
                "type": "string",
                "user_attribute_filter_types": [
                    "string",
                    "advanced_filter_string"
                ],
                "value_format": null,
                "view": "customer_analysis",
                "view_label": "Customer Analysis",
                "dynamic": false,
                "week_start_day": "monday",
                "original_view": "users",
                "dimension_group": null,
                "error": null,
                "field_group_variant": "User Purchase Status",
                "measure": false,
                "parameter": false,
                "primary_key": false,
                "project_name": "anush-dev-project",
                "scope": "customer_analysis",
                "suggest_dimension": "customer_analysis.user_purchase_status",
                "suggest_explore": "customer_analysis",
                "suggestable": true,
                "is_fiscal": false,
                "is_timeframe": false,
                "can_time_filter": false,
                "time_interval": null,
                "lookml_link": "/projects/anush-dev-project/files/views%2Fusers.view.lkml?line=52",
                "period_over_period_params": null,
                "permanent": null,
                "source_file": "views/users.view.lkml",
                "source_file_path": "anush-dev-project/views/users.view.lkml",
                "sql": "CASE\n        WHEN ${user_metrics.purchase_count} = 0 THEN 'Prospect'\n        WHEN ${user_metrics.purchase_count} = 1 THEN 'New Customer'\n        WHEN ${user_metrics.purchase_count} BETWEEN 2 AND 5 THEN 'Happy Customer'\n        ELSE 'Loyal Customer'\n      END ",
                "sql_case": null,
                "filters": null,
                "times_used": 0
            }
        ]
        Returns:
            List of field names in Looker API format
        """
        view_fields: List[str] = []
        # Get the current view name to filter fields
        current_view_name = self.view_context.name()

        try:
            logger.debug(
                f"Attempting to get explore details from Looker API for explore: {explore_name} and view: {current_view_name}"
            )
            # Only query for the fields we need to optimize API performance
            explore: LookmlModelExplore = self.get_explore_fields_from_looker_api(
                self.looker_client, self.looker_view_id_cache.model_name, explore_name
            )

            if explore and explore.fields:
                logger.debug(
                    f"Looker API response for explore fields: {explore.fields}"
                )
                # Creating a map to de-dup dimension group fields - adding all of them adds to the query length, we dont need all of them for CLL
                dimension_group_fields_mapping: Dict[str, str] = {}
                # Get dimensions from API
                if explore.fields.dimensions:
                    for dim_field in explore.fields.dimensions:
                        if dim_field.name and self._is_field_from_current_view(
                            dim_field, current_view_name
                        ):
                            # Handle dimension group fields - only add one field per dimension group
                            if dim_field.dimension_group:
                                # Skip if this dimension group already has a field
                                if (
                                    dim_field.dimension_group
                                    in dimension_group_fields_mapping
                                ):
                                    continue
                                # Add this field as the representative for this dimension group
                                dimension_group_fields_mapping[
                                    dim_field.dimension_group
                                ] = dim_field.name

                            view_fields.append(dim_field.name)
                            logger.debug(
                                f"Added dimension field from API: {dim_field.name} (dimension_group: {dim_field.dimension_group})"
                            )

                # Get measures from API
                if explore.fields.measures:
                    for measure_field in explore.fields.measures:
                        if measure_field.name and self._is_field_from_current_view(
                            measure_field, current_view_name
                        ):
                            view_fields.append(measure_field.name)
                            logger.debug(
                                f"Added measure field from API: {measure_field.name}"
                            )
            else:
                logger.warning(
                    f"No fields found in explore '{explore_name}' from Looker API, falling back to view context"
                )

        except Exception:
            logger.warning(
                f"Failed to get explore details from Looker API for explore '{explore_name}'. Current view: {self.view_context.name()} and view_fields: {view_fields}. Falling back to view csontext.",
                exc_info=True,
            )
            # Resetting view_fields to trigger fallback to view context
            view_fields = []

        return view_fields

    def _get_fields_from_view_context(self) -> List[str]:
        """
        Get fields from view context as fallback.

        Returns:
            List of field names in Looker API format
        """
        view_fields: List[str] = []

        logger.debug(
            f"Using view context as fallback for view: {self.view_context.name()}"
        )

        # Add dimension fields in the format: <view_name>.<dimension_name> or <view_name>.<measure_name>
        for field in self.view_context.dimensions() + self.view_context.measures():
            field_name = field.get(NAME)
            assert field_name  # Happy linter
            view_fields.append(self._get_looker_api_field_name(field_name))

        for dim_group in self.view_context.dimension_groups():
            dim_group_type_str = dim_group.get(VIEW_FIELD_TYPE_ATTRIBUTE)

            logger.debug(
                f"Processing dimension group from view context: {dim_group.get(NAME, 'unknown')}, type: {dim_group_type_str}"
            )

            if dim_group_type_str is None:
                logger.warning(
                    f"Dimension group '{dim_group.get(NAME, 'unknown')}' has None type, skipping"
                )
                continue

            try:
                dim_group_type: ViewFieldDimensionGroupType = (
                    ViewFieldDimensionGroupType(dim_group_type_str)
                )

                if dim_group_type == ViewFieldDimensionGroupType.TIME:
                    view_fields.append(
                        self._get_looker_api_field_name(
                            self._get_time_dim_group_field_name(dim_group)
                        )
                    )
                elif dim_group_type == ViewFieldDimensionGroupType.DURATION:
                    view_fields.append(
                        self._get_looker_api_field_name(
                            self._get_duration_dim_group_field_name(dim_group)
                        )
                    )
            except Exception:
                logger.error(
                    f"Failed to process dimension group for View-name: {self.view_context.name()}",
                    exc_info=True,
                )
                # Continue processing other fields instead of failing completely
                continue

        return view_fields

    def _get_sql_write_query(self) -> WriteQuery:
        """
        Constructs a WriteQuery object to obtain the SQL representation of the current Looker view.

        This method now uses the Looker API to get explore details directly, providing more comprehensive
        field information compared to relying solely on view context. It falls back to view context
        if API calls fail.

        The method uses the view_to_explore_map to determine the correct explore name to use in the WriteQuery.
        This is crucial because the Looker Query API expects the explore name (not the view name) as the "view" parameter.

        Ref: https://cloud.google.com/looker/docs/reference/param-field-sql#sql_for_dimensions

        Returns:
            WriteQuery: The WriteQuery object if fields are found and explore name is available, otherwise None.

        Raises:
            ValueError: If no fields are found for the view.
        """

        # Use explore name from view_to_explore_map if available
        # explore_name is always present in the view_to_explore_map because of the check in view_upstream.create_view_upstream
        explore_name = self.view_to_explore_map.get(self.view_context.name())
        assert explore_name  # Happy linter

        # Try to get fields from Looker API first for more comprehensive information
        view_fields = self._get_fields_from_looker_api(explore_name)

        # Fallback to view context if API didn't provide fields or failed
        if not view_fields:
            view_fields = self._get_fields_from_view_context()

        if not view_fields:
            raise ValueError(
                f"No fields found for view '{self.view_context.name()}'. Cannot proceed with Looker API for view lineage."
            )

        logger.debug(
            f"Final field list for view '{self.view_context.name()}': {view_fields}"
        )

        # Construct and return the WriteQuery object.
        # The 'limit' is set to "1" as the query is only used to obtain SQL, not to fetch data.
        return WriteQuery(
            model=self.looker_view_id_cache.model_name,
            view=explore_name,
            fields=view_fields,
            filters={},
            limit="1",
        )

    def _execute_query(self, query: WriteQuery) -> str:
        """
        Executes a Looker SQL query using the Looker API and returns the SQL string.

        Ref: https://cloud.google.com/looker/docs/reference/looker-api/latest/methods/Query/run_inline_query

        Example Request:
            WriteQuery:
                {
                    "model": "test_model",
                    "view": "users",
                    "fields": [
                        "users.email", "users.lifetime_purchase_count"
                    ],
                    "limit": "1",
                    "cache": true
                }

            Response:
                "
                SELECT
                    users."EMAIL"  AS "users.email",
                    COUNT(DISTINCT ( purchases."PK"  ) ) AS "users.lifetime_purchase_count"
                FROM "ECOMMERCE"."USERS"  AS users
                LEFT JOIN "ECOMMERCE"."PURCHASES"  AS purchases ON (users."PK") = (purchases."USER_FK")
                GROUP BY
                    1
                ORDER BY
                    2 DESC
                FETCH NEXT 1 ROWS ONLY
                "
        Args:
            query (WriteQuery): The Looker WriteQuery object to execute.

        Returns:
            str: The SQL string returned by the Looker API, or an empty string if execution fails.
        """

        # Record the start time for latency measurement.
        start_time = datetime.now()

        # Execute the query using the Looker client.
        sql_response = self.looker_client.generate_sql_query(
            write_query=query, use_cache=self.config.use_api_cache_for_view_lineage
        )

        # Record the end time after query execution.
        end_time = datetime.now()

        # Attempt to get the LookerViewId for reporting.
        looker_view_id: Optional[LookerViewId] = (
            self.looker_view_id_cache.get_looker_view_id(
                view_name=self.view_context.name(),
                base_folder_path=self.view_context.base_folder_path,
            )
        )

        # Report the query API latency if the view ID is available.
        if looker_view_id is not None:
            self.reporter.report_looker_query_api_latency(
                looker_view_id.get_urn(self.config),
                end_time - start_time,
            )

        # Validate the response structure.
        if not sql_response:
            raise ValueError(
                f"No SQL found in response for view '{self.view_context.name()}'. Response: {sql_response}"
            )

        # Extract the SQL string from the response.
        return sql_response

    def __get_upstream_dataset_urn(self) -> List[Urn]:
        """
        Extract upstream dataset URNs by parsing the SQL for the current view.

        Returns:
            List[Urn]: List of upstream dataset URNs, or an empty list if parsing fails.
        """
        # Attempt to get the SQL parsing result for the current view.
        spr: SqlParsingResult = self._get_spr()

        # Remove any 'hive.' prefix from upstream table URNs.
        upstream_dataset_urns: List[str] = [
            _drop_hive_dot(urn) for urn in spr.in_tables
        ]

        # Fix any derived view references present in the URNs.
        upstream_dataset_urns = fix_derived_view_urn(
            urns=upstream_dataset_urns,
            looker_view_id_cache=self.looker_view_id_cache,
            base_folder_path=self.view_context.base_folder_path,
            config=self.config,
        )

        return upstream_dataset_urns

    def _get_looker_api_field_name(self, field_name: str) -> str:
        """
        Translate the field name to the looker api field name

        Example:
            pk -> purchases.pk
        """
        return f"{self.view_context.name()}.{field_name}"

    def _get_field_name_from_looker_api_field_name(
        self, looker_api_field_name: str
    ) -> str:
        """
        Translate the looker api field name to the field name

        Example:
            purchases.pk -> pk
        """
        # Remove the view name at the start and the dot from the looker_api_field_name, but only if it matches the current view name
        prefix = f"{self.view_context.name()}."
        if looker_api_field_name.startswith(prefix):
            return looker_api_field_name[len(prefix) :]
        else:
            # Don't throw an error, just return the original field name
            return looker_api_field_name

    def get_upstream_dataset_urn(self) -> List[Urn]:
        """Get upstream dataset URNs"""
        return self._get_upstream_dataset_urn()

    def get_upstream_column_ref(
        self, field_context: LookerFieldContext
    ) -> List[ColumnRef]:
        """Return upstream column references for a given field."""
        spr: SqlParsingResult = self._get_spr()
        if not spr.column_lineage:
            return []

        field_type: Optional[ViewFieldDimensionGroupType] = None
        field_name = field_context.name()
        try:
            # Try if field is a dimension group
            field_type = ViewFieldDimensionGroupType(
                field_context.raw_field.get(VIEW_FIELD_TYPE_ATTRIBUTE)
            )

            if field_type == ViewFieldDimensionGroupType.TIME:
                field_name = self._get_time_dim_group_field_name(
                    field_context.raw_field
                )
            elif field_type == ViewFieldDimensionGroupType.DURATION:
                field_name = self._get_duration_dim_group_field_name(
                    field_context.raw_field
                )

        except Exception:
            # Not a dimension group, no modification needed
            logger.debug(
                f"view-name={self.view_context.name()}, field-name={field_name}, field-type={field_context.raw_field.get(VIEW_FIELD_TYPE_ATTRIBUTE)}"
            )

        field_api_name = self._get_looker_api_field_name(field_name).lower()

        upstream_refs: List[ColumnRef] = []

        for lineage in spr.column_lineage:
            if lineage.downstream.column.lower() == field_api_name:
                for upstream in lineage.upstreams:
                    upstream_refs.append(
                        ColumnRef(table=upstream.table, column=upstream.column)
                    )

        return _drop_hive_dot_from_upstream(upstream_refs)

    def create_fields(self) -> List[ViewField]:
        """Create ViewField objects from SQL parsing result."""
        spr: SqlParsingResult = self._get_spr()

        if not spr.column_lineage:
            return []

        fields: List[ViewField] = []

        for lineage in spr.column_lineage:
            fields.append(
                ViewField(
                    name=self._get_field_name_from_looker_api_field_name(
                        lineage.downstream.column
                    ),
                    label="",
                    type=lineage.downstream.native_column_type or "unknown",
                    description="",
                    field_type=ViewFieldType.UNKNOWN,
                    upstream_fields=_drop_hive_dot_from_upstream(lineage.upstreams),
                )
            )
        return fields


class SqlBasedDerivedViewUpstream(AbstractViewUpstream, ABC):
    """
    Handle the case where upstream dataset is defined in derived_table.sql
    """

    def __init__(
        self,
        view_context: LookerViewContext,
        looker_view_id_cache: LookerViewIdCache,
        config: LookMLSourceConfig,
        reporter: LookMLSourceReport,
        ctx: PipelineContext,
    ):
        super().__init__(view_context, looker_view_id_cache, config, reporter, ctx)
        # These are the function where we need to catch the response once calculated
        self._get_spr = lru_cache(maxsize=1)(self.__get_spr)
        self._get_upstream_dataset_urn = lru_cache(maxsize=1)(
            self.__get_upstream_dataset_urn
        )

    def __get_spr(self) -> Optional[SqlParsingResult]:
        # for backward compatibility
        if not self.config.parse_table_names_from_sql:
            return None

        spr = create_lineage_sql_parsed_result(
            query=self.get_sql_query(),
            default_schema=self.view_context.view_connection.default_schema,
            default_db=self.view_context.view_connection.default_db,
            platform=self.view_context.view_connection.platform,
            platform_instance=self.view_context.view_connection.platform_instance,
            env=self.view_context.view_connection.platform_env or self.config.env,
            graph=self.ctx.graph,
        )
        return spr

    def __get_upstream_dataset_urn(self) -> List[Urn]:
        sql_parsing_result: Optional[SqlParsingResult] = self._get_spr()

        if sql_parsing_result is None:
            return []

        if sql_parsing_result.debug_info.table_error is not None:
            logger.debug(
                f"view-name={self.view_context.name()}, sql_query={self.get_sql_query()}"
            )
            self.reporter.report_warning(
                title="Table Level Lineage Missing",
                message="Error in parsing derived sql",
                context=f"view-name: {self.view_context.name()}, platform: {self.view_context.view_connection.platform}",
                exc=sql_parsing_result.debug_info.table_error,
            )
            return []

        upstream_dataset_urns: List[str] = [
            _drop_hive_dot(urn) for urn in sql_parsing_result.in_tables
        ]

        # fix any derived view reference present in urn
        upstream_dataset_urns = fix_derived_view_urn(
            urns=upstream_dataset_urns,
            looker_view_id_cache=self.looker_view_id_cache,
            base_folder_path=self.view_context.base_folder_path,
            config=self.config,
        )

        return upstream_dataset_urns

    def create_fields(self) -> List[ViewField]:
        spr: Optional[SqlParsingResult] = self._get_spr()

        if spr is None:
            return []

        if spr.debug_info.column_error is not None:
            self.reporter.report_warning(
                title="Column Level Lineage Missing",
                message="Error in parsing derived sql for CLL",
                context=f"View-name: {self.view_context.name()}",
                exc=spr.debug_info.column_error,
            )
            return []

        fields: List[ViewField] = []

        column_lineages: List[ColumnLineageInfo] = (
            spr.column_lineage if spr.column_lineage is not None else []
        )

        for cll in column_lineages:
            fields.append(
                ViewField(
                    name=cll.downstream.column,
                    label="",
                    type=(
                        cll.downstream.native_column_type
                        if cll.downstream.native_column_type is not None
                        else "unknown"
                    ),
                    description="",
                    field_type=ViewFieldType.UNKNOWN,
                    upstream_fields=_drop_hive_dot_from_upstream(cll.upstreams),
                )
            )

        return fields

    def get_upstream_column_ref(
        self, field_context: LookerFieldContext
    ) -> List[ColumnRef]:
        sql_parsing_result: Optional[SqlParsingResult] = self._get_spr()

        if sql_parsing_result is None:
            return []

        if sql_parsing_result.debug_info.column_error is not None:
            self.reporter.report_warning(
                title="Column Level Lineage Missing",
                message="Error in parsing derived sql for CLL",
                context=f"View-name: {self.view_context.name()}. "
                f"Error: {sql_parsing_result.debug_info.column_error}",
            )
            return []

        upstreams_column_refs: List[ColumnRef] = []
        if sql_parsing_result.column_lineage:
            for cll in sql_parsing_result.column_lineage:
                if cll.downstream.column == field_context.name():
                    upstreams_column_refs = cll.upstreams
                    break

        # field might get skip either because of Parser not able to identify the column from GMS
        # in-case of "select * from look_ml_view.SQL_TABLE_NAME" or extra field are defined in the looker view which is
        # referring to upstream table
        if self._get_upstream_dataset_urn() and not upstreams_column_refs:
            upstreams_column_refs = self.create_upstream_column_refs(
                upstream_urn=self._get_upstream_dataset_urn()[
                    0
                ],  # 0th index has table of from clause,
                downstream_looker_columns=field_context.column_name_in_sql_attribute(),
            )

        # fix any derived view reference present in urn
        upstreams_column_refs = resolve_derived_view_urn_of_col_ref(
            column_refs=upstreams_column_refs,
            looker_view_id_cache=self.looker_view_id_cache,
            base_folder_path=self.view_context.base_folder_path,
            config=self.config,
        )

        return _drop_hive_dot_from_upstream(upstreams_column_refs)

    def get_upstream_dataset_urn(self) -> List[Urn]:
        return self._get_upstream_dataset_urn()

    @abstractmethod
    def get_sql_query(self) -> str:
        pass


class DirectQueryUpstreamSource(SqlBasedDerivedViewUpstream):
    """
    Pattern 7 as per view-context documentation
    """

    def get_sql_query(self) -> str:
        return self.view_context.datahub_transformed_sql_table_name()


class DerivedQueryUpstreamSource(SqlBasedDerivedViewUpstream):
    """
    Pattern 4 as per view-context documentation
    """

    def get_sql_query(self) -> str:
        return self.view_context.datahub_transformed_sql()


class NativeDerivedViewUpstream(AbstractViewUpstream):
    """
    Handle the case where upstream dataset is defined as derived_table.explore_source
    """

    upstream_dataset_urns: List[str]
    explore_column_mapping: Dict

    def __init__(
        self,
        view_context: LookerViewContext,
        looker_view_id_cache: LookerViewIdCache,
        config: LookMLSourceConfig,
        reporter: LookMLSourceReport,
        ctx: PipelineContext,
    ):
        super().__init__(view_context, looker_view_id_cache, config, reporter, ctx)

        self._get_upstream_dataset_urn = lru_cache(maxsize=1)(
            self.__get_upstream_dataset_urn
        )
        self._get_explore_column_mapping = lru_cache(maxsize=1)(
            self.__get_explore_column_mapping
        )

    def __get_upstream_dataset_urn(self) -> List[str]:
        current_view_id: Optional[LookerViewId] = (
            self.looker_view_id_cache.get_looker_view_id(
                view_name=self.view_context.name(),
                base_folder_path=self.view_context.base_folder_path,
            )
        )

        # Current view will always be present in cache. assert  will silence the lint
        assert current_view_id

        # We're creating a "LookerExplore" just to use the urn generator.
        upstream_dataset_urns: List[str] = [
            LookerExplore(
                name=self.view_context.explore_source()[NAME],
                model_name=current_view_id.model_name,
            ).get_explore_urn(self.config)
        ]

        return upstream_dataset_urns

    def __get_explore_column_mapping(self) -> Dict:
        explore_columns: Dict = self.view_context.explore_source().get("columns", {})

        explore_column_mapping = {}

        for column in explore_columns:
            explore_column_mapping[column[NAME]] = column

        return explore_column_mapping

    def get_upstream_column_ref(
        self, field_context: LookerFieldContext
    ) -> List[ColumnRef]:
        upstream_column_refs: List[ColumnRef] = []

        if not self._get_upstream_dataset_urn():
            # No upstream explore dataset found
            logging.debug(
                f"upstream explore not found for field {field_context.name()} of view {self.view_context.name()}"
            )
            return upstream_column_refs

        explore_urn: str = self._get_upstream_dataset_urn()[0]
        expected_columns: List[str] = []

        for column in field_context.column_name_in_sql_attribute():
            if column in self._get_explore_column_mapping():
                explore_column: Dict = self._get_explore_column_mapping()[column]
                expected_columns.append(
                    explore_column.get("field", explore_column[NAME])
                )

        return self.create_upstream_column_refs(
            upstream_urn=explore_urn, downstream_looker_columns=expected_columns
        )

    def get_upstream_dataset_urn(self) -> List[Urn]:
        return self._get_upstream_dataset_urn()


class RegularViewUpstream(AbstractViewUpstream):
    """
    Handle the case where upstream dataset name is equal to view-name
    """

    upstream_dataset_urn: Optional[str]

    def __init__(
        self,
        view_context: LookerViewContext,
        looker_view_id_cache: LookerViewIdCache,
        config: LookMLSourceConfig,
        reporter: LookMLSourceReport,
        ctx: PipelineContext,
    ):
        super().__init__(view_context, looker_view_id_cache, config, reporter, ctx)
        self.upstream_dataset_urn = None

        self._get_upstream_dataset_urn = lru_cache(maxsize=1)(
            self.__get_upstream_dataset_urn
        )

    def __get_upstream_dataset_urn(self) -> Urn:
        # In regular case view's upstream dataset is either same as view-name or mentioned in "sql_table_name" field
        # view_context.datahub_transformed_sql_table_name() handle this condition to return dataset name
        qualified_table_name: str = _generate_fully_qualified_name(
            sql_table_name=self.view_context.datahub_transformed_sql_table_name(),
            connection_def=self.view_context.view_connection,
            reporter=self.view_context.reporter,
            view_name=self.view_context.name(),
        )

        self.upstream_dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.view_context.view_connection.platform,
            name=qualified_table_name.lower(),
            platform_instance=self.view_context.view_connection.platform_instance,
            env=self.view_context.view_connection.platform_env or self.config.env,
        )

        return self.upstream_dataset_urn

    def get_upstream_column_ref(
        self, field_context: LookerFieldContext
    ) -> List[ColumnRef]:
        return self.create_upstream_column_refs(
            upstream_urn=self._get_upstream_dataset_urn(),
            downstream_looker_columns=field_context.column_name_in_sql_attribute(),
        )

    def get_upstream_dataset_urn(self) -> List[Urn]:
        return [self._get_upstream_dataset_urn()]


class DotSqlTableNameViewUpstream(AbstractViewUpstream):
    """
    Handle the case where upstream dataset name is mentioned as sql_table_name: ${view-name.SQL_TABLE_NAME}
    """

    upstream_dataset_urn: List[Urn]

    def __init__(
        self,
        view_context: LookerViewContext,
        looker_view_id_cache: LookerViewIdCache,
        config: LookMLSourceConfig,
        reporter: LookMLSourceReport,
        ctx: PipelineContext,
    ):
        super().__init__(view_context, looker_view_id_cache, config, reporter, ctx)
        self.upstream_dataset_urn = []

        self._get_upstream_dataset_urn = lru_cache(maxsize=1)(
            self.__get_upstream_dataset_urn
        )

    def __get_upstream_dataset_urn(self) -> List[Urn]:
        # In this case view_context.datahub_transformed_sql_table_name() refers to derived view name
        looker_view_id = get_derived_looker_view_id(
            qualified_table_name=_generate_fully_qualified_name(
                self.view_context.datahub_transformed_sql_table_name(),
                self.view_context.view_connection,
                self.view_context.reporter,
                self.view_context.name(),
            ),
            base_folder_path=self.view_context.base_folder_path,
            looker_view_id_cache=self.looker_view_id_cache,
        )

        if looker_view_id is not None:
            self.upstream_dataset_urn = [
                looker_view_id.get_urn(
                    config=self.config,
                )
            ]

        return self.upstream_dataset_urn

    def get_upstream_column_ref(
        self, field_context: LookerFieldContext
    ) -> List[ColumnRef]:
        upstream_column_ref: List[ColumnRef] = []

        if not self._get_upstream_dataset_urn():
            return upstream_column_ref

        return self.create_upstream_column_refs(
            upstream_urn=self._get_upstream_dataset_urn()[0],
            downstream_looker_columns=field_context.column_name_in_sql_attribute(),
        )

    def get_upstream_dataset_urn(self) -> List[Urn]:
        return self._get_upstream_dataset_urn()


class EmptyImplementation(AbstractViewUpstream):
    def get_upstream_column_ref(
        self, field_context: LookerFieldContext
    ) -> List[ColumnRef]:
        return []

    def get_upstream_dataset_urn(self) -> List[Urn]:
        return []


def create_view_upstream(
    view_context: LookerViewContext,
    looker_view_id_cache: LookerViewIdCache,
    config: LookMLSourceConfig,
    ctx: PipelineContext,
    reporter: LookMLSourceReport,
    looker_client: Optional["LookerAPI"] = None,
    view_to_explore_map: Optional[Dict[str, str]] = None,
) -> AbstractViewUpstream:
    # Looker client is required for LookerQueryAPIBasedViewUpstream also enforced by config.use_api_for_view_lineage
    # view_to_explore_map is required for Looker query API args
    # Only process if view exists in view_to_explore_map, because we cannot query views which are not reachable from an explore
    if (
        config.use_api_for_view_lineage
        and looker_client
        and view_to_explore_map
        and view_context.name() in view_to_explore_map
    ):
        try:
            return LookerQueryAPIBasedViewUpstream(
                view_context=view_context,
                config=config,
                reporter=reporter,
                ctx=ctx,
                looker_view_id_cache=looker_view_id_cache,
                looker_client=looker_client,
                view_to_explore_map=view_to_explore_map,
            )
        except Exception as e:
            # Falling back to custom regex-based parsing - best effort approach
            reporter.report_warning(
                title="Looker Query API based View Upstream Failed",
                message="Error in getting upstream lineage for view using Looker Query API",
                context=f"View-name: {view_context.name()}",
                exc=e,
            )
    else:
        logger.debug(
            f"Skipping Looker Query API for view: {view_context.name()} because one or more conditions are not met: "
            f"use_api_for_view_lineage={config.use_api_for_view_lineage}, "
            f"looker_client={'set' if looker_client else 'not set'}, "
            f"view_to_explore_map={'set' if view_to_explore_map else 'not set'}, "
            f"view_in_view_to_explore_map={view_context.name() in view_to_explore_map if view_to_explore_map else False}"
        )

    if view_context.is_regular_case():
        return RegularViewUpstream(
            view_context=view_context,
            config=config,
            reporter=reporter,
            ctx=ctx,
            looker_view_id_cache=looker_view_id_cache,
        )

    if view_context.is_sql_table_name_referring_to_view():
        return DotSqlTableNameViewUpstream(
            view_context=view_context,
            config=config,
            reporter=reporter,
            ctx=ctx,
            looker_view_id_cache=looker_view_id_cache,
        )

    if any(
        [
            view_context.is_sql_based_derived_case(),
            view_context.is_sql_based_derived_view_without_fields_case(),
        ]
    ):
        return DerivedQueryUpstreamSource(
            view_context=view_context,
            config=config,
            reporter=reporter,
            ctx=ctx,
            looker_view_id_cache=looker_view_id_cache,
        )

    if view_context.is_direct_sql_query_case():
        return DirectQueryUpstreamSource(
            view_context=view_context,
            config=config,
            reporter=reporter,
            ctx=ctx,
            looker_view_id_cache=looker_view_id_cache,
        )

    if view_context.is_native_derived_case():
        return NativeDerivedViewUpstream(
            view_context=view_context,
            config=config,
            reporter=reporter,
            ctx=ctx,
            looker_view_id_cache=looker_view_id_cache,
        )

    reporter.report_warning(
        title="ViewUpstream Implementation Not Found",
        message="No implementation found to resolve upstream of the view",
        context=f"view_name={view_context.name()} , view_file_name={view_context.view_file_name()}",
    )

    return EmptyImplementation(
        view_context=view_context,
        config=config,
        reporter=reporter,
        ctx=ctx,
        looker_view_id_cache=looker_view_id_cache,
    )
