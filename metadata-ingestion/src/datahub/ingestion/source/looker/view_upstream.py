import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime
from functools import lru_cache
from typing import Dict, List, Optional

from looker_sdk.sdk.api40.models import (
    WriteQuery,
)

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.looker.looker_common import (
    LookerExplore,
    LookerViewId,
    ViewField,
    ViewFieldType,
)
from datahub.ingestion.source.looker.looker_config import LookerConnectionDefinition
from datahub.ingestion.source.looker.looker_lib_wrapper import (
    LookerAPI,
    LookerQueryResponseFormat,
)
from datahub.ingestion.source.looker.looker_view_id_cache import LookerViewIdCache
from datahub.ingestion.source.looker.lookml_concept_context import (
    LookerFieldContext,
    LookerViewContext,
)
from datahub.ingestion.source.looker.lookml_config import (
    DERIVED_VIEW_SUFFIX,
    NAME,
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


class LookerQueryAPIBasedViewUpstream(AbstractViewUpstream):
    """
    Implements Looker view upstream lineage extraction using the Looker Query API.

    This class leverages the Looker API to generate the fully resolved SQL for a Looker view by constructing a WriteQuery
    that includes all dimensions, dimension groups and measures. The SQL is then parsed to extract column-level lineage.
    The Looker client is required for this class, as it is used to execute the WriteQuery and retrieve the SQL.

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
    explores to access the view's fiels

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
      - `_execute_query`: Looker API invocation and SQL retrieval
      - Field name translation: `_get_looker_api_field_name` and `_get_field_name_from_looker_api_field_name`

    Note: This class is intended to be robust and to report warnings if SQL parsing or API calls fail, and will fall back to
    other implementations if necessary.
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
        self._get_spr = lru_cache(maxsize=1)(self.__get_spr)
        self._get_upstream_dataset_urn = lru_cache(maxsize=1)(
            self.__get_upstream_dataset_urn
        )

        # Initialize the cache
        # Done to fallback to other implementations if the Looker Query API fails
        self._get_spr()

    def __get_spr(self) -> Optional[SqlParsingResult]:
        """
        Retrieves the SQL parsing result for the current Looker view by:
        1. Building a WriteQuery for the view.
        2. Executing the query via the Looker API to get the SQL.
        3. Parsing the SQL to extract lineage information.

        Returns:
            SqlParsingResult if successful, otherwise None.
        Raises:
            Exception if the Looker API call or SQL parsing fails.
        """
        try:
            # Build the WriteQuery for the current view.
            sql_query: Optional[WriteQuery] = self._get_sql_write_query()
            if not sql_query:
                # No query could be constructed (e.g., no fields found).
                return None

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
            return spr

        except Exception as exc:
            self.reporter.report_warning(
                f"view-{self.view_context.name()}",
                f"Failed to get SQL representation: {exc}",
            )
            # Reraise the exception to allow higher-level handling.
            raise

    def _get_sql_write_query(self) -> Optional[WriteQuery]:
        """
        Constructs a WriteQuery object to obtain the SQL representation of the current Looker view.

        We need to list all the fields for the view to get the SQL representation of the view - this fully resolved SQL for view dimensions and measures.

        The method uses the view_to_explore_map to determine the correct explore name to use in the WriteQuery.
        This is crucial because the Looker Query API expects the explore name (not the view name) as the "view" parameter.

        Ref: https://cloud.google.com/looker/docs/reference/param-field-sql#sql_for_dimensions
        Ref: https://linear.app/acryl-data/issue/ING-970/lookerml-column-lineage-not-handling-intra-view-references

        Returns:
            Optional[WriteQuery]: The WriteQuery object if fields are found and explore name is available, otherwise None.

        Raises:
            ValueError: If the explore name is not found in the view_to_explore_map for the current view.
        """

        # Collect all dimension and measure fields for the view.
        view_fields: List[str] = []

        # Add dimension fields in the format: <view_name>.<dimension_name>
        for dim in self.view_context.dimensions():
            dim_name = dim.get("name")
            if dim_name:
                view_fields.append(self._get_looker_api_field_name(dim_name))

        # Add measure fields in the format: <view_name>.<measure_name>
        for measure in self.view_context.measures():
            measure_name = measure.get("name")
            if measure_name:
                view_fields.append(self._get_looker_api_field_name(measure_name))

        # NOTE: Dimension groups are not currently handled.
        # If needed, uncomment and implement the following:
        # for dim_group in self.view_context.dimension_groups():
        #     dim_group_name = dim_group.get("name")
        #     if dim_group_name:
        #         view_fields.append(f"{view_name}.{dim_group_name}")

        # If no fields are found, log a warning and return None.
        if not view_fields:
            self.reporter.report_warning(
                f"view-{self.view_context.name()}",
                f"No fields found for view '{self.view_context.name()}'. Cannot proceed with Looker API for view lineage.",
            )
            return None

        # Construct and return the WriteQuery object.
        # The 'limit' is set to "1" as the query is only used to obtain SQL, not to fetch data.
        # Use explore name from view_to_explore_map if available
        explore_name = self.view_to_explore_map.get(self.view_context.name())

        # Raise exception if explore name is not found
        if not explore_name:
            raise ValueError(
                f"Explore name mapping not found for view '{self.view_context.name()}'. Cannot proceed with Looker API for view lineage."
            )

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

        Ref: https://cloud.google.com/looker/docs/reference/looker-api/latest/methods/Query/run_query

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
        sql_response_str: str = ""

        try:
            # Record the start time for latency measurement.
            start_time = datetime.now()

            # Execute the query using the Looker client.
            sql_response = self.looker_client.execute_query(
                write_query=query,
                result_format=LookerQueryResponseFormat.SQL,
                use_cache=self.config.use_api_for_view_lineage,
            )

            # Record the end time after query execution.
            end_time = datetime.now()

            # Log the time taken to retrieve the SQL query.
            logger.debug(
                "LookerQueryApiStats: Retrieved SQL query in %.2f seconds",
                (end_time - start_time).total_seconds(),
            )

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
            if not sql_response or len(sql_response) != 1:
                logger.debug(
                    "Unexpected SQL response format for view '%s': %s",
                    self.view_context.name(),
                    sql_response,
                )
                return ""

            # Extract the SQL string from the response.
            sql = sql_response[0].get("sql")
            if sql is None:
                logger.debug(
                    "No SQL found in response for view '%s'. Response: %s",
                    self.view_context.name(),
                    sql_response,
                )
                return ""

            sql_response_str = sql

        except Exception as exc:
            self.reporter.report_warning(
                f"view-{self.view_context.name()}",
                "Failed to execute query for view",
                exc=exc,
            )

        return sql_response_str

    def __get_upstream_dataset_urn(self) -> List[Urn]:
        """
        Extract upstream dataset URNs by parsing the SQL for the current view.

        Returns:
            List[Urn]: List of upstream dataset URNs, or an empty list if parsing fails.
        """
        # Attempt to get the SQL parsing result for the current view.
        sql_parsing_result: Optional[SqlParsingResult] = self._get_spr()
        if sql_parsing_result is None:
            # No parsing result available; return empty list.
            return []

        # Check for errors encountered during table extraction.
        table_error = sql_parsing_result.debug_info.table_error
        if table_error is not None:
            logger.warning(
                "SQL parsing failed for view '%s': %s",
                self.view_context.name(),
                table_error,
            )
            self.reporter.report_warning(
                f"view-{self.view_context.name()}",
                "Error in parsing SQL for upstream tables",
                exc=table_error,
            )
            return []

        # Remove any 'hive.' prefix from upstream table URNs.
        upstream_dataset_urns: List[str] = [
            _drop_hive_dot(urn) for urn in sql_parsing_result.in_tables
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
        spr: Optional[SqlParsingResult] = self._get_spr()
        if spr is None:
            return []

        if spr.debug_info.column_error:
            logger.warning(
                "Column-level SQL parsing failed for field %s in view %s: %s",
                field_context.name(),
                self.view_context.name(),
                spr.debug_info.column_error,
            )
            return []

        if not spr.column_lineage:
            return []

        field_name = field_context.name()
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
        spr: Optional[SqlParsingResult] = self._get_spr()
        if spr is None:
            return []

        if spr.debug_info.column_error:
            self.reporter.report_warning(
                title="Column Level Lineage Missing",
                message="Error in parsing derived sql for CLL",
                context=f"View-name: {self.view_context.name()}",
                exc=spr.debug_info.column_error,
            )
            return []

        fields: List[ViewField] = []
        if spr.column_lineage:
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
    # Only use API if emit_reachable_views_only is enabled
    # view_to_explore_map is required for Looker query API args
    if (
        config.use_api_for_view_lineage
        and looker_client
        and config.emit_reachable_views_only
        and view_to_explore_map
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
            # Fallback to other implementations - best effort approach
            reporter.report_warning(
                title="Failed to create upstream lineage for view using Looker Query API, falling back to other implementations",
                message=f"Failed to create upstream lineage for view: {view_context.name()} using Looker Query API: {e}",
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
