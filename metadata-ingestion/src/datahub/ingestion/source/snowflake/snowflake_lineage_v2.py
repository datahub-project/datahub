import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Collection, Iterable, List, Optional, Set, Tuple, Type

from pydantic import BaseModel, Field, validator

from datahub.configuration.datetimes import parse_absolute_time
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.source.aws.s3_util import make_s3_urn_for_lineage
from datahub.ingestion.source.snowflake.constants import (
    LINEAGE_PERMISSION_ERROR,
    SnowflakeEdition,
    SnowflakeObjectDomain,
)
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnection,
    SnowflakePermissionError,
)
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
    SnowflakeFilter,
    SnowflakeIdentifierBuilder,
    split_qualified_name,
)
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
)
from datahub.metadata.schema_classes import DatasetLineageTypeClass, UpstreamClass
from datahub.sql_parsing.sql_parsing_aggregator import (
    KnownLineageMapping,
    KnownQueryLineageInfo,
    SqlParsingAggregator,
    UrnStr,
)
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
)
from datahub.sql_parsing.sqlglot_utils import get_query_fingerprint
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.time import ts_millis_to_datetime

logger: logging.Logger = logging.getLogger(__name__)

EXTERNAL_LINEAGE = "external_lineage"
TABLE_LINEAGE = "table_lineage"
VIEW_LINEAGE = "view_lineage"


def pydantic_parse_json(field: str) -> classmethod:
    def _parse_from_json(cls: Type, v: Any) -> dict:
        if isinstance(v, str):
            return json.loads(v)
        return v

    return validator(field, pre=True, allow_reuse=True)(_parse_from_json)


class UpstreamColumnNode(BaseModel):
    object_name: str
    object_domain: str
    column_name: str


class ColumnUpstreamJob(BaseModel):
    column_upstreams: List[UpstreamColumnNode]
    query_id: str


class ColumnUpstreamLineage(BaseModel):
    column_name: Optional[str]
    upstreams: List[ColumnUpstreamJob] = Field(default_factory=list)


class UpstreamTableNode(BaseModel):
    upstream_object_domain: str
    upstream_object_name: str
    query_id: str


class Query(BaseModel):
    query_id: str
    query_text: str
    start_time: str


class UpstreamLineageEdge(BaseModel):
    DOWNSTREAM_TABLE_NAME: str
    DOWNSTREAM_TABLE_DOMAIN: str
    UPSTREAM_TABLES: Optional[List[UpstreamTableNode]]
    UPSTREAM_COLUMNS: Optional[List[ColumnUpstreamLineage]]
    QUERIES: Optional[List[Query]]

    _json_upstream_tables = pydantic_parse_json("UPSTREAM_TABLES")
    _json_upstream_columns = pydantic_parse_json("UPSTREAM_COLUMNS")
    _json_queries = pydantic_parse_json("QUERIES")


@dataclass(frozen=True)
class SnowflakeColumnId:
    column_name: str
    object_name: str
    object_domain: Optional[str] = None


class SnowflakeLineageExtractor(SnowflakeCommonMixin, Closeable):
    """
    Extracts Lineage from Snowflake.
    Following lineage edges are considered.

    1. "Table to View" lineage via `snowflake.account_usage.object_dependencies` view + View definition SQL parsing.
    2. "S3 to Table" lineage via `show external tables` query and `snowflake.account_usage.copy_history view.
    3. "View to Table" and "Table to Table" lineage via `snowflake.account_usage.access_history` view (requires Snowflake Enterprise Edition or above)

    Edition Note - Snowflake Standard Edition does not have Access History Feature.
    So it does not support lineage extraction for point 3 edges mentioned above.
    """

    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        connection: SnowflakeConnection,
        filters: SnowflakeFilter,
        identifiers: SnowflakeIdentifierBuilder,
        redundant_run_skip_handler: Optional[RedundantLineageRunSkipHandler],
        sql_aggregator: SqlParsingAggregator,
    ) -> None:
        self.config = config
        self.report = report
        self.connection = connection
        self.filters = filters
        self.identifiers = identifiers
        self.redundant_run_skip_handler = redundant_run_skip_handler
        self.sql_aggregator = sql_aggregator

        self.start_time, self.end_time = (
            self.report.lineage_start_time,
            self.report.lineage_end_time,
        ) = self.get_time_window()

    def get_time_window(self) -> Tuple[datetime, datetime]:
        if self.redundant_run_skip_handler:
            return self.redundant_run_skip_handler.suggest_run_time_window(
                (
                    self.config.start_time
                    if not self.config.ignore_start_time_lineage
                    else ts_millis_to_datetime(0)
                ),
                self.config.end_time,
            )
        else:
            return (
                (
                    self.config.start_time
                    if not self.config.ignore_start_time_lineage
                    else ts_millis_to_datetime(0)
                ),
                self.config.end_time,
            )

    def add_time_based_lineage_to_aggregator(
        self,
        discovered_tables: List[str],
        discovered_views: List[str],
    ) -> None:
        if not self._should_ingest_lineage():
            return

        # s3 dataset -> snowflake table
        self._populate_external_upstreams(discovered_tables)

        # snowflake view/table -> snowflake table
        self.populate_table_upstreams(discovered_tables)

    def update_state(self):
        if self.redundant_run_skip_handler:
            # Update the checkpoint state for this run.
            self.redundant_run_skip_handler.update_state(
                (
                    self.config.start_time
                    if not self.config.ignore_start_time_lineage
                    else ts_millis_to_datetime(0)
                ),
                self.config.end_time,
            )

    def populate_table_upstreams(self, discovered_tables: List[str]) -> None:
        if self.report.edition == SnowflakeEdition.STANDARD:
            # TODO: use sql_aggregator.add_observed_query to report queries from
            # snowflake.account_usage.query_history and let Datahub generate lineage, usage and operations
            logger.info(
                "Snowflake Account is Standard Edition. Table to Table and View to Table Lineage Feature is not supported."
            )  # See Edition Note above for why
        else:
            with PerfTimer() as timer:
                results = self._fetch_upstream_lineages_for_tables()

                if not results:
                    return

                self.populate_known_query_lineage(discovered_tables, results)
                self.report.table_lineage_query_secs = timer.elapsed_seconds()
            logger.info(
                f"Upstream lineage detected for {self.report.num_tables_with_known_upstreams} tables.",
            )

    def populate_known_query_lineage(
        self,
        discovered_assets: Collection[str],
        results: Iterable[UpstreamLineageEdge],
    ) -> None:
        for db_row in results:
            dataset_name = self.identifiers.get_dataset_identifier_from_qualified_name(
                db_row.DOWNSTREAM_TABLE_NAME
            )
            if dataset_name not in discovered_assets or not db_row.QUERIES:
                continue

            for query in db_row.QUERIES:
                known_lineage = self.get_known_query_lineage(
                    query, dataset_name, db_row
                )
                if known_lineage and known_lineage.upstreams:
                    self.report.num_tables_with_known_upstreams += 1
                    self.sql_aggregator.add_known_query_lineage(known_lineage, True)
                else:
                    logger.debug(f"No lineage found for {dataset_name}")

    def get_known_query_lineage(
        self, query: Query, dataset_name: str, db_row: UpstreamLineageEdge
    ) -> Optional[KnownQueryLineageInfo]:
        if not db_row.UPSTREAM_TABLES:
            return None

        downstream_table_urn = self.identifiers.gen_dataset_urn(dataset_name)

        known_lineage = KnownQueryLineageInfo(
            query_id=get_query_fingerprint(
                query.query_text, self.identifiers.platform, fast=True
            ),
            query_text=query.query_text,
            downstream=downstream_table_urn,
            upstreams=self.map_query_result_upstreams(
                db_row.UPSTREAM_TABLES, query.query_id
            ),
            column_lineage=(
                self.map_query_result_fine_upstreams(
                    downstream_table_urn,
                    db_row.UPSTREAM_COLUMNS,
                    query.query_id,
                )
                if (self.config.include_column_lineage and db_row.UPSTREAM_COLUMNS)
                else None
            ),
            timestamp=parse_absolute_time(query.start_time),
        )

        return known_lineage

    def _populate_external_upstreams(self, discovered_tables: List[str]) -> None:
        with PerfTimer() as timer:
            self.report.num_external_table_edges_scanned = 0

            for entry in self._get_copy_history_lineage(discovered_tables):
                self.sql_aggregator.add(entry)
            logger.info("Done populating external lineage from copy history. ")

            self.report.external_lineage_queries_secs = timer.elapsed_seconds()

    # Handles the case where a table is populated from an external stage/s3 location via copy.
    # Eg: copy into category_english from @external_s3_stage;
    # Eg: copy into category_english from 's3://acryl-snow-demo-olist/olist_raw_data/category_english'credentials=(aws_key_id='...' aws_secret_key='...')  pattern='.*.csv';
    # NOTE: Snowflake does not log this information to the access_history table.
    def _get_copy_history_lineage(
        self, discovered_tables: List[str]
    ) -> Iterable[KnownLineageMapping]:
        query: str = SnowflakeQuery.copy_lineage_history(
            start_time_millis=int(self.start_time.timestamp() * 1000),
            end_time_millis=int(self.end_time.timestamp() * 1000),
            downstreams_deny_pattern=self.config.temporary_tables_pattern,
        )

        try:
            for db_row in self.connection.query(query):
                known_lineage_mapping = self._process_external_lineage_result_row(
                    db_row, discovered_tables, identifiers=self.identifiers
                )
                if known_lineage_mapping:
                    self.report.num_external_table_edges_scanned += 1
                    yield known_lineage_mapping
        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                error_msg = "Failed to get external lineage. Please grant imported privileges on SNOWFLAKE database. "
                self.warn_if_stateful_else_error(LINEAGE_PERMISSION_ERROR, error_msg)
            else:
                self.structured_reporter.warning(
                    "Error fetching external lineage from Snowflake",
                    exc=e,
                )
            self.report_status(EXTERNAL_LINEAGE, False)

    @classmethod
    def _process_external_lineage_result_row(
        cls,
        db_row: dict,
        discovered_tables: Optional[Collection[str]],
        identifiers: SnowflakeIdentifierBuilder,
    ) -> Optional[KnownLineageMapping]:
        # key is the down-stream table name
        key: str = identifiers.get_dataset_identifier_from_qualified_name(
            db_row["DOWNSTREAM_TABLE_NAME"]
        )
        if discovered_tables is not None and key not in discovered_tables:
            return None

        if db_row["UPSTREAM_LOCATIONS"] is not None:
            external_locations = json.loads(db_row["UPSTREAM_LOCATIONS"])

            loc: str
            for loc in external_locations:
                if loc.startswith("s3://"):
                    return KnownLineageMapping(
                        upstream_urn=make_s3_urn_for_lineage(
                            loc, identifiers.identifier_config.env
                        ),
                        downstream_urn=identifiers.gen_dataset_urn(key),
                    )

        return None

    def _fetch_upstream_lineages_for_tables(self) -> Iterable[UpstreamLineageEdge]:
        query: str = SnowflakeQuery.table_to_table_lineage_history_v2(
            start_time_millis=int(self.start_time.timestamp() * 1000),
            end_time_millis=int(self.end_time.timestamp() * 1000),
            upstreams_deny_pattern=self.config.temporary_tables_pattern,
            include_column_lineage=self.config.include_column_lineage,
        )
        try:
            for db_row in self.connection.query(query):
                edge = self._process_upstream_lineage_row(db_row)
                if edge:
                    yield edge
        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                error_msg = "Failed to get table/view to table lineage. Please grant imported privileges on SNOWFLAKE database. "
                self.warn_if_stateful_else_error(LINEAGE_PERMISSION_ERROR, error_msg)
            else:
                self.structured_reporter.warning(
                    "Failed to extract table/view -> table lineage from Snowflake",
                    exc=e,
                )
            self.report_status(TABLE_LINEAGE, False)

    def _process_upstream_lineage_row(
        self, db_row: dict
    ) -> Optional[UpstreamLineageEdge]:
        try:
            _queries = db_row.get("QUERIES")
            if _queries == "[\n  {}\n]":
                # We are creating an empty object in the list when there are no queries
                # To avoid that causing a pydantic error we are setting it to an empty list
                # instead of a list with an empty object
                db_row["QUERIES"] = "[]"

            # Handle dynamic tables migration - Check if the current table was moved
            # Snowflake access history might show the old location for dynamic tables
            downstream_table_name = db_row.get("DOWNSTREAM_TABLE_NAME")
            if (
                db_row.get("DOWNSTREAM_TABLE_DOMAIN", "").lower()
                == SnowflakeObjectDomain.DYNAMIC_TABLE.lower()
                and downstream_table_name is not None
            ):
                # Track dynamic table in our reporting
                self.report.num_dynamic_tables_with_known_upstreams += 1

                # Try to locate the actual table to ensure we have the correct identifier
                try:
                    # Split the qualified name to extract database and schema
                    parts = split_qualified_name(str(downstream_table_name))
                    if len(parts) == 3:
                        db_name, schema_name, table_name = parts
                        # Query to check if the table exists at the current location
                        check_query = f"SHOW DYNAMIC TABLES LIKE '{table_name}' IN {db_name}.{schema_name}"
                        result = self.connection.query(check_query)

                        # If the dynamic table is not found, it may have been moved
                        location_check_results = list(result)
                        if not location_check_results:
                            # Try to locate the dynamic table across the account
                            locate_query = (
                                f"SHOW DYNAMIC TABLES LIKE '{table_name}' IN ACCOUNT"
                            )
                            locate_result = self.connection.query(locate_query)

                            # If found, update the downstream table name to the new location
                            locate_rows = list(locate_result)
                            if locate_rows and len(locate_rows) > 0:
                                new_location = locate_rows[0]
                                new_db_name = new_location.get("database_name")
                                new_schema_name = new_location.get("schema_name")
                                if new_db_name and new_schema_name:
                                    new_downstream_name = (
                                        f"{new_db_name}.{new_schema_name}.{table_name}"
                                    )
                                    logger.info(
                                        f"Dynamic table downstream moved: {downstream_table_name} -> {new_downstream_name}"
                                    )
                                    # Report the change
                                    self.report.num_dynamic_table_location_changes += 1
                                    # Update the downstream table name in the db_row
                                    db_row["DOWNSTREAM_TABLE_NAME"] = (
                                        new_downstream_name
                                    )
                                    # Update local variable for further processing
                                    downstream_table_name = new_downstream_name
                                    # Update db_name and schema_name for refresh history query
                                    db_name, schema_name = new_db_name, new_schema_name

                        # Get refresh history to track refresh patterns
                        if self.config.include_table_lineage:
                            try:
                                refresh_query = f"SELECT REFRESH_HISTORY FROM {db_name}.INFORMATION_SCHEMA.DYNAMIC_TABLES WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema_name}'"
                                refresh_result = self.connection.query(refresh_query)
                                refresh_data = list(refresh_result)
                                if refresh_data and len(refresh_data) > 0:
                                    self.report.num_dynamic_table_refresh_tracked += 1
                                    # Store refresh history in a custom property if available
                                    if not db_row.get("DYNAMIC_TABLE_METADATA"):
                                        db_row["DYNAMIC_TABLE_METADATA"] = {}
                                    db_row["DYNAMIC_TABLE_METADATA"][
                                        "REFRESH_HISTORY"
                                    ] = refresh_data[0].get("REFRESH_HISTORY")
                            except Exception as e:
                                logger.debug(
                                    f"Error fetching dynamic table refresh history: {e}"
                                )

                        # If the dynamic table is not found, it may have been moved
                        if not list(result):
                            # Try to locate the dynamic table across the account
                            locate_query = (
                                f"SHOW DYNAMIC TABLES LIKE '{table_name}' IN ACCOUNT"
                            )
                            locate_result = self.connection.query(locate_query)

                            # If found, update the downstream table name to the new location
                            locate_rows = list(locate_result)
                            if locate_rows and len(locate_rows) > 0:
                                new_location = locate_rows[0]
                                new_db_name = new_location.get("database_name")
                                new_schema_name = new_location.get("schema_name")
                                if new_db_name and new_schema_name:
                                    new_downstream_name = (
                                        f"{new_db_name}.{new_schema_name}.{table_name}"
                                    )
                                    logger.info(
                                        f"Dynamic table moved: {downstream_table_name} -> {new_downstream_name}"
                                    )
                                    self.report.num_dynamic_table_location_changes += 1
                                    db_row["DOWNSTREAM_TABLE_NAME"] = (
                                        new_downstream_name
                                    )
                except Exception as e:
                    # Log but continue with original name if there's an error
                    logger.debug(f"Error checking dynamic table location: {e}")

            return UpstreamLineageEdge.parse_obj(db_row)
        except Exception as e:
            self.report.num_upstream_lineage_edge_parsing_failed += 1
            upstream_tables = db_row.get("UPSTREAM_TABLES")
            downstream_table = db_row.get("DOWNSTREAM_TABLE_NAME")
            self.structured_reporter.warning(
                "Failed to parse lineage edge",
                # Tricky: sometimes the full row data is too large, and so the context
                # message gets truncated. By pulling out the upstreams and downstream
                # list, we can at least get the important fields if truncation does occur.
                context=f"Upstreams: {upstream_tables} Downstream: {downstream_table} Full row: {db_row}",
                exc=e,
            )
            return None

    def map_query_result_upstreams(
        self, upstream_tables: Optional[List[UpstreamTableNode]], query_id: str
    ) -> List[UrnStr]:
        """We need to make dataset URNs for each upstream."""
        if not upstream_tables:
            return []

        upstream_tables_set = []
        for upstream_table in upstream_tables:
            # Check if this is the right query
            if not upstream_table or upstream_table.query_id != query_id:
                continue

            # No blank upstream names.
            if not upstream_table.upstream_object_name:
                continue

            # Validate that this object is of the right domain.
            if upstream_table.upstream_object_domain.lower() not in {
                SnowflakeObjectDomain.TABLE.lower(),
                SnowflakeObjectDomain.VIEW.lower(),
                SnowflakeObjectDomain.EXTERNAL_TABLE.lower(),
                SnowflakeObjectDomain.MATERIALIZED_VIEW.lower(),
                SnowflakeObjectDomain.ICEBERG_TABLE.lower(),
                SnowflakeObjectDomain.STREAM.lower(),
                SnowflakeObjectDomain.DYNAMIC_TABLE.lower(),
            }:
                # Only add upstream edges for table <-> {table, view, materialized_view, external_table, iceberg_table, stream} objects.
                continue

            # Need additional handling for dynamic tables that could have moved
            original_upstream_name = upstream_table.upstream_object_name
            if (
                upstream_table.upstream_object_domain.lower()
                == SnowflakeObjectDomain.DYNAMIC_TABLE.lower()
            ):
                try:
                    # Split the qualified name to extract database, schema, and table name
                    parts = split_qualified_name(upstream_table.upstream_object_name)
                    if len(parts) == 3:
                        db_name, schema_name, table_name = parts

                        # Check if we can get additional info about the dynamic table's dependencies
                        try:
                            dependency_query = f"SELECT TARGET_LAG, TARGET_LAG_TYPE, WAREHOUSE, SCHEDULE, QUERY_TEXT FROM {db_name}.INFORMATION_SCHEMA.DYNAMIC_TABLES WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema_name}'"
                            result = self.connection.query(dependency_query)
                            dependency_data = list(result)
                            if dependency_data and len(dependency_data) > 0:
                                logger.info(
                                    f"Found additional dynamic table dependency info for {upstream_table.upstream_object_name}"
                                )
                                # We could use this info to create more detailed lineage or properties
                                # This data could be added to the graph through custom properties
                                if self.sql_aggregator:
                                    # Store dependency info for the dynamic table
                                    dependency_info = dependency_data[0]
                                    if (
                                        "QUERY_TEXT" in dependency_info
                                        and dependency_info["QUERY_TEXT"]
                                    ):
                                        # We can potentially parse the query to extract more upstreams
                                        # This would show what tables the dynamic table depends on
                                        dataset_identifier = self.identifiers.get_dataset_identifier_from_qualified_name(
                                            upstream_table.upstream_object_name
                                        )
                                        if dataset_identifier:
                                            dt_urn = self.identifiers.gen_dataset_urn(
                                                dataset_identifier
                                            )
                                            self.sql_aggregator.add_view_definition(
                                                view_urn=dt_urn,
                                                view_definition=dependency_info[
                                                    "QUERY_TEXT"
                                                ],
                                                default_db=db_name,
                                                default_schema=schema_name,
                                            )
                        except Exception as e:
                            logger.debug(
                                f"Error fetching dynamic table dependency info: {e}"
                            )

                        # Check if the dynamic table exists at the specified location
                        check_query = f"SHOW DYNAMIC TABLES LIKE '{table_name}' IN {db_name}.{schema_name}"
                        result = self.connection.query(check_query)

                        # If the dynamic table is not found, it may have been moved
                        if not list(result):
                            # Try to locate the dynamic table across the account
                            locate_query = (
                                f"SHOW DYNAMIC TABLES LIKE '{table_name}' IN ACCOUNT"
                            )
                            locate_result = self.connection.query(locate_query)

                            # If found, update the upstream table name to the new location
                            locate_rows = list(locate_result)
                            if locate_rows and len(locate_rows) > 0:
                                new_location = locate_rows[0]
                                new_db_name = new_location.get("database_name")
                                new_schema_name = new_location.get("schema_name")
                                if new_db_name and new_schema_name:
                                    new_upstream_name = (
                                        f"{new_db_name}.{new_schema_name}.{table_name}"
                                    )
                                    logger.info(
                                        f"Dynamic table upstream moved: {upstream_table.upstream_object_name} -> {new_upstream_name}"
                                    )
                                    self.report.num_dynamic_table_location_changes += 1
                                    upstream_table.upstream_object_name = (
                                        new_upstream_name
                                    )
                except Exception as e:
                    # Log but continue with original name if there's an error
                    logger.debug(f"Error checking dynamic table upstream location: {e}")

            dataset_identifier = (
                self.identifiers.get_dataset_identifier_from_qualified_name(
                    original_upstream_name
                    if upstream_table.upstream_object_domain.lower()
                    != SnowflakeObjectDomain.DYNAMIC_TABLE.lower()
                    else upstream_table.upstream_object_name
                )
            )
            if dataset_identifier:
                if (
                    not self.config.validate_upstreams_against_patterns
                    or self.filters.is_dataset_pattern_allowed(
                        dataset_identifier,
                        upstream_table.upstream_object_domain,
                    )
                ):
                    upstream_dataset_urn = self.identifiers.gen_dataset_urn(
                        dataset_identifier
                    )
                    # Only unique upstreams.
                    if upstream_dataset_urn not in upstream_tables_set:
                        upstream_tables_set.append(upstream_dataset_urn)

        return upstream_tables_set

    def map_query_result_fine_upstreams(
        self,
        dataset_urn: str,
        column_wise_upstreams: Optional[List[ColumnUpstreamLineage]],
        query_id: str,
    ) -> List[ColumnLineageInfo]:
        if not column_wise_upstreams:
            return []
        fine_upstreams: List[ColumnLineageInfo] = []
        for column_with_upstreams in column_wise_upstreams:
            if column_with_upstreams:
                try:
                    self._process_add_single_column_upstream(
                        dataset_urn, fine_upstreams, column_with_upstreams, query_id
                    )
                except Exception as e:
                    logger.debug(e, exc_info=e)
        return fine_upstreams

    def _process_add_single_column_upstream(
        self,
        dataset_urn: str,
        fine_upstreams: List[ColumnLineageInfo],
        column_with_upstreams: ColumnUpstreamLineage,
        query_id: str,
    ) -> None:
        column_name = column_with_upstreams.column_name
        upstream_jobs = column_with_upstreams.upstreams
        if column_name and upstream_jobs:
            for upstream_job in upstream_jobs:
                if not upstream_job or upstream_job.query_id != query_id:
                    continue
                fine_upstream = self.build_finegrained_lineage(
                    dataset_urn=dataset_urn,
                    col=column_name,
                    upstream_columns={
                        SnowflakeColumnId(
                            column_name=col.column_name,
                            object_name=col.object_name,
                            object_domain=col.object_domain,
                        )
                        for col in upstream_job.column_upstreams
                    },
                )
                if not fine_upstream:
                    continue
                fine_upstreams.append(fine_upstream)

    def build_finegrained_lineage(
        self,
        dataset_urn: str,
        col: str,
        upstream_columns: Set[SnowflakeColumnId],
    ) -> Optional[ColumnLineageInfo]:
        column_upstreams = self.build_finegrained_lineage_upstreams(upstream_columns)
        if not column_upstreams:
            return None
        column_lineage = ColumnLineageInfo(
            downstream=DownstreamColumnRef(
                table=dataset_urn, column=self.identifiers.snowflake_identifier(col)
            ),
            upstreams=sorted(column_upstreams),
        )

        return column_lineage

    def build_finegrained_lineage_upstreams(
        self, upstream_columms: Set[SnowflakeColumnId]
    ) -> List[ColumnRef]:
        column_upstreams = []
        for upstream_col in upstream_columms:
            if (
                upstream_col.object_name
                and upstream_col.column_name
                and (
                    not self.config.validate_upstreams_against_patterns
                    or self.filters.is_dataset_pattern_allowed(
                        upstream_col.object_name,
                        upstream_col.object_domain,
                    )
                )
            ):
                upstream_dataset_name = (
                    self.identifiers.get_dataset_identifier_from_qualified_name(
                        upstream_col.object_name
                    )
                )
                column_upstreams.append(
                    ColumnRef(
                        table=self.identifiers.gen_dataset_urn(upstream_dataset_name),
                        column=self.identifiers.snowflake_identifier(
                            upstream_col.column_name
                        ),
                    )
                )
        return column_upstreams

    def get_external_upstreams(self, external_lineage: Set[str]) -> List[UpstreamClass]:
        external_upstreams = []
        for external_lineage_entry in sorted(external_lineage):
            # For now, populate only for S3
            if external_lineage_entry.startswith("s3://"):
                external_upstream_table = UpstreamClass(
                    dataset=make_s3_urn_for_lineage(
                        external_lineage_entry, self.config.env
                    ),
                    type=DatasetLineageTypeClass.COPY,
                )
                external_upstreams.append(external_upstream_table)
        return external_upstreams

    def _should_ingest_lineage(self) -> bool:
        if (
            self.redundant_run_skip_handler
            and self.redundant_run_skip_handler.should_skip_this_run(
                cur_start_time=(
                    self.config.start_time
                    if not self.config.ignore_start_time_lineage
                    else ts_millis_to_datetime(0)
                ),
                cur_end_time=self.config.end_time,
            )
        ):
            # Skip this run
            self.report.report_warning(
                "lineage-extraction",
                "Skip this run as there was already a run for current ingestion window.",
            )
            return False
        return True

    def report_status(self, step: str, status: bool) -> None:
        if self.redundant_run_skip_handler:
            self.redundant_run_skip_handler.report_current_run_status(step, status)

    def close(self) -> None:
        pass
