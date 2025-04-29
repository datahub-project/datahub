import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, Generic, Iterable, List, Optional, Tuple, TypeVar

from sqlalchemy import create_engine, text

from datahub.emitter.aspect import ASPECT_MAP
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.serialization_helper import post_json_transform
from datahub.ingestion.source.datahub.config import DataHubSourceConfig
from datahub.ingestion.source.datahub.report import DataHubSourceReport
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConnectionConfig
from datahub.metadata.schema_classes import ChangeTypeClass, SystemMetadataClass
from datahub.utilities.lossy_collections import LossyDict, LossyList

logger = logging.getLogger(__name__)

# Should work for at least mysql, mariadb, postgres
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
DATE_FORMAT = "%Y-%m-%d"

ROW = TypeVar("ROW", bound=Dict[str, Any])


class VersionOrderer(Generic[ROW]):
    """Orders rows by (createdon, version == 0).

    That is, orders rows first by createdon, and for equal timestamps, puts version 0 rows last.
    """

    def __init__(self, enabled: bool):
        # Stores all version 0 aspects for a given createdon timestamp
        # Once we have emitted all aspects for a given timestamp, we can emit the version 0 aspects
        # Guaranteeing that, for a given timestamp, we always ingest version 0 aspects last
        self.queue: Optional[Tuple[datetime, List[ROW]]] = None
        self.enabled = enabled

    def __call__(self, rows: Iterable[ROW]) -> Iterable[ROW]:
        for row in rows:
            yield from self._process_row(row)
        yield from self._flush_queue()

    def _process_row(self, row: ROW) -> Iterable[ROW]:
        if not self.enabled:
            yield row
            return

        yield from self._attempt_queue_flush(row)
        if row["version"] == 0:
            self._add_to_queue(row)
        else:
            yield row

    def _add_to_queue(self, row: ROW) -> None:
        if self.queue is None:
            self.queue = (row["createdon"], [row])
        else:
            self.queue[1].append(row)

    def _attempt_queue_flush(self, row: ROW) -> Iterable[ROW]:
        if self.queue is None:
            return

        if row["createdon"] > self.queue[0]:
            yield from self._flush_queue()

    def _flush_queue(self) -> Iterable[ROW]:
        if self.queue is not None:
            yield from self.queue[1]
            self.queue = None


class DataHubDatabaseReader:
    def __init__(
        self,
        config: DataHubSourceConfig,
        connection_config: SQLAlchemyConnectionConfig,
        report: DataHubSourceReport,
    ):
        self.config = config
        self.report = report
        self.engine = create_engine(
            url=connection_config.get_sql_alchemy_url(),
            **connection_config.options,
        )

        # Cache for available dates to avoid redundant queries
        self.available_dates_cache: Optional[List[datetime]] = None

    def get_available_dates_query(
        self, from_createdon: datetime, stop_time: datetime
    ) -> str:
        """
        Query to get all available dates that have data in the specified time range.
        """
        return f"""
            SELECT 
                DISTINCT DATE(mav.createdon) as created_date 
            FROM {self.engine.dialect.identifier_preparer.quote(self.config.database_table_name)} as mav
            WHERE 1 = 1
                {"" if not self.config.exclude_aspects else "AND mav.aspect NOT IN %(exclude_aspects)s"}
                AND mav.createdon >= %(since_createdon)s 
                AND mav.createdon < %(end_createdon)s
            ORDER BY created_date ASC
        """

    @property
    def soft_deleted_urns_query(self) -> str:
        return f"""
            SELECT DISTINCT mav.urn
            FROM {self.engine.dialect.identifier_preparer.quote(self.config.database_table_name)} as mav
            JOIN (
                SELECT *,
                JSON_EXTRACT(metadata, '$.removed') as removed
                FROM {self.engine.dialect.identifier_preparer.quote(self.config.database_table_name)}
                WHERE aspect = "status" AND version = 0
            ) as sd ON sd.urn = mav.urn
            WHERE sd.removed = true
            ORDER BY mav.urn
            LIMIT %(limit)s OFFSET %(offset)s
        """

    def query(self, set_structured_properties_filter: bool) -> str:
        """
        Main query that gets data for specified date range with appropriate filters.
        """
        structured_prop_filter = f" AND urn {'' if set_structured_properties_filter else 'NOT'} like 'urn:li:structuredProperty:%%'"

        return f"""
        SELECT *
        FROM (
            SELECT
                mav.urn,
                mav.aspect,
                mav.metadata,
                mav.systemmetadata,
                mav.createdon,
                mav.version,
                removed
            FROM {self.engine.dialect.identifier_preparer.quote(self.config.database_table_name)} as mav
            LEFT JOIN (
                SELECT
                    *,
                    JSON_EXTRACT(metadata, '$.removed') as removed
                FROM {self.engine.dialect.identifier_preparer.quote(self.config.database_table_name)}
                WHERE aspect = 'status'
                AND version = 0
            ) as sd ON sd.urn = mav.urn
            WHERE 1 = 1
                {"" if self.config.include_all_versions else "AND mav.version = 0"}
                {"" if not self.config.exclude_aspects else "AND mav.aspect NOT IN %(exclude_aspects)s"}
                AND mav.createdon >= %(since_createdon)s
                AND mav.createdon < %(end_createdon)s
            ORDER BY
                createdon,
                urn,
                aspect,
                version
        ) as t
        WHERE 1=1
            {"" if self.config.include_soft_deleted_entities else " AND (removed = false or removed is NULL)"}
            {structured_prop_filter}
        ORDER BY
            createdon,
            urn,
            aspect,
            version
        """

    def execute_with_params(
        self, query: str, params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute query with proper parameter binding that works with your database"""
        with self.engine.connect() as conn:
            result = conn.execute(query, params or {})
            return [dict(row) for row in result.fetchall()]

    def execute_server_cursor(
        self, query: str, params: Dict[str, Any]
    ) -> Iterable[Dict[str, Any]]:
        """Execute a query with server-side cursor"""
        with self.engine.connect() as conn:
            if self.engine.dialect.name in ["postgresql", "mysql", "mariadb"]:
                with (
                    conn.begin()
                ):  # Transaction required for PostgreSQL server-side cursor
                    # Set query timeout at the connection level
                    if self.config.query_timeout:
                        if self.engine.dialect.name == "postgresql":
                            conn.execute(
                                text(
                                    f"SET statement_timeout = {self.config.query_timeout * 1000}"
                                )
                            )  # milliseconds
                        elif self.engine.dialect.name in ["mysql", "mariadb"]:
                            conn.execute(
                                text(
                                    f"SET max_execution_time = {self.config.query_timeout * 1000}"
                                )
                            )  # milliseconds

                    # Stream results with batch size
                    conn = conn.execution_options(
                        stream_results=True,
                        yield_per=self.config.database_query_batch_size,
                    )

                    # Execute query - using native parameterization without text()
                    # to maintain compatibility with your original code
                    result = conn.execute(query, params)
                    for row in result:
                        yield dict(row)
                    return  # Success, exit the retry loop
            else:
                raise ValueError(f"Unsupported dialect: {self.engine.dialect.name}")

    def get_available_dates(
        self, from_createdon: datetime, stop_time: datetime
    ) -> List[datetime]:
        """Get all dates that have data within the date range"""
        if self.available_dates_cache:
            return self.available_dates_cache
        try:
            logger.info(
                f"Fetching available dates from {from_createdon} to {stop_time}"
            )

            # Prepare parameters using named parameters
            params: Dict[str, Any] = {
                "since_createdon": from_createdon.strftime(DATETIME_FORMAT),
                "end_createdon": stop_time.strftime(DATETIME_FORMAT),
            }

            if hasattr(self.config, "exclude_aspects") and self.config.exclude_aspects:
                params["exclude_aspects"] = tuple(self.config.exclude_aspects)

            # Execute the query using the appropriate method
            result = self.execute_with_params(
                self.get_available_dates_query(from_createdon, stop_time), params
            )

            # Extract dates from result
            available_dates = []
            for row in result:
                if row.get("created_date"):
                    # For MySQL, the result may be a date object
                    if isinstance(row["created_date"], datetime):
                        date_obj = (
                            row["created_date"]
                            if row["created_date"] > from_createdon
                            else from_createdon
                        )

                    # Otherwise it might be a string
                    else:
                        date_str = str(row["created_date"])
                        date_obj = datetime.strptime(date_str, DATE_FORMAT)

                    available_dates.append(date_obj)

            if available_dates:
                available_dates.append(stop_time)

            logger.info(f"Found {len(available_dates)} dates with data in the range")
            self.available_dates_cache = available_dates
            return available_dates

        except Exception as e:
            logger.error(f"Error getting available dates: {str(e)}")
            # If the query fails, return an empty list
            return []

    def get_date_batches(
        self, dates: List[datetime]
    ) -> Iterable[Tuple[datetime, datetime]]:
        """
        Group consecutive dates into batches based on the days_per_query config.
        Returns a list of (start_date, end_date) tuples, where end_date is exclusive.
        """
        i = 0

        end_date: datetime = datetime.min

        while i < len(dates):
            end_idx = min(i + self.config.days_per_query, len(dates))
            if end_idx - 1 > i:  # Ensure we have at least two items to create a range
                start_date = dates[i]
                end_date = dates[end_idx - 1]
                yield (start_date, end_date)
            else:
                # If there's only one date left, yield it with itself
                yield (end_date, dates[i])
            i += self.config.days_per_query

    def _get_rows_for_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        set_structured_properties_filter: bool,
    ) -> Iterable[Dict[str, Any]]:
        """Get rows for a specific date range"""
        try:
            # Set up query and parameters - using named parameters
            query = self.query(set_structured_properties_filter)
            params: Dict[str, Any] = {
                "since_createdon": start_date.strftime(DATETIME_FORMAT),
                "end_createdon": end_date.strftime(DATETIME_FORMAT),
            }

            # Add exclude_aspects if needed
            if hasattr(self.config, "exclude_aspects") and self.config.exclude_aspects:
                params["exclude_aspects"] = tuple(self.config.exclude_aspects)

            logger.info(
                f"Querying data from {start_date.strftime(DATETIME_FORMAT)} to {end_date.strftime(DATETIME_FORMAT)} "
                f"(inclusive range)"
            )

            rows_processed = 0
            # Execute query with server-side cursor
            for row in self.execute_server_cursor(query, params):
                rows_processed += 1
                yield row

            logger.info(f"Processed {rows_processed} rows for date range")

        except Exception as e:
            logger.error(
                f"Error processing date range {start_date} to {end_date}: {str(e)}"
            )
            # Re-raise the exception after logging
            raise

    def _get_rows(
        self,
        from_createdon: datetime,
        stop_time: datetime,
        set_structured_properties_filter: bool = False,
    ) -> Iterable[Dict[str, Any]]:
        """Get rows by first identifying dates with data, then querying those dates in batches"""
        # Get all dates that have data
        available_dates = self.get_available_dates(from_createdon, stop_time)

        if not available_dates:
            logger.info(f"No data available between {from_createdon} and {stop_time}")
            return

        # Group available dates into batches
        if set_structured_properties_filter:
            date_batches = [(from_createdon, stop_time)]
        else:
            date_batches = list(self.get_date_batches(available_dates))

        logger.info(
            f"Processing {len(date_batches)} date batches with {self.config.days_per_query} days per query"
        )

        # Process each batch
        for i, (batch_start, batch_end) in enumerate(date_batches):
            logger.info(
                f"Processing batch {i + 1}/{len(date_batches)}: "
                f"{batch_start.strftime(DATETIME_FORMAT)} to {(batch_end).strftime(DATETIME_FORMAT)}"
            )

            # Query and yield rows for this date batch
            yield from self._get_rows_for_date_range(
                batch_start,
                batch_end,
                set_structured_properties_filter,
            )

    def get_all_aspects(
        self, from_createdon: datetime, stop_time: datetime
    ) -> Iterable[Tuple[MetadataChangeProposalWrapper, datetime]]:
        logger.info("Fetching Structured properties aspects")
        yield from self.get_aspects(
            from_createdon=from_createdon,
            stop_time=stop_time,
            set_structured_properties_filter=True,
        )

        logger.info(
            f"Waiting for {self.config.structured_properties_template_cache_invalidation_interval} seconds for structured properties cache to invalidate"
        )

        time.sleep(
            self.config.structured_properties_template_cache_invalidation_interval
        )

        logger.info("Fetching aspects")
        yield from self.get_aspects(
            from_createdon=from_createdon,
            stop_time=stop_time,
            set_structured_properties_filter=False,
        )

    def get_aspects(
        self,
        from_createdon: datetime,
        stop_time: datetime,
        set_structured_properties_filter: bool = False,
    ) -> Iterable[Tuple[MetadataChangeProposalWrapper, datetime]]:
        orderer = VersionOrderer[Dict[str, Any]](
            enabled=self.config.include_all_versions
        )
        rows = self._get_rows(
            from_createdon=from_createdon,
            stop_time=stop_time,
            set_structured_properties_filter=set_structured_properties_filter,
        )
        for row in orderer(rows):
            mcp = self._parse_row(row)
            if mcp:
                yield mcp, row["createdon"]

    def get_soft_deleted_rows(self) -> Iterable[Dict[str, Any]]:
        """
        Fetches all soft-deleted entities from the database using pagination.

        Yields:
            Row objects containing URNs of soft-deleted entities
        """
        offset = 0
        batch_size = self.config.database_query_batch_size

        while True:
            try:
                # Use named parameters consistent with your original code
                params = {"limit": batch_size, "offset": offset}

                with self.engine.connect() as conn:
                    # Execute without text() to maintain compatibility with your code
                    result = conn.execute(self.soft_deleted_urns_query, params)
                    rows = result.fetchall()

                    if not rows:
                        break  # No more rows to fetch

                    column_names = result.keys()
                    for row in rows:
                        yield dict(zip(column_names, row))

                    # Move to next batch
                    offset += batch_size

                    logger.debug(
                        f"Fetched batch of soft-deleted URNs (offset: {offset - batch_size})"
                    )

            except Exception as e:
                logger.error(
                    f"Error fetching soft-deleted rows (offset {offset}): {str(e)}"
                )
                # Error handling with more details
                if offset > 0:
                    logger.info("Returning results collected so far...")
                    break
                else:
                    raise

    def _parse_row(
        self, row: Dict[str, Any]
    ) -> Optional[MetadataChangeProposalWrapper]:
        try:
            json_aspect = post_json_transform(json.loads(row["metadata"]))
            json_metadata = post_json_transform(
                json.loads(row["systemmetadata"] or "{}")
            )
            system_metadata = SystemMetadataClass.from_obj(json_metadata)
            return MetadataChangeProposalWrapper(
                entityUrn=row["urn"],
                aspect=ASPECT_MAP[row["aspect"]].from_obj(json_aspect),
                systemMetadata=system_metadata,
                changeType=ChangeTypeClass.UPSERT,
            )
        except Exception as e:
            logger.warning(
                f"Failed to parse metadata for {row['urn']}: {e}", exc_info=True
            )
            self.report.num_database_parse_errors += 1
            self.report.database_parse_errors.setdefault(
                str(e), LossyDict()
            ).setdefault(row["aspect"], LossyList()).append(row["urn"])
            return None
