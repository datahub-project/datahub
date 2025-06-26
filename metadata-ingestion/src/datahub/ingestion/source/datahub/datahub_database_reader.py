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
        LIMIT %(limit)s
        OFFSET %(offset)s
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

    def _get_rows(
        self,
        start_date: datetime,
        end_date: datetime,
        set_structured_properties_filter: bool,
        limit: int,
    ) -> Iterable[Dict[str, Any]]:
        """
        Retrieves data rows within a specified date range using pagination.

        Implements a hybrid pagination strategy that switches between time-based and
        offset-based approaches depending on the returned data. Uses server-side
        cursors for efficient memory usage.

        Note: May return duplicate rows across batch boundaries when multiple rows
        share the same 'createdon' timestamp. This is expected behavior when
        transitioning between pagination methods.

        Args:
        start_date: Beginning of date range (inclusive)
        end_date: End of date range (exclusive)
        set_structured_properties_filter: Whether to apply structured filtering
        limit: Maximum rows to fetch per query

        Returns:
            An iterable of database rows as dictionaries
        """
        offset = 0
        last_createdon = None
        first_iteration = True

        while True:
            try:
                # Set up query and parameters - using named parameters
                query = self.query(set_structured_properties_filter)
                params: Dict[str, Any] = {
                    "since_createdon": start_date.strftime(DATETIME_FORMAT),
                    "end_createdon": end_date.strftime(DATETIME_FORMAT),
                    "limit": limit,
                    "offset": offset,
                }

                # Add exclude_aspects if needed
                if (
                    hasattr(self.config, "exclude_aspects")
                    and self.config.exclude_aspects
                ):
                    params["exclude_aspects"] = tuple(self.config.exclude_aspects)

                logger.info(
                    f"Querying data from {start_date.strftime(DATETIME_FORMAT)} to {end_date.strftime(DATETIME_FORMAT)} "
                    f"with limit {limit} and offset {offset} (inclusive range)"
                )

                # Execute query with server-side cursor
                rows = self.execute_server_cursor(query, params)
                # Process and yield rows
                rows_processed = 0
                for row in rows:
                    if first_iteration:
                        start_date = row.get("createdon", start_date)
                        first_iteration = False

                    last_createdon = row.get("createdon")
                    rows_processed += 1
                    yield row

                # If we processed fewer than the limit or no last_createdon, we're done
                if rows_processed < limit or not last_createdon:
                    break

                # Update parameters for next iteration
                if start_date != last_createdon:
                    start_date = last_createdon
                    offset = 0
                else:
                    offset += limit

                logger.info(
                    f"Processed {rows_processed} rows for date range {start_date} to {end_date}. Continuing to next batch."
                )

            except Exception as e:
                logger.error(
                    f"Error processing date range {start_date} to {end_date}: {str(e)}"
                )
                # Re-raise the exception after logging
                raise

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
            start_date=from_createdon,
            end_date=stop_time,
            set_structured_properties_filter=set_structured_properties_filter,
            limit=self.config.database_query_batch_size,
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
        try:
            params: Dict = {}

            logger.debug("Fetching soft-deleted URNs")

            # Use server-side cursor implementation
            rows = self.execute_server_cursor(self.soft_deleted_urns_query, params)
            processed_rows = 0
            # Process and yield rows
            for row in rows:
                processed_rows += 1
                yield row

            logger.debug(f"Fetched batch of {processed_rows} soft-deleted URNs")

        except Exception:
            logger.exception("Error fetching soft-deleted row", exc_info=True)
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
