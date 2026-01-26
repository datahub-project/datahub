"""Semantic View Usage Extraction for Snowflake.

This module extracts usage statistics and query entities for Snowflake Semantic Views.
It queries QUERY_HISTORY (not ACCESS_HISTORY) because semantic views do NOT appear
in ACCESS_HISTORY's DIRECT_OBJECTS_ACCESSED. Pattern matching on SEMANTIC_VIEW()
function calls in query_text is used to detect semantic view usage.

Emits:
- DatasetUsageStatistics: Usage metrics per time bucket
- DatasetProfile: Column/dimension/fact/metric counts for Stats tab
- Query entities: Individual queries for Queries tab
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set

from datahub.emitter.mce_builder import make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import SnowflakeConnection
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SemanticViewProfileCounts,
    SemanticViewQuery,
    SemanticViewUsageRecord,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetProfile,
    DatasetUsageStatistics,
    DatasetUserUsageCounts,
)
from datahub.metadata.com.linkedin.pegasus2avro.query import (
    QueryLanguage,
    QueryProperties,
    QuerySource,
    QueryStatement,
    QuerySubject,
    QuerySubjects,
)
from datahub.metadata.com.linkedin.pegasus2avro.timeseries import TimeWindowSize
from datahub.metadata.schema_classes import AuditStampClass
from datahub.metadata.urns import QueryUrn

logger: logging.Logger = logging.getLogger(__name__)


class SemanticViewUsageExtractor:
    """Extracts usage statistics and query entities for Snowflake Semantic Views."""

    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        connection: SnowflakeConnection,
        identifiers: SnowflakeIdentifierBuilder,
    ) -> None:
        self.config = config
        self.report = report
        self.connection = connection
        self.identifiers = identifiers

    def get_semantic_view_usage_workunits(
        self,
        discovered_semantic_views: Set[str],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Extract usage statistics for semantic views.

        Args:
            discovered_semantic_views: Set of discovered semantic view identifiers
                                       (e.g., "db.schema.view_name")

        Yields:
            MetadataWorkUnit for DatasetUsageStatistics
        """
        if not self.config.semantic_views.include_usage:
            return

        if not discovered_semantic_views:
            logger.info("No semantic views discovered, skipping usage extraction")
            return

        logger.info(
            f"Extracting usage statistics for {len(discovered_semantic_views)} semantic views"
        )

        try:
            start_time_millis = int(self.config.start_time.timestamp() * 1000)
            end_time_millis = int(self.config.end_time.timestamp() * 1000)

            results = self.connection.query(
                SnowflakeQuery.semantic_view_usage_statistics(
                    start_time_millis=start_time_millis,
                    end_time_millis=end_time_millis,
                    time_bucket_size=self.config.bucket_duration,
                )
            )

            usage_records = self._parse_usage_results(results)
            logger.info(f"Found {len(usage_records)} usage records for semantic views")

            for record in usage_records:
                # Normalize the semantic view name to match discovered datasets
                normalized_name = self._normalize_semantic_view_name(
                    record.semantic_view_name
                )
                if normalized_name not in discovered_semantic_views:
                    logger.debug(
                        f"Skipping usage for {record.semantic_view_name} - not in discovered semantic views"
                    )
                    continue

                wu = self._build_usage_statistics_workunit(record, normalized_name)
                if wu:
                    yield wu

        except Exception as e:
            logger.warning(f"Failed to extract semantic view usage: {e}", exc_info=True)
            self.report.warning(
                "semantic-view-usage",
                f"Failed to extract semantic view usage statistics: {e}",
            )

    def _parse_usage_results(
        self, results: Iterable[Dict[str, Any]]
    ) -> List[SemanticViewUsageRecord]:
        """Parse query results into SemanticViewUsageRecord objects."""
        records = []
        for row in results:
            user_counts_raw = row.get("USER_COUNTS")
            user_counts = []
            if user_counts_raw:
                try:
                    if isinstance(user_counts_raw, str):
                        user_counts = json.loads(user_counts_raw)
                    else:
                        user_counts = list(user_counts_raw)
                except (json.JSONDecodeError, TypeError) as e:
                    logger.debug(f"Failed to parse user_counts: {e}")

            record = SemanticViewUsageRecord(
                semantic_view_name=row["SEMANTIC_VIEW_NAME"],
                bucket_start_time=row["BUCKET_START_TIME"].astimezone(tz=timezone.utc),
                total_queries=row["TOTAL_QUERIES"],
                unique_users=row["UNIQUE_USERS"],
                direct_sql_queries=row["DIRECT_SQL_QUERIES"],
                cortex_analyst_queries=row["CORTEX_ANALYST_QUERIES"],
                avg_execution_time_ms=row["AVG_EXECUTION_TIME_MS"] or 0.0,
                total_rows_produced=row["TOTAL_ROWS_PRODUCED"] or 0,
                user_counts=user_counts,
            )
            records.append(record)
        return records

    def _normalize_semantic_view_name(self, name: str) -> str:
        """Normalize semantic view name to lowercase for matching."""
        return name.lower()

    def _build_usage_statistics_workunit(
        self, record: SemanticViewUsageRecord, dataset_identifier: str
    ) -> Optional[MetadataWorkUnit]:
        """Build a DatasetUsageStatistics workunit for a semantic view."""
        try:
            user_counts = self._map_user_counts(record.user_counts)

            stats = DatasetUsageStatistics(
                timestampMillis=int(record.bucket_start_time.timestamp() * 1000),
                eventGranularity=TimeWindowSize(
                    unit=self.config.bucket_duration, multiple=1
                ),
                totalSqlQueries=record.total_queries,
                uniqueUserCount=record.unique_users,
                userCounts=user_counts,
                # Store query source breakdown in custom properties via fieldCounts
                # (we don't have actual field counts for semantic views)
            )

            dataset_urn = self.identifiers.gen_dataset_urn(dataset_identifier)
            return MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=stats,
            ).as_workunit()

        except Exception as e:
            logger.debug(
                f"Failed to build usage statistics for {dataset_identifier}: {e}"
            )
            return None

    def _map_user_counts(
        self, user_counts: List[Dict[str, Any]]
    ) -> List[DatasetUserUsageCounts]:
        """Map user counts to DatasetUserUsageCounts."""
        result = []
        for user_count in user_counts:
            user_name = user_count.get("user_name")
            if not user_name:
                continue

            # Generate email if email_domain is configured
            user_email = None
            if self.config.email_domain:
                user_email = f"{user_name}@{self.config.email_domain}".lower()

            result.append(
                DatasetUserUsageCounts(
                    user=make_user_urn(
                        self.identifiers.get_user_identifier(user_name, user_email)
                    ),
                    count=user_count.get("query_count", 0),
                    userEmail=user_email,
                )
            )
        return sorted(result, key=lambda v: v.user)

    def get_semantic_view_profile_workunits(
        self,
        db_name: str,
        discovered_semantic_views: Set[str],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Extract profile (stats) for semantic views.

        This populates the Stats tab with dimension/fact/metric counts.

        Args:
            db_name: Database name to query
            discovered_semantic_views: Set of discovered semantic view identifiers

        Yields:
            MetadataWorkUnit for DatasetProfile
        """
        if not self.config.semantic_views.emit_profile:
            return

        if not discovered_semantic_views:
            return

        logger.info(f"Extracting profile counts for semantic views in {db_name}")

        try:
            results = self.connection.query(
                SnowflakeQuery.semantic_view_profile_counts(db_name)
            )

            for row in results:
                profile_counts = SemanticViewProfileCounts(
                    semantic_view_catalog=row["SEMANTIC_VIEW_CATALOG"],
                    semantic_view_schema=row["SEMANTIC_VIEW_SCHEMA"],
                    semantic_view_name=row["SEMANTIC_VIEW_NAME"],
                    dimension_count=row["DIMENSION_COUNT"],
                    fact_count=row["FACT_COUNT"],
                    metric_count=row["METRIC_COUNT"],
                    table_count=row["TABLE_COUNT"],
                    total_column_count=row["TOTAL_COLUMN_COUNT"],
                )

                dataset_identifier = f"{profile_counts.semantic_view_catalog}.{profile_counts.semantic_view_schema}.{profile_counts.semantic_view_name}".lower()

                if dataset_identifier not in discovered_semantic_views:
                    continue

                wu = self._build_profile_workunit(profile_counts, dataset_identifier)
                if wu:
                    yield wu

        except Exception as e:
            logger.warning(
                f"Failed to extract semantic view profiles for {db_name}: {e}",
                exc_info=True,
            )

    def _build_profile_workunit(
        self, counts: SemanticViewProfileCounts, dataset_identifier: str
    ) -> Optional[MetadataWorkUnit]:
        """Build a DatasetProfile workunit for a semantic view."""
        try:
            profile = DatasetProfile(
                timestampMillis=int(datetime.now(timezone.utc).timestamp() * 1000),
                columnCount=counts.total_column_count,
                # Use custom properties to store semantic view specific counts
                # since DatasetProfile doesn't have fields for dimension/fact/metric
            )

            dataset_urn = self.identifiers.gen_dataset_urn(dataset_identifier)
            return MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=profile,
            ).as_workunit()

        except Exception as e:
            logger.debug(f"Failed to build profile for {dataset_identifier}: {e}")
            return None

    def get_semantic_view_query_workunits(
        self,
        discovered_semantic_views: Set[str],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Extract queries against semantic views.

        This populates the Queries tab with Query entities.

        Args:
            discovered_semantic_views: Set of discovered semantic view identifiers

        Yields:
            MetadataWorkUnit for Query entities
        """
        if not self.config.semantic_views.emit_query_entities:
            return

        if not discovered_semantic_views:
            return

        logger.info("Extracting queries for semantic views")

        try:
            start_time_millis = int(self.config.start_time.timestamp() * 1000)
            end_time_millis = int(self.config.end_time.timestamp() * 1000)

            results = self.connection.query(
                SnowflakeQuery.semantic_view_queries(
                    start_time_millis=start_time_millis,
                    end_time_millis=end_time_millis,
                    max_queries=self.config.semantic_views.max_queries_per_view
                    * len(discovered_semantic_views),
                )
            )

            # Group queries by semantic view to respect max_queries_per_view
            queries_by_view: Dict[str, List[SemanticViewQuery]] = {}

            for row in results:
                query = SemanticViewQuery(
                    query_id=row["QUERY_ID"],
                    query_text=row["QUERY_TEXT"],
                    semantic_view_name=row["SEMANTIC_VIEW_NAME"],
                    user_name=row["USER_NAME"],
                    role_name=row["ROLE_NAME"],
                    warehouse_name=row["WAREHOUSE_NAME"],
                    start_time=row["START_TIME"].astimezone(tz=timezone.utc),
                    total_elapsed_time=row["TOTAL_ELAPSED_TIME"],
                    rows_produced=row["ROWS_PRODUCED"] or 0,
                    query_source=row["QUERY_SOURCE"],
                )

                normalized_name = self._normalize_semantic_view_name(
                    query.semantic_view_name
                )
                if normalized_name not in discovered_semantic_views:
                    continue

                if normalized_name not in queries_by_view:
                    queries_by_view[normalized_name] = []
                queries_by_view[normalized_name].append(query)

            # Emit query entities, respecting max_queries_per_view
            for view_name, queries in queries_by_view.items():
                limited_queries = queries[
                    : self.config.semantic_views.max_queries_per_view
                ]
                logger.debug(
                    f"Emitting {len(limited_queries)} query entities for {view_name}"
                )

                for query in limited_queries:
                    yield from self._build_query_workunits(query, view_name)

        except Exception as e:
            logger.warning(
                f"Failed to extract semantic view queries: {e}", exc_info=True
            )

    def _build_query_workunits(
        self, query: SemanticViewQuery, dataset_identifier: str
    ) -> Iterable[MetadataWorkUnit]:
        """Build Query entity workunits for a query."""
        query_urn = QueryUrn(query.query_id).urn()
        dataset_urn = self.identifiers.gen_dataset_urn(dataset_identifier)

        # Generate user email if email_domain is configured
        user_email = None
        if self.config.email_domain and query.user_name:
            user_email = f"{query.user_name}@{self.config.email_domain}".lower()

        user_urn = make_user_urn(
            self.identifiers.get_user_identifier(query.user_name, user_email)
        )

        timestamp_millis = int(query.start_time.timestamp() * 1000)

        # 1. QueryProperties
        description = (
            "Cortex Analyst generated query"
            if query.query_source == "CORTEX_ANALYST"
            else "Direct SQL query against semantic view"
        )
        query_properties = QueryProperties(
            statement=QueryStatement(
                value=query.query_text,
                language=QueryLanguage.SQL,
            ),
            source=QuerySource.SYSTEM,
            name=self._generate_query_name(query.query_text),
            description=description,
            created=AuditStampClass(time=timestamp_millis, actor=user_urn),
            lastModified=AuditStampClass(time=timestamp_millis, actor=user_urn),
            customProperties={
                "query_source": query.query_source,
                "query_id": query.query_id,
                "warehouse": query.warehouse_name or "",
                "role_name": query.role_name or "",
                "execution_time_ms": str(query.total_elapsed_time),
                "rows_produced": str(query.rows_produced),
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=query_urn,
            aspect=query_properties,
        ).as_workunit()

        # 2. QuerySubjects - link query to semantic view
        query_subjects = QuerySubjects(subjects=[QuerySubject(entity=dataset_urn)])

        yield MetadataChangeProposalWrapper(
            entityUrn=query_urn,
            aspect=query_subjects,
        ).as_workunit()

    def _generate_query_name(self, query_text: str, max_length: int = 50) -> str:
        """Generate a readable name from SQL query text."""
        import re

        # Extract semantic view name from SEMANTIC_VIEW(db.schema.name ...) pattern
        match = re.search(r"SEMANTIC_VIEW\s*\(\s*([\w\.]+)", query_text, re.IGNORECASE)
        if match:
            view_name = match.group(1).split(".")[-1]  # Get last part
            name = f"Query on {view_name}"
            return name[:max_length] if len(name) > max_length else name

        # Fallback: truncate query text
        if len(query_text) > max_length:
            return query_text[: max_length - 3] + "..."
        return query_text
