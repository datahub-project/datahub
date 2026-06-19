import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Iterable, List, Optional, Set

from databricks.sdk.service.sql import QueryStatementType

from datahub.ingestion.api.source_helpers import (
    auto_empty_dataset_usage_statistics,
    auto_workunit,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import Query, TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing._models import _TableName
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    PreparsedQuery,
    SqlParsingAggregator,
    UrnStr,
)
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.sql_parsing.sqlglot_utils import get_query_fingerprint

logger = logging.getLogger(__name__)

_STATEMENT_TYPE_TO_QUERY_TYPE = {
    QueryStatementType.SELECT: QueryType.SELECT,
    QueryStatementType.INSERT: QueryType.INSERT,
    QueryStatementType.COPY: QueryType.INSERT,
    QueryStatementType.UPDATE: QueryType.UPDATE,
    QueryStatementType.MERGE: QueryType.MERGE,
    QueryStatementType.DELETE: QueryType.DELETE,
    QueryStatementType.TRUNCATE: QueryType.DELETE,
    QueryStatementType.CREATE: QueryType.CREATE_TABLE_AS_SELECT,
    QueryStatementType.REPLACE: QueryType.CREATE_TABLE_AS_SELECT,
}


@dataclass(eq=False)
class UnityCatalogUsageExtractor:
    config: UnityCatalogSourceConfig
    report: UnityCatalogReport
    proxy: UnityCatalogApiProxy
    table_urn_builder: Callable[[TableReference], str]
    user_urn_builder: Callable[[str], str]
    schema_resolver: SchemaResolver
    platform: str = "databricks"

    def _use_system_tables_join(self) -> bool:
        return self.config.usage_uses_system_tables(self.proxy.warehouse_id)

    def _build_aggregator(
        self,
        is_allowed_table: Optional[Callable[[str], bool]] = None,
    ) -> SqlParsingAggregator:
        # UnityCatalogSourceConfig extends BaseUsageConfig so self.config satisfies
        # the usage_config parameter type.
        return SqlParsingAggregator(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            schema_resolver=self.schema_resolver,
            generate_lineage=False,  # lineage stays on system.access
            generate_queries=self.config.include_queries,
            generate_query_usage_statistics=self.config.include_query_usage_statistics,
            generate_usage_statistics=True,
            generate_operations=self.config.include_operational_stats,
            usage_config=self.config,
            format_queries=False,
            is_allowed_table=is_allowed_table,
        )

    def _fetch_queries(self) -> Iterable[Query]:
        include_ops = self.config.include_operational_stats
        if self._use_system_tables_join():
            catalog_pattern = (
                self.config.catalog_pattern
                if self.config.push_down_database_pattern_access_history
                else None
            )
            return self.proxy.get_query_history_via_system_tables(
                self.config.start_time,
                self.config.end_time,
                catalog_pattern=catalog_pattern,
                include_operational_stats=include_ops,
            )
        return self.proxy.query_history(
            self.config.start_time,
            self.config.end_time,
            include_operational_stats=include_ops,
        )

    @staticmethod
    def _normalize_timestamp(ts: Optional[datetime]) -> Optional[datetime]:
        if ts is None:
            return None
        if ts.tzinfo is not None:
            return ts.astimezone(timezone.utc)
        return ts.replace(tzinfo=timezone.utc)

    def _user_urn(self, query: Query) -> Optional[CorpUserUrn]:
        if not query.user_name:
            return None
        return CorpUserUrn.from_string(self.user_urn_builder(query.user_name))

    def _full_name_to_urn(self, full_name: str) -> Optional[UrnStr]:
        parts = full_name.split(".")
        if len(parts) != 3:
            logger.debug("Skipping unexpected table full name: %s", full_name)
            self.report.num_lineage_tables_unresolvable += 1
            return None
        catalog, schema, table = parts
        urn, _ = self.schema_resolver.resolve_table(
            _TableName(database=catalog, db_schema=schema, table=table)
        )
        if urn is None:
            logger.debug(
                "Could not resolve lineage table name to URN: %s",
                full_name,
            )
            self.report.num_lineage_tables_unresolvable += 1
        return urn

    def _resolve_table_urns(self, full_names: Iterable[str]) -> List[UrnStr]:
        urns: List[UrnStr] = []
        seen: Set[UrnStr] = set()
        for full_name in full_names:
            urn = self._full_name_to_urn(full_name)
            if urn and urn not in seen:
                seen.add(urn)
                urns.append(urn)
        return urns

    @staticmethod
    def _query_type(statement_type: Optional[QueryStatementType]) -> QueryType:
        if statement_type is None:
            return QueryType.UNKNOWN
        return _STATEMENT_TYPE_TO_QUERY_TYPE.get(statement_type, QueryType.UNKNOWN)

    def _can_use_preparsed_query(self, query: Query) -> bool:
        return self._use_system_tables_join() and query.has_system_table_lineage

    @staticmethod
    def _statement_type_label(query: Query) -> str:
        if query.statement_type is None:
            return "unknown"
        return str(query.statement_type.value)

    @staticmethod
    def _query_preview(query: Query, max_len: int = 120) -> str:
        text = (query.query_text or "").replace("\n", " ").strip()
        if len(text) <= max_len:
            return text
        return f"{text[:max_len]}..."

    def _log_usage_routing_summary(self) -> None:
        total = self.report.num_queries
        if not self._use_system_tables_join():
            logger.debug(
                "Unity usage routing summary (warehouse API path): "
                "queries=%s sqlglot=%s",
                total,
                self.report.num_queries_observed_sqlglot,
            )
            return

        preparsed = self.report.num_queries_preparsed_from_lineage
        without_lineage = self.report.num_queries_without_system_table_lineage
        fallback = self.report.num_queries_preparsed_fallback_to_sqlglot
        preparsed_pct = round(100 * preparsed / total, 1) if total else 0.0
        logger.debug(
            "Unity usage routing summary (system-table join): "
            "total=%s preparsed=%s (%.1f%%) "
            "sqlglot_no_lineage=%s sqlglot_urn_fallback=%s sqlglot_total=%s "
            "unresolvable_lineage_tables=%s",
            total,
            preparsed,
            preparsed_pct,
            without_lineage,
            fallback,
            self.report.num_queries_observed_sqlglot,
            self.report.num_lineage_tables_unresolvable,
        )

    def _to_preparsed_queries(self, query: Query) -> List[PreparsedQuery]:
        upstreams = self._resolve_table_urns(query.source_table_full_names)
        targets = self._resolve_table_urns(query.target_table_full_names)
        ts = self._normalize_timestamp(query.start_time)
        user = self._user_urn(query)
        query_type = self._query_type(query.statement_type)
        query_id = get_query_fingerprint(query.query_text, self.platform, fast=True)

        if not targets:
            return [
                PreparsedQuery(
                    query_id=query_id,
                    query_text=query.query_text,
                    upstreams=upstreams,
                    downstream=None,
                    confidence_score=1.0,
                    query_count=1,
                    user=user,
                    timestamp=ts,
                    query_type=query_type,
                )
            ]

        preparsed: List[PreparsedQuery] = []
        for i, downstream in enumerate(targets):
            secondary_id = downstream if len(targets) > 1 else None
            preparsed.append(
                PreparsedQuery(
                    query_id=get_query_fingerprint(
                        query.query_text,
                        self.platform,
                        fast=True,
                        secondary_id=secondary_id,
                    ),
                    query_text=query.query_text,
                    upstreams=upstreams,
                    downstream=downstream,
                    confidence_score=1.0,
                    query_count=1 if i == 0 else 0,
                    user=user,
                    timestamp=ts,
                    query_type=query_type,
                )
            )
        return preparsed

    def _add_query_to_aggregator(
        self,
        aggregator: SqlParsingAggregator,
        query: Query,
        default_db: Optional[str],
    ) -> None:
        if self._can_use_preparsed_query(query):
            preparsed_queries = self._to_preparsed_queries(query)
            if preparsed_queries and (
                any(p.upstreams for p in preparsed_queries)
                or any(p.downstream for p in preparsed_queries)
            ):
                for preparsed in preparsed_queries:
                    aggregator.add_preparsed_query(preparsed)
                self.report.num_queries_preparsed_from_lineage += 1
                logger.debug(
                    "Usage query routed to preparsed lineage path "
                    "(statement_id=%s statement_type=%s "
                    "lineage_sources=%s lineage_targets=%s "
                    "resolved_upstream_urns=%s resolved_downstream_urns=%s)",
                    query.query_id,
                    self._statement_type_label(query),
                    query.source_table_full_names,
                    query.target_table_full_names,
                    sum(len(p.upstreams) for p in preparsed_queries),
                    sum(1 for p in preparsed_queries if p.downstream),
                )
                return

            self.report.num_queries_preparsed_fallback_to_sqlglot += 1
            logger.debug(
                "Usage query fell back to sqlglot: system-table lineage present but "
                "no resolvable dataset URNs "
                "(statement_id=%s statement_type=%s "
                "lineage_sources=%s lineage_targets=%s preview=%r)",
                query.query_id,
                self._statement_type_label(query),
                query.source_table_full_names,
                query.target_table_full_names,
                self._query_preview(query),
            )
        elif self._use_system_tables_join():
            self.report.num_queries_without_system_table_lineage += 1
            logger.debug(
                "Usage query routed to sqlglot: no system.access.table_lineage rows "
                "for statement_id in configured time window "
                "(statement_id=%s statement_type=%s preview=%r)",
                query.query_id,
                self._statement_type_label(query),
                self._query_preview(query),
            )

        aggregator.add_observed_query(
            ObservedQuery(
                query=query.query_text,
                timestamp=self._normalize_timestamp(query.start_time),
                user=self._user_urn(query),
                default_db=default_db,
                default_schema=None,
            )
        )
        self.report.num_queries_observed_sqlglot += 1

    def _report_usage_lineage_warnings(self) -> None:
        if self.report.num_queries_preparsed_fallback_to_sqlglot > 0:
            self.report.report_warning(
                title="System-table lineage fell back to SQL parsing",
                message=(
                    f"{self.report.num_queries_preparsed_fallback_to_sqlglot} "
                    "queries had table lineage from system tables but no resolvable "
                    "dataset URNs; those queries were parsed with sqlglot instead."
                ),
            )
        if self.report.num_lineage_tables_unresolvable > 0:
            self.report.report_warning(
                title="Unresolvable lineage table names",
                message=(
                    f"{self.report.num_lineage_tables_unresolvable} table name(s) "
                    "from system.access.table_lineage could not be mapped to dataset "
                    "URNs and were omitted from preparsed usage."
                ),
            )

    def get_usage_workunits(
        self, table_refs: Set[TableReference]
    ) -> Iterable[MetadataWorkUnit]:
        # Restrict emission to tables this recipe ingested, matching the old behavior.
        # The aggregator's _name_from_urn strips the platform_instance prefix from the URN,
        # yielding a bare "catalog.schema.table" name that it passes to the predicate.
        # TableReference.qualified_table_name returns the same 3-part bare form, so we use
        # it directly — using DatasetUrn.name here would include the platform_instance prefix
        # when one is configured, causing a mismatch that filters out all usage.
        allowed_names = {ref.qualified_table_name.lower() for ref in table_refs}
        is_allowed_table: Optional[Callable[[str], bool]] = (
            (lambda name: name.lower() in allowed_names) if allowed_names else None
        )

        # Databricks query history has no per-query session catalog/schema (unlike
        # Snowflake), so we can't derive a per-query default_db.  When the recipe
        # ingests a single catalog we use it as the default, recovering 2-part
        # "schema.table" references (BigQuery-style).  Multi-catalog recipes get
        # None because there is no unambiguous default.  1-part bare table names
        # still cannot be resolved (no default schema is available).
        catalogs = {ref.catalog for ref in table_refs}
        default_db: Optional[str] = next(iter(catalogs)) if len(catalogs) == 1 else None

        # Snapshot before the feed loop so we can detect fetch failures that occurred
        # during iteration (proxy increments this counter inside _execute_sql_query_streaming).
        fetch_failures_before = self.report.num_usage_query_fetch_failures

        aggregator: Optional[SqlParsingAggregator] = None
        try:
            try:
                aggregator = self._build_aggregator(is_allowed_table=is_allowed_table)
                for query in self._fetch_queries():
                    self.report.num_queries += 1
                    try:
                        self._add_query_to_aggregator(
                            aggregator, query, default_db=default_db
                        )
                    except (
                        MemoryError,
                        SystemExit,
                        KeyboardInterrupt,
                    ):  # never swallow system-level errors as a "dropped query"
                        raise
                    except Exception as per_query_exc:
                        self.report.num_queries_dropped += 1
                        logger.warning(
                            "Skipping query due to error during usage processing "
                            "(query_id=%s): %r",
                            query.query_id,
                            per_query_exc,
                            exc_info=True,
                        )
                        self.report.report_warning(
                            title="Skipped query during usage extraction",
                            message="A query from query history could not be processed and was skipped, so its usage is not counted.",
                            context=f"query_id={query.query_id}",
                            exc=per_query_exc,
                        )
                self._log_usage_routing_summary()
                self._report_usage_lineage_warnings()
            except (MemoryError, SystemExit, KeyboardInterrupt):
                raise
            except Exception as e:
                logger.error("Error processing usage", exc_info=True)
                self.report.report_failure(
                    title="Usage extraction failed",
                    message=f"Usage extraction failed: {e!r}",
                    exc=e,
                )
                return

            fetch_failed = (
                self.report.num_usage_query_fetch_failures > fetch_failures_before
            )
            if fetch_failed:
                # Surface a query-history fetch failure as a run failure — covers both
                # the zero-rows case and a mid-stream failure that yielded partial data.
                self.report.report_failure(
                    title="Failed to fetch query history",
                    message="Could not fully read query history from system tables; usage statistics may be incomplete or missing. See the related SQL query failure warning for the underlying error.",
                )
            if self.report.num_queries == 0:
                if not fetch_failed:
                    if self._use_system_tables_join():
                        hint = (
                            "verify SELECT privilege on system.query and system.access "
                            "and that the time window covers recent activity"
                        )
                        if (
                            self.config.push_down_database_pattern_access_history
                            and self.config.catalog_pattern is not None
                        ):
                            hint += (
                                "; if catalog pushdown is enabled, also verify "
                                "catalog_pattern allow/deny rules are not over-restrictive"
                            )
                    else:
                        hint = (
                            "verify CAN_MANAGE privilege on the SQL warehouse "
                            "and that the time window covers recent activity"
                        )
                    self.report.report_warning(
                        title="No queries found for usage",
                        message=f"No queries were found in the configured time range. {hint}.",
                    )
                # Skip resetting per-table usage when we couldn't read any queries at
                # all (empty history or missing permission).  Emitting zero-usage aspects
                # in that case would wrongly wipe existing usage stats in DataHub.
                return

            yield from auto_empty_dataset_usage_statistics(
                auto_workunit(aggregator.gen_metadata()),
                dataset_urns={self.table_urn_builder(ref) for ref in table_refs},
                config=self.config,
            )
        finally:
            if aggregator is not None:
                try:
                    aggregator.close()
                except (
                    Exception
                ) as close_exc:  # surface close failures in the report, not just logs
                    logger.warning(
                        "Failed to close SqlParsingAggregator", exc_info=True
                    )
                    self.report.report_warning(
                        title="Failed to close usage aggregator",
                        message="The usage aggregator failed to close cleanly; its temporary resources may not have been released.",
                        exc=close_exc,
                    )
