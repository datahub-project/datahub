import logging
import pathlib
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional, Set, TypeVar

from databricks.sdk.service.sql import QueryStatementType

from datahub.ingestion.api.source_helpers import (
    auto_empty_dataset_usage_statistics,
    auto_workunit,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.identifier_helper import split_databricks_identifier
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import Query, TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.ingestion.source.usage.usage_common import normalize_timestamp_to_utc
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    PreparsedQuery,
    SqlParsingAggregator,
    UrnStr,
)
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.sql_parsing.sqlglot_utils import get_query_fingerprint
from datahub.utilities.file_backed_collections import ConnectionWrapper, FileBackedList

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# Bump whenever a buffered Query changes in a way that makes an older cache file unsafe
# to reload, so the cache resolves to a different filename — a clean miss that re-fetches.
# This is the ONLY guard against an incompatible cache: FileBackedList pickles Query, and
# pickle does not validate against the current class, so bump on ANY of:
#   - field added/renamed/removed (a renamed/removed field deserializes to a default or a
#     stray attribute rather than erroring),
#   - a field's type or meaning changing (the bytes still load but mean something else —
#     a read-back check cannot detect this, only a version bump can).
_AUDIT_LOG_FORMAT_VERSION = 1

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

    @staticmethod
    def _require(value: Optional[_T], message: str) -> _T:
        # Explicit invariant check that survives `python -O`, which strips `assert`.
        if value is None:
            raise AssertionError(message)
        return value

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

    def _audit_log_filename(self) -> str:
        # Key the cache file by format version, usage source, and time window so a file
        # left behind by a crashed run is only reused for an identical window+format,
        # never a stale window or an incompatible serialization.
        mode = "systables" if self._use_system_tables_join() else "api"
        start = int(self.config.start_time.timestamp())
        end = int(self.config.end_time.timestamp())
        return (
            f"unity_usage_audit_log_v{_AUDIT_LOG_FORMAT_VERSION}"
            f"_{mode}_{start}_{end}.sqlite"
        )

    def _fetch_queries(self) -> Iterable[Query]:
        include_ops = self.config.include_operational_stats
        if self._use_system_tables_join():
            catalog_pattern = (
                self.config.catalog_pattern
                if (
                    self.config.push_down_database_pattern_access_history
                    and not self.config.include_column_usage_stats
                )
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

    def _user_urn(self, query: Query) -> Optional[CorpUserUrn]:
        if not query.user_name:
            return None
        return CorpUserUrn.from_string(self.user_urn_builder(query.user_name))

    def _full_name_to_urn(self, full_name: str) -> Optional[UrnStr]:
        parts = split_databricks_identifier(full_name)
        if parts is None or len(parts) != 3:
            logger.debug("Skipping unexpected table full name: %s", full_name)
            self.report.num_lineage_tables_unresolvable += 1
            self.report.lineage_tables_unresolvable_sample.append(full_name)
            return None
        catalog, schema, table = parts
        # resolve_table_parts always returns a (synthesized) URN; the SchemaInfo is
        # the resolution signal. It is None when the table is not in the schema
        # resolver cache, i.e. not one this recipe ingested. Treating those as
        # unresolvable keeps preparsed usage scoped to known datasets and lets the
        # caller fall back to sqlglot instead of emitting confident lineage to a
        # possibly-nonexistent URN.
        urn, schema_info = self.schema_resolver.resolve_table_parts(
            database=catalog, db_schema=schema, table=table
        )
        if schema_info is None:
            logger.debug(
                "Could not resolve lineage table name to a known dataset: %s",
                full_name,
            )
            self.report.num_lineage_tables_unresolvable += 1
            self.report.lineage_tables_unresolvable_sample.append(full_name)
            return None
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

        if self.config.include_column_usage_stats:
            logger.info(
                "Unity usage routing summary (system-table join, column usage stats): "
                "total=%s sqlglot=%s (include_column_usage_stats forces full sqlglot "
                "parsing)",
                total,
                self.report.num_queries_observed_sqlglot,
            )
            return

        preparsed = self.report.num_queries_preparsed_from_lineage
        without_lineage = self.report.num_queries_without_system_table_lineage
        skipped_no_lineage = (
            self.report.num_queries_skipped_without_system_table_lineage
        )
        fallback = self.report.num_queries_preparsed_fallback_to_sqlglot
        preparsed_pct = round(100 * preparsed / total, 1) if total else 0.0
        logger.info(
            "Unity usage routing summary (system-table join): "
            "total=%s preparsed=%s (%.1f%%) "
            "sqlglot_no_lineage=%s skipped_no_system_table_lineage=%s "
            "sqlglot_urn_fallback=%s sqlglot_total=%s unresolvable_lineage_tables=%s",
            total,
            preparsed,
            preparsed_pct,
            without_lineage,
            skipped_no_lineage,
            fallback,
            self.report.num_queries_observed_sqlglot,
            self.report.num_lineage_tables_unresolvable,
        )

    def _query_fingerprint(
        self, query: Query, secondary_id: Optional[str] = None
    ) -> str:
        """Fingerprint a query, falling back to its statement_id on parse error.

        get_query_fingerprint runs sqlglot tokenization, which can still raise on
        malformed SQL even in fast mode. On the system-tables path the lineage is
        already resolved, so a fingerprinting hiccup must not drop the query — fall
        back to a deterministic id derived from the (always-present) statement_id.
        """
        try:
            return get_query_fingerprint(
                query.query_text,
                self.platform,
                fast=True,
                secondary_id=secondary_id,
            )
        except Exception as e:
            self.report.num_queries_preparsed_fingerprint_fallback += 1
            logger.debug(
                "Falling back to statement_id for query fingerprint "
                "(statement_id=%s): %r",
                query.query_id,
                e,
            )
            base = f"unity-stmt-{query.query_id}"
            return f"{base}-{secondary_id}" if secondary_id else base

    def _to_preparsed_queries(self, query: Query) -> List[PreparsedQuery]:
        upstreams = self._resolve_table_urns(query.source_table_full_names)
        targets = self._resolve_table_urns(query.target_table_full_names)
        ts = normalize_timestamp_to_utc(query.start_time)
        user = self._user_urn(query)
        query_type = self._query_type(query.statement_type)

        if not targets:
            return [
                PreparsedQuery(
                    query_id=self._query_fingerprint(query),
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
                    query_id=self._query_fingerprint(query, secondary_id=secondary_id),
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

    def _add_observed_query(
        self,
        aggregator: SqlParsingAggregator,
        query: Query,
        default_db: Optional[str],
    ) -> None:
        aggregator.add_observed_query(
            ObservedQuery(
                query=query.query_text,
                timestamp=normalize_timestamp_to_utc(query.start_time),
                user=self._user_urn(query),
                default_db=default_db,
                default_schema=None,
            )
        )
        self.report.num_queries_observed_sqlglot += 1

    def _add_query_to_aggregator(
        self,
        aggregator: SqlParsingAggregator,
        query: Query,
        default_db: Optional[str],
    ) -> None:
        if self.config.include_column_usage_stats:
            self._add_observed_query(aggregator, query, default_db)
            return

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
            if self.config.skip_sqlglot_when_system_table_lineage_missing:
                self.report.num_queries_skipped_without_system_table_lineage += 1
                logger.debug(
                    "Usage query skipped: no system.access.table_lineage rows "
                    "for statement_id in configured time window "
                    "(statement_id=%s statement_type=%s preview=%r)",
                    query.query_id,
                    self._statement_type_label(query),
                    self._query_preview(query),
                )
                return

            self.report.num_queries_without_system_table_lineage += 1
            logger.debug(
                "Usage query routed to sqlglot: no system.access.table_lineage rows "
                "for statement_id in configured time window "
                "(statement_id=%s statement_type=%s preview=%r)",
                query.query_id,
                self._statement_type_label(query),
                self._query_preview(query),
            )

        self._add_observed_query(aggregator, query, default_db)

    def _report_usage_lineage_warnings(self) -> None:
        # Uniform count-only warnings, emitted only when their counter is non-zero.
        for count, title, message in (
            (
                self.report.num_queries_without_system_table_lineage,
                "Queries missing system-table lineage",
                "Queries had no matching rows in system.access.table_lineage and "
                "were parsed with sqlglot instead.",
            ),
            (
                self.report.num_queries_skipped_without_system_table_lineage,
                "Queries skipped without system-table lineage",
                "Queries had no matching rows in system.access.table_lineage and "
                "were skipped because "
                "skip_sqlglot_when_system_table_lineage_missing is enabled.",
            ),
            (
                self.report.num_queries_preparsed_fallback_to_sqlglot,
                "System-table lineage fell back to SQL parsing",
                "Queries had table lineage from system tables but no resolvable "
                "dataset URNs; those queries were parsed with sqlglot instead.",
            ),
        ):
            if count > 0:
                self.report.warning(
                    title=title,
                    message=message,
                    context=f"count={count}",
                    log=False,
                )

        # Handled separately: it appends sample table names to the context.
        if self.report.num_lineage_tables_unresolvable > 0:
            sample = list(self.report.lineage_tables_unresolvable_sample)
            context = f"count={self.report.num_lineage_tables_unresolvable}"
            if sample:
                context += f"; examples={', '.join(sample[:3])}"
            self.report.warning(
                title="Unresolvable lineage table names",
                message=(
                    "Table names from system.access.table_lineage could not be mapped "
                    "to dataset URNs and were omitted from preparsed usage."
                ),
                context=context,
                log=False,
            )

    def _report_no_queries(self) -> None:
        # Zero usable queries: distinguish unparseable rows from an empty read,
        # with a path-specific permission hint.
        if self.report.num_queries_missing_info > 0:
            self.report.warning(
                title="Query history rows could not be parsed",
                message=(
                    "Statements from system.query.history could not be "
                    "parsed and were skipped, so no usage was extracted."
                ),
                context=f"count={self.report.num_queries_missing_info}",
                log=False,
            )
            return

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
        self.report.warning(
            title="No queries found for usage",
            message=(
                "No queries were found in the configured time range "
                "for usage extraction."
            ),
            context=hint,
            log=False,
        )

    def _parse_buffered_queries(
        self,
        aggregator: SqlParsingAggregator,
        buffered_queries: FileBackedList[Query],
        default_db: Optional[str],
    ) -> None:
        # Iterate by index so a single unreadable row (e.g. a corrupt cached entry) can
        # be skipped without aborting the rest — a generator closes on the first raise.
        # buffered_queries[i] deserializes the row, which happens before the per-query
        # try below, so reading it needs its own guard.
        #
        # Per-row failures use report.warning(log=False): the structured report groups
        # them by title (bounded sample + count) WITHOUT emitting a log line per row, so
        # a pathological run (e.g. every query unparseable) can't flood the logs. The
        # full traceback stays available at DEBUG.
        for i in range(len(buffered_queries)):
            try:
                query = buffered_queries[i]
            except (MemoryError, SystemExit, KeyboardInterrupt):
                raise
            except Exception as read_exc:
                self.report.num_queries_dropped += 1
                logger.debug(
                    "Skipping buffered query that could not be read back (index=%s)",
                    i,
                    exc_info=True,
                )
                self.report.warning(
                    title="Skipped unreadable buffered query",
                    message="A buffered query could not be read back from the audit-log "
                    "buffer and was skipped, so its usage is not counted.",
                    context=f"index={i}",
                    exc=read_exc,
                    log=False,
                )
                continue
            try:
                self._add_query_to_aggregator(aggregator, query, default_db=default_db)
            except (
                MemoryError,
                SystemExit,
                KeyboardInterrupt,
            ):  # never swallow system-level errors as a "dropped query"
                raise
            except Exception as per_query_exc:
                self.report.num_queries_dropped += 1
                logger.debug(
                    "Skipping query due to error during usage processing (query_id=%s)",
                    query.query_id,
                    exc_info=True,
                )
                self.report.warning(
                    title="Skipped query during usage extraction",
                    message="A query from query history could not be processed and was skipped, so its usage is not counted.",
                    context=f"query_id={query.query_id}",
                    exc=per_query_exc,
                    log=False,
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
        shared_connection: Optional[ConnectionWrapper] = None
        # Drain query history to a disk-backed buffer before parsing: holding the
        # warehouse cursor open across the slow sqlglot parse risks the server-side
        # operation handle being evicted (non-retryable RESOURCE_DOES_NOT_EXIST after
        # ~20-30 min). See https://github.com/databricks/databricks-sql-python/pull/785.
        # With local_temp_path set, the buffer is a window-keyed SQLite file reused
        # across runs (use_cached_audit_log); otherwise it is ephemeral and self-cleaning.
        audit_log_file: Optional[pathlib.Path] = None
        if isinstance(self.config.local_temp_path, pathlib.Path):
            audit_log_file = self.config.local_temp_path / self._audit_log_filename()
        use_cached_audit_log = audit_log_file is not None and audit_log_file.exists()

        buffered_queries: Optional[FileBackedList[Query]] = None
        try:
            try:
                aggregator = self._build_aggregator(is_allowed_table=is_allowed_table)
                if use_cached_audit_log:
                    # use_cached_audit_log is only True when audit_log_file is set.
                    audit_log_file = self._require(
                        audit_log_file,
                        "audit_log_file unexpectedly None with use_cached_audit_log",
                    )
                    try:
                        logger.info(
                            "Using cached query-history audit log at %s", audit_log_file
                        )
                        shared_connection = ConnectionWrapper(audit_log_file)
                        buffered_queries = FileBackedList(shared_connection)
                    except (MemoryError, SystemExit, KeyboardInterrupt):
                        raise
                    except Exception as cache_exc:
                        # The persisted cache could not be opened (corrupt, or written by
                        # an incompatible build despite the version-keyed name). Discard
                        # it and re-fetch rather than failing the run.
                        if buffered_queries is not None:
                            buffered_queries.close()
                        if shared_connection is not None:
                            shared_connection.close()
                        buffered_queries = None
                        shared_connection = None
                        audit_log_file.unlink(missing_ok=True)
                        use_cached_audit_log = False
                        self.report.warning(
                            title="Discarded unreadable cached audit log",
                            message="The persisted query-history cache could not be read "
                            "and was discarded; re-fetching query history.",
                            context=str(audit_log_file),
                            exc=cache_exc,
                            log=False,
                        )

                if not use_cached_audit_log:
                    if audit_log_file is not None:
                        audit_log_file.unlink(missing_ok=True)
                        shared_connection = ConnectionWrapper(audit_log_file)
                        buffered_queries = FileBackedList(shared_connection)
                    else:
                        buffered_queries = FileBackedList[Query]()
                    with self.report.usage_query_fetch_timer:
                        for query in self._fetch_queries():
                            # Drain only — the cursor/connection is released once the
                            # generator is fully consumed here.
                            buffered_queries.append(query)
                # Invariant: assigned on every branch above. Explicit raise (not assert,
                # which `python -O` strips) so a regression surfaces clearly.
                # Assigned on every branch above; narrow it once (the non-None type
                # carries through to the parse loop below).
                buffered_queries = self._require(
                    buffered_queries, "buffered_queries unexpectedly None after setup"
                )
                self.report.num_queries = len(buffered_queries)
            except (MemoryError, SystemExit, KeyboardInterrupt):
                raise
            except Exception as e:
                logger.error("Error processing usage", exc_info=True)
                # Drop a partially-written named file so it cannot be mistaken for a
                # valid cache on the next run.
                if audit_log_file is not None:
                    audit_log_file.unlink(missing_ok=True)
                self.report.failure(
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
                self.report.failure(
                    title="Failed to fetch query history",
                    message="Could not fully read query history from system tables; usage statistics may be incomplete or missing. See the related SQL query failure warning for the underlying error.",
                )
                return
            if self.report.num_queries == 0:
                # Successful but empty read (a fetch failure returns above): warn, then
                # fall through to auto_empty so the idle window records a zero datapoint,
                # matching Snowflake/BigQuery/Redshift. (Timeseries UPSERT — adds a
                # current-bucket zero, does not delete history.)
                self._report_no_queries()

            with self.report.usage_parsing_timer:
                self._parse_buffered_queries(aggregator, buffered_queries, default_db)
            self._log_usage_routing_summary()
            self._report_usage_lineage_warnings()

            yield from auto_empty_dataset_usage_statistics(
                auto_workunit(aggregator.gen_metadata()),
                dataset_urns={self.table_urn_builder(ref) for ref in table_refs},
                config=self.config,
            )
        finally:
            # Closing a shared connection also closes its dependent FileBackedList, so
            # close exactly one to avoid a redundant double-close. Closing flushes the
            # SQLite file but does NOT delete a named file — the window-keyed cache is
            # intentionally left for the next run to reload (use_cached_audit_log).
            buffer_closeable = shared_connection or buffered_queries
            if buffer_closeable is not None:
                try:
                    buffer_closeable.close()
                except (
                    Exception
                ) as close_exc:  # surface close failures in the report, not just logs
                    logger.warning("Failed to close audit-log buffer", exc_info=True)
                    self.report.warning(
                        title="Failed to close usage buffer",
                        message="The query-history buffer failed to close cleanly; its "
                        "temporary resources may not have been released.",
                        exc=close_exc,
                        log=False,
                    )
            if aggregator is not None:
                try:
                    aggregator.close()
                except (
                    Exception
                ) as close_exc:  # surface close failures in the report, not just logs
                    logger.warning(
                        "Failed to close SqlParsingAggregator", exc_info=True
                    )
                    self.report.warning(
                        title="Failed to close usage aggregator",
                        message="The usage aggregator failed to close cleanly; its temporary resources may not have been released.",
                        exc=close_exc,
                        log=False,
                    )
