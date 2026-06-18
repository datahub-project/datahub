import logging
from dataclasses import dataclass
from datetime import timezone
from typing import Callable, Iterable, Optional, Set

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
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)

logger = logging.getLogger(__name__)


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
            generate_queries=self.config.emit_queries,
            generate_query_usage_statistics=self.config.emit_queries,
            generate_usage_statistics=True,
            generate_operations=self.config.include_operational_stats,
            usage_config=self.config,
            format_queries=False,
            is_allowed_table=is_allowed_table,
        )

    def _fetch_queries(self) -> Iterable[Query]:
        if self._use_system_tables_join():
            return self.proxy.get_query_history_via_system_tables(
                self.config.start_time, self.config.end_time
            )
        return self.proxy.query_history(self.config.start_time, self.config.end_time)

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
            aggregator = self._build_aggregator(is_allowed_table=is_allowed_table)
            for query in self._fetch_queries():
                self.report.num_queries += 1
                # query.start_time may be None (see proxy_types.Query) or naive in
                # practice despite the annotation.  Normalize to timezone.utc so
                # SqlParsingAggregator's tzinfo assertion holds:
                #   - aware datetime  → astimezone(utc) preserves the instant
                #   - naive datetime  → replace(tzinfo=utc) treats it as UTC
                #   - None            → passed through unchanged
                ts = query.start_time
                if ts is not None:
                    ts = (
                        ts.astimezone(timezone.utc)
                        if ts.tzinfo is not None
                        else ts.replace(tzinfo=timezone.utc)
                    )
                try:
                    aggregator.add_observed_query(
                        ObservedQuery(
                            query=query.query_text,
                            timestamp=ts,
                            user=(
                                CorpUserUrn.from_string(
                                    self.user_urn_builder(query.user_name)
                                )
                                if query.user_name
                                else None
                            ),
                            default_db=default_db,
                            default_schema=None,
                        )
                    )
                except Exception as per_query_exc:
                    self.report.num_queries_dropped += 1
                    logger.warning(
                        "Skipping query due to error during add_observed_query "
                        "(query_id=%s): %r",
                        query.query_id,
                        per_query_exc,
                        exc_info=True,
                    )
                    self.report.report_warning(
                        "usage-query-dropped",
                        context=f"query_id={query.query_id}",
                        exc=per_query_exc,
                    )
            if self.report.num_queries == 0:
                if self.report.num_usage_query_fetch_failures > fetch_failures_before:
                    # The query-history fetch failed (proxy reported a sql-query-failed
                    # warning and incremented the counter).  Surface this as a run failure
                    # so operators are not misled by a seemingly successful but empty run.
                    self.report.report_failure(
                        "usage-fetch-failed",
                        "Query-history fetch failed; see the 'sql-query-failed' warning "
                        "for details. Usage statistics were not updated.",
                    )
                    return
                if self._use_system_tables_join():
                    hint = (
                        "verify SELECT privilege on system.query and system.access "
                        "and that the time window covers recent activity"
                    )
                else:
                    hint = (
                        "verify CAN_MANAGE privilege on the SQL warehouse "
                        "and that the time window covers recent activity"
                    )
                self.report.report_warning(
                    "no-queries-found",
                    f"No queries found in the configured time range. {hint}.",
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
        except Exception as e:
            logger.error("Error processing usage", exc_info=True)
            self.report.report_failure(
                "usage-extraction",
                f"Usage extraction failed: {e!r}",
                exc=e,
            )
        finally:
            if aggregator is not None:
                try:
                    aggregator.close()
                except Exception:
                    logger.warning(
                        "Failed to close SqlParsingAggregator", exc_info=True
                    )
