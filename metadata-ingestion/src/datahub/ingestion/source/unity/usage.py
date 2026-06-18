import logging
from dataclasses import dataclass
from datetime import timezone
from typing import Callable, Iterable, Optional, Set

from datahub.ingestion.api.source_helpers import auto_workunit
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
    platform: str = "databricks"
    schema_resolver: Optional[SchemaResolver] = None

    def _use_system_tables_join(self) -> bool:
        return self.config.usage_uses_system_tables(self.proxy.warehouse_id)

    def _build_aggregator(
        self,
        is_allowed_table: Optional[Callable[[str], bool]] = None,
    ) -> SqlParsingAggregator:
        # Use the pre-populated resolver passed from the source so that
        # unqualified / partial table references in queries are resolved against
        # the schemas ingested in this run.  Fall back to a fresh empty resolver
        # when none is provided (e.g. unit tests).
        schema_resolver = self.schema_resolver or SchemaResolver(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )
        # UnityCatalogSourceConfig extends BaseUsageConfig so self.config satisfies
        # the usage_config parameter type.
        return SqlParsingAggregator(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            schema_resolver=schema_resolver,
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

        aggregator: Optional[SqlParsingAggregator] = None
        try:
            aggregator = self._build_aggregator(is_allowed_table=is_allowed_table)
            for query in self._fetch_queries():
                self.report.num_queries += 1
                ts = query.start_time
                if ts is not None and ts.tzinfo is not None:
                    ts = ts.astimezone(timezone.utc)
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
                        default_db=None,
                        default_schema=None,
                    )
                )
            if self.report.num_queries == 0:
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
            yield from auto_workunit(aggregator.gen_metadata())
        except Exception as e:
            logger.error("Error processing usage", exc_info=True)
            self.report.report_warning("usage-extraction", str(e))
        finally:
            if aggregator is not None:
                try:
                    aggregator.close()
                except Exception:
                    logger.warning(
                        "Failed to close SqlParsingAggregator", exc_info=True
                    )
