"""Fabric OneLake usage / operation extractor.

Reads `queryinsights.exec_requests_history` from each Lakehouse / Warehouse
SQL Analytics Endpoint and feeds the rows into a `SqlParsingAggregator`. The
aggregator parses each query via sqlglot and emits `datasetUsageStatistics`
and `operation` aspects through the same `gen_metadata()` drain that already
emits view lineage.

Reference:
- https://learn.microsoft.com/en-us/fabric/data-warehouse/query-insights
- https://learn.microsoft.com/en-us/sql/relational-databases/system-views/queryinsights-exec-requests-history-transact-sql?view=fabric
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

from datahub.emitter.mce_builder import make_user_urn
from datahub.ingestion.source.fabric.onelake.config import FabricUsageConfig
from datahub.ingestion.source.fabric.onelake.models import FabricQueryInsightsRow
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantUsageRunSkipHandler,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.fabric.onelake.schema_client import (
        SchemaExtractionClient,
    )

logger = logging.getLogger(__name__)


class FabricUsageExtractor:
    """Streams queryinsights rows for a single item into the shared aggregator."""

    def __init__(
        self,
        config: FabricUsageConfig,
        aggregator: SqlParsingAggregator,
        report: FabricOneLakeSourceReport,
        redundant_run_skip_handler: Optional[RedundantUsageRunSkipHandler] = None,
    ):
        self.config = config
        self.aggregator = aggregator
        self.report = report
        self.redundant_run_skip_handler = redundant_run_skip_handler

        self.start_time, self.end_time = self._resolve_time_window()
        self.report.usage_start_time = self.start_time
        self.report.usage_end_time = self.end_time

    def _resolve_time_window(self) -> tuple[datetime, datetime]:
        if self.redundant_run_skip_handler:
            return self.redundant_run_skip_handler.suggest_run_time_window(
                self.config.start_time, self.config.end_time
            )
        return self.config.start_time, self.config.end_time

    def should_skip_run(self) -> bool:
        """Whether this run's time window was fully covered by a previous successful run."""
        if not self.redundant_run_skip_handler:
            return False
        if self.redundant_run_skip_handler.should_skip_this_run(
            cur_start_time=self.config.start_time,
            cur_end_time=self.config.end_time,
        ):
            self.report.usage_run_skipped = True
            self.report.info(
                title="Usage extraction skipped",
                message=(
                    "Usage extraction skipped because the configured time window was "
                    "fully covered by a previous successful run."
                ),
            )
            return True
        return False

    def update_state_on_success(self) -> None:
        """Persist the resolved time window so future runs can short-circuit.

        Use the resolved (`self.start_time` / `self.end_time`) — not the raw
        config window — so we don't over-claim coverage when the skip handler
        narrowed the window to only the uncovered portion.
        """
        if self.redundant_run_skip_handler:
            self.redundant_run_skip_handler.update_state(
                self.start_time,
                self.end_time,
                self.config.bucket_duration,
            )

    def extract(
        self,
        workspace_id: str,
        item_id: str,
        item_display_name: str,
        schema_client: "SchemaExtractionClient",
    ) -> None:
        """Stream this item's queryinsights rows into the aggregator.

        Returns nothing — the aggregator's internal state holds the parsed queries
        and the source drains it via `gen_metadata()` after all items are processed.
        Per-item exceptions are caught here so one bad item doesn't poison the run.
        """
        item_key = f"{workspace_id}/{item_id}"
        started_at = time.monotonic()

        try:
            row_iter = schema_client.stream_usage_history(
                workspace_id=workspace_id,
                item_id=item_id,
                start_time=self.start_time,
                end_time=self.end_time,
                skip_failed_queries=self.config.skip_failed_queries,
            )
            for row in row_iter:
                self._handle_row(row, workspace_id, item_id, item_display_name)
        except Exception as e:
            self.report.report_warning(
                title="Failed to Extract Usage",
                message=(
                    "Error reading queryinsights.exec_requests_history. "
                    "Usage stats for this item will be skipped."
                ),
                context=f"workspace_id={workspace_id}, item_id={item_id}",
                exc=e,
            )
        finally:
            self.report.usage_extraction_per_item_sec[item_key] = round(
                time.monotonic() - started_at, 3
            )

    def _handle_row(
        self,
        row: FabricQueryInsightsRow,
        workspace_id: str,
        item_id: str,
        item_display_name: str,
    ) -> None:
        if not row.command or not row.command.strip():
            self.report.report_usage_query_skipped("empty_command")
            return

        # Entra UPNs are case-insensitive by Microsoft spec, but pyodbc preserves the
        # case Fabric sent. Lowercase here so the resulting CorpUserUrn matches the
        # lowercased URNs that Azure AD / Okta / Google identity sources produce.
        login_name = row.login_name.lower() if row.login_name else None

        if login_name and not self.config.user_email_pattern.allowed(login_name):
            self.report.report_usage_query_skipped("user_filtered")
            return

        timestamp = self._normalize_timestamp(row.start_time)
        if timestamp is None:
            self.report.report_usage_query_skipped("missing_start_time")
            return

        # The view-lineage path uses default_db=f"{workspace_id}.{item_id}" so the
        # synthetic SQL parser identifier matches the URN we generate for tables.
        # Observed queries must use the same identifier so the parser resolves
        # `<schema>.<table>` references to the same dataset URNs.
        default_db = f"{workspace_id}.{item_id}"
        default_schema: Optional[str] = None

        user = (
            CorpUserUrn.from_string(make_user_urn(login_name)) if login_name else None
        )

        observed = ObservedQuery(
            query=row.command,
            timestamp=timestamp,
            user=user,
            default_db=default_db,
            default_schema=default_schema,
            query_hash=row.query_hash,
            extra_info={
                "fabric_workspace_id": workspace_id,
                "fabric_item_id": item_id,
                "fabric_item_name": item_display_name,
                "statement_type": row.statement_type,
                "row_count": row.row_count,
                "status": row.status,
            },
        )

        self.aggregator.add_observed_query(observed)
        self.report.num_usage_queries_fetched += 1

    @staticmethod
    def _normalize_timestamp(value: Any) -> Optional[datetime]:
        """Coerce queryinsights timestamps to timezone-aware UTC.

        `start_time` from queryinsights is `datetime2` and surfaces as a naive
        datetime via pyodbc. The aggregator expects timezone-aware timestamps
        for bucket boundary comparisons against the (UTC-aware) usage config.
        """
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        return None
