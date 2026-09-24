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
import re
import time
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from datahub.emitter.mce_builder import make_user_urn
from datahub.ingestion.source.fabric.onelake.config import FabricUsageConfig
from datahub.ingestion.source.fabric.onelake.constants import FABRIC_SQL_DEFAULT_SCHEMA
from datahub.ingestion.source.fabric.onelake.models import FabricQueryInsightsRow
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantUsageRunSkipHandler,
)
from datahub.ingestion.source.usage.usage_common import normalize_timestamp_to_utc
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

# T-SQL control-of-flow / session statements. queryinsights logs every
# statement of a batch or procedure body as its own row - including the bodies
# of the ODBC driver's catalog procedures (`set @ODBCVer = 3`,
# `if @data_type = 0`, ...). Such statements carry no table lineage or usage of
# their own (any DML inside a procedure is logged as a separate row), and the
# SQL parser cannot parse them, so they are skipped before parsing.
_PROCEDURAL_KEYWORDS = frozenset(
    {
        "BEGIN",
        "BREAK",
        "COMMIT",
        "CONTINUE",
        "DBCC",
        "DEALLOCATE",
        "DECLARE",
        "ELSE",
        "END",
        "EXEC",
        "EXECUTE",
        "GOTO",
        "IF",
        "PRINT",
        "RAISERROR",
        "RETURN",
        "ROLLBACK",
        "SAVE",
        "SET",
        "THROW",
        "USE",
        "WAITFOR",
        "WHILE",
    }
)
# Leading comments / whitespace, then the first keyword of the statement.
_LEADING_KEYWORD = re.compile(
    r"^(?:\s|--[^\n]*(?:\n|$)|/\*.*?\*/)*([A-Za-z_]+)", re.DOTALL
)
# Statements that can carry table lineage. A row that starts with a procedural
# keyword but contains one of these (e.g.
# `IF OBJECT_ID('dbo.t') IS NOT NULL DROP TABLE dbo.t; CREATE TABLE dbo.t AS
# SELECT ...`, or `SET NOCOUNT ON; INSERT INTO ...`) is still handed to the SQL
# parser: the parser extracts lineage from some of these, and when it cannot,
# the row is counted as a parse failure instead of being hidden as procedural.
_LINEAGE_BEARING = re.compile(
    r"\b(?:INSERT|UPDATE|DELETE|MERGE|CREATE\s+TABLE|SELECT\b[^;]*?\bINTO)\b",
    re.IGNORECASE | re.DOTALL,
)


def _is_procedural_statement(command: str, statement_type: Optional[str]) -> bool:
    """Whether a queryinsights row is a control-of-flow / session statement
    with no statement that could carry table lineage."""
    if statement_type and statement_type.strip().upper() in _PROCEDURAL_KEYWORDS:
        is_procedural = True
    else:
        match = _LEADING_KEYWORD.match(command)
        is_procedural = (
            match is not None and match.group(1).upper() in _PROCEDURAL_KEYWORDS
        )
    return is_procedural and not _LINEAGE_BEARING.search(command)


class FabricUsageExtractor:
    """Streams queryinsights rows into the shared SQL parsing aggregator."""

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

        Note: the handler internally refuses to advance the checkpoint if any
        per-item `extract()` reported failure via `report_current_run_status`.
        So this is a no-op unless every item succeeded; the next run then
        retries the same window.
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

        logger.info(
            f"Starting usage extraction for item='{item_display_name}' "
            f"workspace_id={workspace_id} item_id={item_id}"
        )

        ingestion_login = self._get_ingestion_login(
            workspace_id, item_id, schema_client
        )

        try:
            row_iter = schema_client.stream_usage_history(
                workspace_id=workspace_id,
                item_id=item_id,
                start_time=self.start_time,
                end_time=self.end_time,
                skip_failed_queries=self.config.skip_failed_queries,
            )
            for row in row_iter:
                self._handle_row(
                    row,
                    workspace_id,
                    item_id,
                    item_display_name,
                    ingestion_login=ingestion_login,
                )
            logger.info(f"Finished usage extraction for item='{item_display_name}'")
        except Exception as e:
            logger.warning(
                f"Failed usage extraction for item='{item_display_name}' "
                f"workspace_id={workspace_id} item_id={item_id}: {e}"
            )
            self.report.warning(
                title="Failed to Extract Usage",
                message=(
                    "Error reading queryinsights.exec_requests_history. "
                    "Usage stats for this item will be skipped."
                ),
                context=f"workspace_id={workspace_id}, item_id={item_id}",
                exc=e,
                log=False,
            )
            # Mark this item's step as failed so the skip handler refuses to advance
            # the usage checkpoint. Otherwise a window where every item failed would
            # be permanently marked as covered, and the missed rows are unrecoverable
            # past queryinsights' 30-day retention.
            if self.redundant_run_skip_handler:
                self.redundant_run_skip_handler.report_current_run_status(
                    f"usage-extraction-{workspace_id}/{item_id}", False
                )
        finally:
            self.report.usage_extraction_per_item_sec[item_key] = round(
                time.monotonic() - started_at, 3
            )

    def _get_ingestion_login(
        self,
        workspace_id: str,
        item_id: str,
        schema_client: "SchemaExtractionClient",
    ) -> Optional[str]:
        """Casefolded login of the ingestion identity, or None when
        `skip_ingestion_identity_queries` is off or the lookup failed."""
        if not self.config.skip_ingestion_identity_queries:
            return None
        try:
            login = schema_client.get_current_login(workspace_id, item_id)
        except Exception as e:
            self.report.warning(
                title="Failed to Determine Ingestion Identity",
                message=(
                    "Could not read SUSER_SNAME() on the SQL Analytics Endpoint, "
                    "so queries issued by the ingestion identity are not skipped "
                    "for this item (usage.skip_ingestion_identity_queries)."
                ),
                context=f"workspace_id={workspace_id}, item_id={item_id}",
                exc=e,
                log=False,
            )
            return None
        if not isinstance(login, str) or not login.strip():
            return None
        return login.strip().casefold()

    def _handle_row(
        self,
        row: FabricQueryInsightsRow,
        workspace_id: str,
        item_id: str,
        item_display_name: str,
        ingestion_login: Optional[str] = None,
    ) -> None:
        if not row.command.strip():
            self.report.report_usage_query_skipped("empty_command")
            return

        if (
            ingestion_login is not None
            and row.login_name
            and row.login_name.strip().casefold() == ingestion_login
        ):
            self.report.report_usage_query_skipped("ingestion_identity")
            return

        if _is_procedural_statement(row.command, row.statement_type):
            self.report.report_usage_query_skipped("procedural_statement")
            return

        # Entra UPNs are case-insensitive by Microsoft spec, but pyodbc preserves the
        # case Fabric sent. Lowercase here so the resulting CorpUserUrn matches the
        # lowercased URNs that Azure AD / Okta / Google identity sources produce.
        login_name = row.login_name.lower() if row.login_name else None

        if login_name and not self.config.user_email_pattern.allowed(login_name):
            self.report.report_usage_query_skipped("user_filtered")
            return

        timestamp = normalize_timestamp_to_utc(row.start_time)

        # The view-lineage path uses default_db=f"{workspace_id}.{item_id}" so the
        # synthetic SQL parser identifier matches the URN we generate for tables.
        # Observed queries must use the same identifier so the parser resolves
        # `<schema>.<table>` references to the same dataset URNs.
        default_db = f"{workspace_id}.{item_id}"
        # Unqualified table refs (e.g. `SELECT * FROM customers`) resolve to the
        # T-SQL default schema on Fabric Warehouse / Lakehouse SQL endpoints.
        default_schema = FABRIC_SQL_DEFAULT_SCHEMA

        user = (
            CorpUserUrn.from_string(make_user_urn(login_name)) if login_name else None
        )

        observed = ObservedQuery(
            query=row.command,
            timestamp=timestamp,
            user=user,
            default_db=default_db,
            default_schema=default_schema,
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
