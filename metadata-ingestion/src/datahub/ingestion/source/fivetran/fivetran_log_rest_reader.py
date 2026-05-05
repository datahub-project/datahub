"""REST-only implementation of FivetranConnectorReader.

Reads everything FivetranSource needs from the Fivetran REST API instead of
querying a destination-hosted log database. See `log_reader.py` for the
contract.
"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Dict, List, Optional, Tuple, Type

import pydantic
import requests
import sqlalchemy.exc

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.fivetran.config import (
    MAX_COLUMN_LINEAGE_PER_CONNECTOR_DEFAULT,
    MAX_TABLE_LINEAGE_PER_CONNECTOR_DEFAULT,
    FivetranSourceReport,
)
from datahub.ingestion.source.fivetran.data_classes import (
    ColumnLineage,
    Connector,
    Job,
    TableLineage,
)
from datahub.ingestion.source.fivetran.fivetran_rest_api import FivetranAPIClient
from datahub.ingestion.source.fivetran.log_reader import (
    FivetranJobsReader,
    FivetranLineageReader,
)
from datahub.ingestion.source.fivetran.response_models import (
    FivetranColumn,
    FivetranConnectionSchemas,
    FivetranListedConnection,
)

logger = logging.getLogger(__name__)

# Exceptions we recover from when a single REST call fails: connection/timeout
# errors, HTTP errors, JSON / Pydantic parse errors, and assorted ValueErrors
# raised by the API client (e.g., non-success response codes). Anything else
# (KeyboardInterrupt, programming errors) should propagate.
_RECOVERABLE_REST_ERRORS: Tuple[Type[BaseException], ...] = (
    requests.RequestException,
    pydantic.ValidationError,
    ValueError,
)


class FivetranLogRestReader:
    """REST-API implementation of FivetranConnectorReader."""

    def __init__(
        self,
        api_client: FivetranAPIClient,
        report: FivetranSourceReport,
        max_table_lineage_per_connector: int = MAX_TABLE_LINEAGE_PER_CONNECTOR_DEFAULT,
        max_column_lineage_per_connector: int = MAX_COLUMN_LINEAGE_PER_CONNECTOR_DEFAULT,
        max_workers: int = 4,
        per_connector_timeout_sec: int = 300,
        db_log_reader: Optional[FivetranJobsReader] = None,
        db_lineage_reader: Optional[FivetranLineageReader] = None,
    ) -> None:
        # `api_client` is owned by `FivetranSource` and shared with the
        # source's destination-discovery path. Sharing one instance keeps
        # the `_destination_cache` / session / retry policy consistent
        # across both call paths instead of fragmenting state across two
        # API clients.
        # `db_log_reader` is the optional hybrid mode: REST handles
        # connectors / schemas / lineage, but per-run sync history is
        # fetched from a Fivetran log warehouse (Snowflake / BigQuery /
        # Databricks). When set, DPI events are emitted; when not, only
        # structural metadata is emitted (Fivetran's REST API has no
        # sync-history endpoint). Typed as `FivetranJobsReader` Protocol
        # so future job-fetching backends can plug in without subclassing.
        # `db_lineage_reader` is the lineage-fallback half of hybrid mode.
        # When the Fivetran Metadata API is plan-restricted (Standard+
        # only), REST lineage extraction degrades to table-only via the
        # schemas-config endpoint. With a DB lineage reader available we
        # instead pull full column lineage from the DB log's
        # `column_lineage` table, recovering the capability the customer
        # already has via DB credentials. In typical deployments this is
        # the same `FivetranLogDbReader` instance as `db_log_reader`.
        self.api_client = api_client
        self._report = report
        self._max_table_lineage_per_connector = max_table_lineage_per_connector
        self._max_column_lineage_per_connector = max_column_lineage_per_connector
        self._max_workers = max_workers
        self._per_connector_timeout_sec = per_connector_timeout_sec
        self._db_log_reader = db_log_reader
        self._db_lineage_reader = db_lineage_reader
        # One-shot flag: when DB-log lineage fallback is wired up but
        # fails (e.g. transient warehouse error), we degrade to the
        # schemas-config endpoint. Emit the notice once per ingest
        # instead of per-connector. Set under `_report_lock`.
        self._db_lineage_unavailable_reported = False
        # One-shot flag: per-table column fetch failures (auth blip,
        # transient API error) emit a single warning per ingest.
        # Subsequent failures degrade silently to the (possibly empty)
        # inline column data from the bulk schemas response.
        self._table_columns_failure_reported = False
        # The DataHub structured-log machinery (`LossyDict` / `LossyList`)
        # is not thread-safe. We acquire `_report_lock` around every report
        # mutation made from worker threads so concurrent warnings can't
        # corrupt sample state. The lock is short-held (a few dict/list
        # operations) so contention is negligible.
        self._report_lock = threading.Lock()
        self._user_email_cache: Dict[str, Optional[str]] = {}
        # Discover groups lazily on first connector list.
        self._group_ids: Optional[List[str]] = None

    def _discover_group_ids(self) -> List[str]:
        if self._group_ids is not None:
            return self._group_ids
        # A failure here is fatal — without groups we can't list any
        # connectors — but it should surface as a structured report.failure
        # rather than an unstructured crash so operators see it in the
        # ingest summary.
        try:
            self._group_ids = [g.id for g in self.api_client.list_groups()]
        except _RECOVERABLE_REST_ERRORS as e:
            self._report.failure(
                title="Fivetran group discovery failed",
                message="Could not list Fivetran groups via REST. No connectors will be ingested.",
                exc=e,
            )
            self._group_ids = []
        return self._group_ids

    def get_user_email(self, user_id: Optional[str]) -> Optional[str]:
        # Empty string is the "no connecting user" sentinel set by
        # `_build_connector` (`listed.connected_by or ""`); falsy-check
        # mirrors the DB reader so we don't pay an N-group `list_users`
        # walk for every connector that lacks a user.
        if not user_id:
            return None
        if user_id in self._user_email_cache:
            return self._user_email_cache[user_id]
        # Populate the cache lazily by listing all users across all groups.
        # (Per-user GET would also work; bulk is cheaper for typical accounts.)
        for group_id in self._discover_group_ids():
            try:
                for user in self.api_client.list_users(group_id):
                    self._user_email_cache[user.id] = user.email
            except _RECOVERABLE_REST_ERRORS as e:
                # A single bad group shouldn't crash the run — owner emails
                # for that group's connectors will be missing in the emitted
                # MCEs, but lineage is unaffected.
                self._report.warning(
                    title="Failed to list Fivetran group users",
                    message="Owner email lookups for connectors in this group will return None.",
                    context=group_id,
                    exc=e,
                )
        return self._user_email_cache.get(user_id)

    def get_allowed_connectors_list(
        self,
        connector_patterns: AllowDenyPattern,
        destination_patterns: AllowDenyPattern,
        syncs_interval: int,
    ) -> List[Connector]:
        # One-time notice: Fivetran's REST API doesn't expose a sync-history
        # endpoint, so REST mode emits structural metadata only (no DPI
        # events for individual runs) — unless a `db_log_reader` is wired
        # in for hybrid mode, in which case run history comes from the
        # Fivetran log warehouse.
        if self._db_log_reader is None:
            self._report.info(
                title="REST mode does not emit DPI events for sync runs",
                message=(
                    "Fivetran's REST API has no sync-history endpoint per "
                    "connection. DataFlow / DataJob / dataset / lineage URNs "
                    "are emitted normally; per-run DataProcessInstance events "
                    "are not. Provide a `fivetran_log_config` block alongside "
                    "`log_source: rest_api` to enable hybrid mode (REST for "
                    "lineage, DB log for run history)."
                ),
            )

        # Collect work first (sequential, fast — just listing/filtering)…
        scoped_listed: List[FivetranListedConnection] = []
        for group_id in self._discover_group_ids():
            # Filter groups before issuing list_connections — saves an API
            # call per disallowed group on accounts with many destinations.
            if not destination_patterns.allowed(group_id):
                continue
            try:
                listed_connections = list(self.api_client.list_connections(group_id))
            except _RECOVERABLE_REST_ERRORS as e:
                self._report.warning(
                    title="Failed to list Fivetran connections",
                    message="Skipping group — could not list its connections via REST.",
                    context=group_id,
                    exc=e,
                )
                continue
            for listed in listed_connections:
                # REST mode is new — no backwards-compat baseline to
                # preserve — so we match either `connector_id` (`listed.id`,
                # the stable identifier the Fivetran UI shows) or
                # `connector_name` (`listed.schema_`, the destination schema
                # name; the same string DB mode treats as `connector_name`).
                # Allowing both lets a recipe written against DB-mode names
                # keep working unchanged when the user switches to REST.
                # DB mode itself remains name-only — see
                # `fivetran_log_db_reader.py` for why we don't OR there.
                if not (
                    connector_patterns.allowed(listed.id)
                    or connector_patterns.allowed(listed.schema_)
                ):
                    # Filter pattern check is read-only on patterns + simple
                    # report mutation — do it under lock for symmetry.
                    with self._report_lock:
                        self._report.report_connectors_dropped(listed.id)
                    continue
                scoped_listed.append(listed)

        # …then fan out the per-connector work (network-heavy: schemas +
        # sync_history calls) across a small thread pool.
        #
        # Iteration order: futures are iterated in submission order rather
        # than completion order so the returned connector list matches the
        # API's listing order (deterministic for golden-file tests).
        #
        # Per-future timeout: a single hung HTTP call shouldn't stall the
        # whole ingest. f.result(timeout=...) caps wait time per connector;
        # on timeout we skip it and move on. The worker keeps running in
        # the background until the pool shuts down (Python can't kill
        # threads), but its result is discarded.
        #
        # Fail-fast: on a non-recoverable exception (KeyError etc.) raised
        # from a worker, the `finally` calls `shutdown(wait=False,
        # cancel_futures=True)` so unstarted tasks are cancelled instead
        # of waiting for the whole pool to drain.
        if not scoped_listed:
            return []

        connectors: List[Connector] = []
        ex = ThreadPoolExecutor(max_workers=self._max_workers)
        try:
            submitted = [
                (
                    listed,
                    ex.submit(self._build_connector, listed, syncs_interval),
                )
                for listed in scoped_listed
            ]
            for listed, f in submitted:
                try:
                    connector = f.result(timeout=self._per_connector_timeout_sec)
                except (FuturesTimeoutError, TimeoutError):
                    # Python 3.10 raises `concurrent.futures.TimeoutError`
                    # from `f.result(timeout=...)`, which is a separate class
                    # from the built-in `TimeoutError`. Python 3.11+ unifies
                    # them. Catch both so the timeout path works on all
                    # supported versions.
                    f.cancel()  # only succeeds if not started yet
                    with self._report_lock:
                        self._report.warning(
                            title="Fivetran connector REST fetch timed out",
                            message="Worker exceeded per-connector timeout; connector skipped.",
                            context=listed.id,
                        )
                    continue
                connectors.append(connector)
                # `report_connectors_scanned()` is incremented downstream
                # in `_get_connector_workunits` — the canonical site shared
                # with DB mode. Counting here too would double the metric.
        finally:
            # `wait=False` returns immediately; `cancel_futures=True`
            # (Python 3.9+) cancels any tasks not yet started. In-flight
            # workers continue until they complete naturally.
            ex.shutdown(wait=False, cancel_futures=True)

        # Hybrid mode: REST built the connector list + lineage, now batch
        # the run-history fetch through the DB log reader. One SQL query
        # for all connectors instead of N REST calls (which Fivetran
        # doesn't even expose).
        if self._db_log_reader is not None and connectors:
            try:
                jobs_by_id = self._db_log_reader.fetch_jobs_for_connectors(
                    [c.connector_id for c in connectors],
                    syncs_interval,
                )
            except (
                sqlalchemy.exc.SQLAlchemyError,
                ValueError,
                KeyError,
                AttributeError,
            ) as e:
                # Tuple covers warehouse-data-drift / SQL-transient failures
                # that should degrade gracefully:
                #   - SQLAlchemyError: connection drops, query timeouts.
                #   - ValueError (incl. JSONDecodeError): malformed
                #     end_message_data JSON in the sync-log row.
                #   - KeyError: Fivetran renames a log-table column or
                #     changes the JSON shape of end_message_data
                #     (the comment in `_get_jobs_list` already acknowledges
                #     "json string inside string" drift in the wild).
                #   - AttributeError: `row["start_time"].timestamp()` blows
                #     up when the warehouse returns NULL for an in-flight
                #     sync at the cutoff boundary.
                # TypeError and other exception classes still propagate so
                # genuine programmer bugs aren't masked by the
                # "successfully fell back" warning.
                self._report.warning(
                    title="Failed to fetch Fivetran sync history from log warehouse",
                    message="Connectors will be emitted without per-run DPI events.",
                    exc=e,
                )
                jobs_by_id = {}
            for connector in connectors:
                connector.jobs = jobs_by_id.get(connector.connector_id, [])

        return connectors

    def _build_connector(
        self,
        listed: FivetranListedConnection,
        syncs_interval: int,
    ) -> Connector:
        # Lineage is sourced from the Metadata API
        # (`/v1/metadata/connectors/{id}/{schemas,tables,columns}`) which
        # exposes both `name_in_source` AND `name_in_destination` per
        # entity. The connection-schema-config endpoint
        # (`/v1/connections/{id}/schemas`) only carries
        # `name_in_destination` and is used as a fallback when the
        # Metadata API isn't available (e.g. plan-restricted to
        # Standard+) so we still emit table-level lineage.
        lineage = self._fetch_lineage(listed.id)

        # Sync history (per-run DPI events) is intentionally NOT fetched in
        # REST mode: Fivetran's REST API does not expose a sync-history
        # endpoint per connection. The only authoritative source for run
        # history is the Fivetran log delivered by the Platform Connector
        # to a destination warehouse (use `log_source: log_database` for
        # that). REST mode emits structural metadata (DataFlow, DataJob,
        # datasets, lineage) but no per-run DPI events.
        jobs: List[Job] = []

        return Connector(
            connector_id=listed.id,
            # `listed.schema_` is the destination schema (e.g. `postgres_public`)
            # — the same identifier the Fivetran UI displays as the connector
            # name and that the DB reader pulls from `connection.connection_name`.
            connector_name=listed.schema_,
            connector_type=listed.service,
            paused=listed.paused,
            sync_frequency=listed.sync_frequency,
            destination_id=listed.group_id,
            user_id=listed.connected_by or "",
            lineage=lineage,
            jobs=jobs,
        )

    def _fetch_lineage(
        self,
        connector_id: str,
    ) -> List[TableLineage]:
        """Resolve table+column lineage for one connector.

        Fallback chain, in order of preference:

        1. **DB log lineage tables** (when `db_lineage_reader` is wired
           in — typical hybrid setups). The DB log carries explicit
           `source_column_name` and `destination_column_name` columns
           populated by the Fivetran Platform Connector during sync,
           so this is the most authoritative source — it can distinguish
           rename transformations the schemas-config endpoint can't.

        2. **`/v1/connections/{id}/schemas` (schemas-config endpoint)**.
           Per Fivetran's OpenAPI spec, dict keys here ARE the source-
           side schema/table/column names ("the column name as stored
           in the connection schema config"); `name_in_destination` is
           the post-Fivetran-rename destination name. Available on every
           plan, every account — no tier restrictions.
        """
        # Fallback 1: DB log lineage tables (preferred when the connector
        # writes its log to the configured warehouse — explicit source /
        # destination column names from sync events). When the DB log
        # has NO entry for this connector_id (typical for accounts where
        # different connectors log to different warehouses, or for newly-
        # added connectors that haven't synced yet) we fall through to
        # REST. An empty result is not a failure — REST may still have
        # full lineage for that connector via the schemas+columns path.
        if self._db_lineage_reader is not None:
            try:
                lineage_by_id = self._db_lineage_reader.fetch_lineage_for_connectors(
                    [connector_id]
                )
                db_result = lineage_by_id.get(connector_id, [])
                if db_result:
                    return db_result
                # Fall through to REST: the connector exists in REST's
                # account-wide view but isn't in this DB log warehouse.
            except (
                sqlalchemy.exc.SQLAlchemyError,
                ValueError,
                KeyError,
                AttributeError,
            ) as e:
                # Warehouse-data-drift / SQL-transient classes — degrade
                # to schemas-config rather than crashing the connector.
                # One-shot per ingest: the cause is deployment-level
                # (not specific to this connector) so per-connector
                # warnings would just spam the report.
                with self._report_lock:
                    if not self._db_lineage_unavailable_reported:
                        self._report.warning(
                            title="DB log lineage fallback failed",
                            message=(
                                "Could not read column lineage from the DB log "
                                "warehouse; falling through to the Fivetran "
                                "schemas-config endpoint for the rest of the "
                                "ingest. Subsequent connectors will use the "
                                "REST schemas-config path silently."
                            ),
                            context=connector_id,
                            exc=e,
                        )
                        self._db_lineage_unavailable_reported = True

        # Fallback 2: connection-schema-config endpoint + per-table
        # /columns. Available on every plan tier and works for every
        # connector visible to REST regardless of which warehouse the
        # connector's log lands in.
        try:
            schemas = self.api_client.get_connection_schemas(connector_id)
        except _RECOVERABLE_REST_ERRORS as e:
            with self._report_lock:
                self._report.warning(
                    title="Failed to fetch Fivetran connection schemas",
                    message="Connector will be emitted without table or column lineage.",
                    context=connector_id,
                    exc=e,
                )
            return []
        return self._extract_lineage_from_schemas(schemas, connector_id)

    def _emit_truncation_warnings(
        self, table_truncated: bool, column_truncated: bool, connector_id: str
    ) -> None:
        if table_truncated:
            with self._report_lock:
                self._report.warning(
                    title="Fivetran connector table lineage truncated",
                    message="Connector has more tables than max_table_lineage_per_connector. Increase the limit if you need full coverage.",
                    context=connector_id,
                )
        if column_truncated:
            with self._report_lock:
                self._report.warning(
                    title="Fivetran connector column lineage truncated",
                    message="Connector has more columns than max_column_lineage_per_connector. Increase the limit if you need full coverage.",
                    context=connector_id,
                )

    def _extract_lineage_from_schemas(
        self,
        schemas: FivetranConnectionSchemas,
        connector_id: str,
    ) -> List[TableLineage]:
        """Build lineage from the connection-schema-config response.

        Per Fivetran's OpenAPI spec, the response's nested dict keys
        carry the source-side identifiers and `name_in_destination`
        carries the post-rename destination identifier:

            schemas: { <source_schema_name>: { name_in_destination: ...,
              tables: { <source_table_name>: { name_in_destination: ...,
                columns: { <source_column_name>: { name_in_destination, ... }}}}}

        Note on columns: the bulk schemas endpoint only returns columns
        the user has explicitly modified. To get every column for a
        default-configured table, this method calls the per-table
        endpoint `/v1/connections/{id}/schemas/{schema}/tables/{table}/columns`
        once per emitted table — see `_fetch_table_columns`.

        See https://fivetran.com/assets-docs/openapi/file_v1.json
        ("Each key is the {column,table,schema} name as stored in the
        connection schema config").
        """
        result: List[TableLineage] = []
        column_total = 0
        table_truncated = False
        column_truncated = False

        for schema_name, schema in schemas.schemas.items():
            if table_truncated or column_truncated:
                break
            if not schema.enabled:
                continue
            dest_schema = schema.name_in_destination
            for table_name, table in schema.tables.items():
                if not table.enabled:
                    continue
                if len(result) >= self._max_table_lineage_per_connector:
                    table_truncated = True
                    break

                # The bulk schemas endpoint only includes columns the
                # user has explicitly modified. Fetch the full column
                # set per-table so column lineage is complete by default.
                # Falls back to the inline (often empty) `table.columns`
                # if the per-table call fails — caller still gets table
                # lineage, just no columns for that one table.
                columns = self._fetch_table_columns(
                    connector_id, schema_name, table_name
                )
                if columns is None:
                    columns = table.columns

                column_lineage: List[ColumnLineage] = []
                for col_name, col in columns.items():
                    if not col.enabled:
                        continue
                    if column_total >= self._max_column_lineage_per_connector:
                        column_truncated = True
                        break
                    column_lineage.append(
                        ColumnLineage(
                            source_column=col_name,
                            destination_column=col.name_in_destination,
                        )
                    )
                    column_total += 1
                result.append(
                    TableLineage(
                        source_table=f"{schema_name}.{table_name}",
                        destination_table=(
                            f"{dest_schema}.{table.name_in_destination}"
                        ),
                        column_lineage=column_lineage,
                    )
                )
                if column_truncated:
                    break

        self._emit_truncation_warnings(table_truncated, column_truncated, connector_id)
        return result

    def _fetch_table_columns(
        self,
        connector_id: str,
        schema: str,
        table: str,
    ) -> Optional[Dict[str, FivetranColumn]]:
        """Wrap `api_client.get_table_columns` with structured error
        handling. Returns `None` on transient failure so the caller can
        fall back to the (possibly empty) inline column dict from the
        bulk schemas response.

        Failure is one-shot per ingest at the warning level — the cause
        (network blip, API permissions) is rarely per-table.
        """
        try:
            return self.api_client.get_table_columns(connector_id, schema, table)
        except _RECOVERABLE_REST_ERRORS as e:
            with self._report_lock:
                if not self._table_columns_failure_reported:
                    self._report.warning(
                        title="Failed to fetch per-table column metadata",
                        message=(
                            "Per-table column fetch via "
                            "/v1/connections/{id}/schemas/{schema}/tables/{table}/columns "
                            "failed; affected tables will use the (possibly "
                            "empty) inline column data from the bulk schemas "
                            "endpoint. Subsequent failures suppressed for the "
                            "rest of this ingest."
                        ),
                        context=f"{connector_id}.{schema}.{table}",
                        exc=e,
                    )
                    self._table_columns_failure_reported = True
            return None
