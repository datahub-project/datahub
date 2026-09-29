import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

import tenacity
from pydantic import BaseModel, Field, field_validator

from datahub.ingestion.source.montecarlo.config import MonteCarloSourceConfig
from datahub.ingestion.source.montecarlo.constants import (
    ALERT_TYPE_SCHEMA_CHANGES,
    ASSET_FILTER_INPUT_FIELDS,
    ASSET_FILTER_JOINER_OR,
    SCHEMA_CHANGE_MATCH_MAX_DELTA_SECONDS,
    SCHEMA_CHANGE_PAGE_SIZE,
)
from datahub.ingestion.source.montecarlo.queries import (
    EVALUATE_ASSET_SELECTION_QUERY,
    GET_JOB_EXECUTIONS_QUERY,
    GET_METRICS_V4_QUERY,
    GET_SCHEMA_CHANGES_QUERY,
    GET_TABLE_QUERY,
    TABLE_MONITOR_QUERY,
)
from datahub.ingestion.source.montecarlo.query_builder import (
    IntrospectingQueryBuilder,
    QueryBuilder,
    SchemaDrift,
    StaticQueryBuilder,
)
from datahub.ingestion.source.montecarlo.report import MonteCarloSourceReport
from datahub.utilities.ratelimiter import (
    DailyCallBudget,
    DailyCallBudgetExceeded,
    TokenBucket,
)

logger = logging.getLogger(__name__)


# Transient transport/network exception class names. Matched by name (not by
# isinstance) so the connector depends only on ``pycarlo`` as its external
# dependency — ``requests`` and ``gql`` are pycarlo's internal transport
# choice and are not declared deps of this connector. Importing them directly
# would couple us to pycarlo's transport internals and add undeclared deps.
_TRANSIENT_NETWORK_ERROR_NAMES = frozenset(
    {"ConnectionError", "ReadTimeout", "ConnectTimeout", "ReadTimeoutError"}
)


def _is_transient_network_error(exception: Optional[BaseException]) -> bool:
    """True if ``exception`` is a transient transport/network failure, matched
    by class name across its MRO (so subclasses also match) without importing
    the underlying transport library."""
    if exception is None:
        return False
    return any(
        exc_type.__name__ in _TRANSIENT_NETWORK_ERROR_NAMES
        for exc_type in type(exception).__mro__
    )


def _is_retryable(exception: BaseException) -> bool:
    """Predicate driving a retry loop applied on top of pycarlo.

    pycarlo only auto-retries its own ``GqlError`` when the `retryable` flag is set,
    which defaults to status_code >= 500 (see pycarlo.common.errors.GqlError) — a
    429 (rate limited) is raised immediately with no retry. We additionally retry
    the underlying transient transport failures (``ConnectionError`` /
    ``ReadTimeout`` from the requests client pycarlo uses) so a brief blip during
    pagination backs off instead of aborting the whole phase.

    Only those clearly-transient network errors are retried, matched by class name
    so the connector depends solely on ``pycarlo`` (its only declared external
    dependency) rather than importing ``requests``/``gql`` directly. A broader
    transport/query-level exception is NOT retried blanketly: such types are also
    raised for permanent GraphQL application errors (a 200 carrying an ``errors``
    payload), and retrying those would burn up to 6 daily budget units per bad
    query. A wrapped exception is retried only when its ``__cause__`` /
    ``__context__`` is itself a transient network error.
    """
    if getattr(exception, "status_code", None) == 429:
        return True
    if _is_transient_network_error(exception):
        return True
    return _is_transient_network_error(
        exception.__cause__
    ) or _is_transient_network_error(exception.__context__)


# 401/403 mean the API credentials are rejected/insufficient — a fatal, run-level
# condition (not a per-asset one), so it must abort rather than be demoted to a
# warning. Detected via the same status_code attribute pycarlo exposes for 429.
_AUTH_STATUS_CODES = frozenset({401, 403})


def _is_auth_error(exception: BaseException) -> bool:
    return getattr(exception, "status_code", None) in _AUTH_STATUS_CODES


class MonteCarloAuthError(RuntimeError):
    """Raised when Monte Carlo rejects the API credentials (401/403). A distinct,
    fatal type — propagated unwrapped (like DailyCallBudgetExceeded) so a bad
    token aborts the run with a clear auth error, rather than being demoted to a
    per-asset warning that yields a misleading 'successful' empty run."""


# Run-level failures that must abort the whole run, not be demoted to a per-item
# warning or per-phase failure: an exhausted daily budget or rejected credentials
# is fatal regardless of where in the pipeline it surfaces. Kept in sync with
# source._FATAL_RUN_ERRORS.
_FATAL_RUN_ERRORS: tuple = (DailyCallBudgetExceeded, MonteCarloAuthError)


# 1 initial attempt + 5 retries, matching this source's prior hand-rolled loop.
_RATE_LIMIT_MAX_ATTEMPTS = 6


class MonteCarloComparison(BaseModel):
    """A single comparison within a Monte Carlo monitor/rule.

    MC monitors/rules carry a ``comparisons`` array; DataHub's assertion model is
    single-comparison, so the builder maps ``comparisons[0]`` onto the structured
    ``CustomAssertionInfo`` fields and folds the rest into ``logic`` /
    ``nativeParameters``. All fields are optional — MC
    leaves ``field``/``fields`` unset for table-level or row-predicate checks.
    """

    comparison_type: Optional[str] = None
    operator: Optional[str] = None
    metric: Optional[str] = None
    custom_metric: Optional[str] = None
    field: Optional[str] = None
    fields: List[str] = Field(default_factory=list)
    threshold: Optional[float] = None
    upper_threshold: Optional[float] = None
    lower_threshold: Optional[float] = None

    @field_validator("fields", mode="before")
    @classmethod
    def _coerce_null_fields(cls, value: Any) -> List[str]:
        # getMonitors returns fields: null on table-level comparisons
        # (freshness / schema / volume). List[str] would reject that and
        # _parse_comparisons would drop the whole comparison.
        return value or []


def _parse_comparisons(raw: Any) -> List[MonteCarloComparison]:
    """Normalize the raw ``comparisons`` list (snake_cased by pycarlo's Box) into
    ``MonteCarloComparison`` models, tolerating missing/malformed entries so one
    bad comparison doesn't abort the whole monitor (matches the source's
    continue-on-recoverable-error philosophy).

    ``customMetric`` is a ``CustomMetric`` object on the MCD schema (not a
    scalar), so the query selects ``{ uuid metricName }``; pycarlo snake_cases
    that to ``custom_metric: {uuid, metric_name}``. We flatten it to the
    human-readable ``metric_name`` string for ``MonteCarloComparison.custom_metric``
    (carried on ``nativeParameters`` for rendering); ``uuid`` is dropped since
    DataHub's assertion model has no slot for a per-comparison metric id.
    """
    if not raw:
        return []
    parsed: List[MonteCarloComparison] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        try:
            # Flatten the nested customMetric object before validation, since
            # MonteCarloComparison.custom_metric is Optional[str].
            cm = entry.get("custom_metric")
            if isinstance(cm, dict):
                entry = {**entry, "custom_metric": cm.get("metric_name")}
            parsed.append(MonteCarloComparison.model_validate(entry))
        except Exception:
            # Drop a malformed comparison rather than failing the monitor; the
            # builder's no-comparisons path still emits a valid assertion.
            continue
    return parsed


class MonteCarloAssertionDef(BaseModel):
    """A monitor or custom rule, normalized into a single assertion definition."""

    uuid: str
    name: Optional[str] = None
    description: Optional[str] = None
    monitor_type: Optional[str] = None
    rule_type: Optional[str] = None
    custom_sql: Optional[str] = None
    # whereCondition is a row-filter WHERE clause on metric/comparison
    # monitors, NOT the monitor's SQL body (customSql was removed from Monitor).
    where_condition: Optional[str] = None
    entity_mcons: List[str] = Field(default_factory=list)
    resource_id: Optional[str] = None
    severity: Optional[str] = None
    # priority is the renamed severity (removed from Monitor/CustomRule);
    # assertion.py uses it as the severity fallback.
    priority: Optional[str] = None
    data_quality_dimension: Optional[str] = None
    comparisons: List[MonteCarloComparison] = Field(default_factory=list)

    @property
    def native_type(self) -> str:
        """Monte Carlo's native monitor/rule type, used as the CUSTOM assertion type."""
        return self.monitor_type or self.rule_type or "MONITOR"


class MonteCarloFieldTypeChange(BaseModel):
    """A single column type change from getSchemaChanges.fieldTypeChanges."""

    field_name: str
    old_field_type: str
    new_field_type: str


class MonteCarloSchemaChange(BaseModel):
    """One catalog schema-change event from getSchemaChanges."""

    mcon: str
    start_time: Optional[datetime] = None
    fields_added: List[str] = Field(default_factory=list)
    fields_removed: List[str] = Field(default_factory=list)
    field_type_changes: List[MonteCarloFieldTypeChange] = Field(default_factory=list)

    @field_validator("start_time", mode="before")
    @classmethod
    def _coerce_start_time(cls, value: Any) -> Optional[datetime]:
        return _coerce_iso_datetime(value)


def _coerce_iso_datetime(value: Any) -> Optional[datetime]:
    """Parse Monte Carlo ISO timestamps; null malformed values instead of aborting."""
    if value is None or isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _as_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def closest_schema_change(
    events: List[MonteCarloSchemaChange],
    alert_time: datetime,
    max_delta_seconds: int = SCHEMA_CHANGE_MATCH_MAX_DELTA_SECONDS,
) -> Optional[MonteCarloSchemaChange]:
    """Pick the catalog event nearest to ``alert_time`` within ``max_delta_seconds``.

    SCHEMA_CHANGES alerts do not carry field names; getSchemaChanges does.
    The live join key is temporal: collection writes the SchemaChange a few
    seconds before the alert. Events outside the window are ignored so an
    older unrelated DDL is not attached.
    """
    aware_alert = _as_aware_utc(alert_time)
    best: Optional[MonteCarloSchemaChange] = None
    best_delta: Optional[float] = None
    for event in events:
        if event.start_time is None:
            continue
        delta = abs((_as_aware_utc(event.start_time) - aware_alert).total_seconds())
        if delta > max_delta_seconds:
            continue
        if best_delta is None or delta < best_delta:
            best = event
            best_delta = delta
    return best


class MonteCarloAlert(BaseModel):
    """An alert/incident raised by Monte Carlo, mapped to an assertion failure."""

    uuid: str
    alert_type: Optional[str] = None
    sub_types: List[str] = Field(default_factory=list)
    title: Optional[str] = None
    severity: Optional[str] = None
    priority: Optional[str] = None
    status: Optional[str] = None
    created_time: Optional[datetime] = None
    monitor_uuids: List[str] = Field(default_factory=list)
    asset_mcons: List[str] = Field(default_factory=list)
    # Catalog field diffs joined from getSchemaChanges for SCHEMA_CHANGES
    # alerts, keyed by the alert's asset MCON. Empty for other alert types
    # and when no catalog event falls within the match window.
    schema_changes_by_mcon: Dict[str, MonteCarloSchemaChange] = Field(
        default_factory=dict
    )

    @field_validator("created_time", mode="before")
    @classmethod
    def _coerce_created_time(cls, value: Any) -> Optional[datetime]:
        # Monte Carlo returns createdTime as an ISO string; tolerate missing,
        # non-ISO or otherwise malformed values by nulling rather than letting a
        # ValidationError abort the whole alert page (build_run_event already
        # guards against a missing timestamp).
        return _coerce_iso_datetime(value)


class ResolvedTable(BaseModel):
    """Resolution of an MCON to a concrete warehouse table + connection type."""

    mcon: str
    full_table_id: str = Field(min_length=1)
    connection_type: Optional[str] = None


class MonteCarloJobExecution(BaseModel):
    """A single monitor run from getJobExecutions. monitor_uuid is carried from
    the caller (the API is queried per-monitor and the response doesn't echo it
    back), so the builder can join the execution to the ingested assertion."""

    job_execution_uuid: str
    monitor_uuid: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: Optional[str] = None  # JobExecutionStatus enum value as string
    exceptions: Optional[str] = None
    total_result_count: Optional[int] = None
    evaluated_record_count: Optional[int] = None

    @field_validator("start_time", "end_time", mode="before")
    @classmethod
    def _coerce_datetime(cls, value: Any) -> Optional[datetime]:
        # Monte Carlo returns these as ISO strings; tolerate missing, non-ISO or
        # otherwise malformed values by nulling rather than letting a
        # ValidationError abort the whole job-execution page (the builder guards
        # against a missing timestamp). Mirrors MonteCarloAlert._coerce_created_time.
        if value is None or isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None


class MonteCarloMetricPoint(BaseModel):
    """One measured metric value from getMetricsV4. job_execution_uuid is null
    for table-level metrics, so a per-run join is not possible — the value is a
    best-effort temporal correlation to the latest run, not a proven join."""

    metric: str
    value: float
    field: Optional[str] = None
    measurement_timestamp: Optional[datetime] = None
    upper_threshold: Optional[float] = None
    lower_threshold: Optional[float] = None
    job_execution_uuid: Optional[str] = None


class MonteCarloClient:
    """Thin wrapper over ``pycarlo.core.Client`` handling auth, pagination and parsing.

    Secrets are injected programmatically via ``pycarlo.core.Session`` rather than the
    environment, per the credential-handling rule in AGENTS.md.
    """

    def __init__(
        self,
        config: MonteCarloSourceConfig,
        page_size: int = 100,
        report: Optional[MonteCarloSourceReport] = None,
    ) -> None:
        # Imported lazily so the dependency is only required when the source runs.
        from pycarlo.core import Client, Session

        session_kwargs: Dict[str, Any] = {
            "mcd_id": config.api_id,
            "mcd_token": config.api_token.get_secret_value(),
        }
        if config.api_endpoint:
            session_kwargs["endpoint"] = config.api_endpoint
        self._client = Client(session=Session(**session_kwargs))
        self.config = config
        self.page_size = page_size
        self.report = report

        self._token_bucket: Optional[TokenBucket] = None
        if config.rate_limit_requests_per_second:
            self._token_bucket = TokenBucket(
                rate=config.rate_limit_requests_per_second,
                # Floor the default burst at 1 token so a single request always
                # passes immediately; a sub-1/s rate would otherwise make even
                # the first call wait (capacity < 1).
                capacity=config.rate_limit_burst
                or max(1.0, config.rate_limit_requests_per_second),
            )
        self._daily_budget: Optional[DailyCallBudget] = None
        if config.rate_limit_daily:
            self._daily_budget = DailyCallBudget(config.rate_limit_daily)
        # Builds the drift-prone queries (monitors/custom rules/alerts) dynamically
        # from the live MCD schema so a removed field degrades gracefully instead
        # of failing the whole fetch with a 400. Tests inject a StaticQueryBuilder
        # (via set_query_builder) to bypass introspection; the default introspects
        # once per type (cached). Stored on _query_builder_impl so the
        # _query_builder property can fall back to a StaticQueryBuilder for tests
        # that construct the client via __new__ (skipping __init__).
        self._query_builder_impl: Optional[QueryBuilder] = IntrospectingQueryBuilder(
            self._call, self._warn, fatal_error_types=_FATAL_RUN_ERRORS
        )

    @property
    def _query_builder(self) -> QueryBuilder:
        builder = getattr(self, "_query_builder_impl", None)
        if builder is None:
            return StaticQueryBuilder()
        return builder

    def set_query_builder(self, builder: QueryBuilder) -> None:
        """Inject a query builder (used by tests to bypass introspection)."""
        self._query_builder_impl = builder

    def check_schema_drift(self, strict: bool = False) -> SchemaDrift:
        """Introspect the live Monitor/CustomRule/Alert types and diff them
        against the connector's desired field set. Returns a SchemaDrift whose
        verdict is PROCEED/DEGRADED/ABORT. Introspection is cached for the run;
        a transient introspection failure never fabricates an ABORT (it returns
        PROCEED and a warning was already reported by the builder). Only the
        types actually being ingested are checked (monitors/rules when
        include_assertions, alerts when include_alerts)."""
        types: list = []
        if self.config.include_assertions:
            types.extend(["Monitor", "CustomRule"])
        if self.config.include_alerts:
            types.append("Alert")
        return self._query_builder.check_drift(strict, types=types)

    def _warn(
        self,
        title: str,
        message: str,
        context: str,
        exc: Optional[BaseException] = None,
    ) -> None:
        """Surface a dropped/malformed record in the ingestion report when a report
        is available, falling back to the logger (e.g. during test_connection)."""
        if self.report is not None:
            self.report.warning(title=title, message=message, context=context, exc=exc)
        else:
            logger.warning("%s (%s)", message, context, exc_info=exc)

    def _report_missing_id(self, kind: str, raw: Dict[str, Any]) -> None:
        self._warn(
            title="Skipped item with missing id",
            message="Monte Carlo returned an item without an id/uuid; skipping it.",
            context=f"kind={kind}, raw={raw!r}",
        )

    def _safe_call(
        self,
        query: str,
        variables: Dict[str, Any],
        *,
        title: str,
        message: str,
        context: str,
    ) -> Optional[Dict[str, Any]]:
        """Call the API, treating (DailyCallBudgetExceeded, MonteCarloAuthError) as
        run-level failures (re-raised unwrapped) and any other failure as a
        recoverable, per-caller-scoped one (warned, returns None)."""
        try:
            return self._call(query, variables)
        except (DailyCallBudgetExceeded, MonteCarloAuthError):
            raise
        except Exception as e:
            self._warn(title=title, message=message, context=context, exc=e)
            return None

    def _attempt_call(self, query: str, variables: Dict[str, Any]) -> Any:
        # Re-acquired on every physical attempt (including 429 retries), since
        # each attempt is a real HTTP request against Monte Carlo's own quota.
        # DailyCallBudgetExceeded is deliberately not caught here — it must
        # propagate unwrapped and unretried, distinct from a Monte Carlo 429.
        if self._daily_budget is not None:
            self._daily_budget.acquire()
        if self._token_bucket is not None:
            self._token_bucket.acquire()
        return self._client(query, variables=variables)

    def _log_retryable_retry(self, retry_state: "tenacity.RetryCallState") -> None:
        # retrying(self._attempt_call, query, variables) — args[0] is query,
        # since self._attempt_call is already bound (no separate self arg here).
        query = retry_state.args[0] if retry_state.args else ""
        outcome = retry_state.outcome
        exc = outcome.exception() if outcome is not None else None
        reason = (
            "rate limited (429)"
            if (exc is not None and getattr(exc, "status_code", None) == 429)
            else "transient network error"
        )
        self._warn(
            title="Monte Carlo API call failed; retrying",
            message=f"{reason}; retrying with backoff.",
            context=f"attempt={retry_state.attempt_number}/{_RATE_LIMIT_MAX_ATTEMPTS}, "
            f"query={str(query)[:60]!r}",
        )

    def _call(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        retrying = tenacity.Retrying(
            retry=tenacity.retry_if_exception(_is_retryable),
            wait=tenacity.wait_exponential_jitter(initial=1.0, max=60.0),
            stop=tenacity.stop_after_attempt(_RATE_LIMIT_MAX_ATTEMPTS),
            before_sleep=self._log_retryable_retry,
            reraise=True,
        )
        try:
            response = retrying(self._attempt_call, query, variables)
        except DailyCallBudgetExceeded:
            raise
        except Exception as e:
            if _is_auth_error(e):
                raise MonteCarloAuthError(
                    f"Monte Carlo rejected the API credentials (query={query[:60]!r}); "
                    "check api_id / api_token."
                ) from e
            raise RuntimeError(
                f"Monte Carlo API call failed (query={query[:60]!r})"
            ) from e
        if response is None:
            raise RuntimeError(f"Monte Carlo API returned None (query={query[:60]!r})")
        # pycarlo returns a Box (dict-like); normalize to a plain dict so the rest of
        # the code (and mocked unit tests) can use ordinary item access.
        if hasattr(response, "to_dict"):
            return response.to_dict()
        return dict(response)

    def _paginate(
        self, query: str, root_field: str, variables: Dict[str, Any]
    ) -> Iterable[Dict[str, Any]]:
        # pycarlo normalizes response keys to snake_case (Box camel_killer_box),
        # so root_field and every nested key below must be looked up in snake_case
        # even though the GraphQL query text itself uses camelCase field names.
        #
        # A malformed page (missing root field / edges) or a repeated endCursor
        # raises rather than being treated as an empty page: silently truncating
        # pagination would emit incomplete assertions and could trigger stale
        # deletion of existing ones. The raised RuntimeError is caught by
        # source._emit and surfaced as a phase-level report.failure.
        after: Optional[str] = None
        seen_cursors: set = set()
        while True:
            page_vars = {**variables, "first": self.page_size, "after": after}
            connection = self._call(query, page_vars).get(root_field)
            if not isinstance(connection, dict):
                raise RuntimeError(
                    f"Monte Carlo API returned a malformed response for "
                    f"{root_field} (expected a connection object, got "
                    f"{type(connection).__name__}); aborting pagination."
                )
            edges = connection.get("edges")
            if edges is None:
                raise RuntimeError(
                    f"Monte Carlo API returned a malformed response for "
                    f"{root_field} (missing 'edges'); aborting pagination."
                )
            for edge in edges or []:
                node = edge.get("node") if isinstance(edge, dict) else None
                if node:
                    yield node
            page_info = connection.get("page_info") or {}
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                raise RuntimeError(
                    f"Monte Carlo API indicated more pages for {root_field} "
                    f"but returned no endCursor; cannot continue pagination."
                )
            if after in seen_cursors:
                raise RuntimeError(
                    f"Monte Carlo API returned a repeated endCursor for "
                    f"{root_field} (cursor={after!r}); aborting to avoid an "
                    f"infinite pagination loop."
                )
            seen_cursors.add(after)

    def _paginate_offset(
        self, query: str, root_field: str, variables: Dict[str, Any]
    ) -> Iterable[Dict[str, Any]]:
        # getMonitors returns a plain list rather than a Relay connection, so it is
        # walked with limit/offset (instead of _paginate's cursor) and stops once a
        # short page signals the last batch. A missing/non-list root field raises
        # (see _paginate) rather than being treated as an empty page.
        offset = 0
        while True:
            page_vars = {**variables, "limit": self.page_size, "offset": offset}
            page = self._call(query, page_vars).get(root_field)
            if not isinstance(page, list):
                raise RuntimeError(
                    f"Monte Carlo API returned a malformed response for "
                    f"{root_field} (expected a list, got {type(page).__name__}); "
                    f"aborting pagination."
                )
            yield from page
            if len(page) < self.page_size:
                break
            offset += self.page_size

    def get_monitors(self) -> Iterable[MonteCarloAssertionDef]:
        variables: Dict[str, Any] = {}
        if self.config.domain_ids:
            variables["domainIds"] = self.config.domain_ids
        for raw in self._paginate_offset(
            self._query_builder.monitors_query(), "get_monitors", variables
        ):
            uuid = raw.get("uuid")
            if not uuid:
                self._report_missing_id("monitor", raw)
                continue
            monitor_type = raw.get("monitor_type")
            if not self.config.monitor_type_pattern.allowed(monitor_type or ""):
                continue
            entity_mcons = raw.get("entity_mcons") or []
            resource_id = raw.get("resource_id")
            if not entity_mcons and (monitor_type or "").upper() == "TABLE":
                entity_mcons = self._resolve_table_monitor_entity_mcons(
                    uuid, resource_id
                )
            try:
                yield MonteCarloAssertionDef(
                    uuid=uuid,
                    name=raw.get("name"),
                    description=raw.get("description"),
                    monitor_type=monitor_type,
                    custom_sql=raw.get("custom_sql"),
                    where_condition=raw.get("where_condition"),
                    entity_mcons=entity_mcons,
                    resource_id=resource_id,
                    severity=raw.get("severity"),
                    priority=raw.get("priority"),
                    data_quality_dimension=raw.get("data_quality_dimension"),
                    comparisons=_parse_comparisons(raw.get("comparisons")),
                )
            except _FATAL_RUN_ERRORS:
                raise
            except Exception as e:
                self._warn(
                    title="Skipped malformed monitor",
                    message=(
                        "Could not parse a monitor row from Monte Carlo; skipping it."
                    ),
                    context=f"monitor_uuid={uuid}, raw={raw!r}",
                    exc=e,
                )

    def _resolve_table_monitor_entity_mcons(
        self, monitor_uuid: str, resource_id: Optional[str]
    ) -> List[str]:
        """Resolve a TABLE monitor's entity MCONs via evaluateAssetSelection.

        getMonitors' entityMcons is only populated for single-entity METRIC
        monitors; TABLE monitors cover many tables via asset_selection instead.
        getTableMonitor returns that selection (filters, exclusions, databases);
        evaluateAssetSelection expands it to concrete table MCONs.

        An empty or missing selection is not evaluated — empty input would
        select every table in the warehouse. Failures other than
        (DailyCallBudgetExceeded, MonteCarloAuthError) are demoted to a
        per-monitor warning via _safe_call.
        """
        response = self._safe_call(
            TABLE_MONITOR_QUERY,
            {"monitorUuid": monitor_uuid},
            title="Could not resolve table monitor scope",
            message="getTableMonitor call failed; this monitor's entities "
            "cannot be resolved and it will be skipped.",
            context=f"monitor_uuid={monitor_uuid}",
        )
        if response is None:
            return []
        monitor = response.get("get_table_monitor") or {}
        warehouse_uuid = resource_id or monitor.get("warehouse_uuid")
        if not warehouse_uuid:
            self._warn(
                title="Table monitor has no warehouse",
                message="Cannot evaluate TABLE monitor scope without a warehouse "
                "UUID; skipping this monitor.",
                context=f"monitor_uuid={monitor_uuid}",
            )
            return []
        asset_selection = self._asset_selection_to_input(
            monitor.get("asset_selection") or {}
        )
        if asset_selection is None:
            self._warn(
                title="Table monitor has empty asset selection",
                message="TABLE monitor has no databases, filters, or exclusions; "
                "refusing to evaluate against the whole warehouse. Skipping.",
                context=f"monitor_uuid={monitor_uuid}",
            )
            return []
        return self._evaluate_asset_selection_mcons(
            monitor_uuid=monitor_uuid,
            warehouse_uuid=warehouse_uuid,
            asset_selection=asset_selection,
        )

    def _asset_selection_to_input(
        self, raw: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Convert getTableMonitor's snake_cased AssetSelection into the
        camelCase AssetSelectionInput evaluateAssetSelection expects.

        Returns None when the selection is empty (no databases, filters, or
        exclusions) so callers do not evaluate an unbounded warehouse query.
        """
        databases_in = [
            self._database_to_input(d)
            for d in (raw.get("databases") or [])
            if isinstance(d, dict)
        ]
        databases = [d for d in databases_in if d is not None]
        filters = [
            converted
            for converted in (
                self._filter_to_input(f)
                for f in (raw.get("filters") or [])
                if isinstance(f, dict)
            )
            if converted is not None
        ]
        exclusions = [
            converted
            for converted in (
                self._filter_to_input(f)
                for f in (raw.get("exclusions") or [])
                if isinstance(f, dict)
            )
            if converted is not None
        ]
        if not databases and not filters and not exclusions:
            return None
        return {
            "databases": databases,
            "filters": filters,
            "filtersJoiner": raw.get("filters_joiner") or ASSET_FILTER_JOINER_OR,
            "exclusions": exclusions,
            "exclusionsJoiner": raw.get("exclusions_joiner") or ASSET_FILTER_JOINER_OR,
        }

    @staticmethod
    def _database_to_input(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        name = raw.get("name")
        if not name:
            return None
        converted: Dict[str, Any] = {"name": name}
        schemas = raw.get("schemas")
        if schemas:
            converted["schemas"] = schemas
        return converted

    @staticmethod
    def _filter_to_input(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        filter_type = raw.get("type")
        if not filter_type:
            return None
        converted: Dict[str, Any] = {"type": filter_type}
        if raw.get("negated") is not None:
            converted["negated"] = raw["negated"]
        for snake_name, camel_name in ASSET_FILTER_INPUT_FIELDS.items():
            value = raw.get(snake_name)
            if value is not None:
                converted[camel_name] = value
        return converted

    def _evaluate_asset_selection_mcons(
        self,
        monitor_uuid: str,
        warehouse_uuid: str,
        asset_selection: Dict[str, Any],
    ) -> List[str]:
        """Page evaluateAssetSelection and collect selected table MCONs, capped
        by table_monitor_max_assets so a warehouse-wide TABLE monitor cannot
        explode assertion count / API spend."""
        max_assets = self.config.table_monitor_max_assets
        mcons: List[str] = []
        offset = 0
        truncated = False
        incomplete = False
        while len(mcons) < max_assets:
            response = self._safe_call(
                EVALUATE_ASSET_SELECTION_QUERY,
                {
                    "warehouseUuid": warehouse_uuid,
                    "assetSelection": asset_selection,
                    "monitorUuid": monitor_uuid,
                    "limit": self.page_size,
                    "offset": offset,
                },
                title="Could not evaluate table monitor scope",
                message="evaluateAssetSelection call failed; this monitor's "
                "entities cannot be resolved and it will be skipped."
                if not mcons
                else "evaluateAssetSelection failed after some tables were "
                "already resolved; remaining pages were skipped.",
                context=f"monitor_uuid={monitor_uuid}, offset={offset}",
            )
            if response is None:
                if not mcons:
                    return []
                incomplete = True
                break
            page = response.get("evaluate_asset_selection")
            if not isinstance(page, list):
                self._warn(
                    title="Malformed evaluateAssetSelection response",
                    message="Expected a list of asset selection results; "
                    "stopping pagination for this monitor.",
                    context=f"monitor_uuid={monitor_uuid}, got={type(page).__name__}",
                )
                if mcons:
                    incomplete = True
                else:
                    return []
                break
            if not page:
                break
            selected_mcons: List[str] = []
            for row in page:
                if not isinstance(row, dict) or row.get("selected") is False:
                    continue
                mcon = row.get("mcon")
                if isinstance(mcon, str) and mcon:
                    selected_mcons.append(mcon)
            remaining = max_assets - len(mcons)
            if len(selected_mcons) > remaining:
                truncated = True
            mcons.extend(selected_mcons[:remaining])
            if len(mcons) >= max_assets:
                if len(page) == self.page_size:
                    truncated = True
                break
            if len(page) < self.page_size:
                break
            offset += self.page_size
        if truncated:
            self._warn(
                title="Table monitor coverage truncated",
                message="TABLE monitor covers more tables than "
                "table_monitor_max_assets; remaining tables were skipped.",
                context=f"monitor_uuid={monitor_uuid}, "
                f"ingested={len(mcons)}, cap={max_assets}",
            )
            if self.report is not None:
                self.report.report_table_monitor_scope_truncated()
        if incomplete:
            self._warn(
                title="Table monitor coverage incomplete",
                message="evaluateAssetSelection failed mid-pagination; "
                "only the tables resolved before the failure were ingested.",
                context=f"monitor_uuid={monitor_uuid}, ingested={len(mcons)}",
            )
            if self.report is not None:
                self.report.report_build_failure()
                self.report.report_table_monitor_scope_truncated()
        return mcons

    def get_custom_rules(self) -> Iterable[MonteCarloAssertionDef]:
        for raw in self._paginate(
            self._query_builder.custom_rules_query(), "get_custom_rules", {}
        ):
            uuid = raw.get("uuid")
            if not uuid:
                self._report_missing_id("custom rule", raw)
                continue
            try:
                yield MonteCarloAssertionDef(
                    uuid=uuid,
                    name=raw.get("rule_name"),
                    description=raw.get("description"),
                    rule_type=raw.get("rule_type"),
                    custom_sql=raw.get("custom_sql"),
                    entity_mcons=raw.get("entity_mcons") or [],
                    severity=raw.get("severity"),
                    priority=raw.get("priority"),
                    comparisons=_parse_comparisons(raw.get("comparisons")),
                )
            except _FATAL_RUN_ERRORS:
                raise
            except Exception as e:
                self._warn(
                    title="Skipped malformed custom rule",
                    message=(
                        "Could not parse a custom rule row from Monte Carlo; "
                        "skipping it."
                    ),
                    context=f"rule_uuid={uuid}, raw={raw!r}",
                    exc=e,
                )

    def get_alerts(self) -> Iterable[MonteCarloAlert]:
        now = datetime.now(tz=timezone.utc)
        start_time = now - timedelta(days=self.config.alerts_lookback_days)
        variables = {
            "createdTime": {"after": start_time.isoformat(), "before": now.isoformat()}
        }
        # One getSchemaChanges call per unique SCHEMA_CHANGES asset MCON
        # for the whole alerts lookback, then pick the closest event per alert.
        schema_cache: Dict[str, Optional[List[MonteCarloSchemaChange]]] = {}
        for raw in self._paginate(
            self._query_builder.alerts_query(), "get_alerts", variables
        ):
            alert_id = raw.get("id")
            if not alert_id:
                self._report_missing_id("alert", raw)
                continue
            try:
                asset_mcons = [
                    mcon
                    for a in (raw.get("assets") or [])
                    if isinstance(a, dict)
                    for mcon in [a.get("mcon")]
                    if isinstance(mcon, str) and mcon
                ]
                alert = MonteCarloAlert(
                    uuid=alert_id,
                    alert_type=raw.get("type"),
                    sub_types=raw.get("sub_types") or [],
                    title=raw.get("title"),
                    severity=raw.get("severity"),
                    priority=raw.get("priority"),
                    status=raw.get("status"),
                    created_time=raw.get("created_time"),
                    monitor_uuids=list(raw.get("monitor_uuids") or []),
                    asset_mcons=asset_mcons,
                )
            except _FATAL_RUN_ERRORS:
                raise
            except Exception as e:
                self._warn(
                    title="Skipped malformed alert",
                    message=(
                        "Could not parse an alert row from Monte Carlo; skipping it."
                    ),
                    context=f"alert_id={alert_id}, raw={raw!r}",
                    exc=e,
                )
                continue
            if (alert.alert_type or "").upper() == ALERT_TYPE_SCHEMA_CHANGES:
                alert.schema_changes_by_mcon = self._schema_changes_for_alert(
                    alert, schema_cache, start_time, now
                )
            yield alert

    def _schema_changes_for_alert(
        self,
        alert: MonteCarloAlert,
        cache: Dict[str, Optional[List[MonteCarloSchemaChange]]],
        window_start: datetime,
        window_end: datetime,
    ) -> Dict[str, MonteCarloSchemaChange]:
        """Join getSchemaChanges onto a SCHEMA_CHANGES alert by asset MCON."""
        if alert.created_time is None:
            return {}
        joined: Dict[str, MonteCarloSchemaChange] = {}
        for mcon in alert.asset_mcons:
            if mcon not in cache:
                cache[mcon] = self._fetch_schema_changes(mcon, window_start, window_end)
            events = cache[mcon]
            if events is None:
                continue
            matched = closest_schema_change(events, alert.created_time)
            if matched is not None:
                joined[mcon] = matched
            else:
                self._warn(
                    title="Schema-change alert has no matching catalog event",
                    message="getSchemaChanges returned no event within the "
                    "match window; this SCHEMA_CHANGES alert will be ingested "
                    "without field-level diffs.",
                    context=f"alert_uuid={alert.uuid}, mcon={mcon}",
                )
                if self.report is not None:
                    self.report.report_schema_change_join_missed()
        return joined

    def _fetch_schema_changes(
        self,
        mcon: str,
        start_time: datetime,
        end_time: datetime,
    ) -> Optional[List[MonteCarloSchemaChange]]:
        """One page of catalog schema history for ``mcon``. Returns None when
        the fetch fails so callers do not treat an empty page as a join miss."""
        response = self._safe_call(
            GET_SCHEMA_CHANGES_QUERY,
            {
                "mcon": mcon,
                "startTime": start_time.isoformat(),
                "endTime": end_time.isoformat(),
                "first": SCHEMA_CHANGE_PAGE_SIZE,
            },
            title="Could not fetch schema changes",
            message="getSchemaChanges call failed; this SCHEMA_CHANGES alert "
            "will be ingested without field-level diffs.",
            context=f"mcon={mcon}",
        )
        if response is None:
            return None
        connection = response.get("get_schema_changes")
        if not isinstance(connection, dict):
            self._warn(
                title="Malformed getSchemaChanges response",
                message="Expected a SchemaChangeConnection; this SCHEMA_CHANGES "
                "alert will be ingested without field-level diffs.",
                context=f"mcon={mcon}, got={type(connection).__name__}",
            )
            return None
        page_info = connection.get("page_info") or {}
        if isinstance(page_info, dict) and page_info.get("has_next_page"):
            self._warn(
                title="Schema change history truncated",
                message="getSchemaChanges has more pages than "
                "SCHEMA_CHANGE_PAGE_SIZE; the closest catalog event may be "
                "missing.",
                context=f"mcon={mcon}",
            )
        events: List[MonteCarloSchemaChange] = []
        for edge in connection.get("edges") or []:
            if not isinstance(edge, dict):
                continue
            node = edge.get("node")
            if not isinstance(node, dict):
                continue
            parsed = self._parse_schema_change(node, fallback_mcon=mcon)
            if parsed is not None:
                events.append(parsed)
        return events

    def _parse_schema_change(
        self, raw: Dict[str, Any], fallback_mcon: str
    ) -> Optional[MonteCarloSchemaChange]:
        mcon = raw.get("mcon") or fallback_mcon
        if not isinstance(mcon, str) or not mcon:
            return None
        field_type_changes: List[MonteCarloFieldTypeChange] = []
        for entry in raw.get("field_type_changes") or []:
            if not isinstance(entry, dict):
                continue
            field_name = entry.get("field_name")
            old_type = entry.get("old_field_type")
            new_type = entry.get("new_field_type")
            if (
                isinstance(field_name, str)
                and field_name
                and old_type is not None
                and new_type is not None
            ):
                field_type_changes.append(
                    MonteCarloFieldTypeChange(
                        field_name=field_name,
                        old_field_type=str(old_type),
                        new_field_type=str(new_type),
                    )
                )
        try:
            return MonteCarloSchemaChange(
                mcon=mcon,
                start_time=raw.get("start_time"),
                fields_added=[
                    f for f in (raw.get("fields_added") or []) if isinstance(f, str)
                ],
                fields_removed=[
                    f for f in (raw.get("fields_removed") or []) if isinstance(f, str)
                ],
                field_type_changes=field_type_changes,
            )
        except Exception as e:
            self._warn(
                title="Skipped malformed schema change",
                message="Could not parse a getSchemaChanges row; skipping it.",
                context=f"mcon={mcon}, raw={raw!r}",
                exc=e,
            )
            return None

    def get_table(self, mcon: str) -> Optional[ResolvedTable]:
        table = self._call(GET_TABLE_QUERY, {"mcon": mcon}).get("get_table")
        if not table:
            return None
        full_table_id = table.get("full_table_id")
        if not full_table_id:
            self._warn(
                title="Monte Carlo asset has no table id",
                message="getTable returned no fullTableId; the asset cannot be resolved "
                "to a dataset URN and is skipped.",
                context=f"mcon={mcon}",
            )
            return None
        warehouse = table.get("warehouse") or {}
        return ResolvedTable(
            mcon=table.get("mcon", mcon),
            full_table_id=full_table_id,
            connection_type=warehouse.get("connection_type"),
        )

    def get_job_executions(
        self, monitor_uuid: str, history_days: int, first: int
    ) -> List[MonteCarloJobExecution]:
        """Fetch the most-recent monitor runs (most-recent-first). Does NOT
        paginate — first caps the page, so callers wanting only the latest N
        runs should set first=N. Returns all runs in the page (any status);
        the builder filters for SUCCESS."""
        response = self._safe_call(
            GET_JOB_EXECUTIONS_QUERY,
            {
                "monitorUuid": monitor_uuid,
                "historyDays": history_days,
                "first": first,
            },
            title="Could not fetch monitor run history",
            message="getJobExecutions call failed; run events for this "
            "monitor will not be emitted.",
            context=f"monitor_uuid={monitor_uuid}",
        )
        if response is None:
            return []
        edges = ((response.get("get_job_executions") or {}).get("edges")) or []
        executions: List[MonteCarloJobExecution] = []
        for edge in edges:
            node = (edge or {}).get("node") or {}
            uuid = node.get("job_execution_uuid")
            if not uuid:
                continue
            try:
                executions.append(
                    MonteCarloJobExecution(
                        job_execution_uuid=uuid,
                        monitor_uuid=monitor_uuid,
                        start_time=node.get("start_time"),
                        end_time=node.get("end_time"),
                        status=node.get("status"),
                        exceptions=node.get("exceptions"),
                        total_result_count=node.get("total_result_count"),
                        evaluated_record_count=node.get("evaluated_record_count"),
                    )
                )
            except Exception as exc:
                # Isolate a single malformed execution so one bad record doesn't
                # drop every SUCCESS run for this monitor. The datetime coercer
                # above handles the common timestamp case; this catches any other
                # field-level parse failure (e.g. a non-int total_result_count).
                self._warn(
                    title="Skipped malformed monitor run",
                    message="Monte Carlo returned a monitor run that could not be "
                    "parsed; skipping it. Other runs for this monitor are unaffected.",
                    context=f"monitor_uuid={monitor_uuid}, "
                    f"job_execution_uuid={uuid}, error={exc!r}",
                    exc=exc,
                )
        return executions

    def get_metrics_v4(
        self,
        mcon: str,
        metric_name: str,
        start_time: datetime,
        field: Optional[str] = None,
        first: int = 1,
    ) -> List[MonteCarloMetricPoint]:
        """Fetch measured values for one metric on one asset. startTime is
        required by the MCD schema. first=1 + deduplicateValues=true returns
        only the most-recent point. field is required for field-level metrics
        (null_rate, distinct_count, etc.); omit for table-level metrics."""
        metrics_filter: Dict[str, Any] = {"mcon": mcon}
        if field:
            metrics_filter["field"] = field
        response = self._safe_call(
            GET_METRICS_V4_QUERY,
            {
                "metricName": metric_name,
                "metricsFilter": metrics_filter,
                "startTime": start_time.isoformat(),
                "first": first,
                "deduplicateValues": True,
            },
            title="Could not fetch Monte Carlo metrics",
            message="getMetricsV4 call failed; measured values for this "
            "monitor will not be attached to its run events.",
            context=f"mcon={mcon}, metric={metric_name}",
        )
        if response is None:
            return []
        points = ((response.get("get_metrics_v4") or {}).get("metrics")) or []
        parsed: List[MonteCarloMetricPoint] = []
        for p in points:
            try:
                thresholds = p.get("thresholds") or []
                upper = lower = None
                if thresholds:
                    t0 = thresholds[0] or {}
                    upper = t0.get("upper")
                    lower = t0.get("lower")
                parsed.append(
                    MonteCarloMetricPoint(
                        metric=p.get("metric") or metric_name,
                        value=float(p.get("value")),
                        field=p.get("field"),
                        measurement_timestamp=p.get("measurement_timestamp"),
                        upper_threshold=upper,
                        lower_threshold=lower,
                        job_execution_uuid=p.get("job_execution_uuid"),
                    )
                )
            except (TypeError, ValueError):
                continue
        return parsed
