import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

import tenacity
from pydantic import BaseModel, Field, field_validator

from datahub.ingestion.source.montecarlo.config import MonteCarloSourceConfig
from datahub.ingestion.source.montecarlo.queries import (
    ALERTS_QUERY,
    CUSTOM_RULES_QUERY,
    GET_JOB_EXECUTIONS_QUERY,
    GET_METRICS_V4_QUERY,
    GET_TABLE_BY_FULL_TABLE_ID_QUERY,
    GET_TABLE_QUERY,
    MONITORS_QUERY,
    TABLE_MONITOR_QUERY,
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
    entity_mcons: List[str] = Field(default_factory=list)
    resource_id: Optional[str] = None
    severity: Optional[str] = None
    data_quality_dimension: Optional[str] = None
    comparisons: List[MonteCarloComparison] = Field(default_factory=list)

    @property
    def native_type(self) -> str:
        """Monte Carlo's native monitor/rule type, used as the CUSTOM assertion type."""
        return self.monitor_type or self.rule_type or "MONITOR"


class MonteCarloAlert(BaseModel):
    """An alert/incident raised by Monte Carlo, mapped to an assertion failure."""

    uuid: str
    alert_type: Optional[str] = None
    sub_types: List[str] = Field(default_factory=list)
    severity: Optional[str] = None
    priority: Optional[str] = None
    status: Optional[str] = None
    created_time: Optional[datetime] = None
    monitor_uuids: List[str] = Field(default_factory=list)
    asset_mcons: List[str] = Field(default_factory=list)

    @field_validator("created_time", mode="before")
    @classmethod
    def _coerce_created_time(cls, value: Any) -> Optional[datetime]:
        # Monte Carlo returns createdTime as an ISO string; tolerate missing,
        # non-ISO or otherwise malformed values by nulling rather than letting a
        # ValidationError abort the whole alert page (build_run_event already
        # guards against a missing timestamp).
        if value is None or isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None


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
        for raw in self._paginate_offset(MONITORS_QUERY, "get_monitors", variables):
            uuid = raw.get("uuid")
            if not uuid:
                self._report_missing_id("monitor", raw)
                continue
            monitor_type = raw.get("monitor_type")
            if not self.config.monitor_type_pattern.allowed(monitor_type or ""):
                continue
            entity_mcons = raw.get("entity_mcons") or []
            resource_id = raw.get("resource_id")
            if not entity_mcons and monitor_type == "TABLE" and resource_id:
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
                    entity_mcons=entity_mcons,
                    resource_id=resource_id,
                    severity=raw.get("severity"),
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
        self, monitor_uuid: str, resource_id: str
    ) -> List[str]:
        """Resolve a TABLE monitor's entity MCONs via its asset_selection filters.

        getMonitors' entityMcons is only populated for single-entity METRIC
        monitors; TABLE monitors cover many tables via asset_selection instead.
        Only the FULL_TABLE_ID filter case (an explicit, fixed table list) is
        handled here — pattern-based filters (TABLE_NAME, TABLE_TAG, activity
        filters) would need evaluateAssetSelection to resolve dynamically,
        which isn't implemented yet.

        Deliberately catches Exception broadly (not just an API-specific
        error type) and demotes any failure to a per-monitor warning, matching
        this source's continue-on-recoverable-error philosophy (see _emit in
        source.py) rather than aborting the whole run over one monitor's
        resolution failing. (DailyCallBudgetExceeded, MonteCarloAuthError) are
        the exceptions this does NOT apply to: an exhausted daily quota or a
        rejected credential is a run-level failure, not a per-monitor one, so
        both are re-raised as-is (see MonteCarloClient._safe_call).
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
        monitor = response.get("get_table_monitor")
        full_table_ids = [
            f.get("full_table_id")
            for f in ((monitor or {}).get("asset_selection") or {}).get("filters") or []
            if isinstance(f, dict)
            and f.get("type") == "FULL_TABLE_ID"
            and f.get("full_table_id")
        ]
        mcons = []
        for full_table_id in full_table_ids:
            table_response = self._safe_call(
                GET_TABLE_BY_FULL_TABLE_ID_QUERY,
                {"dwId": resource_id, "fullTableId": full_table_id},
                title="Could not resolve table monitor's full_table_id",
                message="getTable call failed for a FULL_TABLE_ID filter; "
                "skipping this table.",
                context=f"monitor_uuid={monitor_uuid}, full_table_id={full_table_id}",
            )
            if table_response is None:
                continue
            mcon = (table_response.get("get_table") or {}).get("mcon")
            if mcon:
                mcons.append(mcon)
        return mcons

    def get_custom_rules(self) -> Iterable[MonteCarloAssertionDef]:
        for raw in self._paginate(CUSTOM_RULES_QUERY, "get_custom_rules", {}):
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
        for raw in self._paginate(ALERTS_QUERY, "get_alerts", variables):
            alert_id = raw.get("id")
            if not alert_id:
                self._report_missing_id("alert", raw)
                continue
            try:
                yield MonteCarloAlert(
                    uuid=alert_id,
                    alert_type=raw.get("type"),
                    sub_types=raw.get("sub_types") or [],
                    severity=raw.get("severity"),
                    priority=raw.get("priority"),
                    status=raw.get("status"),
                    created_time=raw.get("created_time"),
                    monitor_uuids=list(raw.get("monitor_uuids") or []),
                    asset_mcons=[
                        a.get("mcon")
                        for a in (raw.get("assets") or [])
                        if a.get("mcon")
                    ],
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
