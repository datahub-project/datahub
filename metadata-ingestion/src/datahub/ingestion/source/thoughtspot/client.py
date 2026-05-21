"""ThoughtSpot REST API v2.0 client wrapper using official SDK.

This module provides a client for interacting with the ThoughtSpot REST API v2.0
using the official thoughtspot_rest_api Python SDK. It handles authentication,
error handling, and provides convenience methods for metadata extraction.
"""

import logging
import re
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    ParamSpec,
    Type,
    TypeVar,
)

import yaml
from requests.adapters import HTTPAdapter
from thoughtspot_rest_api import TSRestApiV2
from urllib3.util.retry import Retry

from datahub.ingestion.source.thoughtspot.config import (
    PasswordAuth,
    ThoughtSpotConnectionConfig,
    TrustedAuth,
)
from datahub.ingestion.source.thoughtspot.models import (
    COLUMN_SOURCE_DROPS,
    VISUALIZATION_DROPS,
    AnswerResponse,
    ConnectionResponse,
    LiveboardResponse,
    LogicalTableResponse,
    TagResponse,
    VisualizationResponse,
    WorkspaceResponse,
)
from datahub.ingestion.source.thoughtspot.report import ThoughtSpotReport

logger = logging.getLogger(__name__)

_P = ParamSpec("_P")
_R = TypeVar("_R")

# Hard cap on mid-run re-authentication attempts. Long ingestion runs
# legitimately need one or two refreshes (50-minute token TTL); anything
# beyond this points at a permanently-failed principal (e.g. disabled
# between mint and retry) and we should surface a hard error instead of
# silently looping with ``report.info`` entries that masquerade as
# recovery.
_MAX_REAUTH_ATTEMPTS = 3

# ThoughtSpot search queries reference worksheet columns inside square
# brackets, e.g. "[Revenue] [Region] >= 'EMEA'". We extract the bracketed
# names verbatim and let downstream code map them to worksheet columns.
_SEARCH_QUERY_COLUMN_RE = re.compile(r"\[([^\[\]]+)\]")


def _extract_search_query_columns(search_query: Any) -> List[str]:
    """Parse ``[bracketed]`` column tokens from a TML ``answer.search_query``.

    Used by both the Liveboard visualization and Answer TML paths to
    populate ``source_columns`` for Chart→Dataset column-level lineage
    (the ``InputFields`` aspect). Order-preserving de-dup so a chart
    that references the same column twice doesn't emit duplicate
    edges. Non-string inputs (TML schema drift) return ``[]`` so the
    caller can no-op without a type check.

    Why not ``answer.answer_columns[]``? Despite the name, that field
    carries the chart's *display* column labels — post-aggregation
    metric names like ``"Total Credits"`` or ``"Month(TS Query Start
    Time)"`` — not the underlying worksheet column refs. A live smoke
    showed that preferring ``answer_columns`` over ``search_query``
    regressed 79 KPI tiles whose display labels don't match any
    worksheet schema entry. The ``search_query`` ``[bracket]`` tokens
    remain the correct source for source-column references.
    """
    if not isinstance(search_query, str) or not search_query:
        return []
    seen: set = set()
    columns: List[str] = []
    for match in _SEARCH_QUERY_COLUMN_RE.findall(search_query):
        col_name = match.strip()
        if col_name and col_name not in seen:
            seen.add(col_name)
            columns.append(col_name)
    return columns


def _extract_sql_view_statement(tml: Dict[str, Any]) -> Optional[str]:
    """Extract the raw SQL statement from a ``SQL_VIEW`` TML edoc.

    Field shape (verified against TS Cloud techpartners tenant on
    2026-05-13): ``sql_view.sql_query`` is a **string** carrying the
    SQL directly. Older / hypothetical alternate shapes are accepted
    defensively:

    - ``sql_view.sql_query`` (str): preferred, current TS Cloud shape.
    - ``sql_view.sql_query.statement`` (dict.str): hypothetical
      nested form mentioned in older TS docs.
    - ``sql_view.statement`` (str): defensive flat fallback.

    Returns ``None`` when none of the above holds a non-empty string
    — caller emits the dataset without ``viewLogic`` rather than
    attaching an empty aspect.
    """
    sv = tml.get("sql_view")
    if not isinstance(sv, dict):
        return None
    sq = sv.get("sql_query")
    if isinstance(sq, str) and sq.strip():
        return sq
    if isinstance(sq, dict):
        nested = sq.get("statement")
        if isinstance(nested, str) and nested.strip():
            return nested
    flat = sv.get("statement")
    if isinstance(flat, str) and flat.strip():
        return flat
    return None


def _extract_answer_chart_type(answer_tml: Dict[str, Any]) -> Optional[str]:
    """Extract the chart-type label (``PIE``, ``COLUMN``, …) from a TML
    ``answer`` block.

    TS Cloud 26.x stores the chart type at ``answer.chart.type`` and
    leaves the legacy top-level ``answer.chart_type`` unset — older
    docs still reference the top-level field, so we prefer the
    nested path with a fallback. Returns ``None`` when neither
    location holds a non-empty string so callers can omit the
    ``thoughtspot_chart_type`` custom property entirely rather than
    emitting it as an empty string.
    """
    chart = answer_tml.get("chart")
    if isinstance(chart, dict):
        nested = chart.get("type")
        if isinstance(nested, str) and nested:
            return nested
    legacy = answer_tml.get("chart_type")
    if isinstance(legacy, str) and legacy:
        return legacy
    return None


# ThoughtSpot's ``dataSourceTypeEnum`` → DataHub platform name. Driving
# table for cross-platform lineage: a TS Logical Table backed by a
# connection of type "DATABRICKS" emits its upstream as a Databricks
# DataHub dataset URN.
#
# ``DEFAULT`` / ``FALCON`` are intentionally absent — they're TS's
# sentinels for in-memory or CSV-uploaded data, which has no external
# upstream and is skipped silently at the source layer.
_TS_TO_DATAHUB_PLATFORM: Dict[str, str] = {
    "DATABRICKS": "databricks",
    "SNOWFLAKE": "snowflake",
    "BIGQUERY": "bigquery",
    "REDSHIFT": "redshift",
    "SYNAPSE": "mssql",  # Synapse uses MSSQL URN conventions.
    "ORACLE": "oracle",
    "MSSQL": "mssql",
    "POSTGRES": "postgres",
    "MYSQL": "mysql",
    "TERADATA": "teradata",
    "PRESTO": "presto",
    "TRINO": "trino",
    "ATHENA": "athena",
    "SAPHANA": "hana",
    "DENODO": "denodo",
}


# Each platform has its own canonical dataset-key shape; this table is
# the single source of truth so adding a platform requires editing one
# spot. Schema-less platforms (MySQL, Teradata, Athena) collapse the
# schema component when absent.
_KEY_BUILDERS: Dict[str, Callable[[Optional[str], Optional[str], str], str]] = {
    "databricks": lambda db, sch, tbl: ".".join(p for p in (db, sch, tbl) if p),
    "snowflake": lambda db, sch, tbl: f"{db}.{sch}.{tbl}",
    "bigquery": lambda db, sch, tbl: f"{db}.{sch}.{tbl}",
    "redshift": lambda db, sch, tbl: f"{db}.{sch}.{tbl}",
    "mssql": lambda db, sch, tbl: f"{db}.{sch}.{tbl}",
    "oracle": lambda db, sch, tbl: f"{db}.{sch}.{tbl}" if sch else f"{db}.{tbl}",
    "postgres": lambda db, sch, tbl: f"{db}.{sch}.{tbl}",
    "mysql": lambda db, sch, tbl: f"{db}.{tbl}",
    "teradata": lambda db, sch, tbl: f"{db}.{tbl}",
    "presto": lambda db, sch, tbl: f"{db}.{sch}.{tbl}",
    "trino": lambda db, sch, tbl: f"{db}.{sch}.{tbl}",
    "athena": lambda db, sch, tbl: f"{db}.{tbl}",
    "hana": lambda db, sch, tbl: f"{db}.{sch}.{tbl}",
    "denodo": lambda db, sch, tbl: f"{db}.{sch}.{tbl}",
}


class _TMLYamlLoader(yaml.SafeLoader):
    """SafeLoader variant that tolerates ThoughtSpot TML's use of YAML's
    obscure ``=`` value tag.

    TML formula/filter blocks emit ``oper: =`` which PyYAML's default
    resolution treats as ``tag:yaml.org,2002:value`` — a tag SafeLoader has
    no constructor for, raising ``ConstructorError``. We map it to a plain
    string so liveboards/answers with formula filters parse cleanly instead
    of being silently dropped.
    """


def _value_tag_as_str(loader: yaml.SafeLoader, node: yaml.Node) -> str:
    # add_constructor's callback signature is loose; narrow at runtime so a
    # non-scalar node (sequence/mapping) raises a clear AssertionError instead
    # of a generic PyYAML ConstructorError downstream.
    assert isinstance(node, yaml.ScalarNode)
    return str(loader.construct_scalar(node))


_TMLYamlLoader.add_constructor("tag:yaml.org,2002:value", _value_tag_as_str)


def _safe_load_tml_yaml(tml_content: str) -> Any:
    """Parse a TML YAML payload using ``_TMLYamlLoader``.

    ``_TMLYamlLoader`` subclasses ``yaml.SafeLoader``, so it never
    constructs arbitrary Python objects (no ``!!python/object*``
    constructors are registered). We drive the parser via the explicit
    Loader-instance API instead of ``yaml.load(...)`` so static analysis
    tools don't false-positive on the ``yaml.load`` function name — the
    safety guarantee comes from the loader class, not the entry point.
    """
    loader = _TMLYamlLoader(tml_content)
    try:
        return loader.get_single_data()
    finally:
        loader.dispose()


class ThoughtSpotAPIError(Exception):
    """Base exception for ThoughtSpot API errors."""

    pass


class ThoughtSpotAuthenticationError(ThoughtSpotAPIError):
    """Raised when authentication fails."""

    pass


class ThoughtSpotPermissionError(ThoughtSpotAPIError):
    """Raised when API permissions are insufficient."""

    pass


def _http_status_code(e: BaseException) -> Optional[int]:
    """Best-effort extraction of an HTTP status code from an SDK exception.

    The ``thoughtspot_rest_api`` SDK raises ``requests.exceptions.HTTPError``
    (via ``response.raise_for_status()``) which exposes
    ``e.response.status_code`` — that's the preferred path. As a safety net
    we also probe the stringified message for ``"401"`` / ``"403"`` /
    ``"404"`` so a future SDK wrapping HTTPError in its own type without
    preserving ``.response`` still routes correctly.

    Returns the status code as ``int``, or ``None`` if it can't be determined.
    """
    status = getattr(getattr(e, "response", None), "status_code", None)
    if isinstance(status, int):
        return status
    msg = str(e).lower()
    if "401" in msg or "unauthorized" in msg:
        return 401
    if "403" in msg or "forbidden" in msg:
        return 403
    if "404" in msg:
        return 404
    return None


def _classify_http_error(e: BaseException) -> Optional[Type[ThoughtSpotAPIError]]:
    """Classify an SDK exception to one of our auth-status exception types.

    Returns ``ThoughtSpotAuthenticationError`` for 401,
    ``ThoughtSpotPermissionError`` for 403, ``None`` for everything else
    (caller decides whether to wrap in ``ThoughtSpotAPIError`` or
    propagate / graceful-degrade). The narrow return type lets callers
    invoke ``cls(...)`` and rely on the family invariant — a future typo
    assigning a non-``ThoughtSpotAPIError`` subclass would be a mypy error.
    """
    status = _http_status_code(e)
    if status == 401:
        return ThoughtSpotAuthenticationError
    if status == 403:
        return ThoughtSpotPermissionError
    return None


def _response_items(response: Any, *keys: str) -> List[Dict[str, Any]]:
    """Normalise a TS REST v2 search response to a list of item dicts.

    The SDK returns either a bare list or a dict wrapping items under one
    of several keys (``metadata`` / ``orgs`` / ``tags`` / ``connections``).
    Centralising the wrapper-key handling here keeps every caller a
    one-liner — adding a new wrapper key is one line, not five.

    Returns the first non-empty list found among the supplied keys, or
    ``[]`` if the response is neither a list nor a dict with any of them.
    """
    if isinstance(response, list):
        return response
    if not isinstance(response, dict):
        return []
    for key in keys:
        value = response.get(key)
        if value:
            return value if isinstance(value, list) else []
    return []


class _TimeoutInjectingHTTPAdapter(HTTPAdapter):
    """``HTTPAdapter`` that injects a default ``timeout`` when the caller
    omits one. ``requests.Session`` has no global ``timeout`` — the
    canonical way to enforce a session-wide default is to subclass the
    adapter and override ``send``. This lets ``timeout_seconds`` actually
    apply to every request the SDK fires, including those where the SDK
    doesn't expose a ``timeout`` kwarg on its higher-level methods.
    """

    def __init__(self, *args: Any, default_timeout: int, **kwargs: Any) -> None:
        self._default_timeout = default_timeout
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):  # type: ignore[override]
        kwargs.setdefault("timeout", self._default_timeout)
        return super().send(request, **kwargs)


class ThoughtSpotClient:
    """
    Client for ThoughtSpot REST API v2.0 using official SDK.

    Handles:
    - Trusted authentication (username + secret_key)
    - Password authentication (username + password)
    - Automatic token generation and renewal
    - Pagination for list endpoints
    - Error handling and reporting

    The client uses the official ThoughtSpot Python SDK (thoughtspot_rest_api)
    which handles the low-level REST API calls, authentication, and retries.
    """

    def __init__(
        self,
        config: ThoughtSpotConnectionConfig,
        report: Optional[ThoughtSpotReport] = None,
    ):
        """
        Initialize ThoughtSpot API client using official SDK.

        Args:
            config: Connection configuration with URL, auth, and request settings
            report: Ingestion report; per-object API errors (e.g. forbidden TML
                downloads) are surfaced as warnings via this report. One-shot
                callers like ``test_connection`` can omit it — a throwaway
                ``ThoughtSpotReport()`` is constructed and the warnings are
                silently absorbed.
        """
        self.config = config
        # Construct a throwaway report when none is supplied so every code
        # path can call ``self.report.warning(...)`` without a None guard.
        self.report: ThoughtSpotReport = report or ThoughtSpotReport()

        # Per-run counter of mid-run re-auth attempts triggered by
        # ``_call_sdk`` on 401. Bounded by ``_MAX_REAUTH_ATTEMPTS`` so a
        # permanently-failed principal surfaces as a hard auth error
        # rather than an endless cascade of "token re-minted" info entries.
        self._reauth_attempts = 0

        # Counter for source_table refs that ``_coerce_source_tables`` drops
        # on post-construction assignment. ``validate_assignment=True`` on
        # ThoughtSpotMetadataHeader re-runs the field validator without
        # carrying a pydantic ``context`` (pydantic v2 limitation), so the
        # drops can't be threaded back via ``info.context``. Instead we
        # diff the raw vs validated lengths at the assignment site in
        # ``_enrich_and_yield_answers`` and accumulate here; the caller
        # flushes via :meth:`_consume_source_table_drop_count`.
        self._source_table_drop_count = 0

        # Initialize SDK client
        self.ts_client = TSRestApiV2(server_url=config.base_url)

        # Wire ``max_retries`` to the SDK's ``requests_session`` so the
        # configured retry budget actually applies. The SDK's own
        # constructor doesn't accept a retry argument; mounting an
        # ``HTTPAdapter`` with a ``Retry`` policy is the canonical way
        # to inject session-level retries into a third-party library
        # that exposes its ``requests.Session``. We retry on 429 (rate
        # limit), 5xx (transient server), and use exponential backoff —
        # matching the field's documented contract.
        retry_policy = Retry(
            total=config.max_retries,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            # Connector is read-only — never issues PUT/DELETE. Limiting
            # the retry set avoids accidentally re-trying mutating
            # requests if a future SDK adds them.
            allowed_methods=frozenset(["GET", "POST"]),
            raise_on_status=False,
        )
        retry_adapter = _TimeoutInjectingHTTPAdapter(
            max_retries=retry_policy,
            default_timeout=config.timeout_seconds,
        )
        self.ts_client.requests_session.mount("http://", retry_adapter)
        self.ts_client.requests_session.mount("https://", retry_adapter)

        # Authenticate using configured method
        self._authenticate()

    def _report_dropped(
        self,
        *,
        title: str,
        dropped: int,
        total: int,
        consequence: str,
        exc: Optional[BaseException] = None,
    ) -> None:
        """Emit one aggregated ``report.warning`` when a batch model-parse
        operation dropped one or more entries.

        Per-row drops are logged at ``DEBUG`` for forensic visibility; this
        helper is the single point that escalates them into the ingestion
        report so an operator sees the cumulative impact without being
        spammed with one warning per row.

        Callers should capture the first exception from the per-row drops
        and pass it via ``exc=`` — DataHub's structured-logs layer attaches
        the stack trace so Sentry / log aggregators can group similar
        drops by root cause rather than just by aggregate count.

        Silently no-ops when there's nothing to report (zero drops).
        """
        if dropped <= 0:
            return
        self.report.warning(
            title=title,
            message=(
                "Entries could not be parsed into the response model "
                "and were dropped. " + consequence
            ),
            context=f"dropped={dropped}, total={total}",
            exc=exc,
        )

    def _metadata_tml_export(
        self,
        metadata_ids: List[str],
        export_associated: bool = False,
        export_fqn: bool = False,
        edoc_format: Optional[str] = None,
    ) -> Any:
        """Wrap ``metadata/tml/export`` so per-org scoping is honoured.

        The SDK's ``metadata_tml_export()`` doesn't expose ``org_identifier``
        as a keyword argument, so when an org is configured we build the
        request dict directly (mirroring the SDK's internal shape) and post
        it via ``post_request`` with ``org_identifier`` injected. Without
        this branch, TML lineage extraction in a multi-org tenant would
        silently run against the principal's primary org instead of the
        configured one — producing wrong (or missing) lineage edges with
        no warning surfaced to the user.

        When ``org_identifier`` is unset we delegate to the SDK wrapper to
        keep behaviour identical to the single-org case.
        """
        if not self.config.org_identifier:
            return self._call_sdk(
                self.ts_client.metadata_tml_export,
                metadata_ids=metadata_ids,
                export_associated=export_associated,
                export_fqn=export_fqn,
                edoc_format=edoc_format,
            )

        request: Dict[str, Any] = {
            "metadata": [{"identifier": i} for i in metadata_ids],
            "export_associated": export_associated,
            "export_fqn": export_fqn,
            "org_identifier": self.config.org_identifier,
        }
        if edoc_format is not None and edoc_format.upper() == "YAML":
            request["edoc_format"] = "YAML"
        return self._call_sdk(
            self.ts_client.post_request,
            endpoint="metadata/tml/export",
            request=request,
        )

    def _iter_tml_export_items(
        self,
        metadata_ids: List[str],
        *,
        batch_size: int,
        export_associated: bool = False,
        export_fqn: bool = False,
        edoc_format: Optional[str] = None,
        failure_warning_title: str,
        failure_warning_message: str,
    ) -> Iterator[Dict[str, Any]]:
        """Yield TML export items, chunking ``metadata_ids`` to stay under TS
        server body-size and timeout limits.

        TS's ``metadata/tml/export`` bundles full YAML edocs per object.
        Sending 1000+ IDs in a single POST regularly produces gateway
        timeouts; the response is then a single failure that drops *every*
        object's TML data (and with it, every chart we would have emitted).
        Chunking caps the blast radius at ``batch_size`` and lets the rest
        of the run succeed when one batch is slow or partially forbidden.

        Per-batch exceptions are reported as warnings and the iterator
        continues with the next batch — partial degradation, not total
        failure.
        """
        for start in range(0, len(metadata_ids), batch_size):
            chunk = metadata_ids[start : start + batch_size]
            try:
                response = self._metadata_tml_export(
                    metadata_ids=chunk,
                    export_associated=export_associated,
                    export_fqn=export_fqn,
                    edoc_format=edoc_format,
                )
            except Exception as e:
                self.report.warning(
                    title=failure_warning_title,
                    message=failure_warning_message,
                    context=(
                        f"batch_offset={start}, "
                        f"batch_count={len(chunk)}, "
                        f"total={len(metadata_ids)}"
                    ),
                    exc=e,
                )
                logger.warning(
                    f"TML export batch [{start}..{start + len(chunk)}] failed: {e}"
                )
                continue
            if not isinstance(response, list):
                continue
            for item in response:
                if isinstance(item, dict):
                    yield item

    def _check_tml_item_status(
        self,
        item: Dict[str, Any],
        metadata_type: str,
    ) -> bool:
        """Check per-item TML export status from a ``metadata_tml_export`` response.

        ThoughtSpot's TML export endpoint returns HTTP 200 even when individual
        objects fail (lack of permission, missing dependencies, etc.). Each item
        carries its own ``info.status.status_code`` — items with status ``ERROR``
        have no ``edoc`` and silently produce empty schema/lineage downstream.

        Returns ``True`` if the item is OK, ``False`` if it failed (and was
        reported as a warning).
        """
        info = item.get("info") or {}
        status = info.get("status") or {}
        if status.get("status_code") != "ERROR":
            return True

        error_message = status.get("error_message", "") or ""
        error_code = status.get("error_code")
        object_name = info.get("name", "unknown")
        object_id = info.get("id", "unknown")
        # TS embeds full Java stack traces in error_message; keep just the
        # human-readable first line so the report stays readable.
        short_msg = error_message.split("\n")[0][:300]

        logger.warning(
            f"TML export failed for {metadata_type} '{object_name}' "
            f"(id={object_id}): {short_msg}"
        )
        self.report.warning(
            title="TML Export Failed",
            message=(
                "Cannot download TML for object. Most often this means the "
                "user lacks per-object access to the worksheet's underlying "
                "connection/tables, or the worksheet references a deleted "
                "object. Schema and lineage for this object will be missing "
                "from the ingestion."
            ),
            context=(
                f"metadata_type={metadata_type}, name={object_name}, "
                f"id={object_id}, error_code={error_code}, error={short_msg}"
            ),
        )
        self.report.report_api_error()
        return False

    def _authenticate(self) -> None:
        """
        Authenticate with ThoughtSpot using configured method.

        Supports two authentication methods, both of which mint a short-lived
        bearer token per ingestion run via ``auth_token_full``:

        1. Trusted auth: username + secret_key (recommended for production)
        2. Password auth: username + password

        Raises:
            ThoughtSpotAuthenticationError: If authentication fails
            ThoughtSpotAPIError: If API call fails
        """
        auth = self.config.auth
        try:
            if isinstance(auth, TrustedAuth):
                logger.info(
                    f"Authenticating with trusted auth (username={auth.username})"
                )
                auth_response = self.ts_client.auth_token_full(
                    username=auth.username,
                    secret_key=auth.secret_key.get_secret_value(),
                    validity_time_in_sec=3000,  # 50 minutes
                )
                self.ts_client.bearer_token = auth_response["token"]
                logger.info("Successfully authenticated with trusted auth")
            elif isinstance(auth, PasswordAuth):
                logger.info(f"Authenticating with password (username={auth.username})")
                auth_response = self.ts_client.auth_token_full(
                    username=auth.username,
                    password=auth.password.get_secret_value(),
                    validity_time_in_sec=3000,  # 50 minutes
                )
                self.ts_client.bearer_token = auth_response["token"]
                logger.info("Successfully authenticated with password")
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            cls = _classify_http_error(e) or ThoughtSpotAPIError
            if cls is ThoughtSpotAuthenticationError:
                raise cls(
                    "Authentication failed - invalid credentials. "
                    "Check username/password or secret_key is correct."
                ) from e
            if cls is ThoughtSpotPermissionError:
                raise cls(
                    "Permission denied - insufficient API permissions. "
                    "Ensure the user has 'Can access API' permission."
                ) from e
            raise ThoughtSpotAPIError(f"Authentication failed: {e}") from e

    def _call_sdk(
        self,
        fn: Callable[_P, _R],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _R:
        """Invoke an SDK method, re-authenticating on a 401.

        ``auth_token_full`` mints a bearer token with a 50-minute validity.
        On large tenants ingestion routinely runs longer than that, so any
        downstream SDK call can fail mid-run with a 401. Without this
        wrapper the per-call ``except Exception`` blocks each emit their
        own warning ("Pagination Truncated", "Tag Extraction Failed", …)
        and the operator sees a cascade of warnings instead of the root
        cause, while the run *looks* successful with truncated data.

        Strategy: loop on the wrapped call. Any 401 — whether on the first
        attempt or on a post-reauth retry — routes through the same cap
        path. A retry that itself 401s bumps the counter and triggers
        another reauth cycle, bounded by ``_MAX_REAUTH_ATTEMPTS``. Past
        the cap, raise ``ThoughtSpotAuthenticationError`` with the
        originating SDK call name as breadcrumb so operators can see
        which call first observed the expired token.
        Non-401 exceptions propagate immediately so callers'
        graceful-degradation handlers still kick in for real failures.

        Only wrap idempotent read calls — every retry re-executes side
        effects.

        Type-wise: ``ParamSpec``/``TypeVar`` propagate the wrapped
        callable's signature so callers don't lose mypy coverage on the
        return shape.
        """
        fn_name = getattr(fn, "__name__", "<callable>")
        originating_exc: Optional[BaseException] = None
        while True:
            try:
                result = fn(*args, **kwargs)
                # Reset the reauth budget on every successful call so the
                # cap bounds CONSECUTIVE 401s, not lifetime ones. A long
                # ingestion legitimately needs ≥ ceil(runtime / token_TTL)
                # token refreshes; the prior lifetime cap of 3 would
                # spuriously fail any run >2.5h with a 50-min token TTL.
                self._reauth_attempts = 0
                return result
            except Exception as e:
                if _classify_http_error(e) is not ThoughtSpotAuthenticationError:
                    raise
                if originating_exc is None:
                    originating_exc = e
                if self._reauth_attempts >= _MAX_REAUTH_ATTEMPTS:
                    raise ThoughtSpotAuthenticationError(
                        f"Re-auth limit ({_MAX_REAUTH_ATTEMPTS}) reached while "
                        f"calling {fn_name}; the principal is likely disabled "
                        "or its credentials revoked mid-run. Aborting to "
                        "avoid a silent zero-extraction run."
                    ) from originating_exc
                self._reauth_attempts += 1
                logger.warning(
                    "SDK call %s rejected with 401; re-authenticating "
                    "(attempt %d/%d) and retrying.",
                    fn_name,
                    self._reauth_attempts,
                    _MAX_REAUTH_ATTEMPTS,
                )
                self.report.info(
                    title="Bearer token re-minted mid-run",
                    message=(
                        "ThoughtSpot bearer token expired (50-minute TTL) during "
                        "ingestion; the connector re-authenticated and retried "
                        "the failing call."
                    ),
                    context=(
                        f"fn={fn_name}; "
                        f"attempt={self._reauth_attempts}/{_MAX_REAUTH_ATTEMPTS}"
                    ),
                )
                try:
                    self._authenticate()
                except ThoughtSpotAuthenticationError as auth_exc:
                    # Preserve the originating 401's breadcrumb so operators
                    # can see which SDK call first observed the expired token.
                    raise ThoughtSpotAuthenticationError(
                        f"Re-auth failed while retrying {fn_name}: {auth_exc}"
                    ) from originating_exc
                # Loop continues — retry the wrapped call.

    @staticmethod
    def _parse_source_tables_from_liveboard_detail(
        detail: Dict[str, Any],
    ) -> List[str]:
        """Extract LOGICAL_TABLE GUIDs a liveboard depends on.

        ThoughtSpot's ``metadata/search`` returns a liveboard's full
        ``reportContent`` tree under ``metadata_detail``. Each sheet's
        ``pinboardFilterDetails`` enumerates the logical tables that drive
        the dashboard's filter dropdowns and (in practice) back its
        visualizations. Per-viz table refs live deeper in the tree inside a
        base64-encoded protobuf (``effectiveQuestion.sageContextProto``)
        which we deliberately do not decode here — the filter table list is
        accurate for the common case and avoids pulling in a protobuf
        dependency for a marginal lineage gain.
        """
        ids: List[str] = []
        seen = set()
        report_content = detail.get("reportContent") or {}
        for sheet in report_content.get("sheets") or []:
            if not isinstance(sheet, dict):
                continue
            sheet_content = sheet.get("sheetContent") or {}
            filter_details = sheet_content.get("pinboardFilterDetails") or {}
            for tid in filter_details.get("filterLogicalTableIds") or []:
                if isinstance(tid, str) and tid and tid not in seen:
                    seen.add(tid)
                    ids.append(tid)
            # logicalTableIdToPinboardFilterVizIds maps tableId -> [vizIds];
            # its keys are additional table refs surfaced by ThoughtSpot.
            mapping = filter_details.get("logicalTableIdToPinboardFilterVizIds") or {}
            for tid in mapping:
                if isinstance(tid, str) and tid and tid not in seen:
                    seen.add(tid)
                    ids.append(tid)
        return ids

    @staticmethod
    def _parse_columns_from_metadata_detail(
        detail: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Map a ``metadata_detail.columns`` entry to our column wire shape.

        ThoughtSpot's v2 search response nests the column name + description
        under ``column.header`` and stores the data type at the top level
        (``dataType``) — separate from ``type`` which holds the logical role
        (``ATTRIBUTE``/``MEASURE``). We flatten this into a single dict per
        column so model constructors and downstream code see one consistent
        shape regardless of whether the data came from a list call with
        ``include_details=True`` or an explicit ``get_metadata_details``.
        """
        columns: List[Dict[str, Any]] = []
        for col in detail.get("columns") or []:
            if not isinstance(col, dict):
                continue
            col_header = col.get("header") or {}
            # ``sources`` is the resolved leaf-column lineage TS pre-computes
            # for us — works for both simple LOGICAL_COLUMN_REFERENCE columns
            # and EXPRESSION/formula columns. We pass it through as-is and let
            # the source layer construct DataHub fine-grained lineage edges.
            raw_sources = col.get("sources") or []
            sources: Optional[List[Dict[str, Any]]] = [
                {
                    "tableId": s.get("tableId"),
                    "tableName": s.get("tableName"),
                    "columnId": s.get("columnId"),
                    "columnName": s.get("columnName"),
                }
                for s in raw_sources
                if isinstance(s, dict) and s.get("tableId") and s.get("columnName")
            ] or None
            columns.append(
                {
                    "id": col_header.get("id", ""),
                    "name": col_header.get("name", ""),
                    "data_type": col.get("dataType")
                    or col.get("columnType")
                    or "UNKNOWN",
                    "description": col_header.get("description"),
                    "column_type": col.get("type"),
                    "physical_column_name": col.get("physicalColumnName"),
                    "sources": sources,
                }
            )
        return columns

    def _paginated_metadata_search(
        self,
        object_type: str,
        batch_size: int = 100,
        fetch_ids: Optional[List[str]] = None,
        tag_names: Optional[List[str]] = None,
        include_details: bool = False,
    ) -> Iterator[Dict[str, Any]]:
        """
        Make paginated metadata search request using SDK's metadata_search method.

        ThoughtSpot API v2.0 uses offset-based pagination.

        Args:
            object_type: Type of object to list (LIVEBOARD, ANSWER, LOGICAL_TABLE, etc.)
            batch_size: Number of items per page
            fetch_ids: Optional list of specific object IDs to fetch
            tag_names: Optional list of tags to filter by
            include_details: When ``True``, request ``include_details=True`` so
                the same call returns ``metadata_detail`` (columns, datasource
                refs). The flattened item then carries ``columns``,
                ``data_source_id``, and ``data_source_type`` — eliminating the
                per-object detail call that ``get_logical_table_details`` used
                to make.

        Yields:
            Individual items from all pages (as dicts - converted to models by callers)
        """
        offset = 0
        page = 1
        has_more = True

        while has_more:
            try:
                logger.debug(
                    f"Fetching page {page} with offset={offset}, batch_size={batch_size}"
                )

                # Build metadata_search request
                request: Dict[str, Any] = {
                    "metadata": [{"type": object_type}],
                    "record_offset": offset,
                    "record_size": batch_size,
                    # Always request the per-entity stats block — it's the
                    # source of truth for the global view counter that matches
                    # the TS UI. Costs nothing extra; TS computes it lazily
                    # and bundles it in the same response.
                    "include_stats": True,
                }

                # Add optional filters
                if fetch_ids:
                    request["metadata"][0]["identifier"] = fetch_ids
                if tag_names:
                    # Top-level ``tag_identifiers`` accepts tag names OR
                    # GUIDs (verified against TS REST v2). The earlier
                    # ``metadata[0].tag_name`` shape gets rejected with
                    # 400 Bad Request; the also-tried top-level ``tags``
                    # is silently ignored. ``tag_identifiers`` is the
                    # only documented + working filter.
                    request["tag_identifiers"] = tag_names
                if include_details:
                    request["include_details"] = True
                if self.config.org_identifier:
                    request["org_identifier"] = self.config.org_identifier

                # Call SDK method
                response = self._call_sdk(
                    self.ts_client.metadata_search, request=request
                )

                # Extract items from response
                # Response structure: {"metadata": [...], ...}
                items = []
                if isinstance(response, dict) and "metadata" in response:
                    items = response["metadata"]
                elif isinstance(response, list):
                    items = response

                if not items:
                    break

                # Flatten the response structure: merge metadata_header into top level
                # ThoughtSpot SDK returns: {metadata_id, metadata_header: {id, name, ...}}
                # Source expects: {id, name, ...}
                for item in items:
                    if isinstance(item, dict) and "metadata_header" in item:
                        # Merge metadata_header fields into top level
                        flattened = {**item.get("metadata_header", {})}
                        # Keep original metadata_id as well
                        flattened["metadata_id"] = item.get("metadata_id")
                        # Lift the stats block — see EntityStats model. Pydantic
                        # coerces the nested dict into the EntityStats instance
                        # via normal nested-model parsing on the parent response.
                        if "stats" in item:
                            flattened["stats"] = item["stats"]

                        if include_details:
                            detail = item.get("metadata_detail") or {}
                            if object_type == "LOGICAL_TABLE":
                                flattened["columns"] = (
                                    self._parse_columns_from_metadata_detail(detail)
                                )
                                flattened["data_source_id"] = detail.get("dataSourceId")
                                flattened["data_source_type"] = detail.get(
                                    "dataSourceTypeEnum"
                                )
                                # TS exposes the catalog/schema/table mapping
                                # for federated tables on
                                # ``logicalTableContent.tableMappingInfo`` —
                                # NOT on top-level ``physical*`` keys.
                                table_mapping = (
                                    detail.get("logicalTableContent") or {}
                                ).get("tableMappingInfo") or {}
                                flattened["physical_database_name"] = table_mapping.get(
                                    "databaseName"
                                )
                                flattened["physical_schema_name"] = table_mapping.get(
                                    "schemaName"
                                )
                                flattened["physical_table_name"] = table_mapping.get(
                                    "tableName"
                                )
                                joins = detail.get("joins")
                                if isinstance(joins, list):
                                    flattened["join_count"] = len(joins)
                            elif object_type == "LIVEBOARD":
                                flattened["source_table_ids"] = (
                                    self._parse_source_tables_from_liveboard_detail(
                                        detail
                                    )
                                )
                        yield flattened
                    else:
                        yield item

                # Check if there are more pages
                if len(items) < batch_size:
                    has_more = False
                else:
                    offset += batch_size
                    page += 1

            except Exception as e:
                self.report.warning(
                    title="Pagination Truncated",
                    message=(
                        "Listing was interrupted mid-stream; results may "
                        "be incomplete. Re-run ingestion to retry."
                    ),
                    context=f"object_type={object_type}, page={page}, offset={offset}",
                    exc=e,
                )
                logger.error(
                    f"Pagination failed at page {page} (offset {offset}): {e}. "
                    f"Returning partial results. {offset} items retrieved so far."
                )
                break  # Return what we have

    # ===== Metadata API Methods =====

    def get_metadata_list(
        self,
        metadata_type: str,
        fetch_ids: Optional[List[str]] = None,
        tag_names: Optional[List[str]] = None,
        include_details: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        List metadata objects of a specific type.

        Uses SDK's metadata_search method.

        Args:
            metadata_type: Type of object (LIVEBOARD, ANSWER, LOGICAL_TABLE, etc.)
            fetch_ids: Optional list of specific object IDs to fetch
            tag_names: Optional list of tags to filter by
            include_details: When ``True``, the same call returns column-level
                schema and connection refs so callers don't need a follow-up
                ``get_metadata_details`` per object.

        Returns:
            List of metadata object headers (as dicts - converted to models by specific methods)
        """
        return list(
            self._paginated_metadata_search(
                object_type=metadata_type,
                fetch_ids=fetch_ids,
                tag_names=tag_names,
                include_details=include_details,
            )
        )

    def get_metadata_details(
        self,
        metadata_type: str,
        metadata_ids: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Get column-level schema for specific objects.

        Uses ``metadata/search`` with ``include_details=true``. This endpoint
        returns the full column list (name, data type, description) and is
        gated by ordinary read access on the object — unlike
        ``metadata/tml/export``, which additionally requires per-object access
        to every connection/table the worksheet depends on. We therefore split
        responsibilities: schemas come from ``metadata/search`` here, while
        TML export remains the source for cross-object lineage that only TML
        exposes (see ``_get_answer_dependencies``).

        Args:
            metadata_type: Type of object (e.g. ``LOGICAL_TABLE``)
            metadata_ids: List of object IDs (GUIDs)

        Returns:
            List of ``{id, name, columns: [{name, data_type, description}]}``
        """
        if not metadata_ids:
            return []

        # Chunk so a 100x-scale caller doesn't blow past TS's per-request
        # body-size limit. See ``metadata_fetch_batch_size`` rationale.
        batch_size = self.config.metadata_fetch_batch_size
        parsed_details: List[Dict[str, Any]] = []
        for start in range(0, len(metadata_ids), batch_size):
            chunk_ids = metadata_ids[start : start + batch_size]
            try:
                # API rejects identifier as a list ("String cannot represent
                # a non string value"); we batch by emitting one metadata
                # entry per id.
                request: Dict[str, Any] = {
                    "metadata": [
                        {"type": metadata_type, "identifier": mid} for mid in chunk_ids
                    ],
                    "include_details": True,
                    "record_size": len(chunk_ids),
                }
                if self.config.org_identifier:
                    request["org_identifier"] = self.config.org_identifier
                response = self._call_sdk(
                    self.ts_client.metadata_search, request=request
                )
            except Exception as e:
                self.report.warning(
                    title="Metadata Detail Fetch Failed",
                    message=(
                        "Column-level schemas / detail aspects will be "
                        "missing for these entities until the next "
                        "successful run."
                    ),
                    context=(
                        f"metadata_type={metadata_type}, "
                        f"batch_offset={start}, "
                        f"batch_count={len(chunk_ids)}"
                    ),
                    exc=e,
                )
                logger.error(
                    f"Failed to get metadata details for {metadata_type} "
                    f"batch [{start}..{start + len(chunk_ids)}]: {e}"
                )
                continue

            items = _response_items(response, "metadata")

            for item in items:
                if not isinstance(item, dict):
                    continue

                header = item.get("metadata_header") or {}
                detail = item.get("metadata_detail") or {}
                obj_id = item.get("metadata_id") or header.get("id")
                obj_name = header.get("name", "")

                columns: List[Dict[str, str]] = []
                for col in detail.get("columns") or []:
                    if not isinstance(col, dict):
                        continue
                    col_header = col.get("header") or {}
                    columns.append(
                        {
                            "name": col_header.get("name", ""),
                            # Column data type lives at the top level of the
                            # column dict; "type" is the logical role
                            # (ATTRIBUTE/MEASURE) and not what we want here.
                            "data_type": col.get("dataType")
                            or col.get("columnType")
                            or "UNKNOWN",
                            "description": col_header.get("description", ""),
                        }
                    )

                parsed_details.append(
                    {"id": obj_id, "name": obj_name, "columns": columns}
                )

        logger.debug(
            f"Fetched details for {len(parsed_details)}/{len(metadata_ids)} "
            f"{metadata_type} objects "
            f"({sum(len(d.get('columns', [])) for d in parsed_details)} total columns)"
        )
        return parsed_details

    # ===== Workspace API Methods =====

    def get_workspaces(self) -> List[WorkspaceResponse]:
        """
        Get all workspaces (Orgs).

        ThoughtSpot exposes orgs only through the dedicated ``/orgs/search``
        endpoint — they are not part of the ``SearchMetadataType`` enum used
        by ``/metadata/search``, so trying ``metadata_search(type="ORG")``
        unconditionally returns 400. ``/orgs/search`` itself requires the
        cluster-level ``Has administration privileges`` granted at the system
        org (orgID 0); regular org admins get 403. We treat the 403 as
        "this deployment doesn't expose org enumeration to our principal" and
        degrade gracefully — datasets and dashboards still ingest, just
        without the workspace container hierarchy.

        Returns:
            List of workspace metadata objects as Pydantic models. Empty list
            when the endpoint is forbidden or unavailable.
        """
        try:
            response = self._call_sdk(self.ts_client.orgs_search, request={})
        except Exception as e:
            status = _http_status_code(e)
            if status == 403:
                logger.info(
                    "Skipping workspace (Org) ingestion: /orgs/search returned 403. "
                    "This is expected when the principal is not a system-org "
                    "administrator. Datasets and dashboards will be ingested "
                    "without a workspace container hierarchy."
                )
                self.report.info(
                    title="Workspace extraction skipped (403)",
                    message=(
                        "/orgs/search returned 403; entities will be "
                        "ingested without a workspace container "
                        "hierarchy. Grant the principal "
                        "'Has administration privileges' at the system "
                        "org if you want orgs enumerated."
                    ),
                )
            elif status == 404:
                logger.info(
                    "Skipping workspace (Org) ingestion: /orgs/search not "
                    "available on this ThoughtSpot deployment."
                )
                self.report.info(
                    title="Workspace extraction unavailable (404)",
                    message=(
                        "/orgs/search is not exposed on this "
                        "ThoughtSpot deployment; entities will be "
                        "ingested without a workspace container "
                        "hierarchy."
                    ),
                )
            else:
                self.report.warning(
                    title="Workspace Fetch Failed",
                    message=(
                        "Unable to enumerate ThoughtSpot Orgs; entities "
                        "will be ingested without container hierarchy."
                    ),
                    exc=e,
                )
                logger.warning(f"Failed to fetch workspaces (orgs): {e}")
            return []

        items = _response_items(response, "orgs", "metadata")

        # Normalize each org dict so it satisfies WorkspaceResponse's required
        # fields (``id`` + ``name``). The /orgs/search response uses ``id``
        # directly (numeric or string) and ``name`` at the top level.
        workspaces: List[WorkspaceResponse] = []
        dropped = 0
        first_exc: Optional[BaseException] = None
        for item in items:
            if not isinstance(item, dict):
                continue
            normalized = dict(item)
            if "id" in normalized and not isinstance(normalized["id"], str):
                normalized["id"] = str(normalized["id"])
            try:
                workspaces.append(WorkspaceResponse(**normalized))
            except Exception as e:
                dropped += 1
                if first_exc is None:
                    first_exc = e
                logger.debug(f"Skipping malformed org entry: {e}")
        self._report_dropped(
            title="Workspace Parse Failures",
            dropped=dropped,
            total=dropped + len(workspaces),
            consequence=(
                "Affected workspaces will not appear as container parents in DataHub."
            ),
            exc=first_exc,
        )
        return workspaces

    # ===== Tag API Methods =====

    def get_tags(self) -> List[TagResponse]:
        """Fetch every tag visible to the ingestion principal.

        ThoughtSpot tags are user-defined labels that can be attached to
        liveboards, answers, and worksheets. We fetch the full list once
        per ingestion run so we can resolve the ``tags[]`` references in
        each entity's ``metadata_header`` to human-readable names.

        Returns an empty list (logged at info-level) when the principal
        can't enumerate tags or the deployment has none — tags are
        informational and must not block the rest of the pipeline.
        """
        try:
            if self.config.org_identifier:
                # The SDK's tags_search() wrapper doesn't expose org_identifier,
                # but the underlying REST endpoint accepts it. Fall through to
                # post_request so org-scoping is honored.
                response = self._call_sdk(
                    self.ts_client.post_request,
                    endpoint="tags/search",
                    request={"org_identifier": self.config.org_identifier},
                )
            else:
                response = self._call_sdk(self.ts_client.tags_search)
        except Exception as e:
            # Mirror the discrimination used in ``get_workspaces``: 403/404 is
            # the expected "this principal can't enumerate tags" path and
            # logs at info; any other failure (auth expiry, 5xx, network)
            # should be visible — both in the logs at warning level AND in
            # the ingestion report so users notice if tag extraction broke
            # for a non-permissions reason. Tag emission still degrades to
            # an empty list either way; tags are informational and must
            # not block the pipeline.
            status = _http_status_code(e)
            if status == 403:
                logger.info(
                    "Skipping ThoughtSpot tag extraction: /tags/search returned "
                    "403. This is expected when the principal lacks tag-read "
                    "permission. Entities will be ingested without tag aspects."
                )
                self.report.info(
                    title="Tag extraction skipped (403)",
                    message=(
                        "/tags/search returned 403; entities will be "
                        "ingested without tag aspects. Grant tag-read "
                        "permission to the principal if tags are "
                        "needed."
                    ),
                )
            elif status == 404:
                logger.info(
                    "Skipping ThoughtSpot tag extraction: /tags/search is not "
                    "available on this deployment. Entities will be ingested "
                    "without tag aspects."
                )
                self.report.info(
                    title="Tag extraction unavailable (404)",
                    message=(
                        "/tags/search is not exposed on this "
                        "ThoughtSpot deployment; entities will be "
                        "ingested without tag aspects."
                    ),
                )
            else:
                logger.warning(f"Failed to fetch ThoughtSpot tags: {e}")
                self.report.warning(
                    title="Tag Extraction Failed",
                    message=(
                        "Could not fetch the tag list from ThoughtSpot. "
                        "Entities will be ingested without tag aspects. "
                        "If you expect tags to be present, check the "
                        "principal's permissions and network connectivity."
                    ),
                    exc=e,
                )
            return []

        items = _response_items(response, "tags", "metadata")
        tags: List[TagResponse] = []
        dropped = 0
        first_exc: Optional[BaseException] = None
        for item in items:
            if not isinstance(item, dict):
                continue
            try:
                tags.append(TagResponse(**item))
            except Exception as e:
                dropped += 1
                if first_exc is None:
                    first_exc = e
                logger.debug(f"Skipping malformed tag entry: {e}")
        self._report_dropped(
            title="Tag Parse Failures",
            dropped=dropped,
            total=dropped + len(tags),
            consequence=(
                "Affected tags will not be emitted as DataHub global-tag aspects."
            ),
            exc=first_exc,
        )
        return tags

    def get_connections(self) -> List[ConnectionResponse]:
        """Fetch every TS Connection visible to the ingestion principal.

        Connections back each Logical Table with its external data source
        (Databricks / Snowflake / etc.). One fetch per ingestion run.

        The SDK exposes ``connection_search`` (singular — the REST path
        is ``/connection/search``, not ``/connection/search``). We pass
        ``org_identifier`` through when configured so the connection
        catalog is scoped to a single org. On 403/404/network-error we
        degrade gracefully — external lineage emission is best-effort
        and must not block the rest of the pipeline.
        """
        request: Dict[str, Any] = {}
        if self.config.org_identifier:
            request["org_identifier"] = self.config.org_identifier
        try:
            response = self._call_sdk(self.ts_client.connection_search, request=request)
        except Exception as e:
            # 403/404 are expected when the principal lacks
            # connection-catalog read — mirror the graceful-degradation
            # pattern used by get_workspaces / get_tags so healthy runs
            # don't emit alarming warnings.
            status = _http_status_code(e)
            if status == 403:
                logger.info(
                    "Skipping connection catalog fetch: /connection/search "
                    "returned 403. Cross-platform upstream lineage will be "
                    "missing for this run. Grant the principal 'Can view' "
                    "on the connection page if you want external lineage."
                )
                self.report.info(
                    title="External lineage disabled (403)",
                    message=(
                        "/connection/search returned 403; cross-platform "
                        "upstream lineage (TS Logical Tables → "
                        "Databricks/Snowflake/etc.) will be missing for "
                        "this run."
                    ),
                )
            elif status == 404:
                logger.info(
                    "Skipping connection catalog fetch: /connection/search "
                    "not available on this ThoughtSpot deployment."
                )
                self.report.info(
                    title="External lineage unavailable (404)",
                    message=(
                        "/connection/search is not exposed on this "
                        "ThoughtSpot deployment; cross-platform upstream "
                        "lineage will be missing for this run."
                    ),
                )
            else:
                self.report.warning(
                    title="External Lineage Disabled",
                    message=(
                        "Could not fetch the TS connection catalog. "
                        "Cross-platform upstream lineage (TS Logical "
                        "Tables → Databricks/Snowflake/etc.) will be "
                        "missing for this run."
                    ),
                    exc=e,
                )
                logger.warning(f"Failed to fetch TS connections: {e}")
            return []

        items = _response_items(response, "connections", "metadata")

        conns: List[ConnectionResponse] = []
        dropped = 0
        first_exc: Optional[BaseException] = None
        for item in items:
            if not isinstance(item, dict):
                continue
            try:
                conns.append(ConnectionResponse.model_validate(item))
            except Exception as e:
                dropped += 1
                if first_exc is None:
                    first_exc = e
                logger.debug(f"Skipping malformed connection entry: {e}")
        self._report_dropped(
            title="Connection Parse Failures",
            dropped=dropped,
            total=dropped + len(conns),
            consequence=(
                "External-lineage edges for these connections will be missing."
            ),
            exc=first_exc,
        )
        return conns

    # ===== Liveboard API Methods =====

    def iter_liveboards(
        self,
        liveboard_ids: Optional[List[str]] = None,
        tag_names: Optional[List[str]] = None,
        include_details: bool = True,
    ) -> Iterator[LiveboardResponse]:
        """Stream Liveboards (dashboards) page-by-page.

        At 100×-scale tenants the historical ``List[LiveboardResponse]``
        return shape held ~200KB × N liveboards in memory before any
        workunit was emitted. Streaming enriches and yields in batches of
        ``tml_export_batch_size`` so heap usage stays bounded regardless
        of tenant size.

        Enrichment uses TML to surface per-viz info (Chart layer +
        column-level lineage), and is chunked to the same batch size to
        avoid the unbounded-TML-POST blocker.

        Args:
            liveboard_ids: Optional specific Liveboard IDs
            tag_names: Optional tag filter
            include_details: Set to ``False`` to skip the (large)
                reportContent payload when only header metadata is needed.

        Yields:
            ``LiveboardResponse`` per liveboard, with ``visualizations``
            populated when TML access permits.
        """
        batch_size = self.config.tml_export_batch_size
        buffer: List[LiveboardResponse] = []
        dropped = 0
        first_exc: Optional[BaseException] = None
        viz_drops: List[str] = []
        for item in self._paginated_metadata_search(
            object_type="LIVEBOARD",
            fetch_ids=liveboard_ids,
            tag_names=tag_names,
            include_details=include_details,
        ):
            try:
                buffer.append(
                    LiveboardResponse.model_validate(
                        item, context={VISUALIZATION_DROPS: viz_drops}
                    )
                )
            except Exception as e:
                # Aggregate the drop instead of warning per-item — a
                # systemic mis-shape would flood the report otherwise.
                dropped += 1
                if first_exc is None:
                    first_exc = e
                logger.debug(f"Skipping malformed liveboard: {e}")
                continue
            if len(buffer) >= batch_size:
                yield from self._enrich_and_yield_liveboards(buffer)
                buffer = []
        if buffer:
            yield from self._enrich_and_yield_liveboards(buffer)
        if dropped:
            self.report.malformed_liveboards_dropped += dropped
            self.report.warning(
                title="Liveboards dropped due to malformed wire payload",
                message=(
                    "One or more LIVEBOARD entries from /metadata/search "
                    "could not be parsed into LiveboardResponse and were "
                    "skipped. Affected liveboards will not be ingested."
                ),
                context=f"dropped={dropped}",
                exc=first_exc,
            )
        if viz_drops:
            self.report.malformed_visualizations_dropped += len(viz_drops)
            self.report.warning(
                title="Visualizations dropped due to malformed wire payload",
                message=(
                    "One or more nested visualization entries within "
                    "Liveboards were malformed and skipped. The parent "
                    "Liveboards still emit, but the affected tiles will "
                    "be missing."
                ),
                context=(f"dropped={len(viz_drops)}; first_reasons={viz_drops[:3]}"),
            )

    def _enrich_and_yield_liveboards(
        self, liveboards: List[LiveboardResponse]
    ) -> Iterator[LiveboardResponse]:
        """TML-enrich a batch of liveboards and yield them.

        ``_get_liveboard_visualizations_via_tml`` already chunks internally
        to ``tml_export_batch_size``; calling it with a batch of that size
        means exactly one TML export request per buffer flush.
        """
        if not liveboards:
            return
        try:
            viz_map = self._get_liveboard_visualizations_via_tml(
                [lb.id for lb in liveboards]
            )
            for lb in liveboards:
                if lb.id in viz_map:
                    vizes = list(viz_map[lb.id])
                    # TML doesn't return per-viz audit timestamps or
                    # author info — visualisations live inside the
                    # parent liveboard and share its identity in TS, so
                    # inherit the liveboard's created/modified/author
                    # fields onto each viz. Without this the emitted
                    # Chart entities show ``time=0`` /
                    # ``urn:li:corpuser:unknown`` audit stamps in the
                    # DataHub UI.
                    for viz in vizes:
                        if viz.created is None:
                            viz.created = lb.created
                        if viz.modified is None:
                            viz.modified = lb.modified
                        if viz.author_name is None:
                            viz.author_name = lb.author_name
                        if viz.author_display_name is None:
                            viz.author_display_name = lb.author_display_name
                        if viz.author is None:
                            viz.author = lb.author
                    lb.visualizations = vizes
        except Exception as e:
            self.report.warning(
                title="Liveboard Visualization Fetch Failed",
                message=(
                    "Dashboards in this batch will be emitted with "
                    "direct dataset lineage only — no Chart layer or "
                    "column-level lineage. Check per-object TML "
                    "access for the ingestion principal."
                ),
                context=f"batch_count={len(liveboards)}",
                exc=e,
            )
            logger.warning(
                f"Failed to fetch Liveboard visualizations via TML for "
                f"batch of {len(liveboards)}: {e}. Dashboards will be "
                "emitted with direct dataset lineage only (no Chart layer)."
            )
        yield from liveboards

    def get_liveboards(
        self,
        liveboard_ids: Optional[List[str]] = None,
        tag_names: Optional[List[str]] = None,
        include_details: bool = True,
    ) -> List[LiveboardResponse]:
        """List-returning shim around :meth:`iter_liveboards` for callers
        that want the whole result in memory. Prefer ``iter_liveboards``
        in the source pipeline so memory stays bounded at scale.
        """
        return list(
            self.iter_liveboards(
                liveboard_ids=liveboard_ids,
                tag_names=tag_names,
                include_details=include_details,
            )
        )

    def get_liveboard_details(self, liveboard_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Get detailed Liveboard metadata.

        Args:
            liveboard_ids: Liveboard IDs (GUIDs)

        Returns:
            List of detailed Liveboard objects
        """
        return self.get_metadata_details(
            metadata_type="LIVEBOARD",
            metadata_ids=liveboard_ids,
        )

    # ===== Answer API Methods =====

    def iter_answers(
        self,
        answer_ids: Optional[List[str]] = None,
        tag_names: Optional[List[str]] = None,
    ) -> Iterator[AnswerResponse]:
        """Stream Answers (saved queries/visualizations) page-by-page.

        Same scale-driven pattern as :meth:`iter_liveboards`: buffer up to
        ``tml_export_batch_size`` answers, TML-enrich for lineage in one
        chunk, then yield. Heap stays O(batch_size), not O(total answers).
        """
        batch_size = self.config.tml_export_batch_size
        buffer: List[AnswerResponse] = []
        dropped = 0
        first_exc: Optional[BaseException] = None
        for item in self._paginated_metadata_search(
            object_type="ANSWER",
            fetch_ids=answer_ids,
            tag_names=tag_names,
        ):
            try:
                buffer.append(AnswerResponse(**item))
            except Exception as e:
                dropped += 1
                if first_exc is None:
                    first_exc = e
                logger.debug(f"Skipping malformed answer: {e}")
                continue
            if len(buffer) >= batch_size:
                yield from self._enrich_and_yield_answers(buffer)
                buffer = []
        if buffer:
            yield from self._enrich_and_yield_answers(buffer)
        if dropped:
            self.report.malformed_answers_dropped += dropped
            self.report.warning(
                title="Answers dropped due to malformed wire payload",
                message=(
                    "One or more ANSWER entries from /metadata/search "
                    "could not be parsed into AnswerResponse and were "
                    "skipped. Affected answers will not be ingested."
                ),
                context=f"dropped={dropped}",
                exc=first_exc,
            )
        st_dropped = self._consume_source_table_drop_count()
        if st_dropped:
            self.report.malformed_source_tables_dropped += st_dropped
            self.report.warning(
                title="Source-table refs dropped within Answers",
                message=(
                    "One or more nested source_tables entries within "
                    "Answers were malformed and skipped. Parent Answers "
                    "still emit, but the affected upstream lineage edges "
                    "will be missing."
                ),
                context=f"dropped={st_dropped}",
            )

    def _enrich_and_yield_answers(
        self, answers: List[AnswerResponse]
    ) -> Iterator[AnswerResponse]:
        """TML-enrich a batch of answers with source-table lineage and
        yield them.
        """
        if not answers:
            return
        try:
            details = self._get_answer_dependencies([a.id for a in answers])
            details_map = {d["id"]: d for d in details if "id" in d}
            for answer in answers:
                detail = details_map.get(answer.id)
                if detail is None:
                    continue
                raw_st = detail.get("source_tables") or []
                answer.source_tables = raw_st
                # ``_coerce_source_tables`` drops malformed dict entries
                # (e.g. missing ``id``). Compare raw vs validated lengths
                # so the caller can surface aggregated drops to the
                # report.
                kept = len(answer.source_tables or [])
                if len(raw_st) > kept:
                    self._source_table_drop_count += len(raw_st) - kept
                answer.chart_type = detail.get("chart_type")
                answer.question_text = detail.get("question_text")
                answer.source_columns = detail.get("source_columns")
        except Exception as e:
            self.report.warning(
                title="Answer Dependency Fetch Failed",
                message=(
                    "Answer source-table lineage will be missing for "
                    "this batch. Check per-object TML access."
                ),
                context=f"batch_count={len(answers)}",
                exc=e,
            )
            logger.warning(
                f"Failed to fetch Answer dependencies for batch of {len(answers)}: {e}"
            )
        yield from answers

    def _consume_source_table_drop_count(self) -> int:
        """Read-and-reset the counter populated by
        ``_enrich_and_yield_answers``. Returns ``0`` if no drops happened
        since the last flush.
        """
        count = self._source_table_drop_count
        self._source_table_drop_count = 0
        return count

    def get_answers(
        self,
        answer_ids: Optional[List[str]] = None,
        tag_names: Optional[List[str]] = None,
    ) -> List[AnswerResponse]:
        """List-returning shim around :meth:`iter_answers`. Prefer
        ``iter_answers`` in the source pipeline.
        """
        return list(self.iter_answers(answer_ids=answer_ids, tag_names=tag_names))

    def get_answer_details(self, answer_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Get detailed Answer metadata.

        Args:
            answer_ids: Answer IDs (GUIDs)

        Returns:
            List of detailed Answer objects
        """
        return self.get_metadata_details(
            metadata_type="ANSWER",
            metadata_ids=answer_ids,
        )

    def _get_liveboard_visualizations_via_tml(
        self,
        liveboard_ids: List[str],
    ) -> Dict[str, List[VisualizationResponse]]:
        """Extract per-viz {viz_guid, name, source_table_ids} from liveboard TML.

        ``metadata_search`` doesn't surface viz-level table refs (only
        liveboard-level ``filterLogicalTableIds``), but TML's
        ``liveboard.visualizations[].answer.tables[].fqn`` does — when access
        permits. ``export_fqn=True`` ensures table refs come back as GUIDs
        (the ``fqn`` field) so we can construct dataset URNs directly without
        a name-lookup step.

        Per-liveboard FORBIDDEN errors are surfaced via the existing
        ``_check_tml_item_status`` warning machinery; the affected liveboards
        simply won't have a chart layer (the source falls back to
        dashboard→dataset edges from ``filterLogicalTableIds``).

        Returns:
            Map ``liveboard_id -> [VisualizationResponse]``. Liveboards with
            no accessible TML or no visualizations are absent from the map.
        """
        if not liveboard_ids:
            return {}

        viz_map: Dict[str, List[VisualizationResponse]] = {}
        # Aggregate YAML-parse failures so a tenant returning broken TML
        # for a subset of liveboards surfaces in the ingestion report,
        # not just in DEBUG logs.
        yaml_parse_failures: List[str] = []
        # Track post-YAML-parse TML schema drifts (missing viz_guid /
        # ``tables`` shape mismatch etc.). A future TS API key-rename
        # would otherwise zero out chart lineage silently.
        structural_skips = 0
        for item in self._iter_tml_export_items(
            liveboard_ids,
            batch_size=self.config.tml_export_batch_size,
            export_associated=False,
            export_fqn=True,
            edoc_format="YAML",
            failure_warning_title="Liveboard TML Export Failed",
            failure_warning_message=(
                "Charts will not be emitted for the liveboards in this "
                "batch. Most often caused by FORBIDDEN per-object access — "
                "share the liveboards and their underlying objects with "
                "the ingestion principal, or by a server-side timeout "
                "(reduce ``tml_export_batch_size``)."
            ),
        ):
            if not self._check_tml_item_status(item, "LIVEBOARD"):
                continue

            tml_content = item.get("edoc")
            liveboard_id = item.get("info", {}).get("id")
            if not tml_content or not liveboard_id:
                continue

            try:
                tml_data = _safe_load_tml_yaml(tml_content)
            except yaml.YAMLError as e:
                yaml_parse_failures.append(f"liveboard={liveboard_id}: {e}")
                logger.warning(f"Failed to parse Liveboard TML for {liveboard_id}: {e}")
                continue
            if not isinstance(tml_data, dict):
                structural_skips += 1
                continue

            visualizations: List[VisualizationResponse] = []
            for viz_entry in (tml_data.get("liveboard") or {}).get(
                "visualizations"
            ) or []:
                if not isinstance(viz_entry, dict):
                    structural_skips += 1
                    continue
                viz_guid = viz_entry.get("viz_guid")
                if not viz_guid:
                    structural_skips += 1
                    continue
                answer = viz_entry.get("answer") or {}

                # Pull table-level lineage refs. With ``export_fqn=True`` each
                # entry has a ``fqn`` (the GUID) alongside the human name.
                source_table_ids: List[str] = []
                for tbl in answer.get("tables") or []:
                    if not isinstance(tbl, dict):
                        structural_skips += 1
                        continue
                    fqn = tbl.get("fqn")
                    if isinstance(fqn, str) and fqn:
                        source_table_ids.append(fqn)

                # Parse ``[col]`` tokens from search_query so this viz
                # emits Chart→Dataset column-level lineage downstream.
                # Shared with the Answer TML path via the module-level
                # helper.
                search_query = answer.get("search_query") or ""
                source_columns = _extract_search_query_columns(search_query)

                visualizations.append(
                    VisualizationResponse(
                        id=viz_guid,
                        name=answer.get("name") or viz_guid,
                        description=answer.get("description"),
                        chart_type=_extract_answer_chart_type(answer),
                        question_text=(
                            search_query
                            if isinstance(search_query, str) and search_query
                            else None
                        ),
                        source_table_ids=source_table_ids or None,
                        source_columns=source_columns or None,
                    )
                )

            if visualizations:
                viz_map[liveboard_id] = visualizations

        if yaml_parse_failures:
            self.report.warning(
                title="Liveboard TML Parse Failures",
                message=(
                    "TML export returned syntactically broken YAML for "
                    "one or more liveboards. The Chart layer / source-"
                    "table lineage for these liveboards is missing; "
                    "dashboard-level edges still emit via the "
                    "filterLogicalTableIds fallback."
                ),
                context=(
                    f"count={len(yaml_parse_failures)}; "
                    f"first_reasons={yaml_parse_failures[:3]}"
                ),
            )
        if structural_skips:
            # Info-level rather than warning: the TML parsed, the keys
            # we recognise just weren't all there. Could be benign (a
            # liveboard with zero vizes) or could indicate TS-side
            # schema drift; surface the count so operators notice.
            self.report.info(
                title="Liveboard TML structural skips",
                message=(
                    "TML payloads parsed but had unexpected shape "
                    "(missing viz_guid / tables not a list / etc.). "
                    "Affected visualizations were skipped from chart "
                    "lineage. If this number grows after a TS upgrade, "
                    "suspect a metadata-schema rename."
                ),
                context=f"count={structural_skips}",
            )
        return viz_map

    def get_sql_view_definitions(self, sql_view_ids: List[str]) -> Dict[str, str]:
        """Fetch the raw SQL statement for each ``SQL_VIEW`` via TML export.

        Returns ``{sql_view_id: statement_string}``. SQL views the
        principal can't TML-export (FORBIDDEN per-object ACLs) are
        silently absent from the map — same graceful-degradation
        contract as the Answer TML path. YAML-parse failures and
        structural skips surface as aggregated warnings on the
        report so a tenant returning broken TML for a subset of
        views is visible without flooding the log.
        """
        if not sql_view_ids:
            return {}

        result: Dict[str, str] = {}
        yaml_parse_failures: List[str] = []
        structural_skips = 0
        for item in self._iter_tml_export_items(
            sql_view_ids,
            batch_size=self.config.tml_export_batch_size,
            export_associated=False,
            export_fqn=True,
            edoc_format="YAML",
            failure_warning_title="SQL View TML Export Failed",
            failure_warning_message=(
                "viewLogic / SQL-parsed lineage will not be emitted for "
                "the SQL views in this batch. Most often caused by FORBIDDEN "
                "per-object access — share the SQL views and their "
                "underlying connections with the ingestion principal."
            ),
        ):
            if not self._check_tml_item_status(item, "SQL_VIEW"):
                continue
            edoc = item.get("edoc")
            sv_id = item.get("info", {}).get("id")
            if not edoc or not sv_id:
                continue
            try:
                tml = _safe_load_tml_yaml(edoc)
            except yaml.YAMLError as e:
                yaml_parse_failures.append(f"sql_view={sv_id}: {e}")
                logger.warning(f"Failed to parse SQL View TML for {sv_id}: {e}")
                continue
            if not isinstance(tml, dict):
                structural_skips += 1
                continue
            stmt = _extract_sql_view_statement(tml)
            if stmt is None:
                structural_skips += 1
                continue
            result[sv_id] = stmt

        if yaml_parse_failures:
            self.report.warning(
                title="SQL View TML Parse Failures",
                message=(
                    "TML export returned syntactically broken YAML for "
                    "one or more SQL views. viewLogic and SQL-parsed "
                    "lineage are missing for those views; the existing "
                    "columns[*].sources edges still emit."
                ),
                context=(
                    f"count={len(yaml_parse_failures)}; "
                    f"first_reasons={yaml_parse_failures[:3]}"
                ),
            )
        if structural_skips:
            self.report.info(
                title="SQL View TML structural skips",
                message=(
                    "TML payloads parsed but had unexpected shape "
                    "(no sql_view.sql_query / sql_view.statement). "
                    "Affected views emit without viewLogic. A non-trivial "
                    "count after a TS upgrade suggests a metadata-schema "
                    "rename."
                ),
                context=f"count={structural_skips}",
            )
        return result

    def _get_answer_dependencies(self, answer_ids: List[str]) -> List[Dict[str, Any]]:
        """Extract per-answer TML fields needed by the source.

        Returns one dict per answer with the keys:

        * ``id`` — answer GUID
        * ``source_tables`` — list of ``{id, name}`` (TML ``answer.tables``)
        * ``chart_type`` — TML ``answer.chart_type``
        * ``question_text`` — TML ``answer.search_query``

        Answers that the principal can't TML-export are silently absent —
        the source falls back to the header-only AnswerResponse.
        """
        if not answer_ids:
            return []

        dependencies: List[Dict[str, Any]] = []
        # Aggregate YAML-parse failures so they surface in the report.
        yaml_parse_failures: List[str] = []
        # Post-parse structural skips (TML schema drift). Same purpose
        # as in ``_get_liveboard_visualizations_via_tml``.
        structural_skips = 0
        for item in self._iter_tml_export_items(
            answer_ids,
            batch_size=self.config.tml_export_batch_size,
            export_associated=False,
            export_fqn=True,
            failure_warning_title="Answer TML Export Failed",
            failure_warning_message=(
                "Answer source-table lineage will be missing for the "
                "answers in this batch."
            ),
        ):
            if not self._check_tml_item_status(item, "ANSWER"):
                continue

            tml_content = item.get("edoc")
            answer_id = item.get("info", {}).get("id")
            if not tml_content or not answer_id:
                continue

            try:
                tml_data = _safe_load_tml_yaml(tml_content)
            except yaml.YAMLError as e:
                yaml_parse_failures.append(f"answer={answer_id}: {e}")
                logger.warning(f"Failed to parse Answer TML for dependencies: {e}")
                continue
            if not isinstance(tml_data, dict):
                structural_skips += 1
                continue

            # TML structure: { answer: { tables: [...], chart_type, search_query, ... } }
            # With ``export_fqn=True`` each table ref carries ``fqn`` — the
            # authoritative GUID. The ``id`` and ``name`` fields on a
            # table ref are the display name (e.g. ``"My Model"``); using
            # them as the dataset URN key emits a name-keyed URN that
            # won't match the GUID-keyed worksheet URNs we emit
            # elsewhere — the chart→worksheet edge silently dangles. Skip
            # the ref entirely when no ``fqn`` is present rather than
            # emitting a misleading edge.
            answer_tml = tml_data.get("answer", {})
            tables = answer_tml.get("tables", [])
            source_tables: List[Dict[str, Any]] = []
            for table_ref in tables:
                if not isinstance(table_ref, dict):
                    structural_skips += 1
                    continue
                table_id = table_ref.get("fqn")
                if table_id:
                    source_tables.append(
                        {"id": table_id, "name": table_ref.get("name", "")}
                    )
                else:
                    # No fqn — TML may be from an older TS version without
                    # ``export_fqn=True`` support, or the answer references a
                    # source we can't resolve to a GUID. Count it so a tenant
                    # upgrading TS sees the drift in the report.
                    structural_skips += 1

            search_query = answer_tml.get("search_query")
            # Same ``[col]`` parsing the Liveboard viz path runs, so
            # standalone Answers emit Chart→Dataset column-level
            # lineage symmetrically with the Visualization path.
            source_columns = _extract_search_query_columns(search_query)
            dependencies.append(
                {
                    "id": answer_id,
                    "source_tables": source_tables,
                    "chart_type": _extract_answer_chart_type(answer_tml),
                    "question_text": (
                        search_query
                        if isinstance(search_query, str) and search_query
                        else None
                    ),
                    "source_columns": source_columns or None,
                }
            )

        logger.debug(
            f"Extracted dependencies for {len(dependencies)}/{len(answer_ids)} Answers"
        )
        if yaml_parse_failures:
            self.report.warning(
                title="Answer TML Parse Failures",
                message=(
                    "TML export returned syntactically broken YAML for "
                    "one or more answers. Source-table lineage is missing "
                    "for these answers."
                ),
                context=(
                    f"count={len(yaml_parse_failures)}; "
                    f"first_reasons={yaml_parse_failures[:3]}"
                ),
            )
        if structural_skips:
            self.report.info(
                title="Answer TML structural skips",
                message=(
                    "TML payloads parsed but had unexpected shape "
                    "(table_ref not a dict / missing ``fqn`` / etc.). "
                    "Affected source-table refs were skipped from "
                    "answer lineage. A non-trivial count after a TS "
                    "upgrade suggests metadata-schema drift."
                ),
                context=f"count={structural_skips}",
            )
        return dependencies

    # ===== Worksheet/Table API Methods =====

    def get_logical_tables(
        self,
        table_ids: Optional[List[str]] = None,
        tag_names: Optional[List[str]] = None,
        include_details: bool = True,
    ) -> List[LogicalTableResponse]:
        """
        Get Worksheets (logical tables/views).

        By default this fetches ``include_details=True`` so each returned model
        already carries its ``columns`` (schema) and ``data_source_id``
        (connection ref). Callers no longer need a follow-up
        ``get_logical_table_details`` per table.

        Args:
            table_ids: Optional specific Worksheet IDs
            tag_names: Optional tag filter
            include_details: Set to ``False`` to skip column/datasource detail
                (e.g. for a fast list of names/IDs only).

        Returns:
            List of Worksheet metadata objects as Pydantic models
        """
        raw_data = self.get_metadata_list(
            metadata_type="LOGICAL_TABLE",
            fetch_ids=table_ids,
            tag_names=tag_names,
            include_details=include_details,
        )
        # Per-item try matching the ``iter_answers`` / ``iter_liveboards``
        # pattern: a single malformed worksheet must not kill the whole
        # batch. Column-source drops are routed through the model
        # validation context so they can be surfaced aggregated to the
        # report rather than buried in DEBUG logs.
        results: List[LogicalTableResponse] = []
        dropped = 0
        first_exc: Optional[BaseException] = None
        column_source_drops: List[str] = []
        context = {COLUMN_SOURCE_DROPS: column_source_drops}
        for item in raw_data:
            try:
                results.append(
                    LogicalTableResponse.model_validate(item, context=context)
                )
            except Exception as e:
                dropped += 1
                if first_exc is None:
                    first_exc = e
                logger.debug(f"Skipping malformed logical table: {e}")
        if dropped:
            self.report.malformed_logical_tables_dropped += dropped
            self.report.warning(
                title="Logical tables dropped due to malformed wire payload",
                message=(
                    "One or more LOGICAL_TABLE entries from /metadata/search "
                    "could not be parsed and were skipped. Affected "
                    "datasets will not be ingested."
                ),
                context=f"dropped={dropped}",
                exc=first_exc,
            )
        if column_source_drops:
            self.report.malformed_column_sources_dropped += len(column_source_drops)
            self.report.warning(
                title="Column sources dropped within Logical Tables",
                message=(
                    "One or more nested column source entries were "
                    "malformed and skipped. Parent columns still emit, "
                    "but the affected fine-grained column lineage edges "
                    "will be missing."
                ),
                context=(
                    f"dropped={len(column_source_drops)}; "
                    f"first_reasons={column_source_drops[:3]}"
                ),
            )

        # Single batched TML export over every SQL_VIEW in the result
        # set so each emitted dataset can carry a ViewProperties
        # aspect with the raw SQL plus sqlglot-parsed upstream
        # lineage. Other LOGICAL_TABLE subtypes (WORKSHEET,
        # ONE_TO_ONE_LOGICAL, etc.) leave ``sql_view_definition`` as
        # None — they don't have a SQL statement.
        sql_view_ids = [t.id for t in results if t.type == "SQL_VIEW"]
        if sql_view_ids:
            sql_map = self.get_sql_view_definitions(sql_view_ids)
            for t in results:
                if t.type == "SQL_VIEW":
                    t.sql_view_definition = sql_map.get(t.id)
        return results

    def get_logical_table_details(self, table_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Get detailed Worksheet metadata.

        Args:
            table_ids: Worksheet IDs (GUIDs)

        Returns:
            List of detailed Worksheet objects
        """
        return self.get_metadata_details(
            metadata_type="LOGICAL_TABLE",
            metadata_ids=table_ids,
        )

    def fetch_logical_tables_by_id(
        self,
        table_ids: List[str],
    ) -> List[LogicalTableResponse]:
        """Fetch specific LOGICAL_TABLEs by GUID (any subType).

        Used to backfill upstream base tables referenced by worksheet column
        ``sources``. Unlike ``get_logical_tables`` which paginates the
        unfiltered list (returning whatever subTypes the principal can see —
        usually only WORKSHEETs), this issues one ``metadata_search`` with
        an explicit identifier per requested GUID. Per-object visibility
        rules still apply: a user who can see a worksheet's column lineage
        ``sources`` may still be denied access to the base tables those
        sources reference, in which case this returns an empty list.

        Returns:
            ``LogicalTableResponse`` per accessible GUID, with ``columns``
            and ``data_source_id`` populated. GUIDs the principal can't see
            are silently absent from the result.
        """
        if not table_ids:
            return []

        # Chunk so a 100x-scale caller doesn't blow past TS's per-request
        # body-size limit. See ``metadata_fetch_batch_size`` rationale.
        batch_size = self.config.metadata_fetch_batch_size
        results: List[LogicalTableResponse] = []
        dropped = 0
        first_exc: Optional[BaseException] = None
        column_source_drops: List[str] = []
        validation_context = {COLUMN_SOURCE_DROPS: column_source_drops}
        for start in range(0, len(table_ids), batch_size):
            chunk = table_ids[start : start + batch_size]
            request: Dict[str, Any] = {
                "metadata": [
                    {"type": "LOGICAL_TABLE", "identifier": tid} for tid in chunk
                ],
                "include_details": True,
                "include_stats": True,
                "record_size": len(chunk),
            }
            if self.config.org_identifier:
                request["org_identifier"] = self.config.org_identifier
            try:
                response = self._call_sdk(
                    self.ts_client.metadata_search, request=request
                )
            except Exception as e:
                self.report.warning(
                    title="Upstream Table Backfill Failed",
                    message=(
                        "Source tables referenced by ingested entities "
                        "could not be fetched for this batch. Lineage "
                        "edges to these tables will remain dangling in "
                        "DataHub."
                    ),
                    context=(
                        f"batch_offset={start}, batch_count={len(chunk)}, "
                        f"total={len(table_ids)}"
                    ),
                    exc=e,
                )
                logger.warning(
                    f"Failed to fetch source LOGICAL_TABLEs batch "
                    f"[{start}..{start + len(chunk)}]: {e}. Lineage edges to "
                    "these tables will remain dangling in DataHub."
                )
                continue

            items = _response_items(response, "metadata")

            for item in items:
                if not isinstance(item, dict) or "metadata_header" not in item:
                    dropped += 1
                    logger.debug(
                        "Skipping LOGICAL_TABLE search item with unexpected shape "
                        f"(type={type(item).__name__}, "
                        f"has_header={isinstance(item, dict) and 'metadata_header' in item})"
                    )
                    continue
                flattened: Dict[str, Any] = {**item.get("metadata_header", {})}
                flattened["metadata_id"] = item.get("metadata_id")
                # Lift the stats block — keeps this flattener symmetric with
                # _paginated_metadata_search. LOGICAL_TABLE returns stats: null
                # in practice but the lift is harmless.
                if "stats" in item:
                    flattened["stats"] = item["stats"]
                detail = item.get("metadata_detail") or {}
                flattened["columns"] = self._parse_columns_from_metadata_detail(detail)
                flattened["data_source_id"] = detail.get("dataSourceId")
                flattened["data_source_type"] = detail.get("dataSourceTypeEnum")
                # See note in _paginated_metadata_search: the physical
                # mapping lives under logicalTableContent.tableMappingInfo.
                table_mapping = (detail.get("logicalTableContent") or {}).get(
                    "tableMappingInfo"
                ) or {}
                flattened["physical_database_name"] = table_mapping.get("databaseName")
                flattened["physical_schema_name"] = table_mapping.get("schemaName")
                flattened["physical_table_name"] = table_mapping.get("tableName")
                joins = detail.get("joins")
                if isinstance(joins, list):
                    flattened["join_count"] = len(joins)
                try:
                    results.append(
                        LogicalTableResponse.model_validate(
                            flattened, context=validation_context
                        )
                    )
                except Exception as e:
                    dropped += 1
                    if first_exc is None:
                        first_exc = e
                    logger.debug(f"Skipping malformed source LOGICAL_TABLE: {e}")
        self._report_dropped(
            title="Upstream LogicalTable Parse Failures",
            dropped=dropped,
            total=dropped + len(results),
            consequence=(
                "Lineage edges to these tables will remain dangling in DataHub."
            ),
            exc=first_exc,
        )
        if column_source_drops:
            self.report.malformed_column_sources_dropped += len(column_source_drops)
            self.report.warning(
                title="Column sources dropped within upstream Logical Tables",
                message=(
                    "One or more nested column source entries on upstream "
                    "tables were malformed and skipped. Affected fine-grained "
                    "column lineage edges will be missing."
                ),
                context=(
                    f"dropped={len(column_source_drops)}; "
                    f"first_reasons={column_source_drops[:3]}"
                ),
            )
        return results

    # ===== Connection Test =====

    def test_connection(self) -> bool:
        """
        Test API connectivity and authentication.

        Issues a 1-row ``metadata_search`` probe to verify:
        1. Base URL is correct
        2. Authentication credentials work
        3. API permissions are sufficient

        ``get_workspaces()`` is unsuitable for this probe because it
        graceful-degrades on 403 — an unauthorized principal would get
        an empty list and this method would report success, masking the
        failure as an empty ingestion run. The direct ``metadata_search``
        probe propagates HTTP errors so the caller can distinguish
        auth / permission / network failures.

        Returns:
            ``True`` on success.

        Raises:
            ThoughtSpotAuthenticationError: bearer token rejected (401).
            ThoughtSpotPermissionError: principal lacks ``/metadata/search``
                access (403).
            ThoughtSpotAPIError: network, 5xx, or other unexpected failure.
        """
        try:
            request: Dict[str, Any] = {
                "metadata": [{"type": "LIVEBOARD"}],
                "record_size": 1,
            }
            if self.config.org_identifier:
                request["org_identifier"] = self.config.org_identifier
            self._call_sdk(self.ts_client.metadata_search, request=request)
            logger.info("Successfully connected to ThoughtSpot API")
            return True
        except (
            ThoughtSpotAuthenticationError,
            ThoughtSpotPermissionError,
            ThoughtSpotAPIError,
        ):
            # Already classified by ``_authenticate`` / ``_call_sdk`` —
            # propagate as-is so the caller sees the most specific class.
            raise
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            cls = _classify_http_error(e)
            if cls is ThoughtSpotAuthenticationError:
                raise cls(
                    f"Authentication failed - bearer token rejected by "
                    f"/metadata/search. {e}"
                ) from e
            if cls is ThoughtSpotPermissionError:
                raise cls(
                    f"Permission denied - principal cannot call /metadata/search. {e}"
                ) from e
            raise ThoughtSpotAPIError(f"Connection test failed: {e}") from e

    def close(self) -> None:
        """Close HTTP session and release resources.

        The SDK doesn't expose its own teardown, but it does surface
        the underlying ``requests.Session`` we mounted retry/timeout
        adapters on. Closing the session promptly releases pooled
        connections instead of waiting for GC.
        """
        session = getattr(self.ts_client, "requests_session", None)
        if session is not None:
            session.close()

    def __enter__(self) -> "ThoughtSpotClient":
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[object],
    ) -> None:
        """Context manager exit - close session."""
        self.close()
