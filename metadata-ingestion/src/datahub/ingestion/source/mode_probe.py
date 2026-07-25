import logging
from typing import (
    Any,
    Callable,
    Dict,
    List,
    NoReturn,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    TypeVar,
)

import requests

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import (
    UNFILTERED,
    ClientProbe,
    ProbeLevel,
    ProbeSoftError,
)
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.mode import (
    MODE_ADAPTER_PLATFORM_MAP,
    ModeAPIConfig,
    ModeApiSession,
    fetch_json,
)
from datahub.utilities.ratelimiter import RateLimiter

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# Mode's API and its own config field (space_pattern) call this level "space", but
# the connector's emitted container subtype is BIContainerSubTypes.MODE_COLLECTION
# ("Collection") — Mode renamed Spaces to Collections in its product UI. There is no
# shared subtype whose value is "Space", so this level uses the API's own term
# rather than a mismatched container subtype.
MODE_SPACE: ProbeNodeKind = "Space"


class ModeProbeConfig(Protocol):
    """The slice of ModeConfig the probe needs: pagination size, the space/report
    scoping a recipe would actually apply (space_pattern, exclude_personal_collections,
    exclude_restricted, exclude_archived), and the retry/rate-limit knobs fetch_json
    needs. A Protocol (not the concrete ModeConfig class) so tests can pass a
    lightweight duck-typed fake without satisfying every ModeConfig field.
    api_options stays the concrete ModeAPIConfig (not a nested Protocol): mypy
    checks a Protocol's plain-attribute types invariantly, and a second Protocol
    layer there trips that even though the real ModeAPIConfig structurally
    matches it fine standalone -- tests build a real ModeAPIConfig(...) instead
    of a duck-typed fake for this one field."""

    items_per_page: int
    exclude_personal_collections: bool
    exclude_restricted: bool
    exclude_archived: bool
    space_pattern: AllowDenyPattern
    api_options: ModeAPIConfig

    def get_mode_session(self) -> Tuple[ModeApiSession, str]: ...


# Mode's own client is (session, workspace_uri, rate_limiter): the same session
# construction ModeConfig.get_mode_session() gives ModeSource, plus a rate limiter
# built from the recipe's own api_options.requests_per_minute so the probe's many
# requests (pagination, cross-space report search) throttle the same way a real
# ingestion run would.
ModeClient = Tuple[ModeApiSession, str, RateLimiter]


def _build_mode_client(config: ModeProbeConfig) -> ModeClient:
    session, workspace_uri = config.get_mode_session()
    rate_limiter = RateLimiter(
        max_calls=config.api_options.requests_per_minute, period=60
    )
    return session, workspace_uri, rate_limiter


def _close_mode_client(client: ModeClient) -> None:
    session, _workspace_uri, _rate_limiter = client
    session.close()


def _spaces_filter(config: ModeProbeConfig) -> str:
    # Mirrors mode.py's _get_space_name_and_tokens: send filter=custom when the
    # recipe excludes personal collections server-side (the default), else
    # filter=all -- so the probe enumerates exactly the spaces a real ingestion
    # run of this recipe would see, not a superset that includes collections
    # exclude_personal_collections would have dropped.
    return "custom" if config.exclude_personal_collections else "all"


def _is_restricted_space(space: Dict[str, Any]) -> bool:
    # Mirrors mode.py's _get_space_name_and_tokens (":857-861"): both fields
    # can independently signal "restricted" against a live workspace.
    return (
        bool(space.get("restricted"))
        or space.get("default_access_level") == "restricted"
    )


def _raise_soft_or_hard(exc: requests.HTTPError, context: str) -> NoReturn:
    # Mirrors mode.py's four _is_http_404 branches (reports, datasets, queries,
    # charts): a 404 (deleted between listing and fetch) or 403 (restricted,
    # inaccessible to this token) on ONE listing is a normal, expected outcome
    # in production, not a reason to fail the whole probe -- but it must not be
    # silently swallowed either (indistinguishable from a genuinely empty
    # listing). Raises ProbeSoftError so callers can choose how to surface it:
    # the hierarchy listers let it propagate to ClientProbe.list_children,
    # which records it on ProbeResult.warnings and keeps sibling levels; the
    # probe_run provider methods catch it locally via _tolerant (see below),
    # since probe_methods.ProbeMethodResult has no warnings channel to bubble
    # it through. Anything else (auth failures, 5xx, connection errors) is a
    # hard error either way.
    status = exc.response.status_code if exc.response is not None else None
    if status not in (404, 403):
        raise exc
    raise ProbeSoftError(
        f"{context} returned HTTP {status}; treating it as empty."
    ) from exc


def _tolerant(fetch: Callable[[], _T], default: _T) -> _T:
    """probe_run methods have no ProbeResult.warnings channel (see
    agent.probe_methods.ProbeMethodResult), so they keep the log-and-degrade
    behavior locally instead of letting ProbeSoftError propagate to the CLI
    as a hard failure."""
    try:
        return fetch()
    except ProbeSoftError as exc:
        logger.warning(str(exc))
        return default


def _fetch_page(
    session: ModeApiSession,
    config: ModeProbeConfig,
    rate_limiter: RateLimiter,
    url: str,
) -> Dict[str, Any]:
    # Routes every probe request through the exact same rate-limit/timeout/
    # 429-504-retry path mode.py's own ingestion uses (fetch_json), instead of
    # a bare session.get() that would bypass all of it.
    return fetch_json(
        session,
        url,
        timeout=config.api_options.timeout,
        rate_limiter=rate_limiter,
        retry_backoff_multiplier=config.api_options.retry_backoff_multiplier,
        max_retry_interval=config.api_options.max_retry_interval,
        max_attempts=config.api_options.max_attempts,
    )


def _get_embedded(
    session: ModeApiSession,
    config: ModeProbeConfig,
    rate_limiter: RateLimiter,
    url: str,
    key: str,
    context: str,
) -> List[Dict[str, Any]]:
    try:
        payload = _fetch_page(session, config, rate_limiter, url)
    except requests.HTTPError as exc:
        _raise_soft_or_hard(exc, context)
    return list(payload.get("_embedded", {}).get(key, []))


def _get_embedded_paged(
    session: ModeApiSession,
    config: ModeProbeConfig,
    rate_limiter: RateLimiter,
    url: str,
    key: str,
    context: str,
) -> List[Dict[str, Any]]:
    # Mirrors mode.py's _get_paged_request_json: the spaces/reports/datasets
    # listings truncate at one page (default 30 items) unless walked with
    # per_page/page until a page comes back empty. The single-report-scoped
    # /queries and /queries/{token}/charts endpoints do NOT paginate this
    # way — mode.py documents them as not handling pagination properly — so
    # callers must keep routing those through the unpaginated _get_embedded.
    #
    # A soft error partway through (e.g. page 3 of 5 403s) raises rather than
    # returning the pages collected so far: a truncated listing that looks
    # complete is worse than an honest "couldn't finish this, here's why".
    sep = "&" if "?" in url else "?"
    items: List[Dict[str, Any]] = []
    page = 1
    while True:
        page_url = f"{url}{sep}per_page={config.items_per_page}&page={page}"
        try:
            payload = _fetch_page(session, config, rate_limiter, page_url)
        except requests.HTTPError as exc:
            _raise_soft_or_hard(exc, context)
        page_items = list(payload.get("_embedded", {}).get(key, []))
        if not page_items:
            break
        items.extend(page_items)
        page += 1
    return items


def _display_name(item: Dict[str, Any]) -> str:
    # Live Mode workspaces can hold reports with a null "name" (seen against a
    # real workspace); fall back to the token, then "unknown" — mirroring
    # mode.py's own name-or-token-or-"unknown" convention (see
    # construct_query_or_dataset's report_name resolution) rather than letting
    # AllowDenyPattern.allowed() blow up on a non-string name.
    return str(item.get("name") or item.get("token") or "unknown")


def _fetch_spaces(
    session: ModeApiSession,
    config: ModeProbeConfig,
    rate_limiter: RateLimiter,
    workspace_uri: str,
) -> List[Dict[str, Any]]:
    """Every space the workspace has, filtered exactly the way mode.py's own
    ingestion run would see them: the server-side filter=all/custom, plus
    exclude_restricted client-side. The single call site for fetching spaces
    -- every lister that needs a space listing or a name-to-token lookup goes
    through this, so there is exactly one place that mirrors mode.py's space
    visibility (previously this logic was duplicated across three call
    sites, and only some of them applied exclude_restricted)."""
    url = f"{workspace_uri}/spaces?filter={_spaces_filter(config)}"
    spaces = _get_embedded_paged(
        session, config, rate_limiter, url, "spaces", context="workspace spaces listing"
    )
    if config.exclude_restricted:
        spaces = [s for s in spaces if not _is_restricted_space(s)]
    return spaces


def _fetch_reports(
    session: ModeApiSession,
    config: ModeProbeConfig,
    rate_limiter: RateLimiter,
    workspace_uri: str,
    space_token: str,
) -> List[Dict[str, Any]]:
    """Every report in one space, filtered the way mode.py's own ingestion run
    would see them: ?filter=all, paginated, plus exclude_archived client-side.
    The single call site for fetching a space's reports."""
    url = f"{workspace_uri}/spaces/{space_token}/reports?filter=all"
    reports = _get_embedded_paged(
        session,
        config,
        rate_limiter,
        url,
        "reports",
        context=f"reports listing for space token '{space_token}'",
    )
    if config.exclude_archived:
        reports = [r for r in reports if not r.get("archived", False)]
    return reports


def _space_token(
    session: ModeApiSession,
    config: ModeProbeConfig,
    rate_limiter: RateLimiter,
    workspace_uri: str,
    space_name: str,
) -> Optional[str]:
    for space in _fetch_spaces(session, config, rate_limiter, workspace_uri):
        if _display_name(space) == space_name:
            return space.get("token")
    return None


def _report_token(
    session: ModeApiSession,
    config: ModeProbeConfig,
    rate_limiter: RateLimiter,
    workspace_uri: str,
    space_token: str,
    report_name: str,
) -> Optional[str]:
    for report in _fetch_reports(
        session, config, rate_limiter, workspace_uri, space_token
    ):
        if _display_name(report) == report_name:
            return report.get("token")
    return None


def _spaces(
    client: ModeClient, config: ModeProbeConfig, parent_path: List[str]
) -> Sequence[str]:
    session, workspace_uri, rate_limiter = client
    spaces = _fetch_spaces(session, config, rate_limiter, workspace_uri)
    return [_display_name(space) for space in spaces]


def _reports(
    client: ModeClient, config: ModeProbeConfig, parent_path: List[str]
) -> Sequence[str]:
    session, workspace_uri, rate_limiter = client
    space_token = _space_token(
        session, config, rate_limiter, workspace_uri, parent_path[0]
    )
    if space_token is None:
        return []
    reports = _fetch_reports(session, config, rate_limiter, workspace_uri, space_token)
    return [_display_name(report) for report in reports]


def _datasets(
    client: ModeClient, config: ModeProbeConfig, parent_path: List[str]
) -> Sequence[str]:
    session, workspace_uri, rate_limiter = client
    space_token = _space_token(
        session, config, rate_limiter, workspace_uri, parent_path[0]
    )
    if space_token is None:
        return []
    # Paginated with ?filter=all, same as _fetch_reports — mode.py:1701-1708
    # fetches this identically (per_page/page walk + filter=all). And despite
    # the "/datasets" path, Mode embeds the listing under the "reports" HAL
    # key: a Mode "dataset" is implemented as a special kind of report. Note:
    # mode.py's own dataset listing does NOT apply exclude_archived (only its
    # report listing does), so this deliberately doesn't either.
    url = f"{workspace_uri}/spaces/{space_token}/datasets?filter=all"
    datasets = _get_embedded_paged(
        session,
        config,
        rate_limiter,
        url,
        "reports",
        context=f"datasets listing for space '{parent_path[0]}'",
    )
    return [_display_name(dataset) for dataset in datasets]


def _queries(
    client: ModeClient, config: ModeProbeConfig, parent_path: List[str]
) -> Sequence[str]:
    session, workspace_uri, rate_limiter = client
    space_name, report_name = parent_path[0], parent_path[1]
    space_token = _space_token(session, config, rate_limiter, workspace_uri, space_name)
    if space_token is None:
        return []
    report_token = _report_token(
        session, config, rate_limiter, workspace_uri, space_token, report_name
    )
    if report_token is None:
        return []
    url = f"{workspace_uri}/reports/{report_token}/queries"
    queries = _get_embedded(
        session,
        config,
        rate_limiter,
        url,
        "queries",
        context=f"queries listing for report '{report_name}'",
    )
    return [_display_name(query) for query in queries]


# Mode is a Space holding BOTH Reports and Datasets — the first branching probe,
# reached through the connector's own session (config.get_mode_session()). Mode's
# API is token-addressed while parent_path carries names, so each lister below
# resolves the parent name to its token by re-fetching the parent listing (mirroring
# how grafana_probe resolves a folder title to an id). Dataset and Query take
# UNFILTERED: Mode declares no dataset_pattern/query_pattern to filter them.
MODE_PROBE = ClientProbe(
    client_factory=_build_mode_client,
    close=_close_mode_client,
    levels=[
        ProbeLevel(MODE_SPACE, "space_pattern", _spaces),
        ProbeLevel(
            BIAssetSubTypes.MODE_REPORT, "report_pattern", _reports, parent=MODE_SPACE
        ),
        ProbeLevel(
            BIAssetSubTypes.MODE_DATASET, UNFILTERED, _datasets, parent=MODE_SPACE
        ),
        ProbeLevel(
            BIAssetSubTypes.MODE_QUERY,
            UNFILTERED,
            _queries,
            parent=BIAssetSubTypes.MODE_REPORT,
        ),
    ],
)


def list_mode_children(config: Any, parent_path: List[str], limit: int) -> ProbeResult:
    return MODE_PROBE.list_children(config, parent_path, limit)


def _find_report_token(
    session: ModeApiSession,
    config: ModeProbeConfig,
    rate_limiter: RateLimiter,
    workspace_uri: str,
    report_name: str,
) -> Optional[str]:
    # probe_run commands (unlike the hierarchy probe) get a report name with no
    # containing space, so every IN-SCOPE space's reports must be searched --
    # scoped to config.space_pattern (_fetch_spaces/_fetch_reports already
    # apply the server-side filter, exclude_restricted, and exclude_archived)
    # so a report that lives only in a space the recipe would never ingest
    # (denied by space_pattern, restricted, or excluded via
    # exclude_personal_collections) neither falsely resolves nor falsely
    # triggers an ambiguity error below. A report name is not unique even
    # within that scope -- the same name can sit in two shared spaces -- so
    # every in-scope match is collected; an ambiguous name raises rather than
    # returning whichever space happened to iterate first, mirroring the
    # ambiguity guard the hierarchy probe already applies to same-named
    # sibling levels.
    matches: List[Tuple[str, str]] = []
    for space in _fetch_spaces(session, config, rate_limiter, workspace_uri):
        space_name = _display_name(space)
        if not config.space_pattern.allowed(space_name):
            continue
        space_token = space.get("token")
        if not space_token:
            continue
        for report in _fetch_reports(
            session, config, rate_limiter, workspace_uri, space_token
        ):
            if _display_name(report) != report_name:
                continue
            report_token = report.get("token")
            if report_token:
                matches.append((space_name, report_token))
                break
            # else: a same-named report with no token; keep scanning this
            # space in case a different, valid-token report shares the name
            # (an earlier version of this loop broke here unconditionally,
            # which could skip a real match).
    if not matches:
        return None
    if len(matches) > 1:
        candidate_spaces = ", ".join(sorted({space_name for space_name, _ in matches}))
        raise ValueError(
            f"ambiguous report name '{report_name}': it exists in more than "
            f"one in-scope space ({candidate_spaces}); `probe run` takes only "
            f"--report with no space qualifier to disambiguate — use "
            f"`probe list`/`probe shape` to tell the reports apart"
        )
    return matches[0][1]


def _find_query_token(
    session: ModeApiSession,
    config: ModeProbeConfig,
    rate_limiter: RateLimiter,
    workspace_uri: str,
    report_token: str,
    query_name: str,
) -> Optional[str]:
    url = f"{workspace_uri}/reports/{report_token}/queries"
    queries = _get_embedded(
        session,
        config,
        rate_limiter,
        url,
        "queries",
        context=f"queries listing for report token '{report_token}'",
    )
    for query in queries:
        if _display_name(query) == query_name:
            return query.get("token")
    return None


def _platform_for_adapter(adapter: str, fallback_name: str) -> str:
    # Reuse mode.py's own adapter->platform table (MODE_ADAPTER_PLATFORM_MAP)
    # rather than re-deriving it from the "jdbc:" prefix: several adapters map
    # to a platform name that differs from the driver name itself, e.g.
    # "jdbc:postgresql" -> "postgres" and "jdbc:sqlserver" -> "mssql". For an
    # adapter the table doesn't recognize, mode.py's own fallback
    # (_get_datahub_friendly_platform) is the data source's own name, not the
    # raw adapter string -- reporting a raw "jdbc:vertica"-shaped value would
    # be neither a valid DataHub platform nor what ingestion will actually
    # emit, and an agent building lineage off it would get a wrong prefix.
    return MODE_ADAPTER_PLATFORM_MAP.get(adapter, fallback_name)


def _chart_summary(chart: Dict[str, Any]) -> Dict[str, object]:
    # Chart title/type live under "view" (native charts) or "view_vegas"
    # (Vega-Lite charts) — mirrors mode.py's construct_chart_from_api_data.
    detail = chart.get("view") or chart.get("view_vegas") or {}
    title = detail.get("title") or detail.get("chartTitle") or _display_name(chart)
    chart_type = detail.get("chartType") or detail.get("selectedChart")
    return {"title": title, "chart_type": chart_type}


class ModeMetadataProbe:
    """Metadata-only getters over the Mode API. Never returns query results."""

    def __init__(
        self,
        session: ModeApiSession,
        workspace_uri: str,
        config: ModeProbeConfig,
    ) -> None:
        self._session = session
        self._workspace_uri = workspace_uri
        self._config = config
        self._rate_limiter = RateLimiter(
            max_calls=config.api_options.requests_per_minute, period=60
        )

    def __enter__(self) -> "ModeMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        self._session.close()

    @probe_method()
    def data_sources(self) -> List[Dict[str, object]]:
        """Warehouse connections this Mode workspace can query: name, adapter
        (the DataHub platform name mapped from Mode's connection type, e.g.
        jdbc:bigquery -> bigquery, jdbc:postgresql -> postgres; falls back to
        the data source's own name if the adapter isn't recognized) and
        database. For BigQuery, Mode's own "database" field is always the
        literal string "default"; the real project id is substituted in that
        case only. Tells you which system a report's SQL actually runs
        against. Credentials (username, host, ...) are never returned."""
        url = f"{self._workspace_uri}/data_sources"
        records: List[Dict[str, Any]] = _tolerant(
            lambda: _get_embedded(
                self._session,
                self._config,
                self._rate_limiter,
                url,
                "data_sources",
                context="data sources listing",
            ),
            [],
        )
        result: List[Dict[str, object]] = []
        for ds in records:
            name = _display_name(ds)
            platform = _platform_for_adapter(str(ds.get("adapter") or ""), name)
            database = ds.get("database")
            if platform == "bigquery" and database == "default":
                database = ds.get("host")
            result.append(
                {
                    "name": name,
                    "adapter": platform,
                    "database": database,
                }
            )
        return result

    @probe_method()
    def definitions(self) -> List[Dict[str, object]]:
        """Mode's reusable SQL definitions: name, description, and the
        definition's SQL source. The source is DDL-like reusable-fragment
        metadata, not query results."""
        url = f"{self._workspace_uri}/definitions"
        records: List[Dict[str, Any]] = _tolerant(
            lambda: _get_embedded(
                self._session,
                self._config,
                self._rate_limiter,
                url,
                "definitions",
                context="definitions listing",
            ),
            [],
        )
        return [
            {
                "name": _display_name(d),
                "description": d.get("description"),
                "source": d.get("source"),
            }
            for d in records
        ]

    @probe_method()
    def report_queries(self, report: str) -> List[Dict[str, object]]:
        """The queries inside a report: name, and the SQL text each runs.
        Raises if `report` cannot be resolved to exactly one report: not
        found (misspelled, or only in a space this recipe wouldn't ingest —
        denied by space_pattern, restricted, or a personal collection), or
        ambiguous (the same name in more than one in-scope space). Mode has
        no endpoint to look up a report by name alone."""
        report_token = _tolerant(
            lambda: _find_report_token(
                self._session,
                self._config,
                self._rate_limiter,
                self._workspace_uri,
                report,
            ),
            None,
        )
        if report_token is None:
            raise ValueError(
                f"no report named '{report}' found among the spaces this "
                f"recipe would ingest (space_pattern-allowed, non-restricted, "
                f"non-personal spaces); check the spelling, or whether it "
                f"only exists in a space this recipe wouldn't ingest"
            )
        url = f"{self._workspace_uri}/reports/{report_token}/queries"
        records: List[Dict[str, Any]] = _tolerant(
            lambda: _get_embedded(
                self._session,
                self._config,
                self._rate_limiter,
                url,
                "queries",
                context=f"queries listing for report '{report}'",
            ),
            [],
        )
        return [{"name": _display_name(q), "sql": q.get("raw_query")} for q in records]

    @probe_method()
    def query_charts(self, report: str, query: str) -> List[Dict[str, object]]:
        """Charts built on one query: title and chart type. Raises if
        `report` cannot be resolved to exactly one report: not found
        (misspelled, or only in a space this recipe wouldn't ingest — denied
        by space_pattern, restricted, or a personal collection), or ambiguous
        (the same name in more than one in-scope space). Mode has no
        endpoint to look up a report by name alone."""
        report_token = _tolerant(
            lambda: _find_report_token(
                self._session,
                self._config,
                self._rate_limiter,
                self._workspace_uri,
                report,
            ),
            None,
        )
        if report_token is None:
            raise ValueError(
                f"no report named '{report}' found among the spaces this "
                f"recipe would ingest (space_pattern-allowed, non-restricted, "
                f"non-personal spaces); check the spelling, or whether it "
                f"only exists in a space this recipe wouldn't ingest"
            )
        query_token = _tolerant(
            lambda: _find_query_token(
                self._session,
                self._config,
                self._rate_limiter,
                self._workspace_uri,
                report_token,
                query,
            ),
            None,
        )
        if query_token is None:
            return []
        url = (
            f"{self._workspace_uri}/reports/{report_token}/queries/{query_token}/charts"
        )
        records: List[Dict[str, Any]] = _tolerant(
            lambda: _get_embedded(
                self._session,
                self._config,
                self._rate_limiter,
                url,
                "charts",
                context=f"charts listing for report '{report}' query '{query}'",
            ),
            [],
        )
        return [_chart_summary(c) for c in records]
