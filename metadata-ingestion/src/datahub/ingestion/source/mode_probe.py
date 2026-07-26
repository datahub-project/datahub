import logging
from typing import (
    Any,
    Callable,
    Dict,
    List,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

import requests

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
    ModeConfig,
    ModeSource,
    resolve_data_source_database,
)

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# Mode's API and its own config field (space_pattern) call this level "space", but
# the connector's emitted container subtype is BIContainerSubTypes.MODE_COLLECTION
# ("Collection") — Mode renamed Spaces to Collections in its product UI. There is no
# shared subtype whose value is "Space", so this level uses the API's own term
# rather than a mismatched container subtype.
MODE_SPACE: ProbeNodeKind = "Space"


def _build_mode_client(config: ModeConfig) -> ModeSource:
    """Builds the SAME uninitialized ModeSource shim ModeMetadataProbe uses
    (see ModeConfig.build_probe_provider/ModeSource.for_probe), so the
    branching hierarchy probe and the probe_run getters share exactly one
    fetch path: ModeSource's own _get_request_json/_get_paged_request_json
    (same session/rate-limit/retry path, same debug curl logging a real
    ingestion run gets) -- not a second, module-level reimplementation of
    Mode's request plumbing. __new__, never __init__: __init__ opens its own
    session, hits /api/verify, and resolves space_tokens for ingestion, side
    effects a read-only probe doesn't want repeated."""
    session, workspace_uri = config.get_mode_session()
    return ModeSource.for_probe(config, session, workspace_uri)


def _close_mode_client(client: ModeSource) -> None:
    client.session.close()


def _spaces_filter(config: ModeConfig) -> str:
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
    # which records it on ProbeResult.warnings and keeps sibling levels. The
    # probe_run provider methods are split in two: a NAME-TO-TOKEN RESOLVER
    # (_find_report_token/_find_query_token) must NOT catch this -- a 403
    # partway through resolving "report X" is not the same fact as "report X
    # doesn't exist", and swallowing it would tell the caller "check the
    # spelling" when the honest answer is "I couldn't check." Those propagate
    # all the way to the CLI as a genuine failure (exit 3). A provider's own
    # FINAL data fetch (the thing the caller actually asked for, once any name
    # was already resolved) instead degrades locally via _tolerant, appending
    # to ProbeMethodResult.warnings, since returning an honest partial answer
    # there ("here's what I could read, and here's what I couldn't") is more
    # useful than failing the whole command over one degraded endpoint.
    #
    # This split is deliberately NOT the same policy mode.py's own
    # _get_space_name_and_tokens/_get_reports/_get_datasets/_get_queries/
    # _get_charts/_get_data_sources_by_id/_get_definitions_map apply (those
    # catch every HTTP/JSON error uniformly and degrade -- some by returning
    # {}/[], some by a generator simply ending -- because ingestion would
    # rather return partial metadata from a flaky run than fail outright, and
    # none of them can signal "I hit an error" back to a caller at all). A
    # diagnostic's whole job is telling an agent the difference between
    # "there is nothing here" (404/403) and "I could not look" (auth failure,
    # 5xx) -- so every probe fetch below (_get_embedded/_get_embedded_paged)
    # goes through ModeSource's own request layer (_get_request_json/
    # _get_paged_request_json: same session/rate-limit/retry path, same debug
    # curl logging a real ingestion run gets) but applies THIS split, not any
    # of the connector's own always-degrade wrappers around it.
    # Anything other than 404/403 (auth failures, 5xx, connection errors) is a
    # hard error either way.
    status = exc.response.status_code if exc.response is not None else None
    if status not in (404, 403):
        raise exc
    raise ProbeSoftError(
        f"{context} returned HTTP {status}; treating it as empty."
    ) from exc


def _tolerant(fetch: Callable[[], _T], default: _T, warnings: List[str]) -> _T:
    """For a single provider method's OWN final data fetch (not for resolving
    a name to a token -- see the BLOCKER note on report_queries/query_charts
    below): catches ProbeSoftError, logs it, appends it to `warnings` (the
    provider's own `self.warnings`, which run_probe_method reads back into
    ProbeMethodResult.warnings), and returns `default` instead of letting the
    CLI hard-fail on it."""
    try:
        return fetch()
    except ProbeSoftError as exc:
        logger.warning(str(exc))
        warnings.append(str(exc))
        return default


def _get_embedded(
    source: ModeSource, url: str, key: str, context: str
) -> List[Dict[str, Any]]:
    """The single unpaginated fetch path: every probe request -- hierarchy
    listers and probe_run getters alike -- goes through the connector's own
    bound _get_request_json (see ModeSource.for_probe/_build_mode_client),
    which shares its session/rate-limiter/retry path and debug curl logging
    with a real ingestion run, instead of a bare session.get() or a second,
    module-level fetch_json reimplementation.

    Deliberately NOT delegated to _get_data_sources_by_id/_get_definitions_map/
    _get_queries/_get_charts (the higher-level connector methods that
    actually wrap this endpoint during ingestion): those catch every
    HTTP/JSON error themselves and always degrade to an empty result --
    correct for ingestion, which would rather return partial metadata from a
    flaky run, but wrong for a diagnostic, whose whole job is distinguishing
    "nothing here" from "I could not look." So this applies the soft/hard
    split _raise_soft_or_hard defines (404/403 degrade, everything else is a
    hard error) around the connector's fetch, rather than inheriting the
    connector's own always-degrade policy. _get_definitions_map specifically
    is also lossy for a second, independent reason (its cache keeps only
    {name: source}, discarding description -- it exists for `{{@name}}`
    template expansion, not for reporting)."""
    try:
        payload = source._get_request_json(url)
    except requests.HTTPError as exc:
        _raise_soft_or_hard(exc, context)
    return list(payload.get("_embedded", {}).get(key, []))


def _get_embedded_paged(
    source: ModeSource, url: str, key: str, context: str
) -> List[Dict[str, Any]]:
    """Like _get_embedded, but walks every page via the connector's own
    _get_paged_request_json (mode.py) instead of a single request -- the
    spaces/reports/datasets listings truncate at one page (default 30 items)
    unless walked with per_page/page until a page comes back empty.

    Deliberately NOT delegated to _get_space_name_and_tokens/_get_reports/
    _get_datasets (the higher-level connector methods that actually wrap this
    endpoint during ingestion): each of those swallows every HTTP error into
    a `self.report` warning/failure and simply stops yielding, with no way to
    signal "I hit an error" back to a caller -- a probe built on them could
    never distinguish a 403 from a genuinely empty listing, and
    _get_space_name_and_tokens additionally applies space_pattern itself,
    which would silently drop a denied space instead of letting the framework
    report it as an excluded node (see test_spaces_apply_space_pattern). So
    this walks _get_paged_request_json directly -- the layer beneath all
    three -- and applies the same soft/hard split _get_embedded does.

    A soft error partway through (e.g. page 3 of 5 403s) raises rather than
    returning the pages collected so far: a truncated listing that looks
    complete is worse than an honest "couldn't finish this, here's why"."""
    items: List[Dict[str, Any]] = []
    try:
        for page in source._get_paged_request_json(
            url, key, source.config.items_per_page
        ):
            items.extend(page)
    except requests.HTTPError as exc:
        _raise_soft_or_hard(exc, context)
    return items


def _display_name(item: Dict[str, Any]) -> str:
    # Live Mode workspaces can hold reports with a null "name" (seen against a
    # real workspace); fall back to the token, then "unknown" — mirroring
    # mode.py's own name-or-token-or-"unknown" convention (see
    # construct_query_or_dataset's report_name resolution) rather than letting
    # AllowDenyPattern.allowed() blow up on a non-string name.
    return str(item.get("name") or item.get("token") or "unknown")


def _space_pattern_name(space: Dict[str, Any]) -> str:
    # mode.py's own space_pattern check (_get_space_name_and_tokens) tests the
    # raw "name" field, with NO token fallback -- unlike reports, where
    # mode.py's own report_pattern check (_collect_space_work_items) uses the
    # same name-or-token-or-"unknown" convention _display_name does. So
    # spaces alone need this second, filter-only target: using _display_name
    # (which falls back to the token) would test a different string than a
    # real ingestion run does for a null-named space, and could report it
    # in- or out-of-scope wrongly. Used for BOTH the space_pattern check
    # itself (_find_report_token) and the hierarchy probe's own space nodes
    # (_spaces/_space_token) -- they must test and address spaces by the
    # identical string, or the same physical space could resolve "in scope"
    # down one path and "excluded" down the other. `or ""` (not mode.py's
    # literal `.get("name", "")`) so an explicit `"name": null` normalizes to
    # "" instead of passing None into .allowed(), which raises on a
    # non-string.
    return space.get("name") or ""


def _fetch_spaces(source: ModeSource) -> List[Dict[str, Any]]:
    """Every space the workspace has, filtered exactly the way mode.py's own
    ingestion run would see them: the server-side filter=all/custom, plus
    exclude_restricted client-side. The single call site for fetching spaces
    -- every lister that needs a space listing or a name-to-token lookup goes
    through this, so there is exactly one place that mirrors mode.py's space
    visibility."""
    url = f"{source.workspace_uri}/spaces?filter={_spaces_filter(source.config)}"
    spaces = _get_embedded_paged(
        source, url, "spaces", context="workspace spaces listing"
    )
    if source.config.exclude_restricted:
        spaces = [s for s in spaces if not _is_restricted_space(s)]
    return spaces


def _fetch_reports(source: ModeSource, space_token: str) -> List[Dict[str, Any]]:
    """Every report in one space, filtered the way mode.py's own ingestion run
    would see them: ?filter=all, paginated, plus exclude_archived client-side.
    The single call site for fetching a space's reports."""
    url = f"{source.workspace_uri}/spaces/{space_token}/reports?filter=all"
    reports = _get_embedded_paged(
        source,
        url,
        "reports",
        context=f"reports listing for space token '{space_token}'",
    )
    if source.config.exclude_archived:
        reports = [r for r in reports if not r.get("archived", False)]
    return reports


def _space_token(source: ModeSource, space_name: str) -> Optional[str]:
    # Matches on _space_pattern_name, not _display_name: _spaces() (below)
    # reports nodes by that same raw name, so a --parent value the caller
    # copied from a `probe list`/`probe shape` node resolves back to the
    # same space it named.
    for space in _fetch_spaces(source):
        if _space_pattern_name(space) == space_name:
            return space.get("token")
    return None


def _report_token(
    source: ModeSource, space_token: str, report_name: str
) -> Optional[str]:
    for report in _fetch_reports(source, space_token):
        if _display_name(report) == report_name:
            return report.get("token")
    return None


def _spaces(
    client: ModeSource, config: ModeConfig, parent_path: List[str]
) -> Sequence[str]:
    # `config` is unused: it is always the identical object as client.config
    # (see _build_mode_client) -- kept only to satisfy ClientProbe's
    # LevelLister shape, which every level's list_names must match.
    #
    # _space_pattern_name, not _display_name: a null-named space then has no
    # usable name (see probe.py's UNNAMED handling) and is reported as an
    # unaddressable "<unnamed>" node rather than being falsely addressable
    # via its token -- Mode's own space_pattern check has no such fallback
    # either, so this is the same string _find_report_token tests, not a
    # friendlier one this level invents for itself.
    return [_space_pattern_name(space) for space in _fetch_spaces(client)]


def _reports(
    client: ModeSource, config: ModeConfig, parent_path: List[str]
) -> Sequence[str]:
    space_token = _space_token(client, parent_path[0])
    if space_token is None:
        return []
    return [_display_name(r) for r in _fetch_reports(client, space_token)]


def _datasets(
    client: ModeSource, config: ModeConfig, parent_path: List[str]
) -> Sequence[str]:
    space_token = _space_token(client, parent_path[0])
    if space_token is None:
        return []
    # Paginated with ?filter=all, same as _fetch_reports — mode.py:1701-1708
    # fetches this identically (per_page/page walk + filter=all). And despite
    # the "/datasets" path, Mode embeds the listing under the "reports" HAL
    # key: a Mode "dataset" is implemented as a special kind of report. Note:
    # mode.py's own dataset listing does NOT apply exclude_archived (only its
    # report listing does), so this deliberately doesn't either.
    url = f"{client.workspace_uri}/spaces/{space_token}/datasets?filter=all"
    datasets = _get_embedded_paged(
        client,
        url,
        "reports",
        context=f"datasets listing for space '{parent_path[0]}'",
    )
    return [_display_name(dataset) for dataset in datasets]


def _queries(
    client: ModeSource, config: ModeConfig, parent_path: List[str]
) -> Sequence[str]:
    space_name, report_name = parent_path[0], parent_path[1]
    space_token = _space_token(client, space_name)
    if space_token is None:
        return []
    report_token = _report_token(client, space_token, report_name)
    if report_token is None:
        return []
    url = f"{client.workspace_uri}/reports/{report_token}/queries"
    queries = _get_embedded(
        client,
        url,
        "queries",
        context=f"queries listing for report '{report_name}'",
    )
    return [_display_name(query) for query in queries]


# Mode is a Space holding BOTH Reports and Datasets — the first branching probe,
# reached through the connector's own shimmed ModeSource (_build_mode_client).
# Mode's API is token-addressed while parent_path carries names, so each lister
# below resolves the parent name to its token by re-fetching the parent listing.
# Dataset and Query take UNFILTERED: Mode declares no dataset_pattern/query_pattern
# to filter them.
MODE_PROBE = ClientProbe(
    client_factory=_build_mode_client,
    close=_close_mode_client,
    levels=[
        ProbeLevel(MODE_SPACE, list_names=_spaces),
        ProbeLevel(BIAssetSubTypes.MODE_REPORT, list_names=_reports, parent=MODE_SPACE),
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


def _find_report_token(source: ModeSource, report_name: str) -> Optional[str]:
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
    try:
        for space in _fetch_spaces(source):
            space_name = _display_name(space)
            if not source.config.space_pattern.allowed(_space_pattern_name(space)):
                continue
            space_token = space.get("token")
            if not space_token:
                continue
            for report in _fetch_reports(source, space_token):
                if _display_name(report) != report_name:
                    continue
                report_token = report.get("token")
                if report_token:
                    matches.append((space_name, report_token))
                    break
                # else: a same-named report with no token; keep scanning this
                # space in case a different, valid-token report shares the
                # name (an earlier version of this loop broke here
                # unconditionally, which could skip a real match).
    except ProbeSoftError as exc:
        # Rephrase at this re-raise site rather than reusing
        # _raise_soft_or_hard's message verbatim: "...treating it as empty"
        # is accurate on ProbeResult.warnings, where a sub-listing genuinely
        # is treated as empty and the hierarchy probe moves on to its
        # siblings. Here the exception aborts this whole search instead (see
        # the BLOCKER note in report_queries/query_charts) and propagates to
        # the CLI as a hard failure -- telling the caller anything was
        # "treated as empty" would say the opposite of what happened, and an
        # agent reading it could wrongly conclude the report has no queries
        # rather than that the search itself could not finish.
        raise ProbeSoftError(
            f"could not determine whether a report named '{report_name}' "
            f"exists -- the search across spaces did not complete: {exc}"
        ) from exc
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
    source: ModeSource, report_token: str, query_name: str
) -> Optional[str]:
    # Matches on _display_name (name-or-token-or-"unknown"), the same
    # convention mode.py's own report_name resolution uses (mode.py:2078) --
    # a query with a null "name" is addressable by its token, since that's
    # what _display_name reports for it and what report_queries would show.
    # Collects every match rather than returning on the first one, so two
    # queries sharing a name (or both falling back to "unknown") raise an
    # ambiguity error instead of one silently winning.
    url = f"{source.workspace_uri}/reports/{report_token}/queries"
    try:
        queries = _get_embedded(
            source,
            url,
            "queries",
            context=f"queries listing for report token '{report_token}'",
        )
    except ProbeSoftError as exc:
        # See _find_report_token's identical rephrasing: this exception
        # aborts the query-name search rather than being treated as "this
        # report has no queries", and must say so.
        raise ProbeSoftError(
            f"could not determine whether a query named '{query_name}' "
            f"exists in report token '{report_token}' -- the queries "
            f"listing did not complete: {exc}"
        ) from exc
    matches = [
        query.get("token")
        for query in queries
        if _display_name(query) == query_name and query.get("token")
    ]
    if not matches:
        return None
    if len(matches) > 1:
        raise ValueError(
            f"ambiguous query name '{query_name}': more than one query in "
            f"this report resolves to that name (or both fall back to their "
            f"token because they have no name); use report_queries to find "
            f"a token unique to the one you mean"
        )
    return matches[0]


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
        source: ModeSource,
        config: ModeConfig,
    ) -> None:
        # `source` is the uninitialized shim ModeSource.for_probe() builds
        # (see mode.py) -- every fetch below goes through its bound
        # _get_request_json/_get_paged_request_json, reusing the connector's
        # own session/rate-limit/retry path (and debug curl logging) instead
        # of re-deriving it. `config` is always the identical object as
        # source.config (see ModeConfig.build_probe_provider) -- kept as its
        # own parameter only for constructor-signature stability; nothing
        # below reads it separately from source.config.
        self._source = source
        self._config = config
        # Read by agent.probe_methods.run_probe_method after the bound method
        # returns, and copied onto ProbeMethodResult.warnings -- see
        # _tolerant.
        self.warnings: List[str] = []

    def __enter__(self) -> "ModeMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        self._source.session.close()

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
        url = f"{self._source.workspace_uri}/data_sources"
        records: List[Dict[str, Any]] = _tolerant(
            lambda: _get_embedded(
                self._source, url, "data_sources", context="data sources listing"
            ),
            [],
            self.warnings,
        )
        result: List[Dict[str, object]] = []
        for ds in records:
            # Both derivations call the connector's own methods directly
            # (bound to this shim, whose .report absorbs
            # _get_datahub_friendly_platform's report_warning on an
            # unrecognized adapter) rather than a second copy of this logic:
            # _get_datahub_friendly_platform is mode.py:1037,
            # resolve_data_source_database is the pure two-line BigQuery
            # database-swap ingestion's own _get_platform_and_dbname calls.
            #
            # The unmapped-adapter fallback is ds.get("name", "") -- mode.py's
            # own literal convention -- not _display_name(ds) (name-or-token-
            # or-"unknown"): that fallback is this getter's own display
            # policy for the "name" field below, and using it here too would
            # report a friendlier adapter value than ingestion would ever
            # actually emit (mode.py's own behavior is authoritative on what
            # a real run would do, even where it's a plain "" for a nameless,
            # unrecognized-adapter data source).
            platform = self._source._get_datahub_friendly_platform(
                ds.get("adapter", ""), ds.get("name", "")
            )
            database = resolve_data_source_database(
                platform, ds.get("database", ""), ds.get("host", "")
            )
            result.append(
                {
                    "name": _display_name(ds),
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
        url = f"{self._source.workspace_uri}/definitions"
        records: List[Dict[str, Any]] = _tolerant(
            lambda: _get_embedded(
                self._source, url, "definitions", context="definitions listing"
            ),
            [],
            self.warnings,
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
        denied by space_pattern, restricted, or a personal collection),
        ambiguous (the same name in more than one in-scope space), or the
        search itself hit a backend error partway through (surfaced as-is,
        rather than reported as not-found). Mode has no endpoint to look up
        a report by name alone."""
        # BLOCKER (name resolution must not soften a soft error): a
        # ProbeSoftError here means the search across spaces/reports could
        # not complete -- e.g. a 403 on the one space that holds the report.
        # That is not the same fact as "no report named X exists", and
        # swallowing it would tell the caller to "check the spelling" when
        # the honest answer is "I couldn't check." Let it propagate to the
        # CLI as a real failure.
        report_token = _find_report_token(self._source, report)
        if report_token is None:
            raise ValueError(
                f"no report named '{report}' found among the spaces this "
                f"recipe would ingest (space_pattern-allowed, non-restricted, "
                f"non-personal spaces); check the spelling, or whether it "
                f"only exists in a space this recipe wouldn't ingest"
            )
        url = f"{self._source.workspace_uri}/reports/{report_token}/queries"
        records: List[Dict[str, Any]] = _tolerant(
            lambda: _get_embedded(
                self._source,
                url,
                "queries",
                context=f"queries listing for report '{report}'",
            ),
            [],
            self.warnings,
        )
        return [{"name": _display_name(q), "sql": q.get("raw_query")} for q in records]

    @probe_method()
    def query_charts(self, report: str, query: str) -> List[Dict[str, object]]:
        """Charts built on one query: title and chart type. Raises if
        `report` cannot be resolved to exactly one report: not found
        (misspelled, or only in a space this recipe wouldn't ingest — denied
        by space_pattern, restricted, or a personal collection), or ambiguous
        (the same name in more than one in-scope space). Mode has no
        endpoint to look up a report by name alone. Also raises if `query`
        cannot be resolved to exactly one query within that report — call
        report_queries first to see the valid names (a query with no name of
        its own is listed there by its token, which is also a valid `query`
        value here)."""
        # BLOCKER (name resolution must not soften a soft error either): see
        # report_queries' docstring/comment -- a soft error mid-search is not
        # "not found".
        report_token = _find_report_token(self._source, report)
        if report_token is None:
            raise ValueError(
                f"no report named '{report}' found among the spaces this "
                f"recipe would ingest (space_pattern-allowed, non-restricted, "
                f"non-personal spaces); check the spelling, or whether it "
                f"only exists in a space this recipe wouldn't ingest"
            )
        query_token = _find_query_token(self._source, report_token, query)
        if query_token is None:
            raise ValueError(
                f"no query named '{query}' found in report '{report}'; call "
                f"report_queries(report='{report}') to see its queries by "
                f"name — a query with no name of its own is listed there by "
                f"its token instead, which is also a valid `query` value here"
            )
        url = f"{self._source.workspace_uri}/reports/{report_token}/queries/{query_token}/charts"
        records: List[Dict[str, Any]] = _tolerant(
            lambda: _get_embedded(
                self._source,
                url,
                "charts",
                context=f"charts listing for report '{report}' query '{query}'",
            ),
            [],
            self.warnings,
        )
        return [_chart_summary(c) for c in records]
