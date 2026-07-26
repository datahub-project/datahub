from typing import Any, Dict, List, NoReturn, Optional, Sequence

import requests

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import (
    UNFILTERED,
    ClientProbe,
    ProbeLevel,
    ProbeSoftError,
)
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.mode import (
    ModeConfig,
    ModeSource,
    is_archived_report,
    is_restricted_space,
)

# Mode's API and its own config field (space_pattern) call this level "space", but
# the connector's emitted container subtype is BIContainerSubTypes.MODE_COLLECTION
# ("Collection") — Mode renamed Spaces to Collections in its product UI. There is no
# shared subtype whose value is "Space", so this level uses the API's own term
# rather than a mismatched container subtype.
MODE_SPACE: ProbeNodeKind = "Space"


def _build_mode_client(config: ModeConfig) -> ModeSource:
    """Builds the SAME uninitialized ModeSource shim ModeConfig.build_probe_provider
    builds (see ModeSource.for_probe), so the branching hierarchy probe below and
    the data_sources/definitions probe commands (annotated directly on ModeSource
    in mode.py) share exactly one fetch path: ModeSource's own
    _get_request_json/_get_paged_request_json (same session/rate-limit/retry
    path, same debug curl logging a real ingestion run gets) -- not a second,
    module-level reimplementation of Mode's request plumbing. __new__, never
    __init__: __init__ opens its own session, hits /api/verify, and resolves
    space_tokens for ingestion, side effects a read-only probe doesn't want
    repeated."""
    session, workspace_uri = config.get_mode_session()
    return ModeSource.for_probe(config, session, workspace_uri)


def _close_mode_client(client: ModeSource) -> None:
    client.session.close()


class ModeProbeSource(ModeSource):
    """The provider `build_probe_provider` returns for `probe run` -- exists
    for exactly one reason: ModeSource's own __exit__ (inherited from
    Closeable) calls close(), which only closes its report, correctly, since
    a real ingestion run's session lives for the whole pipeline and is never
    explicitly closed early. The probe's session is different: for_probe()
    opens it ad hoc, outside that pipeline lifecycle, and a `probe run`
    invocation is a single short CLI call, so it should close cleanly when
    the `with` block exits -- mirroring _close_mode_client's identical
    session.close() for the hierarchy probe's client, above. No other method
    lives here: data_sources/definitions are @probe_method-annotated
    directly on ModeSource itself, and for_probe (inherited, unchanged)
    already returns an instance of whichever class it's called on."""

    def __exit__(self, *exc: object) -> None:
        self.session.close()


def _raise_soft_or_hard(exc: requests.HTTPError, context: str) -> NoReturn:
    # Mirrors mode.py's four _is_http_404 branches (reports, datasets, queries,
    # charts): a 404 (deleted between listing and fetch) or 403 (restricted,
    # inaccessible to this token) on ONE listing is a normal, expected outcome
    # in production, not a reason to fail the whole probe -- but it must not be
    # silently swallowed either (indistinguishable from a genuinely empty
    # listing). Raises ProbeSoftError so ClientProbe.list_children can record it
    # on ProbeResult.warnings and keep sibling levels, rather than the whole
    # hierarchy call failing over one degraded level.
    #
    # This split is deliberately NOT the same policy mode.py's own
    # _get_space_name_and_tokens/_get_reports/_get_datasets/_get_queries/
    # _get_charts apply (those catch every HTTP/JSON error uniformly and
    # degrade -- some by returning {}/[], some by a generator simply ending --
    # because ingestion would rather return partial metadata from a flaky run
    # than fail outright, and none of them can signal "I hit an error" back to
    # a caller at all). The hierarchy probe's whole job is telling an agent the
    # difference between "there is nothing here" (404/403) and "I could not
    # look" (auth failure, 5xx) -- so every fetch below (_get_embedded/
    # _get_embedded_paged) goes through ModeSource's own request layer
    # (_get_request_json/_get_paged_request_json: same session/rate-limit/retry
    # path, same debug curl logging a real ingestion run gets) but applies THIS
    # split, not any of the connector's own always-degrade wrappers around it.
    # The connector's own _get_data_sources_by_id/_get_definitions_map are NOT
    # part of this split at all: their probe commands (mode.py) are the raw
    # fetchers themselves, annotated directly, so they carry ingestion's
    # always-degrade policy unchanged rather than this file's soft/hard
    # distinction.
    # Anything other than 404/403 (auth failures, 5xx, connection errors) is a
    # hard error either way.
    status = exc.response.status_code if exc.response is not None else None
    if status not in (404, 403):
        raise exc
    raise ProbeSoftError(
        f"{context} returned HTTP {status}; treating it as empty."
    ) from exc


def _get_embedded(
    source: ModeSource, url: str, key: str, context: str
) -> List[Dict[str, Any]]:
    """The single unpaginated fetch path the hierarchy probe's Query level
    uses, through the connector's own bound _get_request_json (see
    ModeSource.for_probe/_build_mode_client), which shares its
    session/rate-limiter/retry path and debug curl logging with a real
    ingestion run, instead of a bare session.get() or a second, module-level
    fetch_json reimplementation.

    Deliberately NOT delegated to _get_queries/_get_charts (the higher-level
    connector methods that actually wrap this endpoint during ingestion):
    those catch every HTTP/JSON error themselves and always degrade to an
    empty result -- correct for ingestion, which would rather return partial
    metadata from a flaky run, but wrong for a diagnostic, whose whole job is
    distinguishing "nothing here" from "I could not look." So this applies
    the soft/hard split _raise_soft_or_hard defines (404/403 degrade,
    everything else is a hard error) around the connector's fetch, rather
    than inheriting the connector's own always-degrade policy."""
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
    # in- or out-of-scope wrongly. Used for BOTH the hierarchy probe's own
    # space nodes (_spaces) and resolving a --parent value back to a space
    # (_space_token) -- they must test and address spaces by the identical
    # string, or the same physical space could resolve "in scope" down one
    # path and "excluded" down the other. `or ""` (not mode.py's literal
    # `.get("name", "")`) so an explicit `"name": null` normalizes to ""
    # instead of passing None into .allowed(), which raises on a non-string.
    return space.get("name") or ""


def _fetch_spaces(source: ModeSource) -> List[Dict[str, Any]]:
    """Every space the workspace has, filtered exactly the way mode.py's own
    ingestion run would see them: the server-side filter=all/custom (via
    ModeConfig.space_filter_param, shared with _get_space_name_and_tokens),
    plus exclude_restricted client-side (via is_restricted_space, also
    shared) -- so this stays in lockstep with ingestion's own space
    visibility instead of carrying an independent copy of that decision. The
    single call site for fetching spaces -- every lister that needs a space
    listing or a name-to-token lookup goes through this."""
    url = f"{source.workspace_uri}/spaces?filter={source.config.space_filter_param()}"
    spaces = _get_embedded_paged(
        source, url, "spaces", context="workspace spaces listing"
    )
    if source.config.exclude_restricted:
        spaces = [s for s in spaces if not is_restricted_space(s)]
    return spaces


def _fetch_reports(source: ModeSource, space_token: str) -> List[Dict[str, Any]]:
    """Every report in one space, filtered the way mode.py's own ingestion run
    would see them: ?filter=all, paginated, plus exclude_archived client-side
    (via is_archived_report, shared with _get_reports). The single call site
    for fetching a space's reports."""
    url = f"{source.workspace_uri}/spaces/{space_token}/reports?filter=all"
    reports = _get_embedded_paged(
        source,
        url,
        "reports",
        context=f"reports listing for space token '{space_token}'",
    )
    if source.config.exclude_archived:
        reports = [r for r in reports if not is_archived_report(r)]
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
    # either, so this is the same string _space_token tests, not a friendlier
    # one this level invents for itself.
    return [_space_pattern_name(space) for space in _fetch_spaces(client)]


def _reports(
    client: ModeSource, config: ModeConfig, parent_path: List[str]
) -> Sequence[str]:
    space_name = parent_path[0]
    space_token = _space_token(client, space_name)
    if space_token is None:
        # A typo'd/unresolvable --parent must not look identical to "this
        # space genuinely has no reports": raise so ClientProbe.list_children
        # records a warning instead of silently reporting an empty level. The
        # sibling Dataset level resolves the same parent independently (see
        # _datasets), so this level's failure alone must not abort it --
        # ProbeSoftError, not a bare exception, is what buys that (caught
        # per-level, not per-call).
        raise ProbeSoftError(
            f"no space named '{space_name}' found among this workspace's "
            f"spaces; cannot list its reports"
        )
    return [_display_name(r) for r in _fetch_reports(client, space_token)]


def _datasets(
    client: ModeSource, config: ModeConfig, parent_path: List[str]
) -> Sequence[str]:
    space_name = parent_path[0]
    space_token = _space_token(client, space_name)
    if space_token is None:
        # See _reports' identical rationale: this level (Dataset) resolves
        # the same parent name independently of its Report sibling, so it
        # must report its own warning rather than a false empty listing.
        raise ProbeSoftError(
            f"no space named '{space_name}' found among this workspace's "
            f"spaces; cannot list its datasets"
        )
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
        context=f"datasets listing for space '{space_name}'",
    )
    return [_display_name(dataset) for dataset in datasets]


def _queries(
    client: ModeSource, config: ModeConfig, parent_path: List[str]
) -> Sequence[str]:
    space_name, report_name = parent_path[0], parent_path[1]
    space_token = _space_token(client, space_name)
    if space_token is None:
        raise ProbeSoftError(
            f"no space named '{space_name}' found among this workspace's "
            f"spaces; cannot list queries under it"
        )
    report_token = _report_token(client, space_token, report_name)
    if report_token is None:
        raise ProbeSoftError(
            f"no report named '{report_name}' found in space '{space_name}'; "
            f"cannot list its queries"
        )
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
