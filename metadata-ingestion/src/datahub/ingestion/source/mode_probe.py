from typing import Any, Dict, List, Optional, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import (
    UNFILTERED,
    ClientProbe,
    ProbeLevel,
    ProbeSoftError,
    soft_on_status,
)
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.mode import (
    ModeConfig,
    ModeSource,
    is_archived_report,
    is_restricted_space,
)

# Mode's API calls this level "space"; the connector's emitted container subtype
# is "Collection" (Mode renamed Spaces to Collections in its UI). No shared
# subtype means "Space", so this uses the API's own term.
MODE_SPACE: ProbeNodeKind = "Space"


def _build_mode_client(config: ModeConfig) -> ModeSource:
    """Builds the same uninitialized ModeSource shim as
    ModeConfig.build_probe_provider (see ModeSource.for_probe), so this probe and
    the data_sources/definitions probe commands share one fetch path. Uses
    __new__, never __init__: __init__ opens a session, hits /api/verify, and
    resolves space_tokens -- side effects a read-only probe must not repeat."""
    session, workspace_uri = config.get_mode_session()
    return ModeSource.for_probe(config, session, workspace_uri)


def _close_mode_client(client: ModeSource) -> None:
    client.session.close()


class ModeProbeSource(ModeSource):
    """Exists because ModeSource's inherited Closeable.__exit__ closes only the
    report, deliberately not the session -- a real ingestion run's session lives
    for the whole pipeline and must not close early. Pipeline.run() calls
    __exit__ on every source, so putting this override on ModeSource itself
    would change ingestion (it broke 4 integration tests when tried). The
    probe's ad hoc session (for_probe) should close when this short-lived `with`
    block exits, mirroring _close_mode_client for the hierarchy probe's
    client."""

    def __exit__(self, *exc: object) -> None:
        self.session.close()


def _get_embedded(
    source: ModeSource, url: str, key: str, context: str
) -> List[Dict[str, Any]]:
    """The hierarchy probe's Query-level fetch: goes through ModeSource's own
    bound _get_request_json (see for_probe/_build_mode_client) rather than a
    bare session.get(), so it shares session/rate-limit/retry/debug-logging
    with a real ingestion run.

    Deliberately NOT delegated to _get_queries/_get_charts: those always
    degrade HTTP/JSON errors to an empty result, which is correct for
    ingestion but hides the distinction a probe exists to report. This wraps
    the fetch in soft_on_status instead."""
    with soft_on_status(403, 404, context=context):
        payload = source._get_request_json(url)
    return list(payload.get("_embedded", {}).get(key, []))


def _get_embedded_paged(
    source: ModeSource, url: str, key: str, context: str
) -> List[Dict[str, Any]]:
    """Like _get_embedded, but walks every page via the connector's own
    _get_paged_request_json -- the spaces/reports/datasets listings truncate
    at one page (default 30 items) unless walked with per_page/page until a
    page comes back empty.

    Deliberately NOT delegated to _get_space_name_and_tokens/_get_reports/
    _get_datasets: each of those swallows every HTTP error and simply stops
    yielding, so a probe built on them could never distinguish a 403 from a
    genuinely empty listing (and _get_space_name_and_tokens additionally
    applies space_pattern itself, which would silently drop a denied space --
    see test_spaces_apply_space_pattern). A soft error partway through (e.g.
    page 3 of 5 403s) raises rather than returning the pages collected so
    far: a truncated listing that looks complete is worse than an honest
    "couldn't finish this, here's why"."""
    items: List[Dict[str, Any]] = []
    with soft_on_status(403, 404, context=context):
        for page in source._get_paged_request_json(
            url, key, source.config.items_per_page
        ):
            items.extend(page)
    return items


def _display_name(item: Dict[str, Any]) -> str:
    # Exists because live Mode workspaces can have reports with a null "name";
    # AllowDenyPattern.allowed(None) raises TypeError. Falls back to token, then
    # "unknown" -- mirroring mode.py's own name-or-token-or-"unknown" convention.
    return str(item.get("name") or item.get("token") or "unknown")


def _space_pattern_name(space: Dict[str, Any]) -> str:
    # Deliberately has NO token fallback: mode.py's own space_pattern check tests
    # only the raw "name" field. Used for both _spaces' nodes and _space_token's
    # --parent resolution, so a space is tested and addressed by the identical
    # string. `or ""`, not `.get("name", "")`, so an explicit null doesn't reach
    # .allowed(), which raises on a non-string.
    return space.get("name") or ""


def _fetch_spaces(source: ModeSource) -> List[Dict[str, Any]]:
    """Every space, filtered exactly as mode.py's own ingestion run would see
    them (server-side filter param + exclude_restricted) -- can't delegate to
    _get_space_name_and_tokens, which applies space_pattern internally and
    swallows errors. The single call site for fetching spaces."""
    url = f"{source.workspace_uri}/spaces?filter={source.config.space_filter_param()}"
    spaces = _get_embedded_paged(
        source, url, "spaces", context="workspace spaces listing"
    )
    if source.config.exclude_restricted:
        spaces = [s for s in spaces if not is_restricted_space(s)]
    return spaces


def _fetch_reports(source: ModeSource, space_token: str) -> List[Dict[str, Any]]:
    """Every report in one space, filtered as mode.py's own ingestion run would
    see them (?filter=all, paginated, exclude_archived) -- can't delegate to
    _get_reports, which swallows errors. The single call site for a space's
    reports."""
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
    # Matches on _space_pattern_name (see its docstring): must test the same
    # string _spaces() reports so a --parent value resolves to the right space.
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
    # `config` unused: identical object to client.config; kept to satisfy
    # ClientProbe's LevelLister shape.
    return [_space_pattern_name(space) for space in _fetch_spaces(client)]


def _reports(
    client: ModeSource, config: ModeConfig, parent_path: List[str]
) -> Sequence[str]:
    space_name = parent_path[0]
    space_token = _space_token(client, space_name)
    if space_token is None:
        # Raise (not return []) so ClientProbe.list_children records a warning
        # instead of a false empty listing; the sibling Dataset level resolves
        # this independently, so this failure alone must not abort it.
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
        # See _reports -- this level resolves the same parent independently.
        raise ProbeSoftError(
            f"no space named '{space_name}' found among this workspace's "
            f"spaces; cannot list its datasets"
        )
    # Paginated with ?filter=all, like _fetch_reports. Embedded under the
    # "reports" HAL key despite the "/datasets" path -- a Mode "dataset" is a
    # special kind of report. mode.py's own dataset listing does not apply
    # exclude_archived, so this doesn't either.
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


# Mode is a Space holding BOTH Reports and Datasets -- the first branching probe.
# Its API is token-addressed while parent_path carries names, so each lister
# above resolves the parent name to its token via a re-fetch. Dataset and Query
# take UNFILTERED: Mode declares no dataset_pattern/query_pattern for them.
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
