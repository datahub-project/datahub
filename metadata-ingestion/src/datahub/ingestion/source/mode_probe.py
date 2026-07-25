from typing import Any, Dict, List, Optional, Protocol, Sequence, Tuple

import requests

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import UNFILTERED, ClientProbe, ProbeLevel
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.mode import (
    DEFAULT_API_ITEMS_PER_PAGE,
    MODE_ADAPTER_PLATFORM_MAP,
)

# Mode's own client is (session, workspace_uri), built by ModeConfig.get_mode_session()
# so the probe and ModeSource share one construction path.
ModeClient = Tuple[requests.Session, str]

# Mode's API and its own config field (space_pattern) call this level "space", but
# the connector's emitted container subtype is BIContainerSubTypes.MODE_COLLECTION
# ("Collection") — Mode renamed Spaces to Collections in its product UI. There is no
# shared subtype whose value is "Space", so this level uses the API's own term
# rather than a mismatched container subtype.
MODE_SPACE: ProbeNodeKind = "Space"


class _ModeApiSession(Protocol):
    """Structural session type satisfied by both requests.Session and the
    duck-typed fakes used in tests, so ModeMetadataProbe doesn't need `Any`
    just to accept a test double. Only the two methods this module calls are
    part of the contract."""

    def get(self, url: str) -> Any: ...

    def close(self) -> None: ...


def _get_embedded(session: _ModeApiSession, url: str, key: str) -> List[Dict[str, Any]]:
    # Raise on failure rather than returning [] — a 401/403 (expired token,
    # revoked key) must surface as an error, not render as "no data sources"/
    # "no queries" indistinguishable from a workspace that is genuinely empty.
    response = session.get(url)
    response.raise_for_status()
    return list(response.json().get("_embedded", {}).get(key, []))


def _get_embedded_paged(
    session: _ModeApiSession, url: str, key: str, items_per_page: int
) -> List[Dict[str, Any]]:
    # Mirrors mode.py's _get_paged_request_json: the spaces/reports listings
    # truncate at one page (default 30 items) unless walked with per_page/page
    # until a page comes back empty. The single-report-scoped /queries and
    # /queries/{token}/charts endpoints do NOT paginate this way — mode.py
    # documents them as not handling pagination properly — so callers must
    # keep routing those through the unpaginated _get_embedded instead.
    sep = "&" if "?" in url else "?"
    items: List[Dict[str, Any]] = []
    page = 1
    while True:
        response = session.get(f"{url}{sep}per_page={items_per_page}&page={page}")
        response.raise_for_status()
        page_items = list(response.json().get("_embedded", {}).get(key, []))
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


def _space_token(
    session: _ModeApiSession, workspace_uri: str, space_name: str, items_per_page: int
) -> Optional[str]:
    spaces = _get_embedded_paged(
        session, f"{workspace_uri}/spaces", "spaces", items_per_page
    )
    for space in spaces:
        if _display_name(space) == space_name:
            return space.get("token")
    return None


def _report_token(
    session: _ModeApiSession,
    workspace_uri: str,
    space_token: str,
    report_name: str,
    items_per_page: int,
) -> Optional[str]:
    url = f"{workspace_uri}/spaces/{space_token}/reports?filter=all"
    reports = _get_embedded_paged(session, url, "reports", items_per_page)
    for report in reports:
        if _display_name(report) == report_name:
            return report.get("token")
    return None


def _spaces(client: ModeClient, config: Any, parent_path: List[str]) -> Sequence[str]:
    session, workspace_uri = client
    url = f"{workspace_uri}/spaces"
    return [
        _display_name(space)
        for space in _get_embedded_paged(session, url, "spaces", config.items_per_page)
    ]


def _reports(client: ModeClient, config: Any, parent_path: List[str]) -> Sequence[str]:
    session, workspace_uri = client
    space_token = _space_token(
        session, workspace_uri, parent_path[0], config.items_per_page
    )
    if space_token is None:
        return []
    url = f"{workspace_uri}/spaces/{space_token}/reports?filter=all"
    return [
        _display_name(report)
        for report in _get_embedded_paged(
            session, url, "reports", config.items_per_page
        )
    ]


def _datasets(client: ModeClient, config: Any, parent_path: List[str]) -> Sequence[str]:
    session, workspace_uri = client
    space_token = _space_token(
        session, workspace_uri, parent_path[0], config.items_per_page
    )
    if space_token is None:
        return []
    # Paginated with ?filter=all, same as _reports — mode.py:1701-1708 fetches
    # this identically (per_page/page walk + filter=all). And despite the
    # "/datasets" path, Mode embeds the listing under the "reports" HAL key:
    # a Mode "dataset" is implemented as a special kind of report.
    url = f"{workspace_uri}/spaces/{space_token}/datasets?filter=all"
    return [
        _display_name(dataset)
        for dataset in _get_embedded_paged(
            session, url, "reports", config.items_per_page
        )
    ]


def _queries(client: ModeClient, config: Any, parent_path: List[str]) -> Sequence[str]:
    session, workspace_uri = client
    space_name, report_name = parent_path[0], parent_path[1]
    space_token = _space_token(
        session, workspace_uri, space_name, config.items_per_page
    )
    if space_token is None:
        return []
    report_token = _report_token(
        session, workspace_uri, space_token, report_name, config.items_per_page
    )
    if report_token is None:
        return []
    url = f"{workspace_uri}/reports/{report_token}/queries"
    return [_display_name(query) for query in _get_embedded(session, url, "queries")]


# Mode is a Space holding BOTH Reports and Datasets — the first branching probe,
# reached through the connector's own session (config.get_mode_session()). Mode's
# API is token-addressed while parent_path carries names, so each lister below
# resolves the parent name to its token by re-fetching the parent listing (mirroring
# how grafana_probe resolves a folder title to an id). Dataset and Query take
# UNFILTERED: Mode declares no dataset_pattern/query_pattern to filter them.
MODE_PROBE = ClientProbe(
    client_factory=lambda config: config.get_mode_session(),
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
    session: _ModeApiSession,
    workspace_uri: str,
    report_name: str,
    items_per_page: int,
) -> Optional[str]:
    # probe_run commands (unlike the hierarchy probe) get a report name with no
    # containing space, so every space's reports must be searched rather than
    # resolving within one already-known space token (see _report_token
    # above). A report name is not unique across spaces — the same name
    # routinely exists in both a personal space and a shared one — so every
    # match is collected; an ambiguous name raises rather than silently
    # returning whichever space happened to iterate first, mirroring the
    # ambiguity guard the hierarchy probe already applies to same-named
    # sibling levels (see ClientProbe's Report/Dataset sibling handling).
    matches: List[Tuple[str, str]] = []
    spaces = _get_embedded_paged(
        session, f"{workspace_uri}/spaces", "spaces", items_per_page
    )
    for space in spaces:
        space_token = space.get("token")
        if not space_token:
            continue
        url = f"{workspace_uri}/spaces/{space_token}/reports?filter=all"
        for report in _get_embedded_paged(session, url, "reports", items_per_page):
            if _display_name(report) == report_name:
                report_token = report.get("token")
                if report_token:
                    matches.append((_display_name(space), report_token))
                break
    if not matches:
        return None
    if len(matches) > 1:
        candidate_spaces = ", ".join(sorted({space_name for space_name, _ in matches}))
        raise ValueError(
            f"ambiguous report name '{report_name}': it exists in more than "
            f"one space ({candidate_spaces}); `probe run` takes only "
            f"--report with no space qualifier to disambiguate — use "
            f"`probe list`/`probe shape` to tell the reports apart"
        )
    return matches[0][1]


def _find_query_token(
    session: _ModeApiSession, workspace_uri: str, report_token: str, query_name: str
) -> Optional[str]:
    url = f"{workspace_uri}/reports/{report_token}/queries"
    for query in _get_embedded(session, url, "queries"):
        if _display_name(query) == query_name:
            return query.get("token")
    return None


def _platform_for_adapter(adapter: str) -> str:
    # Reuse mode.py's own adapter->platform table (MODE_ADAPTER_PLATFORM_MAP)
    # rather than re-deriving it from the "jdbc:" prefix: several adapters map
    # to a platform name that differs from the driver name itself, e.g.
    # "jdbc:postgresql" -> "postgres" and "jdbc:sqlserver" -> "mssql".
    return MODE_ADAPTER_PLATFORM_MAP.get(adapter, adapter)


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
        session: _ModeApiSession,
        workspace_uri: str,
        items_per_page: int = DEFAULT_API_ITEMS_PER_PAGE,
    ) -> None:
        self._session = session
        self._workspace_uri = workspace_uri
        self._items_per_page = items_per_page

    def __enter__(self) -> "ModeMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        self._session.close()

    @probe_method()
    def data_sources(self) -> List[Dict[str, object]]:
        """Warehouse connections this Mode workspace can query: name, adapter
        (the DataHub platform name mapped from Mode's connection type, e.g.
        jdbc:bigquery -> bigquery, jdbc:postgresql -> postgres) and database.
        For BigQuery, Mode's own "database" field is always the literal string
        "default"; the real project id is substituted in that case only.
        Tells you which system a report's SQL actually runs against.
        Credentials (username, host, ...) are never returned."""
        url = f"{self._workspace_uri}/data_sources"
        result: List[Dict[str, object]] = []
        for ds in _get_embedded(self._session, url, "data_sources"):
            platform = _platform_for_adapter(str(ds.get("adapter") or ""))
            database = ds.get("database")
            if platform == "bigquery" and database == "default":
                database = ds.get("host")
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
        url = f"{self._workspace_uri}/definitions"
        return [
            {
                "name": _display_name(d),
                "description": d.get("description"),
                "source": d.get("source"),
            }
            for d in _get_embedded(self._session, url, "definitions")
        ]

    @probe_method()
    def report_queries(self, report: str) -> List[Dict[str, object]]:
        """The queries inside a report: name, and the SQL text each runs.
        Raises if `report`'s name exists in more than one space, since Mode
        has no endpoint to look up a report by name alone."""
        report_token = _find_report_token(
            self._session, self._workspace_uri, report, self._items_per_page
        )
        if report_token is None:
            return []
        url = f"{self._workspace_uri}/reports/{report_token}/queries"
        return [
            {"name": _display_name(q), "sql": q.get("raw_query")}
            for q in _get_embedded(self._session, url, "queries")
        ]

    @probe_method()
    def query_charts(self, report: str, query: str) -> List[Dict[str, object]]:
        """Charts built on one query: title and chart type. Raises if
        `report`'s name exists in more than one space, since Mode has no
        endpoint to look up a report by name alone."""
        report_token = _find_report_token(
            self._session, self._workspace_uri, report, self._items_per_page
        )
        if report_token is None:
            return []
        query_token = _find_query_token(
            self._session, self._workspace_uri, report_token, query
        )
        if query_token is None:
            return []
        url = (
            f"{self._workspace_uri}/reports/{report_token}/queries/{query_token}/charts"
        )
        return [_chart_summary(c) for c in _get_embedded(self._session, url, "charts")]
