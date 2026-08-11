from typing import Any, Callable, Dict, List, Optional

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import ProbeSoftError, soft_on_status
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.mode import (
    ModeConfig,
    ModeSource,
    is_archived_report,
    is_restricted_space,
)


class ModeProbeSource(ModeSource):
    """Exists because ModeSource's inherited Closeable.__exit__ closes only the
    report, deliberately not the session -- a real ingestion run's session lives
    for the whole pipeline and must not close early. Pipeline.run() calls
    __exit__ on every source, so putting this override on ModeSource itself
    would change ingestion (it broke 4 integration tests when tried). The
    probe's ad hoc session (for_probe) should close when this short-lived `with`
    block exits, mirroring _close_mode_client for the hierarchy probe's
    client."""

    # Read back by run_probe_method after each command. Declared here (rather
    # than only assigned in for_probe) because for_probe builds via __new__,
    # which no type checker can see priming an attribute.
    warnings: List[str]

    # Read endpoints `probe api` may reach: the escape hatch for a question no
    # getter anticipated (a report's last-run time, whether a space is
    # restricted).
    #
    # Three of these look redundant against the getters below and are not -- they
    # are the bootstrap. Mode addresses objects by token, and no getter returns
    # one: they all return the display name a pattern is matched against (see
    # test_spaces_report_the_same_raw_name_space_token_resolves_by). So the raw
    # /spaces listing is the only route to a space token, and a space's raw
    # reports listing the only route to a report token. Trim the "duplicates" and
    # the four token-addressed entries below become unreachable, which is to say
    # `probe api` stops working. test_every_token_addressed_endpoint_has_a_route
    # pins the chain.
    #
    # This does NOT replace the getters. A raw record leaves the caller to guess
    # which field a pattern is matched against, and for a Space that is the raw
    # "name" with no token fallback (see _space_pattern_name) -- connector
    # knowledge a passthrough cannot carry.
    api_allowlist = (
        "GET /spaces",
        "GET /spaces/{token}/reports",
        "GET /spaces/{token}/datasets",
        "GET /reports/{token}/queries",
        "GET /reports/{token}",
        "GET /data_sources",
        "GET /definitions",
    )

    @probe_method(name="api", scoped_path_param="path")
    def api(self, path: str) -> object:
        """Fetch one read endpoint from Mode's API, for a question no other
        getter answers. Only GET, and only a path in api_allowlist above -- the
        framework checks `path` before calling this. Prefer a typed getter where
        one exists: it returns the names patterns are matched against, whereas a
        raw record leaves you guessing which field that is."""
        return self._get_request_json(f"{self.workspace_uri}{path}")

    def __exit__(self, *exc: object) -> None:
        self.session.close()

    @classmethod
    def for_config(cls, config: ModeConfig) -> "ModeProbeSource":
        """Open an ad hoc session for this probe call.

        Separate from for_probe below, which takes an already-built session so a
        test can supply a fake one.
        """
        session, workspace_uri = config.get_mode_session()
        return cls.for_probe(config, session, workspace_uri)

    @classmethod
    def for_probe(
        cls, config: ModeConfig, session: Any, workspace_uri: str
    ) -> "ModeProbeSource":
        probe = super().for_probe(config, session, workspace_uri)
        # run_probe_method reads this back after each command, so a listing that
        # degraded reports why instead of looking like an empty workspace.
        probe.warnings = []
        return probe

    def _listing(self, fetch: Callable[[], List[str]]) -> List[str]:
        """Run one listing, turning a soft error into a warning rather than a
        silent empty result -- the distinction between "nothing here" and "I
        could not look" is the whole point of a diagnostic."""
        try:
            return fetch()
        except ProbeSoftError as exc:
            message = str(exc)
            if message not in self.warnings:
                self.warnings.append(message)
            return []

    @probe_method(kind="Space")
    def spaces(self) -> List[str]:
        """Every space (Mode's UI calls them Collections) in this workspace,
        including ones space_pattern would exclude -- a denied space is
        reported, not hidden, so `probe filter` can explain it."""
        return self._listing(
            lambda: [_space_pattern_name(space) for space in _fetch_spaces(self)]
        )

    @probe_method(kind=BIAssetSubTypes.MODE_REPORT)
    def reports(self, space: str) -> List[str]:
        """Reports in one space, by space name. Excludes archived reports when
        the recipe sets exclude_archived, matching what ingestion would see."""
        return self._listing(lambda: [_display_name(r) for r in self._reports(space)])

    @probe_method(kind=BIAssetSubTypes.MODE_DATASET)
    def datasets(self, space: str) -> List[str]:
        """Datasets in one space, by space name. A Mode dataset is a special
        kind of report, so these come from the space's /datasets endpoint."""
        return self._listing(lambda: [_display_name(d) for d in self._datasets(space)])

    @probe_method(kind=BIAssetSubTypes.MODE_QUERY)
    def queries(self, space: str, report: str) -> List[str]:
        """Queries belonging to one report, addressed by space and report name."""
        return self._listing(
            lambda: [_display_name(q) for q in self._queries(space, report)]
        )

    def _space_token_or_raise(self, space: str) -> str:
        token = _space_token(self, space)
        if token is None:
            raise ProbeSoftError(
                f"no space named '{space}' found among this workspace's spaces"
            )
        return token

    def _reports(self, space: str) -> List[Dict[str, Any]]:
        return _fetch_reports(self, self._space_token_or_raise(space))

    def _datasets(self, space: str) -> List[Dict[str, Any]]:
        token = self._space_token_or_raise(space)
        url = f"{self.workspace_uri}/spaces/{token}/datasets?filter=all"
        return _get_embedded_paged(
            self, url, "reports", context=f"datasets listing for space '{space}'"
        )

    def _queries(self, space: str, report: str) -> List[Dict[str, Any]]:
        space_token = self._space_token_or_raise(space)
        report_token = _report_token(self, space_token, report)
        if report_token is None:
            raise ProbeSoftError(f"no report named '{report}' found in space '{space}'")
        url = f"{self.workspace_uri}/reports/{report_token}/queries"
        return _get_embedded(
            self, url, "queries", context=f"queries listing for report '{report}'"
        )


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
    _get_paged_request_json -- the datasets listing truncates at one page
    (default 30 items) unless walked with per_page/page until a page comes
    back empty. Mode's own dataset getter has no thin-fetch/policy split to
    delegate to (mode.py's ingestion path never lists datasets separately
    from reports), so this walks the endpoint directly.

    A soft error partway through (e.g. page 3 of 5 403s) raises rather than
    returning the pages collected so far: a truncated listing that looks
    complete is worse than an honest "couldn't finish this, here's why"."""
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
    them (server-side filter param + exclude_restricted). Delegates the raw
    paged listing to mode.py's own fetch_spaces, so paging/errors match
    ingestion byte-for-byte; only the client-side exclude_restricted filter
    lives here, since mode.py's space_pattern filter is exactly what a probe
    must not apply (see test_spaces_apply_space_pattern)."""
    with soft_on_status(403, 404, context="workspace spaces listing"):
        spaces = source.fetch_spaces()
    if source.config.exclude_restricted:
        spaces = [s for s in spaces if not is_restricted_space(s)]
    return spaces


def _fetch_reports(source: ModeSource, space_token: str) -> List[Dict[str, Any]]:
    """Every report in one space, filtered as mode.py's own ingestion run
    would see them (?filter=all, exclude_archived). Delegates the raw paged
    listing to mode.py's own fetch_reports for the same reason as
    _fetch_spaces -- fetch_reports is itself a generator of pages (unlike
    fetch_spaces), so this flattens it: the probe has no streaming consumer
    to preserve, unlike ingestion's threaded per-report workers."""
    with soft_on_status(
        403, 404, context=f"reports listing for space token '{space_token}'"
    ):
        reports = [r for page in source.fetch_reports(space_token) for r in page]
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
