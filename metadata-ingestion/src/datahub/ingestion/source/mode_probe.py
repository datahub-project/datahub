from typing import Any, Callable, Dict, List, Optional, TypeVar

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import echoed, soft_listing
from datahub.ingestion.agent.rest_passthrough import RestApiPassthrough
from datahub.ingestion.agent.verdicts import ProbeArgumentError, ProbeSoftError
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.mode import (
    ModeConfig,
    ModeSource,
    is_archived_report,
    is_restricted_space,
)


class ModeProbeSource(RestApiPassthrough, ModeSource):
    """Mode's probe provider: the probe methods, the `api` passthrough, and the
    closing of the probe's own session.

    The probe opens its session for one short `with` block (for_config), so
    this __exit__ closes it. That override cannot live on ModeSource: a
    pipeline calls __exit__ on every source, and an ingestion run's session
    must live for the whole pipeline, which is why ModeSource's inherited
    Closeable.__exit__ closes only the report."""

    @property
    def probe_report(self) -> object:
        """The ingestion report these commands write into, so its failures reach
        the caller.

        `data_sources` and `definitions` call ModeSource's fetchers verbatim,
        which on a ModeRequestError record self.report.failure() and return
        {}: right for an ingestion run. Unless the report is read back, a 403
        on data_sources is an empty dict at exit 0, indistinguishable from a
        workspace with no warehouse connections -- the "so no lineage"
        diagnosis, the most consequential answer this probe gives.
        """
        return self.report

    # Read endpoints `probe api` may reach: the escape hatch for a question no
    # getter anticipated (a report's last-run time, whether a space is
    # restricted).
    #
    # Some of these look redundant against the getters below and are not -- they
    # are the bootstrap. Mode addresses objects by token, and no getter returns
    # one: they all return the display name a pattern is matched against (see
    # test_spaces_report_the_same_raw_name_space_token_resolves_by). So the raw
    # /spaces listing is the only route to a space token, and a space's raw
    # reports listing the only route to a report token. Trim the "duplicates" and
    # the token-addressed entries below become unreachable, which is to say
    # `probe api` stops working. test_every_token_addressed_endpoint_has_a_route
    # pins the chain. The `api` command itself is inherited from
    # RestApiPassthrough; only this list and the fetcher below are Mode's.
    #
    # This does NOT replace the getters. A raw record leaves the caller to guess
    # which field a pattern is matched against, and for a Space that is the raw
    # "name" with no token fallback (see _space_pattern_name) -- connector
    # knowledge a passthrough cannot carry.
    #
    # Deliberately absent, by the rule hex_probe applies to /cells: an endpoint
    # returning raw SQL is a route for row values (WHERE literals) to arrive by.
    #   /reports/{token}/queries -- each query's raw_query
    #   /definitions             -- each definition's SQL source
    api_allowlist = (
        "GET /spaces",
        "GET /spaces/{token}/reports",
        "GET /spaces/{token}/datasets",
        "GET /reports/{token}",
        "GET /data_sources",
    )

    def api_fetch_json(self, url: str) -> object:
        # Mode's own fetcher, not the mixin's requests call: it logs a curl
        # equivalent and counts rate-limit/timeout retries, so a probe request
        # behaves and reports exactly as an ingestion request does.
        return self._get_request_json(url)

    def __exit__(self, *exc: object) -> None:
        self.session.close()
        # Stops at ProbeProviderBase, which does not chain on: ModeSource's
        # Closeable.__exit__ (closing an ingestion report) is never reached.
        super().__exit__(*exc)

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
        # Mode's base carries the workspace segment, so `api` appends a path to the
        # workspace URI rather than to the host. Primed here rather than as a
        # property because for_probe builds via __new__ and primes every attribute
        # the probe needs.
        probe.api_base_url = workspace_uri
        return probe

    def _listing(
        self, fetch: Callable[[], List[str]], *, note_on_success: str = ""
    ) -> List[str]:
        """Run one listing, turning a soft error into a warning rather than a
        silent empty result -- the distinction between "nothing here" and "I
        could not look" is the whole point of a diagnostic.

        `note_on_success` is a narrowing the caller wants explained, and it is
        attached HERE so it cannot outlive the listing it explains. Appended by
        the caller instead, it rode along with a failure: a 403 on /spaces came
        back as [] plus "Mode filtered personal spaces out server-side",
        offering a config reason for an outcome the config did not cause.
        """
        with soft_listing(self._warn):
            names = fetch()
            if note_on_success:
                self._warn(note_on_success)
            return names
        return []

    # Declared here, not on ModeSource: a declaration error raises at import, and
    # in mode.py that would break Mode ingestion rather than just the probe.
    # Both delegate to the ingestion fetchers, so they fetch and degrade exactly
    # as a run does.
    @probe_method(name="data_sources")
    def probe_data_sources(self) -> Dict[int, dict]:
        """Warehouse connections this Mode workspace can query, as Mode's own
        raw API records, keyed by the data source's integer id (arrives in
        JSON output as a string key). Each record is Mode's payload verbatim
        -- e.g. "adapter" is Mode's own connector string (like
        "jdbc:postgresql"), not a DataHub platform name, and "database" is
        Mode's raw value (for BigQuery this is always the literal "default",
        not the real project id). On a transient API failure this returns an
        empty dict and reports the failure, matching what a real ingestion run
        does -- run this again to retry."""
        return self._get_data_sources_by_id()

    @probe_method(name="definitions")
    def probe_definitions(self) -> List[str]:
        """Names of Mode's reusable SQL definitions in this workspace -- the
        `{{@name}}` a query can expand. Names only: the SQL bodies are withheld,
        for the reason the API allowlist omits /definitions. On a transient API
        failure this returns an empty list and reports the failure -- run this
        again to retry."""
        return sorted(self._get_definitions_map())

    @probe_method(kind="Space")
    def spaces(self) -> List[str]:
        """Spaces (Mode's UI calls them Collections) in this workspace,
        including ones space_pattern would exclude -- a denied space is
        reported, not hidden, so `probe filter` can explain it.

        Personal spaces are the exception, and the result says so when they
        are missing: exclude_personal_collections makes Mode filter them
        server-side (?filter=custom), so they never reach us to be reported as
        excluded. A short list with no explanation is the failure this
        interface exists to prevent, so the narrowing is reported as a
        warning rather than left for the caller to notice."""
        return self._listing(
            lambda: [_space_pattern_name(space) for space in _fetch_spaces(self)],
            note_on_success=(
                "exclude_personal_collections is set, so Mode filtered personal "
                "spaces out server-side (?filter=custom); they are absent from "
                "this list rather than reported as excluded, and ingestion will "
                "not see them either"
            )
            if self.config.exclude_personal_collections
            else "",
        )

    # parent_params on these three, as the SQL family does for `schema`: the
    # container travels back in the result, so `probe filter` needs no
    # --parent restating what the call already said. Mode filters on the bare
    # name, so this does not change any verdict -- it stops the result
    # dropping context it was handed.
    @probe_method(kind=BIAssetSubTypes.MODE_REPORT, parent_params=("space",))
    def reports(self, space: str) -> List[str]:
        """Reports in one space, by space name. Excludes archived reports when
        the recipe sets exclude_archived, matching what ingestion would see."""
        return self._listing(lambda: [_display_name(r) for r in self._reports(space)])

    @probe_method(kind=BIAssetSubTypes.MODE_DATASET, parent_params=("space",))
    def datasets(self, space: str) -> List[str]:
        """Datasets in one space, by space name. A Mode dataset is a special
        kind of report, so these come from the space's /datasets endpoint."""
        return self._listing(lambda: [_display_name(d) for d in self._datasets(space)])

    @probe_method(kind=BIAssetSubTypes.MODE_QUERY, parent_params=("space", "report"))
    def queries(self, space: str, report: str) -> List[str]:
        """Queries belonging to one report, addressed by space and report name."""
        return self._listing(
            lambda: [_display_name(q) for q in self._queries(space, report)]
        )

    def _space_token_or_raise(self, space: str) -> str:
        # A space the caller named that the listing does not hold is their
        # argument (exit 2); a listing that could not be read stays a
        # ProbeSoftError from _fetch_spaces, so "could not look" is a warning.
        token = _space_token(self, space)
        if token is None:
            raise ProbeArgumentError(
                f"no space named {echoed(space)} among this workspace's spaces "
                f"(as the recipe sees them); run `spaces` for the names"
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
            raise ProbeArgumentError(
                f"no report named {echoed(report)} in space {echoed(space)}; run "
                f"`reports --space` for the names"
            )
        url = f"{self.workspace_uri}/reports/{report_token}/queries"
        return _get_embedded(
            self, url, "queries", context=f"queries listing for report '{report}'"
        )


_T = TypeVar("_T")


def _soft_fetch(fetch: Callable[[], _T], context: str) -> _T:
    """`fetch()`, with Mode's 403 and 404 ("nothing here") raised as a
    ProbeSoftError naming `context`, for the command's _listing to report:
    each step names what it could not read, and the command decides the
    fallback, so a failed step is never mistaken for an empty one."""
    reasons: List[str] = []
    with soft_listing(reasons.append, 403, 404, context=context):
        return fetch()
    raise ProbeSoftError(reasons[0])


def _get_embedded(
    source: ModeSource, url: str, key: str, context: str
) -> List[Dict[str, Any]]:
    """Queries under one report: goes through ModeSource's own
    bound _get_request_json (see for_probe) rather than a
    bare session.get(), so it shares session/rate-limit/retry/debug-logging
    with a real ingestion run.

    Deliberately NOT delegated to _get_queries/_get_charts: those always
    degrade HTTP/JSON errors to an empty result, which is correct for
    ingestion but hides the distinction a probe exists to report. This wraps
    the fetch in _soft_fetch instead."""
    payload = _soft_fetch(lambda: source._get_request_json(url), context)
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

    def walk() -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for page in source._get_paged_request_json(
            url, key, source.config.items_per_page
        ):
            items.extend(page)
        return items

    return _soft_fetch(walk, context)


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
    # list() here on purpose: fetch_spaces yields per space so an ingestion
    # run keeps what it read before a paging failure, but a probe has no
    # streaming consumer and needs the failure to surface before it reports a
    # count.
    spaces = _soft_fetch(
        lambda: list(source.fetch_spaces()), "workspace spaces listing"
    )
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
    reports = _soft_fetch(
        lambda: [r for page in source.fetch_reports(space_token) for r in page],
        f"reports listing for space token '{space_token}'",
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
