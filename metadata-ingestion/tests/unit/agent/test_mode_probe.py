from types import SimpleNamespace
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

import pytest
import requests

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.probe import UNNAMED, ProbeSoftError
from datahub.ingestion.source.mode import ModeAPIConfig, ModeConfig, ModeSource
from datahub.ingestion.source.mode_probe import (
    MODE_PROBE,
    ModeProbeSource,
    _get_embedded_paged,
    list_mode_children,
)

_WORKSPACE = "https://app.mode.com/api/acryltest"

# Every body below plants extra keys a real Mode record carries beyond what a
# hierarchy node's name projects (ids, tokens, _links, _forms, timestamps, ...),
# so equality asserts can tell "observed and used" apart from "observed and
# ignored." data_sources/definitions are the opposite case on purpose: they
# assert those extra keys survive verbatim, since those two commands are
# mode.py's own raw fetchers annotated in place, not a probe-side projection.
# Shapes are drawn from tests/integration/mode/setup/*.json.
_RESPONSES: Dict[str, Dict[str, Any]] = {
    f"{_WORKSPACE}/spaces": {
        "spaces": [
            {"name": "Personal", "token": "sp1"},
            {"name": "Archive", "token": "sp2"},
            {"name": "SharedSpaceA", "token": "sp3"},
            # Present regardless of exclude_restricted -- filtering happens
            # client-side in _fetch_spaces, not by the fake session.
            {"name": "RestrictedSpace", "token": "sp5", "restricted": True},
            # No "name" key at all -- for proving the space_pattern filter
            # test uses mode.py's raw-name-or-"" semantics, not _display_name
            # (which would fall back to this token).
            {"token": "sp7"},
        ]
    },
    f"{_WORKSPACE}/spaces/sp1/reports": {
        "reports": [{"name": "Weekly", "token": "r1"}]
    },
    # Despite the "/datasets" path, Mode embeds this listing under the
    # "reports" HAL key (see tests/integration/mode/setup/datasets_*.json) --
    # a Mode "dataset" is implemented as a special kind of report. Keying this
    # fixture as "datasets" would encode the bug it exists to catch.
    f"{_WORKSPACE}/spaces/sp1/datasets": {"reports": [{"name": "Seed", "token": "d1"}]},
    # One archived, one not -- exercises exclude_archived's client-side filter.
    f"{_WORKSPACE}/spaces/sp3/reports": {
        "reports": [
            {"name": "NewReport", "token": "r-new"},
            {"name": "OldReport", "token": "r-old", "archived": True},
        ]
    },
    f"{_WORKSPACE}/reports/r1/queries": {
        "queries": [
            {
                "id": 10149707,
                "token": "q1",
                "name": "q_main",
                "raw_query": "select 1",
                "data_source_id": 34499,
                "last_run_id": 1897576958,
                "_links": {"self": {"href": "/api/acryltest/reports/r1/queries/q1"}},
            }
        ]
    },
    f"{_WORKSPACE}/data_sources": {
        # Every entry needs a unique "id" -- like a real Mode payload (see
        # tests/integration/mode/setup/data_sources.json) -- since
        # source._get_data_sources_by_id() (mode.py) indexes this listing by
        # int(id) for O(1) lookup; entries sharing a (missing) id would
        # collide and silently overwrite each other in that dict.
        "data_sources": [
            {
                "id": "34499",
                "name": "PostgreSQL",
                "adapter": "jdbc:postgresql",
                "database": "dvdrental",
                "host": "72.38.17.64",
                "username": "postgres",
            },
            {
                "id": "34500",
                "name": "BigQueryConn",
                "adapter": "jdbc:bigquery",
                # BigQuery's raw "database" is always the literal "default";
                # data_sources returns it as-is -- no probe-side substitution.
                "database": "default",
                "host": "some-project-id",
                "username": "should-not-appear",
            },
        ]
    },
    f"{_WORKSPACE}/definitions": {
        "definitions": [
            {
                "id": 40065,
                "token": "d575d5553bd6",
                "name": "active_users",
                "description": "Users active in 30d",
                "source": "SELECT user_id FROM users WHERE active",
                "data_source_id": 34499,
                "_links": {"self": {"href": "/api/acryltest/definitions/d575d5553bd6"}},
            }
        ]
    },
}


class _FakeSession:
    """Answers the Mode endpoints the branching and method probes use.

    Any request for page > 1 gets an empty page, terminating pagination --
    every fixture above fits on one page, mirroring how the real Mode API
    (and tests/integration/mode/test_mode.py's own mock) signals "no more
    pages" by returning an empty `_embedded` list.
    """

    def __init__(self):
        self.calls = []
        self.timeouts = []
        self.closed = False
        # mode.py's own _get_request_json (unlike fetch_json's narrower
        # ModeApiSession Protocol) logs a curl-equivalent via
        # make_curl_command before every request, which reads session.headers
        # and session.auth directly -- data_sources/definitions now go
        # through it (see ModeSource.for_probe), so every session fake used
        # with _method_probe needs both attributes, not just get()/close().
        self.headers: Dict[str, str] = {}
        self.auth = None

    def get(self, url, **kw):
        self.calls.append(url)
        self.timeouts.append(kw.get("timeout"))
        page = int(parse_qs(urlparse(url).query).get("page", ["1"])[0])
        base_url = url.split("?")[0]
        body = {} if page > 1 else _RESPONSES.get(base_url, {})
        return SimpleNamespace(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: {"_embedded": body},
        )

    def close(self):
        self.closed = True


class _DatasetsFailSession(_FakeSession):
    """Like _FakeSession, but Personal's datasets endpoint always 404s --
    proving one sibling level's failure doesn't take down the other."""

    def get(self, url, **kw):
        base_url = url.split("?")[0]
        if base_url == f"{_WORKSPACE}/spaces/sp1/datasets":
            self.calls.append(url)
            response = SimpleNamespace(status_code=404, text="mocked error body")

            def _raise() -> None:
                raise requests.HTTPError(
                    f"404 Client Error: Not Found for url: {url}", response=response
                )

            response.raise_for_status = _raise
            return response
        return super().get(url, **kw)


class _StatusSession:
    """Every request fails with the given HTTP status code."""

    def __init__(self, status_code: int):
        self._status_code = status_code
        self.closed = False
        # See _FakeSession.__init__ -- mode.py's _get_request_json reads both.
        self.headers: Dict[str, str] = {}
        self.auth = None

    def get(self, url, **kw):
        response = SimpleNamespace(
            status_code=self._status_code, text="mocked error body"
        )

        def _raise() -> None:
            raise requests.HTTPError(
                f"{self._status_code} Client Error for url: {url}", response=response
            )

        response.raise_for_status = _raise
        return response

    def close(self):
        self.closed = True


class _TwoPageSession:
    """Two non-empty pages of "things", then an empty third page."""

    def __init__(self):
        self.pages = {1: [{"name": "a"}], 2: [{"name": "b"}]}
        # See _FakeSession.__init__ -- mode.py's _get_request_json reads both.
        self.headers: Dict[str, str] = {}
        self.auth = None

    def get(self, url, **kw):
        page = int(parse_qs(urlparse(url).query).get("page", ["1"])[0])
        items = self.pages.get(page, [])
        return SimpleNamespace(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: {"_embedded": {"things": items}},
        )

    def close(self):
        pass


class _SoftErrorOnSecondPageSession:
    """Page 1 succeeds with real data; page 2 (and beyond) 403s."""

    def __init__(self):
        # See _FakeSession.__init__ -- mode.py's _get_request_json reads both.
        self.headers: Dict[str, str] = {}
        self.auth = None

    def get(self, url, **kw):
        page = int(parse_qs(urlparse(url).query).get("page", ["1"])[0])
        if page >= 2:
            response = SimpleNamespace(status_code=403, text="mocked error body")

            def _raise() -> None:
                raise requests.HTTPError("403 Client Error", response=response)

            response.raise_for_status = _raise
            return response
        return SimpleNamespace(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: {"_embedded": {"things": [{"name": "a"}]}},
        )

    def close(self):
        pass


def _api_options(**over):
    base: Dict[str, Any] = dict(
        timeout=40,
        retry_backoff_multiplier=2,
        max_retry_interval=60,
        max_attempts=1,
        requests_per_minute=1000,
    )
    base.update(over)
    return ModeAPIConfig(**base)


def _cfg(session=None, **over):
    base = dict(
        space_pattern=AllowDenyPattern(allow=[".*"], deny=["^Archive$"]),
        report_pattern=AllowDenyPattern.allow_all(),
        items_per_page=100,
        exclude_personal_collections=False,
        exclude_restricted=False,
        exclude_archived=False,
        api_options=_api_options(),
    )
    base.update(over)
    session = session or _FakeSession()
    cfg = SimpleNamespace(
        get_mode_session=lambda: (session, _WORKSPACE),
        # Mirrors ModeConfig.space_filter_param (mode.py) -- _fetch_spaces
        # (mode_probe.py) calls this the same way on a real ModeConfig.
        space_filter_param=lambda: (
            "custom" if base["exclude_personal_collections"] else "all"
        ),
        **base,
    )
    cfg._session = session  # not read by mode_probe.py; exposed for assertions
    return cfg


def _real_config(**over):
    """A real ModeConfig (not _cfg()'s duck-typed SimpleNamespace), for the
    method-probe tests below: ModeSource.for_probe() takes the concrete
    ModeConfig -- the same type build_probe_provider passes in production --
    since its shim's `.config` is read by mode.py's own _get_request_json
    (self.config.api_options.*)."""
    base: Dict[str, Any] = dict(
        token="test-token",
        password="test-password",
        workspace="acryltest",
        space_pattern=AllowDenyPattern(allow=[".*"], deny=["^Archive$"]),
        report_pattern=AllowDenyPattern.allow_all(),
        items_per_page=100,
        exclude_personal_collections=False,
        exclude_restricted=False,
        exclude_archived=False,
        api_options=_api_options(),
    )
    base.update(over)
    return ModeConfig(**base)


def test_mode_shape_branches_under_space():
    shape = MODE_PROBE.shape().to_dict()
    assert shape["kind"] == "Space"
    children = shape["children"]
    assert isinstance(children, list)
    # A Space holds BOTH reports and datasets — the branch.
    assert sorted(c["kind"] for c in children) == ["Dataset", "Report"]
    assert MODE_PROBE.is_linear is False


def test_spaces_apply_space_pattern():
    by_name = {n.name: n for n in list_mode_children(_cfg(), [], 100).nodes}
    assert by_name["Personal"].included is True
    assert by_name["Personal"].pattern_field == "space_pattern"
    assert by_name["Archive"].included is False
    assert by_name["Archive"].excluded_by == "space_pattern"


def test_spaces_hierarchy_tests_the_same_raw_name_space_token_does():
    # Regression guard: _spaces() used to report a null-named space by its
    # DISPLAY name (token fallback), while _space_token() (used to resolve a
    # --parent value back to a space) already tested the raw name -- so a
    # pattern denying the literal token string (e.g. "^sp7$") excluded the
    # space from the hierarchy listing but NOT from resolution: the same
    # physical space got two different verdicts depending which code path
    # asked. sp7 (see _RESPONSES) has no "name" key at all. Both paths now
    # test "" for it, so it can no longer be addressed by its token either --
    # it surfaces as an unnamed, unaddressable node (probe.py's own
    # convention for a lister with no usable name to filter or descend into),
    # not a space_pattern-excluded one.
    cfg = _cfg(space_pattern=AllowDenyPattern(allow=[".*"], deny=["^sp7$"]))
    nodes = list_mode_children(cfg, [], 100).nodes
    unnamed = [n for n in nodes if n.name == UNNAMED]
    assert len(unnamed) == 1
    assert unnamed[0].excluded_by == "unnamed"
    assert "sp7" not in {n.name for n in nodes}


def test_listing_a_space_merges_reports_and_datasets():
    nodes = list_mode_children(_cfg(), ["Personal"], 100).nodes
    kinds = {n.name: str(n.kind) for n in nodes}
    assert kinds == {"Weekly": "Report", "Seed": "Dataset"}
    # Reports are filterable; datasets are not (Mode offers no dataset_pattern).
    by_name = {n.name: n for n in nodes}
    assert by_name["Weekly"].pattern_field == "report_pattern"
    assert by_name["Seed"].pattern_field is None


def test_descending_into_a_report_needs_a_qualifier():
    with pytest.raises(ValueError, match="ambiguous|Report:"):
        list_mode_children(_cfg(), ["Personal", "Weekly"], 100)


def test_qualified_descent_lists_queries():
    nodes = list_mode_children(_cfg(), ["Personal", "Report:Weekly"], 100).nodes
    assert [n.name for n in nodes] == ["q_main"]


def test_hierarchy_probe_closes_session():
    cfg = _cfg()
    list_mode_children(cfg, [], 100)
    assert cfg._session.closed is True


def test_exclude_restricted_hides_restricted_spaces():
    cfg = _cfg(exclude_restricted=True)
    names = {n.name for n in list_mode_children(cfg, [], 100).nodes}
    assert "RestrictedSpace" not in names


def test_restricted_spaces_visible_by_default():
    cfg = _cfg()  # exclude_restricted defaults to False, matching ModeConfig
    names = {n.name for n in list_mode_children(cfg, [], 100).nodes}
    assert "RestrictedSpace" in names


def test_exclude_archived_hides_archived_reports():
    cfg = _cfg(exclude_archived=True)
    names = {n.name for n in list_mode_children(cfg, ["SharedSpaceA"], 100).nodes}
    assert names == {"NewReport"}


def test_archived_reports_visible_by_default():
    cfg = _cfg()  # exclude_archived defaults to False, matching ModeConfig
    names = {n.name for n in list_mode_children(cfg, ["SharedSpaceA"], 100).nodes}
    assert names == {"NewReport", "OldReport"}


def test_datasets_404_degrades_to_empty_and_records_a_warning():
    # Regression guard: list_children fans Report and Dataset out as two
    # independent sibling levels under one Space with no built-in
    # containment; if a lister raises on a 404 the whole call used to die
    # with zero nodes, discarding the reports that had already succeeded.
    # And a token that can read Reports but 403s on Datasets must be
    # distinguishable from "this space genuinely has no datasets" -- the
    # warning is that distinction.
    cfg = _cfg(session=_DatasetsFailSession())
    result = list_mode_children(cfg, ["Personal"], 100)
    kinds = {n.name: str(n.kind) for n in result.nodes}
    assert kinds == {"Weekly": "Report"}  # Datasets degraded to [], not raised
    assert len(result.warnings) == 1
    assert "404" in result.warnings[0]


def test_unresolvable_parent_produces_warnings_not_a_silent_empty_listing():
    # Regression guard: a typo'd --parent must not look identical to "this
    # space genuinely has no reports/datasets" -- both sibling levels resolve
    # the same parent name independently, so each contributes its own
    # warning (see _reports/_datasets) instead of the call quietly returning
    # nodes=[], warnings=[].
    result = list_mode_children(_cfg(), ["NoSuchSpace"], 100)
    assert result.nodes == []
    assert len(result.warnings) == 2
    assert all("NoSuchSpace" in w for w in result.warnings)


def test_spaces_listing_sends_filter_all_and_pagination_params_by_default():
    cfg = _cfg(exclude_personal_collections=False)
    list_mode_children(cfg, [], 100)
    spaces_calls = [
        c for c in cfg._session.calls if c.split("?")[0] == f"{_WORKSPACE}/spaces"
    ]
    assert spaces_calls, "expected at least one request to the spaces endpoint"
    assert "filter=all" in spaces_calls[0]
    assert "per_page=100" in spaces_calls[0]
    assert "page=1" in spaces_calls[0]


def test_spaces_listing_sends_filter_custom_when_excluding_personal_collections():
    cfg = _cfg(exclude_personal_collections=True)
    list_mode_children(cfg, [], 100)
    spaces_calls = [
        c for c in cfg._session.calls if c.split("?")[0] == f"{_WORKSPACE}/spaces"
    ]
    assert spaces_calls
    assert "filter=custom" in spaces_calls[0]


def test_requests_use_the_configured_timeout():
    cfg = _cfg(api_options=_api_options(timeout=7))
    list_mode_children(cfg, [], 100)
    assert cfg._session.timeouts
    assert all(t == 7 for t in cfg._session.timeouts)


def _method_probe(session=None, **cfg_over):
    # data_sources/definitions delegate their fetch to a real ModeSource shim
    # -- see ModeSource.for_probe -- so this needs a real ModeConfig,
    # matching what build_probe_provider passes in production. Builds a
    # ModeProbeSource, not a plain ModeSource, matching what
    # build_probe_provider actually returns (its __exit__ closes the ad hoc
    # session -- see ModeProbeSource's docstring).
    session = session or _FakeSession()
    cfg = _real_config(**cfg_over)
    return ModeProbeSource.for_probe(cfg, session, _WORKSPACE)


def test_data_sources_returns_mode_s_raw_records_indexed_by_id():
    # data_sources is mode.py's own _get_data_sources_by_id, annotated in
    # place -- no probe-side re-projection. Exact equality (not a subset
    # check) proves this is Mode's payload verbatim: username/host survive,
    # the adapter stays the raw "jdbc:..." string, and BigQuery's "database"
    # stays the literal "default" rather than being replaced by the project
    # id from "host".
    with _method_probe() as p:
        result = p._get_data_sources_by_id()
    assert result == {
        34499: {
            "id": "34499",
            "name": "PostgreSQL",
            "adapter": "jdbc:postgresql",
            "database": "dvdrental",
            "host": "72.38.17.64",
            "username": "postgres",
        },
        34500: {
            "id": "34500",
            "name": "BigQueryConn",
            "adapter": "jdbc:bigquery",
            "database": "default",
            "host": "some-project-id",
            "username": "should-not-appear",
        },
    }


def test_definitions_returns_raw_name_to_source_map():
    # definitions is mode.py's own _get_definitions_map, annotated in place --
    # the same {name: source} cache `{{@name}}` template expansion uses, so
    # "description" is not returned even though Mode's API has one.
    with _method_probe() as p:
        result = p._get_definitions_map()
    assert result == {"active_users": "SELECT user_id FROM users WHERE active"}


def test_probe_source_context_manager_closes_session():
    # ModeProbeSource.__exit__ exists only for the probe's ad hoc session --
    # a real ingestion run relies on ModeSource's own inherited (Closeable)
    # __exit__, which closes its report, not its session (see
    # ModeProbeSource's docstring).
    session = _FakeSession()
    probe = _method_probe(session=session)
    with probe:
        pass
    assert session.closed is True


@pytest.mark.parametrize("status_code", [404, 403, 401, 500])
def test_data_sources_degrades_to_empty_dict_on_any_http_error(status_code):
    # _get_data_sources_by_id (mode.py) is annotated in place, not wrapped by
    # a probe-side soft/hard split -- so it keeps its own ingestion policy of
    # degrading to {} on ANY HTTP error (not just 404/403), reported to its
    # own (ephemeral) ModeSourceReport rather than raising.
    with _method_probe(session=_StatusSession(status_code)) as p:
        assert p._get_data_sources_by_id() == {}


def test_get_embedded_paged_aggregates_multiple_pages():
    # _get_embedded_paged now fetches through a real ModeSource shim (see
    # ModeSource.for_probe) rather than a bare (session, config, rate_limiter)
    # tuple -- _real_config(), not _cfg()'s duck type, for the same reason
    # _method_probe() above uses it.
    source = ModeSource.for_probe(_real_config(), _TwoPageSession(), "https://x")
    result = _get_embedded_paged(
        source, "https://x/things?filter=all", "things", context="test"
    )
    assert [r["name"] for r in result] == ["a", "b"]


def test_get_embedded_paged_raises_instead_of_returning_partial_pages():
    # A soft error on page 2 must not return page 1's items as if the
    # listing were complete -- that's indistinguishable from "there really
    # is only one page", silently truncating the result.
    source = ModeSource.for_probe(
        _real_config(), _SoftErrorOnSecondPageSession(), "https://x"
    )
    with pytest.raises(ProbeSoftError):
        _get_embedded_paged(
            source, "https://x/things?filter=all", "things", context="test"
        )


def test_probe_methods_registered():
    from datahub.ingestion.agent.probe_methods import _iter_specs

    # _iter_specs uses dir(), which includes inherited attributes -- so
    # data_sources/definitions (annotated on ModeSource itself) are found on
    # ModeProbeSource too, the class build_probe_provider actually returns.
    commands = [c for c, _ in _iter_specs(ModeProbeSource)]
    assert commands == ["data_sources", "definitions"]
