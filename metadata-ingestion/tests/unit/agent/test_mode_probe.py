from types import SimpleNamespace
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

import pytest
import requests

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.probe import ProbeSoftError
from datahub.ingestion.source.mode import ModeAPIConfig
from datahub.ingestion.source.mode_probe import (
    MODE_PROBE,
    ModeMetadataProbe,
    _find_query_token,
    _get_embedded_paged,
    list_mode_children,
)
from datahub.utilities.ratelimiter import RateLimiter

_WORKSPACE = "https://app.mode.com/api/acryltest"

# Every body below plants extra keys a real Mode record carries beyond what the
# probe methods project (ids, tokens, _links, _forms, timestamps, ...) so an
# equality assert against the getter's output can only pass if those extra
# keys were actually dropped, not merely if the projected ones happen to be
# present. Shapes are drawn from tests/integration/mode/setup/*.json.
_RESPONSES: Dict[str, Dict[str, Any]] = {
    f"{_WORKSPACE}/spaces": {
        "spaces": [
            {"name": "Personal", "token": "sp1"},
            {"name": "Archive", "token": "sp2"},
            {"name": "SharedSpaceA", "token": "sp3"},
            {"name": "SharedSpaceB", "token": "sp4"},
            # Present regardless of exclude_restricted -- filtering happens
            # client-side in _fetch_spaces, not by the fake session.
            {"name": "RestrictedSpace", "token": "sp5", "restricted": True},
            {"name": "QueryTestSpace", "token": "sp6"},
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
    # "Archive" is denied by _cfg()'s space_pattern -- a report that lives
    # only there must be invisible to report_queries/query_charts, not just
    # to the hierarchy probe.
    f"{_WORKSPACE}/spaces/sp2/reports": {
        "reports": [{"name": "ArchiveOnlyReport", "token": "r-archive-only"}]
    },
    # "DupReport" exists in two spaces that ARE in scope (neither denied nor
    # excluded), so the ambiguity guard still fires for a name that's
    # genuinely ambiguous among the spaces a recipe would actually ingest.
    # "OldReport" is archived, to test exclude_archived without disturbing
    # the ambiguity fixture or test_listing_a_space_merges_reports_and_datasets
    # (which asserts Personal's reports/datasets exactly).
    f"{_WORKSPACE}/spaces/sp3/reports": {
        "reports": [
            {"name": "DupReport", "token": "r-dup-3"},
            {"name": "OldReport", "token": "r-old", "archived": True},
        ]
    },
    f"{_WORKSPACE}/spaces/sp4/reports": {
        "reports": [{"name": "DupReport", "token": "r-dup-4"}]
    },
    f"{_WORKSPACE}/spaces/sp5/reports": {
        "reports": [{"name": "RestrictedOnlyReport", "token": "r-restricted"}]
    },
    f"{_WORKSPACE}/spaces/sp6/reports": {
        "reports": [{"name": "QueryEdgeCasesReport", "token": "r-qec"}]
    },
    # Query-name-resolution edge cases, kept out of r1 so they don't disturb
    # test_qualified_descent_lists_queries' exact `== ["q_main"]` assertion:
    # two queries sharing a name (ambiguous), and one with no name at all
    # (addressable only by its token, via _display_name's fallback).
    f"{_WORKSPACE}/reports/r-qec/queries": {
        "queries": [
            {"name": "DupQuery", "token": "q-dup-1"},
            {"name": "DupQuery", "token": "q-dup-2"},
            {"token": "q-unnamed"},
        ]
    },
    f"{_WORKSPACE}/reports/r-qec/queries/q-unnamed/charts": {
        "charts": [
            {"token": "c-unnamed", "view": {"title": "Untitled", "chartType": "table"}}
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
    f"{_WORKSPACE}/reports/r1/queries/q1/charts": {
        "charts": [
            {
                "token": "c1",
                "created_at": "2021-12-10T20:14:08.856Z",
                "color_palette_token": "should-not-appear",
                "_links": {
                    "self": {"href": "/api/acryltest/reports/r1/queries/q1/charts/c1"}
                },
                "view": {"title": "Revenue", "chartType": "bar"},
            }
        ]
    },
    f"{_WORKSPACE}/data_sources": {
        "data_sources": [
            {
                "name": "PostgreSQL",
                "adapter": "jdbc:postgresql",
                "database": "dvdrental",
                # Fields a real Mode data source can carry that must NOT
                # survive projection into the probe result.
                "host": "72.38.17.64",
                "username": "postgres",
            },
            {
                "name": "AcrylBQ",
                "adapter": "jdbc:bigquery",
                # BigQuery's raw "database" is always the literal "default";
                # the real project id is only ever in "host".
                "database": "default",
                "host": "some-project-id",
                "username": "should-not-appear",
            },
            {
                # No "name" at all -- _display_name must fall back to token.
                "token": "ds3",
                "adapter": "jdbc:sqlserver",
                "database": "analytics",
                "host": "should-not-appear",
            },
            {
                # An adapter MODE_ADAPTER_PLATFORM_MAP doesn't recognize --
                # the fallback must be the data source's own name, not the
                # raw adapter string (which is neither a valid DataHub
                # platform nor what ingestion would emit).
                "name": "VerticaConn",
                "adapter": "jdbc:vertica",
                "database": "mydb",
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
    cfg = SimpleNamespace(get_mode_session=lambda: (session, _WORKSPACE), **base)
    cfg._session = session  # not part of ModeProbeConfig; exposed for assertions
    return cfg


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
    assert names == {"DupReport"}


def test_archived_reports_visible_by_default():
    cfg = _cfg()  # exclude_archived defaults to False, matching ModeConfig
    names = {n.name for n in list_mode_children(cfg, ["SharedSpaceA"], 100).nodes}
    assert names == {"DupReport", "OldReport"}


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
    cfg = _cfg(session=session, **cfg_over)
    return ModeMetadataProbe(cfg._session, _WORKSPACE, config=cfg)


def test_data_sources_projects_named_fields_only():
    with _method_probe() as p:
        result = p.data_sources()
    # Exact equality (not a subset check) proves username/host from the raw
    # API payload were dropped in the general case, that the postgres adapter
    # maps to "postgres" (not a naive "postgresql"), that BigQuery's "default"
    # database is replaced by the real project id from "host", that a data
    # source with no "name" falls back to its token, and that an adapter the
    # mapping table doesn't recognize falls back to the data source's own
    # name (not the raw "jdbc:vertica" string).
    assert result == [
        {"name": "PostgreSQL", "adapter": "postgres", "database": "dvdrental"},
        {"name": "AcrylBQ", "adapter": "bigquery", "database": "some-project-id"},
        {"name": "ds3", "adapter": "mssql", "database": "analytics"},
        {"name": "VerticaConn", "adapter": "VerticaConn", "database": "mydb"},
    ]


def test_definitions_projects_name_description_and_source():
    with _method_probe() as p:
        result = p.definitions()
    assert result == [
        {
            "name": "active_users",
            "description": "Users active in 30d",
            "source": "SELECT user_id FROM users WHERE active",
        }
    ]


def test_report_queries_resolves_report_name_across_spaces():
    with _method_probe() as p:
        result = p.report_queries(report="Weekly")
    assert result == [{"name": "q_main", "sql": "select 1"}]


def test_report_queries_raises_when_report_not_found():
    # Not-found must be distinguishable from "this report has no queries" --
    # returning [] here would be indistinguishable from that (and from a
    # misspelling, or the report only existing in an out-of-scope space).
    with _method_probe() as p:
        with pytest.raises(ValueError, match="no report named 'Nonexistent'"):
            p.report_queries(report="Nonexistent")


def test_report_queries_raises_for_a_report_only_in_a_denied_space():
    # "ArchiveOnlyReport" only exists in "Archive", which _cfg()'s
    # space_pattern denies. This must be indistinguishable, from the caller's
    # perspective, from any other not-found case -- not silently return [].
    with _method_probe() as p:
        with pytest.raises(ValueError, match="no report named 'ArchiveOnlyReport'"):
            p.report_queries(report="ArchiveOnlyReport")


def test_report_queries_raises_for_a_report_only_in_a_restricted_space():
    with _method_probe(exclude_restricted=True) as p:
        with pytest.raises(ValueError, match="no report named 'RestrictedOnlyReport'"):
            p.report_queries(report="RestrictedOnlyReport")


def test_report_queries_raises_on_ambiguous_report_name():
    # "DupReport" lives in SharedSpaceA and SharedSpaceB, both in scope.
    with _method_probe() as p:
        with pytest.raises(
            ValueError,
            match="ambiguous report name 'DupReport'.*SharedSpaceA, SharedSpaceB",
        ):
            p.report_queries(report="DupReport")


def test_query_charts_resolves_report_and_query_to_tokens():
    with _method_probe() as p:
        result = p.query_charts(report="Weekly", query="q_main")
    assert result == [{"title": "Revenue", "chart_type": "bar"}]


def test_query_charts_raises_when_query_not_found():
    # Symmetric with report resolution: an unresolvable query name must not
    # be indistinguishable from "this query has no charts" -- an agent that
    # mistypes a query name would otherwise get [] and conclude it drives no
    # charts.
    with _method_probe() as p:
        with pytest.raises(ValueError, match="no query named 'Nonexistent'"):
            p.query_charts(report="Weekly", query="Nonexistent")


def test_query_charts_raises_on_ambiguous_query_name():
    with _method_probe() as p:
        with pytest.raises(ValueError, match="ambiguous query name 'DupQuery'"):
            p.query_charts(report="QueryEdgeCasesReport", query="DupQuery")


def test_query_charts_resolves_an_unnamed_query_by_its_token():
    # _find_query_token matches on _display_name, which falls back to a
    # query's token when its "name" is null (mode.py has the same
    # name-or-token convention) -- so the token is itself a valid `query`.
    with _method_probe() as p:
        result = p.query_charts(report="QueryEdgeCasesReport", query="q-unnamed")
    assert result == [{"title": "Untitled", "chart_type": "table"}]


def test_find_query_token_matches_a_null_named_query_by_token():
    cfg = _cfg()
    rate_limiter = RateLimiter(max_calls=1000, period=60)
    token = _find_query_token(
        cfg._session, cfg, rate_limiter, _WORKSPACE, "r-qec", "q-unnamed"
    )
    assert token == "q-unnamed"


def test_query_charts_raises_when_report_not_found():
    with _method_probe() as p:
        with pytest.raises(ValueError, match="no report named 'Nonexistent'"):
            p.query_charts(report="Nonexistent", query="q_main")


def test_exit_closes_session():
    session = _FakeSession()
    probe = _method_probe(session=session)
    with probe:
        pass
    assert session.closed is True


def test_data_sources_degrades_to_empty_on_404():
    with _method_probe(session=_StatusSession(404)) as p:
        assert p.data_sources() == []


def test_data_sources_degrades_to_empty_on_403():
    with _method_probe(session=_StatusSession(403)) as p:
        assert p.data_sources() == []


def test_data_sources_raises_on_401_auth_failure():
    with _method_probe(session=_StatusSession(401)) as p:
        with pytest.raises(requests.HTTPError):
            p.data_sources()


def test_data_sources_raises_on_500():
    with _method_probe(session=_StatusSession(500)) as p:
        with pytest.raises(requests.HTTPError):
            p.data_sources()


def test_get_embedded_paged_aggregates_multiple_pages():
    cfg = _cfg()
    rate_limiter = RateLimiter(max_calls=1000, period=60)
    result = _get_embedded_paged(
        _TwoPageSession(),
        cfg,
        rate_limiter,
        "https://x/things",
        "things",
        context="test",
    )
    assert [r["name"] for r in result] == ["a", "b"]


def test_get_embedded_paged_raises_instead_of_returning_partial_pages():
    # A soft error on page 2 must not return page 1's items as if the
    # listing were complete -- that's indistinguishable from "there really
    # is only one page", silently truncating the result.
    cfg = _cfg()
    rate_limiter = RateLimiter(max_calls=1000, period=60)
    with pytest.raises(ProbeSoftError):
        _get_embedded_paged(
            _SoftErrorOnSecondPageSession(),
            cfg,
            rate_limiter,
            "https://x/things",
            "things",
            context="test",
        )


def test_probe_methods_registered():
    from datahub.ingestion.agent.probe_methods import _iter_specs

    commands = [c for c, _ in _iter_specs(ModeMetadataProbe)]
    for expected in ["data_sources", "definitions", "report_queries", "query_charts"]:
        assert expected in commands
