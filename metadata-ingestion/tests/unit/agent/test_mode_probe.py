from types import SimpleNamespace
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

import pytest
import requests

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.mode_probe import (
    MODE_PROBE,
    ModeMetadataProbe,
    _get_embedded_paged,
    list_mode_children,
)

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
            {"name": "SharedSpace", "token": "sp3"},
        ]
    },
    f"{_WORKSPACE}/spaces/sp1/reports": {
        "reports": [{"name": "Weekly", "token": "r1"}]
    },
    f"{_WORKSPACE}/spaces/sp1/datasets": {
        "datasets": [{"name": "Seed", "token": "d1"}]
    },
    # "DupReport" deliberately exists in two different spaces (Archive and
    # SharedSpace, neither of which any hierarchy test descends into) so a
    # dedicated test can prove report_queries/query_charts raise on an
    # ambiguous name instead of silently returning whichever space iterates
    # first. "Weekly" stays exclusive to Personal so the "clean" tests below
    # keep resolving unambiguously.
    f"{_WORKSPACE}/spaces/sp2/reports": {
        "reports": [{"name": "DupReport", "token": "r-archive-dup"}]
    },
    f"{_WORKSPACE}/spaces/sp3/reports": {
        "reports": [{"name": "DupReport", "token": "r-shared-dup"}]
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
        self.closed = False

    def get(self, url, **kw):
        self.calls.append(url)
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


class _FailingSession:
    """A session whose every request 403s, to prove errors surface instead
    of silently rendering as an empty result."""

    def get(self, url, **kw):
        response = SimpleNamespace(status_code=403)

        def _raise() -> None:
            raise requests.HTTPError(
                f"403 Client Error: Forbidden for url: {url}", response=response
            )

        response.raise_for_status = _raise
        return response

    def close(self):
        pass


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


def _cfg(**over):
    base = dict(
        space_pattern=AllowDenyPattern(allow=[".*"], deny=["^Archive$"]),
        report_pattern=AllowDenyPattern.allow_all(),
        items_per_page=100,
    )
    base.update(over)
    session = _FakeSession()
    return SimpleNamespace(
        get_mode_session=lambda: (session, _WORKSPACE),
        **base,
    )


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


def _method_probe(**kwargs):
    return ModeMetadataProbe(_FakeSession(), _WORKSPACE, **kwargs)


def test_data_sources_projects_named_fields_only():
    with _method_probe() as p:
        result = p.data_sources()
    # Exact equality (not a subset check) proves username/host from the raw
    # API payload were dropped in the general case, that the postgres adapter
    # maps to "postgres" (not a naive "postgresql"), that BigQuery's "default"
    # database is replaced by the real project id from "host", and that a
    # data source with no "name" falls back to its token.
    assert result == [
        {"name": "PostgreSQL", "adapter": "postgres", "database": "dvdrental"},
        {"name": "AcrylBQ", "adapter": "bigquery", "database": "some-project-id"},
        {"name": "ds3", "adapter": "mssql", "database": "analytics"},
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


def test_report_queries_unknown_report_returns_empty():
    with _method_probe() as p:
        assert p.report_queries(report="Nonexistent") == []


def test_report_queries_raises_on_ambiguous_report_name():
    with _method_probe() as p:
        with pytest.raises(ValueError, match="ambiguous report name 'DupReport'"):
            p.report_queries(report="DupReport")


def test_query_charts_resolves_report_and_query_to_tokens():
    with _method_probe() as p:
        result = p.query_charts(report="Weekly", query="q_main")
    assert result == [{"title": "Revenue", "chart_type": "bar"}]


def test_query_charts_unknown_query_returns_empty():
    with _method_probe() as p:
        assert p.query_charts(report="Weekly", query="Nonexistent") == []


def test_exit_closes_session():
    session = _FakeSession()
    probe = ModeMetadataProbe(session, _WORKSPACE)
    with probe:
        pass
    assert session.closed is True


def test_get_embedded_raises_on_http_error_instead_of_silently_empty():
    probe = ModeMetadataProbe(_FailingSession(), _WORKSPACE)
    with pytest.raises(requests.HTTPError):
        probe.data_sources()


def test_get_embedded_paged_aggregates_multiple_pages():
    result = _get_embedded_paged(
        _TwoPageSession(), "https://x/things", "things", items_per_page=1
    )
    assert [r["name"] for r in result] == ["a", "b"]


def test_probe_methods_registered():
    from datahub.ingestion.agent.probe_methods import _iter_specs

    commands = [c for c, _ in _iter_specs(ModeMetadataProbe)]
    for expected in ["data_sources", "definitions", "report_queries", "query_charts"]:
        assert expected in commands
