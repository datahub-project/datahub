from types import SimpleNamespace
from typing import Any, Dict

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.mode_probe import (
    MODE_PROBE,
    ModeMetadataProbe,
    list_mode_children,
)


class _FakeSession:
    """Answers the Mode endpoints the branching and method probes use."""

    def __init__(self):
        self.calls = []
        self.closed = False

    def get(self, url, **kw):
        self.calls.append(url)
        body: Dict[str, Any]
        if url.endswith("/spaces"):
            body = {
                "spaces": [
                    {"name": "Personal", "token": "sp1"},
                    {"name": "Archive", "token": "sp2"},
                ]
            }
        elif url.endswith("/charts"):
            body = {
                "charts": [
                    {"token": "c1", "view": {"title": "Revenue", "chartType": "bar"}}
                ]
            }
        elif url.endswith("/reports"):
            body = {"reports": [{"name": "Weekly", "token": "r1"}]}
        elif url.endswith("/datasets"):
            body = {"datasets": [{"name": "Seed", "token": "d1"}]}
        elif url.endswith("/queries"):
            body = {
                "queries": [{"name": "q_main", "token": "q1", "raw_query": "select 1"}]
            }
        elif url.endswith("/data_sources"):
            body = {
                "data_sources": [
                    {
                        "name": "warehouse",
                        "adapter": "jdbc:bigquery",
                        "database": "analytics",
                        # Fields a real Mode data source can carry that must
                        # NOT survive projection into the probe result.
                        "username": "should-not-appear",
                        "host": "should-not-appear",
                    }
                ]
            }
        elif url.endswith("/definitions"):
            body = {
                "definitions": [
                    {"name": "active_users", "description": "Users active in 30d"}
                ]
            }
        else:
            body = {}
        return SimpleNamespace(
            ok=True, status_code=200, json=lambda: {"_embedded": body}
        )

    def close(self):
        self.closed = True


def _cfg(**over):
    base = dict(
        space_pattern=AllowDenyPattern(allow=[".*"], deny=["^Archive$"]),
        report_pattern=AllowDenyPattern.allow_all(),
    )
    base.update(over)
    session = _FakeSession()
    return SimpleNamespace(
        get_mode_session=lambda: (session, "https://app.mode.com/api/acryltest"),
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


def _method_probe():
    return ModeMetadataProbe(_FakeSession(), "https://app.mode.com/api/acryltest")


def test_data_sources_projects_named_fields_only():
    with _method_probe() as p:
        result = p.data_sources()
    # Exact equality (not a subset check) proves username/host from the raw
    # API payload were dropped, not merely that name/adapter/database exist.
    assert result == [
        {"name": "warehouse", "adapter": "bigquery", "database": "analytics"}
    ]


def test_definitions_projects_name_and_description():
    with _method_probe() as p:
        result = p.definitions()
    assert result == [{"name": "active_users", "description": "Users active in 30d"}]


def test_report_queries_resolves_report_name_across_spaces():
    with _method_probe() as p:
        result = p.report_queries(report="Weekly")
    assert result == [{"name": "q_main", "sql": "select 1"}]


def test_report_queries_unknown_report_returns_empty():
    with _method_probe() as p:
        assert p.report_queries(report="Nonexistent") == []


def test_query_charts_resolves_report_and_query_to_tokens():
    with _method_probe() as p:
        result = p.query_charts(report="Weekly", query="q_main")
    assert result == [{"title": "Revenue", "chart_type": "bar"}]


def test_query_charts_unknown_query_returns_empty():
    with _method_probe() as p:
        assert p.query_charts(report="Weekly", query="Nonexistent") == []


def test_exit_closes_session():
    session = _FakeSession()
    probe = ModeMetadataProbe(session, "https://app.mode.com/api/acryltest")
    with probe:
        pass
    assert session.closed is True


def test_probe_methods_registered():
    from datahub.ingestion.agent.probe_methods import _iter_specs

    commands = [c for c, _ in _iter_specs(ModeMetadataProbe)]
    for expected in ["data_sources", "definitions", "report_queries", "query_charts"]:
        assert expected in commands
