from types import SimpleNamespace

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel, ProbeSoftError
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    DatasetContainerSubTypes,
)

WORKSPACE = DatasetContainerSubTypes.FOLDER  # stands in for a BI workspace
REPORT = BIAssetSubTypes.REPORT
DASHBOARD = BIAssetSubTypes.DASHBOARD


def _names(*names):
    return lambda client, config, parent_path: list(names)


def _cfg():
    return SimpleNamespace(
        folder_pattern=AllowDenyPattern.allow_all(),
        report_pattern=AllowDenyPattern.allow_all(),
        dashboard_pattern=AllowDenyPattern.allow_all(),
        chart_pattern=AllowDenyPattern.allow_all(),
    )


def _bi_probe(report_children=None):
    levels = [
        ProbeLevel(WORKSPACE, "folder_pattern", _names("ws1", "ws2")),
        ProbeLevel(REPORT, "report_pattern", _names("r1", "r2"), parent=WORKSPACE),
        ProbeLevel(DASHBOARD, "dashboard_pattern", _names("d1"), parent=WORKSPACE),
    ]
    if report_children:
        levels.append(
            ProbeLevel(
                BIAssetSubTypes.CHART,
                "chart_pattern",
                _names("c1"),
                parent=REPORT,
            )
        )
    return ClientProbe(client_factory=lambda config: object(), levels=levels)


def test_two_levels_may_share_a_parent():
    probe = _bi_probe()
    assert probe.is_linear is False


def test_listing_a_node_with_sibling_levels_merges_them():
    # "what is inside ws1" = reports AND dashboards, each with its own subtype.
    nodes = _bi_probe().list_children(_cfg(), ["ws1"], 100).nodes
    assert [n.name for n in nodes] == ["r1", "r2", "d1"]
    by_name = {n.name: n for n in nodes}
    assert by_name["r1"].kind == REPORT
    assert by_name["r1"].pattern_field == "report_pattern"
    assert by_name["d1"].kind == DASHBOARD
    assert by_name["d1"].pattern_field == "dashboard_pattern"


def test_merged_listing_truncates_across_the_combined_set():
    result = _bi_probe().list_children(_cfg(), ["ws1"], 2)
    assert [n.name for n in result.nodes] == ["r1", "r2"]
    assert result.truncated is True


def test_a_sibling_level_raising_probesofterror_degrades_without_losing_others():
    # Generic proof that ProbeSoftError/ProbeResult.warnings is a framework
    # mechanism, not something Mode-specific: nothing here is Mode. A level
    # that can't be read cleanly (a 403, a deleted resource) must not take
    # down a sibling level that already succeeded, and must not vanish
    # silently either -- the caller needs to see that something was skipped.
    def _dashboards_403(client, config, parent_path):
        raise ProbeSoftError("dashboards listing for 'ws1' returned HTTP 403")

    probe = ClientProbe(
        client_factory=lambda config: object(),
        levels=[
            ProbeLevel(WORKSPACE, "folder_pattern", _names("ws1")),
            ProbeLevel(REPORT, "report_pattern", _names("r1", "r2"), parent=WORKSPACE),
            ProbeLevel(
                DASHBOARD, "dashboard_pattern", _dashboards_403, parent=WORKSPACE
            ),
        ],
    )
    result = probe.list_children(_cfg(), ["ws1"], 100)
    assert [n.name for n in result.nodes] == ["r1", "r2"]
    assert result.warnings == ["dashboards listing for 'ws1' returned HTTP 403"]


def test_descending_into_an_ambiguous_sibling_requires_a_qualifier():
    probe = _bi_probe(report_children=True)
    with pytest.raises(ValueError, match="ambiguous|qualify|Subtype:"):
        probe.list_children(_cfg(), ["ws1", "r1"], 100)


def test_a_qualified_element_selects_the_sibling_and_passes_a_bare_name_on():
    seen = {}

    def _charts(client, config, parent_path):
        seen["parent_path"] = list(parent_path)
        return ["c1"]

    probe = ClientProbe(
        client_factory=lambda config: object(),
        levels=[
            ProbeLevel(WORKSPACE, "folder_pattern", _names("ws1")),
            ProbeLevel(REPORT, "report_pattern", _names("r1"), parent=WORKSPACE),
            ProbeLevel(DASHBOARD, "dashboard_pattern", _names("d1"), parent=WORKSPACE),
            ProbeLevel("Chart", "chart_pattern", _charts, parent=REPORT),
        ],
    )
    nodes = probe.list_children(_cfg(), ["ws1", f"{REPORT}:r1"], 100).nodes
    assert [n.name for n in nodes] == ["c1"]
    # Listers must never see the qualifier — 24 connector call sites index bare names.
    assert seen["parent_path"] == ["ws1", "r1"]


def test_hierarchy_still_works_for_linear_probes():
    probe = ClientProbe(
        client_factory=lambda config: object(),
        levels=[
            ProbeLevel(WORKSPACE, "folder_pattern", _names("ws1")),
            ProbeLevel(REPORT, "report_pattern", _names("r1"), parent=WORKSPACE),
        ],
    )
    assert probe.is_linear is True
    assert probe.hierarchy() == [WORKSPACE, REPORT]


def test_hierarchy_refuses_a_branching_probe_and_names_the_alternative():
    with pytest.raises(ValueError, match="shape()"):
        _bi_probe().hierarchy()


def test_shape_exposes_the_tree():
    shape = _bi_probe().shape()
    assert shape.kind == WORKSPACE
    assert [c.kind for c in shape.children] == [REPORT, DASHBOARD]
    assert shape.to_dict() == {
        "kind": str(WORKSPACE),
        "children": [
            {"kind": str(REPORT), "children": []},
            {"kind": str(DASHBOARD), "children": []},
        ],
    }


def test_a_colon_in_a_name_is_fine_when_unambiguous():
    # Only parsed as a qualifier where siblings make it necessary.
    probe = ClientProbe(
        client_factory=lambda config: object(),
        levels=[
            ProbeLevel(WORKSPACE, "folder_pattern", _names("a:b")),
            ProbeLevel(REPORT, "report_pattern", _names("r1"), parent=WORKSPACE),
        ],
    )
    assert [n.name for n in probe.list_children(_cfg(), ["a:b"], 100).nodes] == ["r1"]


def test_a_path_running_past_a_linear_leaf_returns_no_children_without_a_client():
    # Regression: a path more than one element past the declared depth used to
    # raise IndexError from _levels_for's ambiguous-element branch (sorted(kinds)
    # on an empty dict) instead of returning "no children" like the pre-tree
    # single len(parent_path) >= len(self._levels) guard did.
    def boom(config):
        raise AssertionError("list_children() must not build a client past depth")

    probe = ClientProbe(
        client_factory=boom,
        levels=[
            ProbeLevel(WORKSPACE, "folder_pattern", _names("ws1")),
            ProbeLevel(REPORT, "report_pattern", _names("r1"), parent=WORKSPACE),
        ],
    )
    result = probe.list_children(_cfg(), ["ws1", "r1", "extra"], 100)
    assert result.supported is True
    assert result.nodes == []


def test_a_path_running_past_a_branching_leaf_returns_no_children_without_a_client():
    def boom(config):
        raise AssertionError("list_children() must not build a client past depth")

    probe = ClientProbe(
        client_factory=boom,
        levels=[
            ProbeLevel(WORKSPACE, "folder_pattern", _names("ws1")),
            ProbeLevel(REPORT, "report_pattern", _names("r1"), parent=WORKSPACE),
            ProbeLevel(DASHBOARD, "dashboard_pattern", _names("d1"), parent=WORKSPACE),
        ],
    )
    # "r1" is ambiguous between the two siblings at depth 1, so it must be
    # qualified; "extra" then runs one element past the (childless) Report leaf.
    result = probe.list_children(_cfg(), ["ws1", f"{REPORT}:r1", "extra"], 100)
    assert result.supported is True
    assert result.nodes == []
