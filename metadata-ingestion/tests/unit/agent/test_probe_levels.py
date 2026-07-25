from types import SimpleNamespace

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.models import ProbeLeafKind
from datahub.ingestion.agent.probe import (
    ClientProbe,
    LevelSource,
    ProbeLevel,
    pattern_verdict,
)
from datahub.ingestion.source.common.subtypes import DatasetSubTypes

_CFG = SimpleNamespace(
    table_pattern=AllowDenyPattern(allow=[".*"], deny=["^tmp_.*"]),
    view_pattern=AllowDenyPattern(allow=[".*"]),
)


def _probe(*levels):
    return ClientProbe(client_factory=lambda config: object(), levels=list(levels))


def _lister(*names):
    return lambda client, config, parent_path: list(names)


def test_merged_level_dedups_first_source_wins_with_per_node_kind_and_pattern():
    probe = _probe(
        ProbeLevel(
            DatasetSubTypes.TABLE,
            sources=[
                LevelSource(
                    _lister("orders", "shared"), DatasetSubTypes.TABLE, "table_pattern"
                ),
                LevelSource(
                    _lister("shared", "v_orders"), DatasetSubTypes.VIEW, "view_pattern"
                ),
            ],
        )
    )
    nodes = probe.list_children(_CFG, [], 100).nodes
    assert [n.name for n in nodes] == ["orders", "shared", "v_orders"]
    by_name = {n.name: n for n in nodes}
    assert by_name["shared"].kind == DatasetSubTypes.TABLE
    assert by_name["shared"].pattern_field == "table_pattern"
    assert by_name["v_orders"].kind == DatasetSubTypes.VIEW
    assert by_name["v_orders"].pattern_field == "view_pattern"


def test_merged_level_applies_each_sources_own_pattern():
    probe = _probe(
        ProbeLevel(
            DatasetSubTypes.TABLE,
            sources=[
                LevelSource(
                    _lister("orders", "tmp_x"), DatasetSubTypes.TABLE, "table_pattern"
                ),
                LevelSource(_lister("tmp_v"), DatasetSubTypes.VIEW, "view_pattern"),
            ],
        )
    )
    by_name = {n.name: n for n in probe.list_children(_CFG, [], 100).nodes}
    assert by_name["orders"].included is True
    assert by_name["tmp_x"].included is False
    assert by_name["tmp_x"].excluded_by == "table_pattern"
    assert by_name["tmp_v"].included is True  # view_pattern allows tmp_*


def test_merged_level_truncates_on_the_combined_set():
    probe = _probe(
        ProbeLevel(
            DatasetSubTypes.TABLE,
            sources=[
                LevelSource(_lister("a", "b"), DatasetSubTypes.TABLE, "table_pattern"),
                LevelSource(_lister("c"), DatasetSubTypes.VIEW, "view_pattern"),
            ],
        )
    )
    result = probe.list_children(_CFG, [], 2)
    assert [n.name for n in result.nodes] == ["a", "b"]
    assert result.truncated is True


def test_classify_override_beats_the_default_pattern_check():
    def classify(config, name, node_fqn, pattern_field):
        if name.startswith("sys$"):
            return (False, "system_object")
        return pattern_verdict(config, pattern_field, node_fqn)

    probe = _probe(
        ProbeLevel(
            DatasetSubTypes.TABLE,
            "table_pattern",
            _lister("sys$log", "orders"),
            classify=classify,
        )
    )
    by_name = {n.name: n for n in probe.list_children(_CFG, [], 100).nodes}
    assert by_name["sys$log"].included is False
    assert by_name["sys$log"].excluded_by == "system_object"
    assert by_name["orders"].included is True


def test_level_requires_exactly_one_of_list_names_or_sources():
    with pytest.raises(ValueError):
        ProbeLevel(DatasetSubTypes.TABLE, "table_pattern")
    with pytest.raises(ValueError):
        ProbeLevel(
            DatasetSubTypes.TABLE,
            "table_pattern",
            _lister("a"),
            sources=[LevelSource(_lister("b"), DatasetSubTypes.VIEW, "view_pattern")],
        )


def test_hierarchy_never_builds_a_client():
    def boom(config):
        raise AssertionError("hierarchy() must not build a client")

    probe = ClientProbe(
        client_factory=boom,
        levels=[
            ProbeLevel(DatasetSubTypes.TABLE, "table_pattern", _lister()),
            ProbeLevel(ProbeLeafKind.COLUMN, list_names=_lister()),
        ],
    )
    assert probe.hierarchy() == [DatasetSubTypes.TABLE, ProbeLeafKind.COLUMN]


def test_pattern_verdict_helper():
    assert pattern_verdict(_CFG, None, "anything") == (True, None)
    assert pattern_verdict(_CFG, "table_pattern", "orders") == (True, None)
    assert pattern_verdict(_CFG, "table_pattern", "tmp_x") == (False, "table_pattern")
