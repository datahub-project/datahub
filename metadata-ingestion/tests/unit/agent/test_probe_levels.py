from types import SimpleNamespace

import pytest

from datahub.configuration.common import AllowDenyPattern, ConfigModel
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


# resolve_pattern_field needs a real pydantic config (model_fields), unlike the
# plain SimpleNamespace _CFG above — same table_pattern deny (^tmp_) as _CFG, kept
# on a config class that carries no view_pattern at all.
class _ResolvableConfig(ConfigModel):
    table_pattern: AllowDenyPattern = AllowDenyPattern(allow=[".*"], deny=["^tmp_.*"])


_RESOLVABLE_CFG = _ResolvableConfig()


def _probe(*levels):
    return ClientProbe(client_factory=lambda config: object(), levels=list(levels))


def _lister(*names):
    return lambda client, config, parent_path: list(names)


def test_merged_level_keeps_first_position_but_later_source_kind_and_pattern():
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
    # "shared" keeps its first-sighting position, but a later source's kind/pattern
    # must win: a dialect that reports a view inside its table listing (Hive) still
    # needs that name classified as a view.
    assert [n.name for n in nodes] == ["orders", "shared", "v_orders"]
    by_name = {n.name: n for n in nodes}
    assert by_name["shared"].kind == DatasetSubTypes.VIEW
    assert by_name["shared"].pattern_field == "view_pattern"
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


def test_sources_level_rejects_a_level_wide_pattern_field():
    # A sources level carries its kind/pattern per LevelSource; a level-wide
    # pattern_field would be silently ignored otherwise.
    with pytest.raises(ValueError):
        ProbeLevel(
            DatasetSubTypes.TABLE,
            "table_pattern",
            sources=[LevelSource(_lister("a"), DatasetSubTypes.TABLE, "table_pattern")],
        )


def test_sources_level_rejects_kind_for():
    with pytest.raises(ValueError):
        ProbeLevel(
            DatasetSubTypes.TABLE,
            sources=[LevelSource(_lister("a"), DatasetSubTypes.TABLE, "table_pattern")],
            kind_for=lambda name: DatasetSubTypes.VIEW,
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


def test_list_children_past_declared_depth_never_builds_a_client():
    def boom(config):
        raise AssertionError("list_children() must not build a client past depth")

    probe = ClientProbe(
        client_factory=boom,
        levels=[
            ProbeLevel(DatasetSubTypes.TABLE, "table_pattern", _lister()),
            ProbeLevel(ProbeLeafKind.COLUMN, list_names=_lister()),
        ],
    )
    result = probe.list_children(_CFG, ["db", "orders"], 100)
    assert result.supported is True
    assert result.nodes == []


def test_pattern_verdict_helper():
    assert pattern_verdict(_CFG, None, "anything") == (True, None)
    assert pattern_verdict(_CFG, "table_pattern", "orders") == (True, None)
    assert pattern_verdict(_CFG, "table_pattern", "tmp_x") == (False, "table_pattern")


def test_omitted_pattern_field_resolves_by_convention_and_filters():
    # No pattern_field declared: the level's kind (Table) must resolve to
    # table_pattern on _ResolvableConfig, and that field must actually filter.
    probe = _probe(
        ProbeLevel(DatasetSubTypes.TABLE, list_names=_lister("orders", "tmp_scratch"))
    )
    by_name = {n.name: n for n in probe.list_children(_RESOLVABLE_CFG, [], 100).nodes}
    assert by_name["orders"].pattern_field == "table_pattern"
    assert by_name["orders"].included is True
    assert by_name["tmp_scratch"].pattern_field == "table_pattern"
    assert by_name["tmp_scratch"].included is False
    assert by_name["tmp_scratch"].excluded_by == "table_pattern"


def test_omitted_pattern_field_raises_when_the_kind_has_no_conventional_field():
    # DatasetSubTypes.VIEW has no view_pattern on _ResolvableConfig (only
    # table_pattern), so resolution must fail loudly rather than silently pass.
    probe = _probe(ProbeLevel(DatasetSubTypes.VIEW, list_names=_lister("v_orders")))
    with pytest.raises(ValueError, match="View"):
        probe.list_children(_RESOLVABLE_CFG, [], 100)
