from types import SimpleNamespace

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.models import ProbeLeafKind
from datahub.ingestion.agent.probe import (
    ClientProbe,
    LevelSource,
    ProbeLevel,
    pattern_field_for_config_class,
    pattern_verdict,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)

_CFG = SimpleNamespace(
    table_pattern=AllowDenyPattern(allow=[".*"], deny=["^tmp_.*"]),
    view_pattern=AllowDenyPattern(allow=[".*"]),
)


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
    def classify(ctx):
        if ctx.name.startswith("sys$"):
            return (False, "system_object")
        return pattern_verdict(ctx.config, ctx.pattern_field, ctx.fqn)

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


def test_list_items_level_carries_per_item_kind_and_resolves_patterns():
    # A single listing yields both kinds; items with an explicit pattern_field
    # (BigQuery's real usage) pass it through unchanged, while items that leave
    # it None (as in the brief) still resolve by convention against _CFG's own
    # table_pattern/view_pattern.
    def items(client, config, parent_path):
        return [
            ("orders", DatasetSubTypes.TABLE, None),
            ("v_orders", DatasetSubTypes.VIEW, None),
            ("explicit_t", DatasetSubTypes.TABLE, "table_pattern"),
            ("explicit_v", DatasetSubTypes.VIEW, "view_pattern"),
        ]

    probe = ClientProbe(
        client_factory=lambda config: object(),
        levels=[ProbeLevel(DatasetSubTypes.TABLE, list_items=items)],
    )
    by_name = {n.name: n for n in probe.list_children(_CFG, [], 100).nodes}
    assert by_name["orders"].kind == DatasetSubTypes.TABLE
    assert by_name["orders"].pattern_field == "table_pattern"
    assert by_name["v_orders"].kind == DatasetSubTypes.VIEW
    assert by_name["v_orders"].pattern_field == "view_pattern"
    assert by_name["explicit_t"].pattern_field == "table_pattern"
    assert by_name["explicit_v"].pattern_field == "view_pattern"


def test_level_requires_exactly_one_lister_mode():
    with pytest.raises(ValueError):
        ProbeLevel(DatasetSubTypes.TABLE, "table_pattern")  # none
    with pytest.raises(ValueError):
        ProbeLevel(
            DatasetSubTypes.TABLE,
            list_names=_lister("a"),
            list_items=lambda c, cfg, p: [],
        )  # two
    with pytest.raises(ValueError):
        ProbeLevel(
            DatasetSubTypes.TABLE,
            "table_pattern",
            _lister("a"),
            sources=[LevelSource(_lister("b"), DatasetSubTypes.VIEW, "view_pattern")],
        )  # two
    with pytest.raises(ValueError):
        ProbeLevel(
            DatasetSubTypes.TABLE,
            sources=[LevelSource(_lister("b"), DatasetSubTypes.VIEW, "view_pattern")],
            list_items=lambda c, cfg, p: [],
        )  # two


def test_list_items_rejects_level_wide_pattern_field_and_kind_for():
    with pytest.raises(ValueError):
        ProbeLevel(
            DatasetSubTypes.TABLE, "table_pattern", list_items=lambda c, cfg, p: []
        )
    with pytest.raises(ValueError):
        ProbeLevel(
            DatasetSubTypes.TABLE,
            list_items=lambda c, cfg, p: [],
            kind_for=lambda n: DatasetSubTypes.VIEW,
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
            ProbeLevel(
                ProbeLeafKind.COLUMN, list_names=_lister(), parent=DatasetSubTypes.TABLE
            ),
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
            ProbeLevel(
                ProbeLeafKind.COLUMN, list_names=_lister(), parent=DatasetSubTypes.TABLE
            ),
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
    # No pattern_field declared: the level's kind (Table) must resolve against
    # _CFG's own table_pattern attribute, and that field must actually filter.
    probe = _probe(
        ProbeLevel(DatasetSubTypes.TABLE, list_names=_lister("orders", "tmp_scratch"))
    )
    by_name = {n.name: n for n in probe.list_children(_CFG, [], 100).nodes}
    assert by_name["orders"].pattern_field == "table_pattern"
    assert by_name["orders"].included is True
    assert by_name["tmp_scratch"].pattern_field == "table_pattern"
    assert by_name["tmp_scratch"].included is False
    assert by_name["tmp_scratch"].excluded_by == "table_pattern"


def test_omitted_pattern_field_raises_when_the_kind_has_no_conventional_field():
    # _CFG has no schema_pattern at all — neither an instance attribute nor a
    # model_fields entry — so resolution must fail loudly rather than silently pass.
    probe = _probe(
        ProbeLevel(DatasetContainerSubTypes.SCHEMA, list_names=_lister("public"))
    )
    with pytest.raises(ValueError, match="Schema"):
        probe.list_children(_CFG, [], 100)


def test_instance_attribute_resolves_even_though_the_bare_class_cannot():
    # pattern_field_for_config_class(SimpleNamespace, "Table") is None:
    # SimpleNamespace has no model_fields for the class-level check to
    # introspect. The instance-aware path must still find _CFG's own
    # table_pattern attribute directly.
    # Narrowed via an annotated local: passing `type(_CFG)` inline infers as
    # type[Any], which mypy's lru_cache stub rejects as Hashable.
    cfg_cls: type = type(_CFG)
    assert pattern_field_for_config_class(cfg_cls, DatasetSubTypes.TABLE) is None
    probe = _probe(ProbeLevel(DatasetSubTypes.TABLE, list_names=_lister("orders")))
    by_name = {n.name: n for n in probe.list_children(_CFG, [], 100).nodes}
    assert by_name["orders"].pattern_field == "table_pattern"


def test_column_level_with_classify_does_not_require_a_pattern_field():
    # A Column level that sets classify= skips the leaf fast path (which has no
    # verdict machinery), reaching _resolved with kind=Column. Column has no
    # AllowDenyPattern to resolve, so it must pass through unchanged rather than
    # raise "no AllowDenyPattern field ... filters kind 'Column'".
    def classify(ctx):
        return (False, "sensitive") if ctx.name == "ssn" else (True, None)

    probe = _probe(
        ProbeLevel(
            ProbeLeafKind.COLUMN, list_names=_lister("id", "ssn"), classify=classify
        )
    )
    by_name = {n.name: n for n in probe.list_children(_CFG, [], 100).nodes}
    assert by_name["id"].included is True
    assert by_name["ssn"].included is False
    assert by_name["ssn"].excluded_by == "sensitive"


def test_column_level_with_list_items_does_not_take_the_leaf_fast_path():
    # list_items carries per-item kind/pattern like sources; the leaf fast path
    # asserts list_names is set, so a Column + list_items level must not take it.
    def items(client, config, parent_path):
        return [
            ("id", ProbeLeafKind.COLUMN, None),
            ("name", ProbeLeafKind.COLUMN, None),
        ]

    probe = ClientProbe(
        client_factory=lambda config: object(),
        levels=[ProbeLevel(ProbeLeafKind.COLUMN, list_items=items)],
    )
    nodes = probe.list_children(_CFG, [], 100).nodes
    assert [n.name for n in nodes] == ["id", "name"]
    assert all(n.included for n in nodes)


def test_classifier_receives_the_parent_path():
    seen = {}

    def classify(ctx):
        seen["parent_path"] = ctx.parent_path
        seen["name"] = ctx.name
        seen["fqn"] = ctx.fqn
        seen["pattern_field"] = ctx.pattern_field
        return (True, None)

    probe = ClientProbe(
        client_factory=lambda config: object(),
        levels=[
            ProbeLevel(DatasetSubTypes.TABLE, "table_pattern", _lister("x")),
            ProbeLevel(
                DatasetSubTypes.VIEW,
                "view_pattern",
                _lister("orders"),
                classify=classify,
                parent=DatasetSubTypes.TABLE,
            ),
        ],
    )
    probe.list_children(_CFG, ["my_db"], 100)
    assert seen["parent_path"] == ("my_db",)
    assert seen["name"] == "orders"
    assert seen["fqn"] == "my_db.orders"
    assert seen["pattern_field"] == "view_pattern"


def test_hierarchy_is_derived_from_parent_edges_not_list_order():
    # Declared out of order on purpose: the edges, not the positions, define the shape.
    probe = ClientProbe(
        client_factory=lambda config: object(),
        levels=[
            ProbeLevel(
                ProbeLeafKind.COLUMN, list_names=_lister(), parent=DatasetSubTypes.TABLE
            ),
            ProbeLevel(DatasetContainerSubTypes.SCHEMA, "schema_pattern", _lister()),
            ProbeLevel(
                DatasetSubTypes.TABLE,
                "table_pattern",
                _lister(),
                parent=DatasetContainerSubTypes.SCHEMA,
            ),
        ],
    )
    assert probe.hierarchy() == [
        DatasetContainerSubTypes.SCHEMA,
        DatasetSubTypes.TABLE,
        ProbeLeafKind.COLUMN,
    ]


def test_single_level_probe_needs_no_parent():
    probe = ClientProbe(
        client_factory=lambda config: object(),
        levels=[ProbeLevel(DatasetSubTypes.TOPIC, "topic_patterns", _lister())],
    )
    assert probe.hierarchy() == [DatasetSubTypes.TOPIC]


def test_exactly_one_root_required():
    with pytest.raises(ValueError, match="root"):
        ClientProbe(
            client_factory=lambda config: object(),
            levels=[
                ProbeLevel(
                    DatasetContainerSubTypes.SCHEMA, "schema_pattern", _lister()
                ),
                ProbeLevel(DatasetSubTypes.TABLE, "table_pattern", _lister()),
            ],
        )


def test_parent_must_name_a_declared_level():
    with pytest.raises(ValueError, match="unknown parent|not declared"):
        ClientProbe(
            client_factory=lambda config: object(),
            levels=[
                ProbeLevel(
                    DatasetContainerSubTypes.SCHEMA, "schema_pattern", _lister()
                ),
                ProbeLevel(
                    DatasetSubTypes.TABLE,
                    "table_pattern",
                    _lister(),
                    parent=DatasetContainerSubTypes.DATABASE,
                ),
            ],
        )


def test_branching_levels_are_accepted_and_form_a_tree():
    # Two levels sharing a parent is a tree, not a chain — see
    # test_probe_branching.py for the full tree-shaped listing behaviour this
    # enables. Sibling levels off the same parent are no longer rejected.
    probe = ClientProbe(
        client_factory=lambda config: object(),
        levels=[
            ProbeLevel(DatasetContainerSubTypes.SCHEMA, "schema_pattern", _lister()),
            ProbeLevel(
                DatasetSubTypes.TABLE,
                "table_pattern",
                _lister(),
                parent=DatasetContainerSubTypes.SCHEMA,
            ),
            ProbeLevel(
                DatasetSubTypes.VIEW,
                "view_pattern",
                _lister(),
                parent=DatasetContainerSubTypes.SCHEMA,
            ),
        ],
    )
    assert probe.is_linear is False
    assert [c.kind for c in probe.shape().children] == [
        DatasetSubTypes.TABLE,
        DatasetSubTypes.VIEW,
    ]


def test_a_cycle_with_no_root_is_rejected_as_rootless():
    # Every level has a parent, so there is no root at all.
    with pytest.raises(ValueError, match="root"):
        ClientProbe(
            client_factory=lambda config: object(),
            levels=[
                ProbeLevel(
                    DatasetSubTypes.TABLE,
                    "table_pattern",
                    _lister(),
                    parent=ProbeLeafKind.COLUMN,
                ),
                ProbeLevel(
                    ProbeLeafKind.COLUMN,
                    list_names=_lister(),
                    parent=DatasetSubTypes.TABLE,
                ),
            ],
        )


def test_a_cycle_disjoint_from_the_root_is_rejected_as_unreachable():
    # A valid root plus a separate Table<->Column cycle. The chain walk never
    # enters the cycle, so it must be caught by the reachability check rather
    # than silently dropped — this is the branch the rootless case above cannot
    # reach.
    with pytest.raises(ValueError, match="unreachable|cyclic"):
        ClientProbe(
            client_factory=lambda config: object(),
            levels=[
                ProbeLevel(
                    DatasetContainerSubTypes.SCHEMA, "schema_pattern", _lister()
                ),
                ProbeLevel(
                    DatasetSubTypes.TABLE,
                    "table_pattern",
                    _lister(),
                    parent=ProbeLeafKind.COLUMN,
                ),
                ProbeLevel(
                    ProbeLeafKind.COLUMN,
                    list_names=_lister(),
                    parent=DatasetSubTypes.TABLE,
                ),
            ],
        )
