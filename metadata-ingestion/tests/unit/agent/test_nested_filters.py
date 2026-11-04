"""Filters(...) on a field inside a nested config block.

Dataplex keeps its entry filters under `filter_config.entries`, so the field
that filters a kind is a dotted path from the top-level config. Everything
here must behave for a dotted path exactly as it already does for a bare one.
"""

from typing import Annotated, List, Optional

import pytest
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel, Filters
from datahub.ingestion.agent import filter_check
from datahub.ingestion.agent.config_fields import iter_config_fields
from datahub.ingestion.agent.declarations import filters_field
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.introspect import _pattern_field_for_config_class
from datahub.ingestion.agent.pattern_path import (
    copy_with_pattern_at,
    pattern_at,
    require_pattern_at,
)
from datahub.ingestion.agent.verdicts import ProbeInternalError, pattern_verdict


class _Widgets(ConfigModel):
    pattern: Annotated[AllowDenyPattern, Filters("Widget")] = Field(
        default=AllowDenyPattern.allow_all()
    )


class _Block(ConfigModel):
    widgets: _Widgets = Field(default_factory=_Widgets)


class _Outer(ConfigModel):
    filters: _Block = Field(default_factory=_Block)
    gadget_pattern: Annotated[AllowDenyPattern, Filters("Gadget")] = Field(
        default=AllowDenyPattern.allow_all()
    )


class _Twice(ConfigModel):
    filters: _Block = Field(default_factory=_Block)
    widget_pattern: Annotated[AllowDenyPattern, Filters("Widget")] = Field(
        default=AllowDenyPattern.allow_all()
    )


class _Inner(ConfigModel):
    pattern: Annotated[AllowDenyPattern, Filters("Widget")] = Field(
        default=AllowDenyPattern.allow_all()
    )


class _OptOuter(ConfigModel):
    block: Optional[_Inner] = None


def _outer(deny: List[str]) -> _Outer:
    return _Outer.model_validate({"filters": {"widgets": {"pattern": {"deny": deny}}}})


def test_a_nested_declaration_resolves_to_its_dotted_path() -> None:
    assert (
        _pattern_field_for_config_class(_Outer, "Widget") == "filters.widgets.pattern"
    )
    assert _pattern_field_for_config_class(_Outer, "Gadget") == "gadget_pattern"


def test_one_kind_declared_at_two_depths_is_refused() -> None:
    with pytest.raises(ProbeInternalError):
        filters_field(_Twice, "Widget")


def test_the_walk_does_not_descend_into_a_pattern() -> None:
    paths = [path for path, _ in iter_config_fields(_Outer)]
    assert "filters.widgets.pattern" in paths
    assert not any(p.startswith("filters.widgets.pattern.") for p in paths)


def test_pattern_verdict_reads_the_nested_pattern() -> None:
    config = _outer(deny=["^bad$"])
    assert pattern_verdict(config, "filters.widgets.pattern", "bad").included is False
    assert pattern_verdict(config, "filters.widgets.pattern", "good").included is True


def test_copy_with_pattern_at_does_not_touch_the_source() -> None:
    config = _outer(deny=["^bad$"])
    replacement = AllowDenyPattern(deny=["^good$"])
    copied = copy_with_pattern_at(config, "filters.widgets.pattern", replacement)
    assert require_pattern_at(copied, "filters.widgets.pattern") is replacement
    assert pattern_at(config, "filters.widgets.pattern") == AllowDenyPattern(
        deny=["^bad$"]
    )
    assert isinstance(copied, _Outer)
    assert copied.filters is not config.filters
    assert copied.filters.widgets is not config.filters.widgets


def test_a_missing_segment_reads_as_no_pattern() -> None:
    assert pattern_at(_outer(deny=[]), "filters.nope.pattern") is None
    with pytest.raises(TypeError):
        require_pattern_at(_outer(deny=[]), "filters.nope.pattern")


@pytest.fixture
def _registered_outer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(filter_check, "require_config_class", lambda _st: _Outer)
    monkeypatch.setattr(filter_check, "list_probe_methods", lambda _st: [])


@pytest.mark.usefixtures("_registered_outer")
def test_check_filters_judges_on_the_nested_pattern() -> None:
    result = check_filters(
        source_type="fake",
        config_dict={"filters": {"widgets": {"pattern": {"deny": ["^bad$"]}}}},
        kind="Widget",
        parent_path=[],
        names=["bad", "good"],
    )
    assert result.pattern_field == "filters.widgets.pattern"
    assert result.filtering == "by_pattern"
    assert [r.included for r in result.results] == [False, True]
    assert result.results[0].excluded_by == "filters.widgets.pattern"


@pytest.mark.usefixtures("_registered_outer")
def test_try_deny_reaches_a_nested_pattern() -> None:
    result = check_filters(
        source_type="fake",
        config_dict={},
        kind="Widget",
        parent_path=[],
        names=["bad", "good"],
        try_deny=["^good$"],
    )
    assert [r.included for r in result.results] == [True, False]
    assert result.tried == {"allow": [".*"], "deny": ["^good$"]}


@pytest.fixture
def _registered_opt_outer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(filter_check, "require_config_class", lambda _st: _OptOuter)
    monkeypatch.setattr(filter_check, "list_probe_methods", lambda _st: [])


@pytest.mark.usefixtures("_registered_opt_outer")
def test_an_unset_optional_block_filters_nothing() -> None:
    result = check_filters(
        source_type="fake",
        config_dict={},
        kind="Widget",
        parent_path=[],
        names=["a", "b"],
    )
    assert result.pattern_field == "block.pattern"
    assert [r.included for r in result.results] == [True, True]
    assert any("`block` is unset" in w for w in result.warnings)


@pytest.mark.usefixtures("_registered_opt_outer")
def test_try_on_an_unset_optional_block_is_skipped_with_a_warning() -> None:
    result = check_filters(
        source_type="fake",
        config_dict={},
        kind="Widget",
        parent_path=[],
        names=["a"],
        try_deny=[".*"],
    )
    assert result.tried is None
    assert result.results[0].included is True
    assert any("--try-allow and --try-deny were ignored" in w for w in result.warnings)


@pytest.mark.usefixtures("_registered_opt_outer")
def test_a_set_optional_block_still_filters() -> None:
    result = check_filters(
        source_type="fake",
        config_dict={"block": {"pattern": {"deny": ["^a$"]}}},
        kind="Widget",
        parent_path=[],
        names=["a", "b"],
    )
    assert [r.included for r in result.results] == [False, True]
    assert result.warnings == []


def test_pattern_verdict_includes_under_an_unset_optional_block() -> None:
    # The recipe left `block` out, which is valid and filters nothing there.
    verdict = pattern_verdict(_OptOuter(), "block.pattern", "anything")
    assert verdict.included is True


def test_pattern_verdict_still_raises_on_an_unresolved_path() -> None:
    with pytest.raises(TypeError):
        pattern_verdict(_OptOuter(), "nope.pattern", "anything")


class _Level4(ConfigModel):
    pattern: Annotated[AllowDenyPattern, Filters("Deep")] = Field(
        default=AllowDenyPattern.allow_all()
    )


class _Level3(ConfigModel):
    d: _Level4 = Field(default_factory=_Level4)


class _Level2(ConfigModel):
    c: _Level3 = Field(default_factory=_Level3)


class _Level1(ConfigModel):
    b: _Level2 = Field(default_factory=_Level2)


class _Deep(ConfigModel):
    a: _Level1 = Field(default_factory=_Level1)


def test_a_declaration_five_blocks_deep_resolves() -> None:
    assert _pattern_field_for_config_class(_Deep, "Deep") == "a.b.c.d.pattern"


class _Node(ConfigModel):
    name_pattern: Annotated[AllowDenyPattern, Filters("Node")] = Field(
        default=AllowDenyPattern.allow_all()
    )
    child: Optional["_Node"] = None


class _TwoSiblings(ConfigModel):
    left: _Inner = Field(default_factory=_Inner)
    right: _Inner = Field(default_factory=_Inner)


def test_a_self_referencing_config_terminates() -> None:
    paths = [path for path, _ in iter_config_fields(_Node)]
    assert paths == ["name_pattern", "child"]


def test_siblings_reusing_one_block_type_are_both_walked() -> None:
    paths = {path for path, _ in iter_config_fields(_TwoSiblings)}
    assert {"left.pattern", "right.pattern"} <= paths
