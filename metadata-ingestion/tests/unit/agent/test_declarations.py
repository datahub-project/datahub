"""The probe's field markers: read off a config, and a misdeclared one refused.

Each failure case breaks exactly one rule, so a rule that stops firing, or one
that starts firing on a neighbour's case, fails here.
"""

from typing import Annotated, List, Optional, Set, Tuple, Type

import pytest
from pydantic import Field

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    Enables,
    Filters,
    FiltersByRule,
    Qualifier,
)
from datahub.ingestion.agent import filter_check
from datahub.ingestion.agent.declarations import (
    declared_kind_enablers,
    declared_qualifier,
    declared_rule_filtered_kinds,
    declared_unfiltered_kinds,
    declares_qualifier,
    filters_field,
    marker_problems,
)
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.verdicts import (
    ProbeInternalError,
    Verdict,
    VerdictContext,
)


class _Judged(ConfigModel):
    """A FiltersByRule kind needs an override to judge it."""

    def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
        return None


class _Widgets(ConfigModel):
    pattern: Annotated[AllowDenyPattern, Filters("Widget")] = Field(
        default=AllowDenyPattern.allow_all()
    )


class _WellDeclared(_Judged):
    database: Annotated[Optional[str], Qualifier()] = None
    include_views: Annotated[Optional[bool], Enables("View")] = None
    widgets: _Widgets = Field(default_factory=_Widgets)
    path_specs: Annotated[List[str], FiltersByRule("Table")] = Field(
        default_factory=list
    )


def test_a_well_declared_config_reads_back_from_its_class_or_an_instance() -> None:
    assert marker_problems(_WellDeclared) == []
    assert filters_field(_WellDeclared, "Widget") == "widgets.pattern"
    assert declared_kind_enablers(_WellDeclared) == {"View": "include_views"}
    assert declared_rule_filtered_kinds(_WellDeclared()) == {"Table": "path_specs"}
    assert declares_qualifier(_WellDeclared)
    assert declares_qualifier(_WellDeclared())
    assert declared_qualifier(_WellDeclared(database="db")) == ("db", False)


class _FiltersOnAStr(ConfigModel):
    thing: Annotated[str, Filters("Table")] = "nope"


class _FiltersTwice(ConfigModel):
    widgets: _Widgets = Field(default_factory=_Widgets)
    widget_pattern: Annotated[AllowDenyPattern, Filters("Widget")] = Field(
        default=AllowDenyPattern.allow_all()
    )


class _EnablesOnAStr(ConfigModel):
    include_notebooks: Annotated[str, Enables("Notebook")] = "yes"


class _Jobs(ConfigModel):
    include_jobs: Annotated[bool, Enables("Job")] = True


class _EnablesNested(ConfigModel):
    block: _Jobs = Field(default_factory=_Jobs)


class _EnablesTwice(ConfigModel):
    include_a: Annotated[bool, Enables("View")] = True
    include_b: Annotated[bool, Enables("View")] = True


class _Rules(ConfigModel):
    path_specs: Annotated[List[str], FiltersByRule("Table")] = Field(
        default_factory=list
    )


class _RulesNested(_Judged):
    block: _Rules = Field(default_factory=_Rules)


class _RulesTwice(_Judged):
    path_specs: Annotated[List[str], FiltersByRule("Table")] = Field(
        default_factory=list
    )
    table_rules: Annotated[List[str], FiltersByRule("Table")] = Field(
        default_factory=list
    )


class _RulesAndPattern(_Judged):
    path_specs: Annotated[List[str], FiltersByRule("Table")] = Field(
        default_factory=list
    )
    table_pattern: Annotated[AllowDenyPattern, Filters("Table")] = Field(
        default=AllowDenyPattern.allow_all()
    )


class _RulesAndUnfiltered(_Judged):
    path_specs: Annotated[List[str], FiltersByRule("Table")] = Field(
        default_factory=list
    )

    @classmethod
    def probe_unfiltered_kinds(cls) -> Set[str]:
        return {"Table"}


class _RulesUnjudged(ConfigModel):
    path_specs: Annotated[List[str], FiltersByRule("Table")] = Field(
        default_factory=list
    )


class _Database(ConfigModel):
    database: Annotated[Optional[str], Qualifier()] = None


class _QualifierNested(ConfigModel):
    block: _Database = Field(default_factory=_Database)


class _QualifierTwice(ConfigModel):
    project_ids: Annotated[List[str], Qualifier()] = Field(default_factory=list)
    database: Annotated[Optional[str], Qualifier(authoritative=True)] = None


@pytest.mark.parametrize(
    ("config_cls", "names"),
    [
        pytest.param(_FiltersOnAStr, ("thing",), id="filters-not-a-pattern"),
        pytest.param(
            _FiltersTwice, ("widgets.pattern", "widget_pattern"), id="filters-twice"
        ),
        pytest.param(_EnablesOnAStr, ("include_notebooks",), id="enables-not-a-bool"),
        pytest.param(_EnablesNested, ("block.include_jobs",), id="enables-nested"),
        pytest.param(_EnablesTwice, ("include_a", "include_b"), id="enables-twice"),
        pytest.param(_RulesNested, ("block.path_specs",), id="rules-nested"),
        pytest.param(_RulesTwice, ("path_specs", "table_rules"), id="rules-twice"),
        pytest.param(_RulesAndPattern, ("table_pattern",), id="rules-and-filters"),
        pytest.param(
            _RulesAndUnfiltered, ("probe_unfiltered_kinds",), id="rules-and-unfiltered"
        ),
        pytest.param(_RulesUnjudged, ("probe_verdict_override",), id="rules-unjudged"),
        pytest.param(_QualifierNested, ("block.database",), id="qualifier-nested"),
        pytest.param(
            _QualifierTwice, ("project_ids", "database"), id="qualifier-twice"
        ),
    ],
)
def test_each_misdeclaration_is_one_problem_naming_its_fields(
    config_cls: Type[ConfigModel], names: Tuple[str, ...]
) -> None:
    problems = marker_problems(config_cls)
    assert len(problems) == 1, problems
    assert all(name in problems[0] for name in names), problems


class _TwoProblems(ConfigModel):
    include_notebooks: Annotated[str, Enables("Notebook")] = "yes"
    block: _Database = Field(default_factory=_Database)


def test_a_reader_refuses_a_misdeclared_config_naming_every_problem() -> None:
    with pytest.raises(ProbeInternalError) as info:
        declares_qualifier(_TwoProblems)
    assert "include_notebooks" in str(info.value)
    assert "block.database" in str(info.value)


def test_probe_filter_refuses_a_switch_it_cannot_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An Enables on a str never compares equal to False, so the kind would
    read as switched on whatever the recipe says: refused, not answered."""

    class _StrSwitch(ConfigModel):
        include_views: Annotated[str, Enables("View")] = "no"
        view_pattern: Annotated[AllowDenyPattern, Filters("View")] = Field(
            default=AllowDenyPattern.allow_all()
        )

    monkeypatch.setattr(filter_check, "require_config_class", lambda _st: _StrSwitch)
    monkeypatch.setattr(filter_check, "list_probe_methods", lambda _st: [])
    with pytest.raises(ProbeInternalError, match="include_views"):
        check_filters(
            source_type="fake-source",
            config_dict={},
            kind="View",
            parent_path=[],
            names=["v1"],
        )


class _StringUnfiltered(ConfigModel):
    @classmethod
    def probe_unfiltered_kinds(cls) -> Set[str]:
        # A bare str: iterated, it would declare kinds "T", "a", "b", ...
        return "Table"  # type: ignore[return-value]


class _ListUnfiltered(ConfigModel):
    @classmethod
    def probe_unfiltered_kinds(cls) -> Set[str]:
        return ["Table"]  # type: ignore[return-value]


def test_unfiltered_kinds_must_be_a_collection_of_names() -> None:
    assert declared_unfiltered_kinds(_ListUnfiltered) == {"Table"}
    with pytest.raises(ProbeInternalError):
        declared_unfiltered_kinds(_StringUnfiltered)


class _RulesUnjudgedStringUnfiltered(_RulesUnjudged):
    @classmethod
    def probe_unfiltered_kinds(cls) -> Set[str]:
        return "Table"  # type: ignore[return-value]


def test_a_defective_unfiltered_hook_is_listed_with_the_other_problems() -> None:
    problems = marker_problems(_RulesUnjudgedStringUnfiltered)
    assert len(problems) == 2, problems
    assert any("probe_unfiltered_kinds" in p for p in problems), problems
    assert any("probe_verdict_override" in p for p in problems), problems
