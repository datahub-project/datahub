"""The framework's side of `probe_match_target`, on fake configs.

The framework asks the hook and uses a non-empty string it returns, and the
bare name otherwise. Everything else about the target belongs to the hook.
"""

from typing import Annotated, List, Optional, Sequence, Tuple, Type

import pytest
from pydantic import Field

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    Filters,
    FiltersByRule,
)
from datahub.ingestion.agent import filter_check
from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters
from datahub.ingestion.agent.verdicts import (
    ClassifyContext,
    Verdict,
    VerdictContext,
    parent_required,
)

_NO_PARENT = (
    "no parent given, so these were judged on their bare names; this "
    "source filters on a qualified identifier, so pass the containing "
    "schema/database to get the verdict ingestion actually makes"
)


def _register(monkeypatch: pytest.MonkeyPatch, config_cls: Type[ConfigModel]) -> None:
    monkeypatch.setattr(filter_check, "require_config_class", lambda _st: config_cls)
    monkeypatch.setattr(filter_check, "list_probe_methods", lambda _st: [])


def _judge(
    kind: str, names: List[str], parent_path: Optional[List[str]] = None
) -> FilterCheckResult:
    return check_filters(
        source_type="fake",
        config_dict={},
        kind=kind,
        parent_path=parent_path or [],
        names=names,
    )


class _Namespaced(ConfigModel):
    """A source whose topic_pattern is written for `<namespace>.<topic>`."""

    topic_pattern: Annotated[AllowDenyPattern, Filters("Topic")] = Field(
        default=AllowDenyPattern(allow=[r"^ns\.keep$"])
    )

    def probe_ancestor_kinds(self, kind: str) -> Optional[Sequence[str]]:
        # Topics sit in a namespace that has no pattern of its own, so a
        # --parent naming one judges nothing.
        return ("Namespace",) if kind == "Topic" else None


def test_the_hook_is_told_the_kind_and_its_target_is_matched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: List[str] = []

    class _Recording(_Namespaced):
        def probe_match_target(self, ctx: ClassifyContext) -> Optional[str]:
            seen.append(ctx.kind)
            return f"ns.{ctx.name}"

    _register(monkeypatch, _Recording)
    result = _judge("Topic", ["keep", "drop"])

    assert [(r.target, r.included, r.excluded_by) for r in result.results] == [
        ("ns.keep", True, None),
        ("ns.drop", False, "topic_pattern"),
    ]
    assert seen == ["Topic", "Topic"]
    # The framework adds no warning of its own: whether a parent is needed is
    # the hook's question, and this one needs none.
    assert result.warnings == []


@pytest.mark.parametrize("answer", [None, ""])
def test_a_hook_with_no_answer_leaves_the_bare_name(
    monkeypatch: pytest.MonkeyPatch, answer: Optional[str]
) -> None:
    class _Silent(_Namespaced):
        def probe_match_target(self, ctx: ClassifyContext) -> Optional[str]:
            return answer

    _register(monkeypatch, _Silent)
    for parent_path in ([], ["ns"]):
        result = _judge("Topic", ["keep"], parent_path=parent_path)
        assert [r.target for r in result.results] == ["keep"]
        assert result.warnings == []


def test_a_config_without_the_hook_is_judged_on_the_bare_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _Namespaced)
    result = _judge("Topic", ["keep"], parent_path=["ns"])
    assert [(r.target, r.included) for r in result.results] == [("keep", False)]
    assert result.warnings == []


def test_a_rule_filtered_kind_is_matched_on_its_name_without_asking_the_hook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asked: List[str] = []

    class _Rules(ConfigModel):
        path_specs: Annotated[List[str], FiltersByRule("Folder")] = Field(
            default_factory=list
        )

        def probe_match_target(self, ctx: ClassifyContext) -> Optional[str]:
            asked.append(ctx.name)
            return f"bucket/{ctx.name}"

        def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
            return Verdict.include()

    _register(monkeypatch, _Rules)
    result = _judge("Folder", ["raw"])
    assert [r.target for r in result.results] == ["raw"]
    assert asked == []


def _ctx(parent_path: Tuple[str, ...], messages: List[str]) -> ClassifyContext:
    return ClassifyContext(
        config=None,
        name="orders",
        fqn="orders",
        pattern_field="table_pattern",
        parent_path=parent_path,
        warn=messages.append,
        kind="Table",
    )


def test_parent_required_warns_and_answers_true_without_a_parent() -> None:
    messages: List[str] = []
    assert parent_required(_ctx((), messages)) is True
    assert messages == [_NO_PARENT]


def test_parent_required_is_silent_when_a_parent_is_given() -> None:
    messages: List[str] = []
    assert parent_required(_ctx(("db", "public"), messages)) is False
    assert messages == []
