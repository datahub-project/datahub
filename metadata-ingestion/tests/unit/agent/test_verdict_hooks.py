"""Framework behaviour of the shared verdict hooks, on fake configs.

Each fake is registered through the same two seams test_filter_check's
fixtures use, so nothing here depends on a real connector -- with one
exception: the SQL default-switch test runs the real registered MySQL source
on purpose, to pin the switch it inherits from SQLCommonConfig's fields.
"""

from typing import (
    Annotated,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Type,
)

import pytest
from pydantic import Field

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    Enables,
    Filters,
    FiltersByRule,
)
from datahub.ingestion.agent import filter_check
from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters
from datahub.ingestion.agent.verdicts import (
    ProbeInternalError,
    Verdict,
    VerdictContext,
    pattern_verdict,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sql.sql_config import sql_structural_verdict


def _register(monkeypatch: pytest.MonkeyPatch, config_cls: Type[ConfigModel]) -> None:
    monkeypatch.setattr(filter_check, "require_config_class", lambda _st: config_cls)
    monkeypatch.setattr(filter_check, "list_probe_methods", lambda _st: [])


def _judge(
    kind: str,
    names: List[str],
    config_dict: Optional[Dict[str, object]] = None,
    parent_path: Sequence[str] = (),
    try_allow: Optional[Sequence[str]] = None,
    try_deny: Optional[Sequence[str]] = None,
    attributes: Optional[Sequence[Mapping[str, str]]] = None,
) -> FilterCheckResult:
    return check_filters(
        source_type="fake",
        config_dict=config_dict or {},
        kind=kind,
        parent_path=parent_path,
        names=names,
        try_allow=try_allow,
        try_deny=try_deny,
        attributes=attributes,
    )


class _Switched(ConfigModel):
    extract_lakehouses: Annotated[
        bool, Enables(DatasetContainerSubTypes.FABRIC_LAKEHOUSE)
    ] = True
    lakehouse_pattern: Annotated[
        AllowDenyPattern, Filters(DatasetContainerSubTypes.FABRIC_LAKEHOUSE)
    ] = Field(default=AllowDenyPattern.allow_all())


def test_a_declared_switch_that_is_off_excludes_the_kind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _Switched)
    result = _judge(
        str(DatasetContainerSubTypes.FABRIC_LAKEHOUSE),
        ["lh"],
        config_dict={"extract_lakehouses": False},
    )
    assert [(r.included, r.excluded_by) for r in result.results] == [
        (False, "extract_lakehouses")
    ]


def test_a_declared_switch_that_is_on_leaves_the_pattern_in_charge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _Switched)
    result = _judge(
        str(DatasetContainerSubTypes.FABRIC_LAKEHOUSE),
        ["lh", "other"],
        config_dict={"lakehouse_pattern": {"allow": ["^lh$"]}},
    )
    assert [r.included for r in result.results] == [True, False]


def test_an_optional_switch_left_unset_leaves_the_kind_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _OptionalSwitch(ConfigModel):
        include_things: Annotated[Optional[bool], Enables("Thing")] = None

    _register(monkeypatch, _OptionalSwitch)
    assert _judge("Thing", ["t"]).results[0].included is True
    off = _judge("Thing", ["t"], config_dict={"include_things": False})
    assert off.results[0].excluded_by == "include_things"


def test_a_kind_enabled_by_two_fields_is_a_connector_defect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Twice(ConfigModel):
        include_things: Annotated[bool, Enables("Thing")] = True
        extract_things: Annotated[bool, Enables("Thing")] = True

    _register(monkeypatch, _Twice)
    with pytest.raises(ProbeInternalError):
        _judge("Thing", ["t"])


def test_the_sql_default_switches_still_apply_without_a_declaration() -> None:
    # MySQL declares no switch of its own; the inherited include_views must
    # keep working.
    result = check_filters(
        source_type="mysql",
        config_dict={
            "host_port": "localhost:3306",
            "username": "u",
            "password": "p",
            "include_views": False,
        },
        kind=str(DatasetSubTypes.VIEW),
        parent_path=["db"],
        names=["v"],
    )
    assert result.results[0].excluded_by == "include_views"


class _Overriding(ConfigModel):
    """Things live in Boxes. The override drops anything named `drop*`
    unless it sits in box `a`, and re-checks table_pattern the way Unity
    and Redshift do for views."""

    box_pattern: Annotated[AllowDenyPattern, Filters("Box")] = Field(
        default=AllowDenyPattern.allow_all()
    )
    thing_pattern: Annotated[AllowDenyPattern, Filters("Thing")] = Field(
        default=AllowDenyPattern.allow_all()
    )
    table_pattern: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())

    @classmethod
    def probe_ancestor_kinds(cls, kind: str) -> Optional[Sequence[str]]:
        return {"Box": (), "Thing": ("Box",)}.get(kind)

    def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
        if ctx.kind != "Thing":
            return None
        if not self.table_pattern.allowed(ctx.target):
            return Verdict(False, "table_pattern")
        if ctx.name.startswith("drop"):
            ctx.warn("judged by the override")
            keep = ctx.parent_path == ("a",)
            return Verdict(keep, None if keep else "thing_pattern")
        if ctx.name == "renamed":
            return Verdict(True, None, matched_target="a.renamed")
        # Delegate to the pattern the framework would read, so --try-allow
        # reaches it (Review Focus 1).
        if ctx.name.startswith("pat"):
            assert ctx.pattern_field is not None
            return pattern_verdict(self, ctx.pattern_field, ctx.target)
        return None


class _StringAncestors(_Overriding):
    @classmethod
    def probe_ancestor_kinds(cls, kind: str) -> Optional[Sequence[str]]:
        # A bare str is a sequence too, of one-letter kinds.
        return "Box" if kind == "Thing" else ()  # type: ignore[return-value]


def test_ancestor_kinds_given_as_a_bare_string_are_a_connector_defect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _StringAncestors)
    with pytest.raises(ProbeInternalError):
        _judge("Thing", ["t"], parent_path=["a"])


def test_an_override_decides_with_the_parent_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _Overriding)
    kept = _judge("Thing", ["drop_x"], parent_path=["a"])
    dropped = _judge("Thing", ["drop_x"], parent_path=["b"])
    assert (kept.results[0].included, dropped.results[0].included) == (True, False)
    assert dropped.results[0].excluded_by == "thing_pattern"
    assert "judged by the override" in dropped.warnings


def test_an_override_returning_none_leaves_the_pattern_in_charge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _Overriding)
    result = _judge(
        "Thing",
        ["plain", "denied"],
        config_dict={"thing_pattern": {"deny": ["^denied$"]}},
    )
    assert [(r.included, r.excluded_by) for r in result.results] == [
        (True, None),
        (False, "thing_pattern"),
    ]


def test_an_override_can_apply_a_second_pattern(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _Overriding)
    result = _judge(
        "Thing", ["plain"], config_dict={"table_pattern": {"deny": ["^plain$"]}}
    )
    assert result.results[0].excluded_by == "table_pattern"


def test_an_overrides_matched_target_is_the_reported_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _Overriding)
    assert _judge("Thing", ["renamed"]).results[0].target == "a.renamed"


def test_an_override_sees_the_try_allow_pattern(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _Overriding)
    result = _judge("Thing", ["pat_a", "pat_b"], try_allow=["^pat_a$"])
    assert [r.included for r in result.results] == [True, False]


def test_the_parent_walk_still_applies_after_an_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _Overriding)
    result = _judge(
        "Thing",
        ["drop_x"],
        parent_path=["a"],
        config_dict={"box_pattern": {"deny": ["^a$"]}},
    )
    assert (result.results[0].included, result.results[0].excluded_by) == (
        False,
        "box_pattern",
    )


class _Pinned(ConfigModel):
    """MSSQL's case: a pinned database is read whatever database_pattern and
    the system-database list say."""

    database: Optional[str] = None
    database_pattern: Annotated[
        AllowDenyPattern, Filters(DatasetContainerSubTypes.DATABASE)
    ] = Field(default=AllowDenyPattern.allow_all())

    @classmethod
    def default_databases(cls) -> Set[str]:
        return {"master"}

    def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
        if ctx.kind == DatasetContainerSubTypes.DATABASE and self.database:
            if ctx.name.lower() == self.database.lower():
                return Verdict(True)
            return Verdict(False, "database")
        return sql_structural_verdict(self, ctx)


def test_an_override_can_overrule_the_structural_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _Pinned)
    pinned = _judge(
        str(DatasetContainerSubTypes.DATABASE),
        ["master"],
        config_dict={"database": "master"},
    )
    unpinned = _judge(str(DatasetContainerSubTypes.DATABASE), ["master"])
    assert pinned.results[0].included is True
    assert (unpinned.results[0].included, unpinned.results[0].excluded_by) == (
        False,
        "default_database",
    )


def test_the_override_is_told_the_structural_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: List[Optional[Verdict]] = []

    class _Recording(_Switched):
        def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
            seen.append(ctx.structural)
            return None

    _register(monkeypatch, _Recording)
    _judge(
        str(DatasetContainerSubTypes.FABRIC_LAKEHOUSE),
        ["lh"],
        config_dict={"extract_lakehouses": False},
    )
    assert seen == [Verdict(False, "extract_lakehouses")]


def test_an_override_returning_a_non_verdict_is_a_connector_defect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Wrong(ConfigModel):
        def probe_verdict_override(self, ctx: VerdictContext) -> object:
            return False

    _register(monkeypatch, _Wrong)
    with pytest.raises(ProbeInternalError):
        _judge("Thing", ["x"])


class _ById(ConfigModel):
    """PowerBI's shape: a workspace must pass the name pattern AND the id
    pattern, and the id only arrives as an attribute."""

    workspace_pattern: Annotated[AllowDenyPattern, Filters("Workspace")] = Field(
        default=AllowDenyPattern.allow_all()
    )
    workspace_id_pattern: AllowDenyPattern = Field(default=AllowDenyPattern.allow_all())

    def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
        workspace_id = ctx.attributes.get("id")
        if workspace_id is None:
            ctx.warn("no workspace id given, so workspace_id_pattern was not applied")
            return None
        if not self.workspace_id_pattern.allowed(workspace_id):
            return Verdict(False, "workspace_id_pattern")
        return None


def test_an_attribute_reaches_the_override(monkeypatch: pytest.MonkeyPatch) -> None:
    _register(monkeypatch, _ById)
    result = _judge(
        "Workspace",
        ["Sales", "Ops"],
        attributes=[{"id": "ws-1"}, {"id": "ws-2"}],
        config_dict={"workspace_id_pattern": {"deny": ["^ws-2$"]}},
    )
    assert [(r.included, r.excluded_by) for r in result.results] == [
        (True, None),
        (False, "workspace_id_pattern"),
    ]


def test_without_attributes_the_override_degrades_with_a_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _ById)
    result = _judge("Workspace", ["Sales"])
    assert result.results[0].included is True
    assert any("workspace_id_pattern" in w for w in result.warnings)


def test_attributes_must_align_with_names(monkeypatch: pytest.MonkeyPatch) -> None:
    _register(monkeypatch, _ById)
    with pytest.raises(ValueError):
        _judge("Workspace", ["Sales", "Ops"], attributes=[{"id": "ws-1"}])


class _Rules(ConfigModel):
    """GCS's shape: path_specs decide Tables, and they are not a pattern."""

    path_specs: Annotated[List[str], FiltersByRule("Table")] = Field(
        default_factory=lambda: ["gs://b/data/*"]
    )

    @classmethod
    def probe_ancestor_kinds(cls, kind: str) -> Optional[Sequence[str]]:
        return () if kind == "Table" else None

    def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
        if ctx.kind != "Table":
            return None
        if ctx.name.startswith("gs://b/data/"):
            return Verdict.include()
        return Verdict(False, "path_specs[0].include")


def test_a_rule_kind_is_judged_by_the_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _Rules)
    result = _judge("Table", ["gs://b/data/t1", "gs://b/other/t2"])
    assert result.filtering == "by_rule"
    assert result.pattern_field == "path_specs"
    assert [(r.included, r.excluded_by) for r in result.results] == [
        (True, None),
        (False, "path_specs[0].include"),
    ]
    assert result.results[1].target == "gs://b/other/t2"


def test_try_patterns_on_a_rule_kind_warn_and_are_ignored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(monkeypatch, _Rules)
    result = _judge("Table", ["gs://b/data/t1"], try_deny=[".*"])
    assert result.results[0].included is True
    assert result.tried is None
    assert any("--try-allow" in w for w in result.warnings)


def test_a_rule_kind_without_a_verdict_is_a_connector_defect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Silent(_Rules):
        def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
            return None

    _register(monkeypatch, _Silent)
    with pytest.raises(ProbeInternalError):
        _judge("Table", ["gs://b/data/t1"])


# Built inside the hook: Verdict refuses an exclusion without a reason as it
# is constructed, which an override does while it runs.
@pytest.mark.parametrize(
    "inconsistent",
    [lambda: Verdict(True, "x"), lambda: Verdict(False, None)],
    ids=["included-with-a-reason", "excluded-without-one"],
)
def test_an_inconsistent_override_verdict_is_a_connector_defect(
    monkeypatch: pytest.MonkeyPatch, inconsistent: Callable[[], Verdict]
) -> None:
    class _Contradicts(_Pinned):
        def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
            return inconsistent()

    _register(monkeypatch, _Contradicts)
    with pytest.raises(ProbeInternalError):
        _judge(str(DatasetContainerSubTypes.DATABASE), ["db"])


def test_exclude_names_its_reason_and_keeps_the_matched_target() -> None:
    verdict = Verdict.exclude("table_pattern", matched_target="db.orders")
    assert (verdict.included, verdict.excluded_by, verdict.matched_target) == (
        False,
        "table_pattern",
        "db.orders",
    )


@pytest.mark.parametrize("reason", ["", "   "])
def test_exclude_refuses_an_exclusion_without_a_reason(reason: str) -> None:
    with pytest.raises(ValueError):
        Verdict.exclude(reason)


@pytest.mark.parametrize("reason", [None, "", "   "])
def test_a_verdict_built_directly_refuses_an_exclusion_without_a_reason(
    reason: Optional[str],
) -> None:
    with pytest.raises(ValueError):
        Verdict(False, reason)


def test_a_verdict_built_directly_refuses_an_inclusion_with_a_reason() -> None:
    with pytest.raises(ValueError):
        Verdict(True, "table_pattern")
