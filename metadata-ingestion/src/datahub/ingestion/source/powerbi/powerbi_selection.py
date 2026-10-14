"""Which Power BI workspaces ingestion keeps, as a pure function of the recipe.

Shared by PowerBiDashboardSource.get_allowed_workspaces (ingestion) and
PowerBiDashboardSourceConfig.probe_verdict_override (`probe filter`, through
probe_workspace_verdict). The scan's later drop of non-Active workspaces is a
separate stage with its own missing-state semantics, so it stays where it is;
the probe judges it from a recorded state.
"""

from dataclasses import dataclass
from enum import Enum
from typing import AbstractSet, Literal, Mapping, Optional, Protocol, Sequence, Union

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.verdicts import Verdict, VerdictContext

WORKSPACE_NAME_PATTERN = "workspace_name_pattern"
WORKSPACE_ID_PATTERN = "workspace_id_pattern"
WORKSPACE_TYPE_FILTER = "workspace_type_filter"
# Not a recipe field: the scan drops every workspace whose state is not Active
# (PowerBiAPI.fill_metadata_from_scan_result), whatever the recipe says.
WORKSPACE_STATE = "workspace_state"
ACTIVE_WORKSPACE_STATE = "Active"

# The facts the probe's `reports` and `dashboards` stamp on each record, so a
# saved run lets `probe filter` judge the workspace rules a bare --parent
# cannot. The id, type and state, never the name: a personal workspace is
# named after its owner.
WORKSPACE_ID_FACT = "workspace_id"
WORKSPACE_TYPE_FACT = "workspace_type"
WORKSPACE_STATE_FACT = "workspace_state"


class WorkspaceFilterConfig(Protocol):
    # Properties, not attributes: a Protocol attribute is invariant, so the
    # config's List[Literal[...]] would not satisfy Sequence[str].
    @property
    def workspace_name_pattern(self) -> AllowDenyPattern: ...

    @property
    def workspace_id_pattern(self) -> AllowDenyPattern: ...

    @property
    def workspace_type_filter(self) -> Sequence[str]: ...


class _Unknown(Enum):
    UNKNOWN = "unknown"


# A fact `probe filter` was not given (a bare --name): that rule is skipped.
# Not None, because None is a value ingestion can read: a workspace whose
# type is null fails workspace_type_filter, as it always has.
UNKNOWN: Literal[_Unknown.UNKNOWN] = _Unknown.UNKNOWN


@dataclass(frozen=True)
class WorkspaceFacts:
    name: str
    workspace_id: Union[str, _Unknown] = UNKNOWN
    workspace_type: Union[Optional[str], _Unknown] = UNKNOWN


def workspace_verdict(config: WorkspaceFilterConfig, facts: WorkspaceFacts) -> Verdict:
    """Name, then id, then type. Ingestion reports a name or id miss in one
    list and a type miss in another, and a caller fixing the recipe is
    pointed at the name first, since it can see the name."""
    if not config.workspace_name_pattern.allowed(facts.name):
        return Verdict.exclude(WORKSPACE_NAME_PATTERN)
    if facts.workspace_id is not UNKNOWN and not config.workspace_id_pattern.allowed(
        facts.workspace_id
    ):
        return Verdict.exclude(WORKSPACE_ID_PATTERN)
    if (
        facts.workspace_type is not UNKNOWN
        and facts.workspace_type not in config.workspace_type_filter
    ):
        return Verdict.exclude(WORKSPACE_TYPE_FILTER)
    return Verdict.include()


def _recorded_facts(
    name: str,
    attributes: Mapping[str, str],
    id_key: str,
    type_key: str,
) -> WorkspaceFacts:
    # A saved run drops a null field, so a missing type next to a present id
    # is a null type, which ingestion excludes; with no id either, the record
    # is a bare name and both rules go unjudged.
    workspace_id = attributes.get(id_key, UNKNOWN)
    if workspace_id is UNKNOWN:
        return WorkspaceFacts(name)
    return WorkspaceFacts(name, workspace_id, attributes.get(type_key))


def _not_active(state: Optional[str]) -> bool:
    # Only the admin API reports a state, so its absence says nothing.
    return state is not None and state != ACTIVE_WORKSPACE_STATE


def _warn_unrecorded(ctx: VerdictContext, name: str, fact: str, rule: str) -> None:
    # repr, as provider_helpers.echoed quotes a name; that module is not
    # imported here because ingestion loads this one.
    ctx.warn(
        f"no workspace {fact} for {name!r} here: {rule} is judged only from a "
        "record that carries it, which a saved `probe run workspaces`, "
        "`reports` or `dashboards` run does; pass one with `probe filter "
        "--from-run`"
    )


def _child_verdict(
    config: WorkspaceFilterConfig, ctx: VerdictContext
) -> Optional[Verdict]:
    """A report or dashboard inherits its workspace's verdict. The framework
    judges the --parent workspace on its name; a saved `reports`/`dashboards`
    run also stamps the workspace's id, type and state on each record, so the
    other rules apply here."""
    if not ctx.parent_path:
        # No pattern names a report or dashboard, so without its workspace
        # the include the framework returns decides nothing.
        ctx.warn(
            f"no --parent workspace given, so this {ctx.kind} was judged "
            f"without its workspace: ingestion keeps it exactly when its "
            f"workspace is kept. Pass --parent <workspace>, or judge a "
            f"`probe run` listing of them with --from-run"
        )
        return None
    facts = _recorded_facts(
        ctx.parent_path[-1], ctx.attributes, WORKSPACE_ID_FACT, WORKSPACE_TYPE_FACT
    )
    verdict = workspace_verdict(config, facts)
    if not verdict.included:
        return verdict
    if _not_active(ctx.attributes.get(WORKSPACE_STATE_FACT)):
        return Verdict.exclude(WORKSPACE_STATE)
    return None


def probe_workspace_verdict(
    config: WorkspaceFilterConfig,
    ctx: VerdictContext,
    *,
    workspace_kind: str,
    child_kinds: AbstractSet[str],
) -> Optional[Verdict]:
    """`probe filter`'s verdict for a workspace, or for a report or dashboard
    in one. get_allowed_workspaces requires workspace_id_pattern on the id and
    `type in workspace_type_filter`, and the scan drops a workspace whose state
    is not Active. None is knowable from a name, so they are judged from the
    attributes the probe's listings record; a missing id or type is warned
    about rather than passed silently."""
    if ctx.structural is not None:
        return None
    if ctx.kind in child_kinds:
        return _child_verdict(config, ctx)
    if ctx.kind != workspace_kind:
        return None
    facts = _recorded_facts(ctx.target, ctx.attributes, "id", "type")
    verdict = workspace_verdict(config, facts)
    if verdict.excluded_by == WORKSPACE_NAME_PATTERN:
        # Ingestion drops it on the name; that stays the reported reason.
        return verdict
    # Worded to hold also when this is the --parent of a report or dashboard:
    # the framework judges that parent by name alone, and these notes reach
    # the child, whose own stamped facts the rule did judge.
    if facts.workspace_id is UNKNOWN:
        _warn_unrecorded(ctx, ctx.name, "id", WORKSPACE_ID_PATTERN)
        _warn_unrecorded(ctx, ctx.name, "type", WORKSPACE_TYPE_FILTER)
    if not verdict.included:
        return verdict
    if _not_active(ctx.attributes.get("state")):
        return Verdict.exclude(WORKSPACE_STATE)
    return Verdict.include()
