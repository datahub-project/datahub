"""Capability groups for MCP tool selection.

A group is a coarse, stable bundle of tools (e.g. ``read_metadata``) that an admin
selects instead of listing tools one by one. Membership is computed live from
each tool's tags, so a newly registered tool joins its group everywhere that
group is selected, without the stored selection changing.

These are the groups every DataHub deployment shares. DataHub Cloud defines more
(proposals, evals, ...) and passes its own tuple to ``resolve_tool_refs``.

On DataHub Cloud, an opt-in group (``ingestion``) is never part of "every
tool": an absent or empty selection leaves it out, and only a selection that
names the group or one of its tools serves them. DataHub Core has no opt-in
groups: "every tool" is every group. Groups stay disjoint, so the ingestion
group's write tools carry only the group's tag and say they write through
``readOnlyHint``.
"""

import dataclasses
from typing import Iterable, Optional, Protocol, Sequence

from .mcp_server import ToolType

TOOL_REF_TYPE_TOOL = "TOOL"
TOOL_REF_TYPE_GROUP = "GROUP"

# Always served regardless of tool selection, and never offered for selection.
SYSTEM_TOOL_IDS: frozenset[str] = frozenset({"get_me"})


@dataclasses.dataclass(frozen=True)
class ToolGroup:
    id: str
    label: str
    description: str
    tags: frozenset[str]
    """A tool belongs to the group when its tags intersect these."""

    cloud_opt_in: bool = False
    """On DataHub Cloud, served only when a selection names the group or one of
    its tools. DataHub Core serves it with every tool, like any other group."""


READ_METADATA = ToolGroup(
    id="read_metadata",
    label="Read metadata",
    description="Search, lookup, and other read-only tools.",
    # Keyed on tags, not on the readOnlyHint annotation: USER-tagged tools (get_me)
    # are system tools, outside every group.
    tags=frozenset({ToolType.SEARCH.value, ToolType.READ_ONLY.value}),
)
WRITE_METADATA = ToolGroup(
    id="write_metadata",
    label="Write metadata",
    description="Tools that change metadata (tags, terms, owners, docs, ...).",
    tags=frozenset({ToolType.MUTATION.value}),
)
INGESTION = ToolGroup(
    id="ingestion",
    label="Ingestion",
    description=(
        "Write, check, run and debug ingestion recipes and sources on an executor."
    ),
    tags=frozenset({ToolType.INGESTION.value}),
    # Cloud attaches these to scoped servers only, and this package's default
    # server is Cloud's too, so it must not gain them there. Core serves them
    # by default; an admin deselects the group to turn them off.
    cloud_opt_in=True,
)
TOOL_GROUPS: tuple[ToolGroup, ...] = (READ_METADATA, WRITE_METADATA, INGESTION)


@dataclasses.dataclass(frozen=True)
class ToolRef:
    ref_type: str
    """TOOL_REF_TYPE_TOOL or TOOL_REF_TYPE_GROUP; other types are ignored here."""

    tool_id: str
    """A tool name (TOOL) or a group id (GROUP)."""

    enabled: bool = True


class ToolLike(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def tags(self) -> Optional[Iterable[str]]: ...


def groups_for_tags(
    tags: Optional[Iterable[str]], groups: Sequence[ToolGroup] = TOOL_GROUPS
) -> list[str]:
    tag_set = set(tags or ())
    return [group.id for group in groups if group.tags & tag_set]


def opt_in_group_ids(
    *, is_oss: bool, groups: Sequence[ToolGroup] = TOOL_GROUPS
) -> list[str]:
    """The groups "every tool" leaves out: none on DataHub Core."""
    if is_oss:
        return []
    return [group.id for group in groups if group.cloud_opt_in]


def default_group_ids(
    *, is_oss: bool, groups: Sequence[ToolGroup] = TOOL_GROUPS
) -> list[str]:
    """The groups "every tool" means: all but the opt-in ones."""
    opt_in_ids = set(opt_in_group_ids(is_oss=is_oss, groups=groups))
    return [group.id for group in groups if group.id not in opt_in_ids]


def is_opt_in(
    tags: Optional[Iterable[str]],
    *,
    is_oss: bool,
    groups: Sequence[ToolGroup] = TOOL_GROUPS,
) -> bool:
    """Whether a tool with these tags is in an opt-in group."""
    opt_in_ids = set(opt_in_group_ids(is_oss=is_oss, groups=groups))
    return bool(opt_in_ids.intersection(groups_for_tags(tags, groups)))


def resolve_tool_refs(
    refs: Optional[Sequence[ToolRef]],
    available: Iterable[ToolLike],
    groups: Sequence[ToolGroup] = TOOL_GROUPS,
) -> Optional[frozenset[str]]:
    """Turn a selection into the set of allowed tool names.

    No selection means every tool outside the deployment's opt-in groups,
    returned as None (``is_tool_selected`` applies that rule). GROUP refs
    expand against ``available`` now, so new group members are included. TOOL
    refs apply last, so naming a tool overrides its group. System tools are not added here: the
    caller serves them regardless.
    """
    if not refs:
        return None

    tool_groups = {
        tool.name: set(groups_for_tags(tool.tags, groups)) for tool in available
    }
    allowed: set[str] = set()
    # Groups are disjoint, so their order doesn't matter. Should they ever
    # overlap, a later ref wins for the shared tools.
    for ref in refs:
        if ref.ref_type == TOOL_REF_TYPE_GROUP:
            members = {name for name, ids in tool_groups.items() if ref.tool_id in ids}
            allowed = allowed | members if ref.enabled else allowed - members
    for ref in refs:
        if ref.ref_type == TOOL_REF_TYPE_TOOL:
            if ref.enabled:
                allowed.add(ref.tool_id)
            else:
                allowed.discard(ref.tool_id)
    return frozenset(allowed)


def is_tool_selected(
    tool: ToolLike,
    refs: Optional[Sequence[ToolRef]],
    groups: Sequence[ToolGroup] = TOOL_GROUPS,
    *,
    is_oss: bool,
) -> bool:
    """Whether a selection serves ``tool``.

    System tools are always served. With no selection, every tool is, except
    on DataHub Cloud those in an opt-in group.
    """
    if tool.name in SYSTEM_TOOL_IDS:
        return True
    # A tool's groups come from its own tags, so resolving against it alone
    # gives the same answer as resolving against every tool.
    allowed = resolve_tool_refs(refs, [tool], groups)
    if allowed is None:
        return not is_opt_in(tool.tags, is_oss=is_oss, groups=groups)
    return tool.name in allowed
