"""The instructions an MCP server sends clients when they connect.

SMALL says what DataHub is and when to use it before other connectors. LARGE
adds which tool fits which job: clients that defer tool schemas (Claude Code's
tool search) show the model only tool names until it loads them, so these lines
are where it learns that. Each line names only tools the server serves, so a
server never points at a tool it lacks. NONE sends only the admin's custom text.

Clients cut instructions at about 2,048 characters, and custom instructions come
after this text, so keep it short.
"""

import dataclasses
import enum
from typing import Collection, Optional

INTRO = """\
DataHub is the organization's data catalog and context layer: which tables exist, \
what business terms mean, who owns what, and how data flows.

Check DataHub before you query a warehouse, BI, or dbt connector, and before you \
define a business term from general knowledge. DataHub knows which data to use and \
what it means."""


class InstructionSize(enum.Enum):
    NONE = "none"
    SMALL = "small"
    LARGE = "large"


@dataclasses.dataclass(frozen=True)
class _Step:
    tool: str
    note: str = ""


@dataclasses.dataclass(frozen=True)
class _Route:
    job: str
    steps: tuple[_Step, ...]
    # Steps run in order unless they are alternatives (any one of them does the job).
    alternatives: bool = False


# Ordered by how often users bring the job.
_ROUTES = (
    _Route(
        "Find a table, dashboard, or owner", (_Step("search"), _Step("get_entities"))
    ),
    _Route(
        "What a term or metric means",
        (_Step("search", " the glossary"), _Step("search_documents")),
    ),
    _Route("What depends on X, or where X comes from", (_Step("get_lineage"),)),
    _Route(
        "Change metadata",
        (
            _Step("update_description"),
            _Step("add_tags"),
            _Step("add_terms"),
            _Step("add_owners"),
        ),
        alternatives=True,
    ),
    _Route("Who you are acting as", (_Step("get_me"),)),
)

_LOAD_TOGETHER = (
    "If your client loads tool schemas on demand, load the tools for the job "
    "together in one step."
)
_EMPTY_SEARCH = (
    "An empty search does not prove the data is missing; try other terms or filters."
)


def _use_cases(served: Collection[str]) -> list[str]:
    lines = []
    for route in _ROUTES:
        steps = [f"`{s.tool}`{s.note}" for s in route.steps if s.tool in served]
        if not steps:
            continue
        if route.alternatives and len(steps) > 1:
            joined = f"{', '.join(steps[:-1])}, or {steps[-1]}"
        else:
            joined = ", then ".join(steps)
        lines.append(f"- {route.job}: {joined}.")
    return lines


def build_instructions(
    size: InstructionSize,
    served_tools: Collection[str],
    custom: Optional[str] = None,
) -> Optional[str]:
    """The instructions for a server serving ``served_tools``; None for none."""
    parts: list[str] = []
    if size is not InstructionSize.NONE:
        parts.append(INTRO)
    if size is InstructionSize.LARGE:
        use_cases = _use_cases(served_tools)
        if use_cases:
            parts.append("Use cases:\n" + "\n".join(use_cases) + "\n" + _LOAD_TOGETHER)
        if "search" in served_tools:
            parts.append(_EMPTY_SEARCH)
    if custom and custom.strip():
        parts.append(custom.strip())
    return "\n\n".join(parts) or None
