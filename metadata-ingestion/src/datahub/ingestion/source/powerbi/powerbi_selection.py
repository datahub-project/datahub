"""Which Power BI workspaces ingestion keeps, as a pure function of the recipe.

Shared by PowerBiDashboardSource.get_allowed_workspaces (ingestion) and
PowerBiDashboardSourceConfig.probe_verdict_override (`probe filter`). The
scan's later drop of non-Active workspaces is a separate stage with its own
missing-state semantics, so it stays where it is.
"""

from dataclasses import dataclass
from typing import Optional, Protocol, Sequence

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.verdicts import Verdict

WORKSPACE_NAME_PATTERN = "workspace_name_pattern"
WORKSPACE_ID_PATTERN = "workspace_id_pattern"
WORKSPACE_TYPE_FILTER = "workspace_type_filter"


class WorkspaceFilterConfig(Protocol):
    # Properties, not attributes: a Protocol attribute is invariant, so the
    # config's List[Literal[...]] would not satisfy Sequence[str].
    @property
    def workspace_name_pattern(self) -> AllowDenyPattern: ...

    @property
    def workspace_id_pattern(self) -> AllowDenyPattern: ...

    @property
    def workspace_type_filter(self) -> Sequence[str]: ...


@dataclass(frozen=True)
class WorkspaceFacts:
    name: str
    # None: not known to `probe filter` (a bare --name); that rule is skipped.
    workspace_id: Optional[str] = None
    workspace_type: Optional[str] = None


def workspace_verdict(config: WorkspaceFilterConfig, facts: WorkspaceFacts) -> Verdict:
    """Name, then id, then type. Ingestion reports a name or id miss in one
    list and a type miss in another, and a caller fixing the recipe is
    pointed at the name first, since it can see the name."""
    if not config.workspace_name_pattern.allowed(facts.name):
        return Verdict.exclude(WORKSPACE_NAME_PATTERN)
    if facts.workspace_id is not None and not config.workspace_id_pattern.allowed(
        facts.workspace_id
    ):
        return Verdict.exclude(WORKSPACE_ID_PATTERN)
    if (
        facts.workspace_type is not None
        and facts.workspace_type not in config.workspace_type_filter
    ):
        return Verdict.exclude(WORKSPACE_TYPE_FILTER)
    return Verdict.include()
