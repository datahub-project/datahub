"""Which Power BI workspaces ingestion keeps, as a pure function of the recipe.

Shared by PowerBiDashboardSource.get_allowed_workspaces (ingestion) and
PowerBiDashboardSourceConfig.probe_verdict_override (`probe filter`). The
scan's later drop of non-Active workspaces is a separate stage with its own
missing-state semantics, so it stays where it is.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Literal, Optional, Protocol, Sequence, Union

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
