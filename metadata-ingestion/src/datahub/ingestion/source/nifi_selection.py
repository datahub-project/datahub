"""Which NiFi process groups ingestion walks, as pure functions of the recipe.

Shared by NifiSource (ingestion) and NifiSourceConfig's probe hooks (`probe
filter`, which has no connection), so neither restates the rule.

NiFi ingestion walks the flow from the root group down and stops at a group
whose name process_group_pattern refuses: nothing inside it is read. So a
group is walked when its own name and the name of every group above it, the
root included, are allowed.
"""

import json
from typing import List, Optional, Protocol, Sequence

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.verdicts import Verdict

PROCESS_GROUP_PATTERN = "process_group_pattern"

# The listing attribute carrying a group's ancestors, root first, as a JSON
# array of names. JSON because a name may contain any separator, and a listing
# attribute must be a single string to reach `probe filter --from-run`.
ANCESTORS_ATTRIBUTE = "ancestors"


class ProcessGroupFilterConfig(Protocol):
    # A Protocol so this module never imports nifi.py, which imports it.
    @property
    def process_group_pattern(self) -> AllowDenyPattern: ...


def excluding_ancestor(
    config: ProcessGroupFilterConfig, ancestors: Sequence[str]
) -> Optional[str]:
    """The outermost ancestor process_group_pattern refuses, if any."""
    for ancestor in ancestors:
        if not config.process_group_pattern.allowed(ancestor):
            return ancestor
    return None


def process_group_verdict(
    config: ProcessGroupFilterConfig, name: str, ancestors: Sequence[str] = ()
) -> Verdict:
    """Whether ingestion walks the group `name` under `ancestors` (root first).

    Ingestion passes no ancestors: its walk never reaches a group under an
    excluded one, so by the time it asks, every ancestor was allowed."""
    if excluding_ancestor(config, ancestors) is not None:
        return Verdict.exclude(PROCESS_GROUP_PATTERN)
    if not config.process_group_pattern.allowed(name):
        return Verdict.exclude(PROCESS_GROUP_PATTERN)
    return Verdict.include()


def encode_ancestors(names: Sequence[str]) -> str:
    return json.dumps(list(names))


def decode_ancestors(text: str) -> Optional[List[str]]:
    """The names encode_ancestors wrote, or None for anything else."""
    try:
        value = json.loads(text)
    except ValueError:
        return None
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        return None
    return value
