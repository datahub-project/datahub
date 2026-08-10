from dataclasses import dataclass
from typing import Any, Callable, Optional, Tuple

from typing_extensions import TypeGuard


@dataclass(frozen=True)
class Verdict:
    """A connector's verdict for one node: would it be ingested given the
    recipe's filters plus the source's built-in exclusions?

    excluded_by names the reason a node would be dropped (a *_pattern field,
    "default_schema", "system_object"), or None when included. The filtering
    logic itself lives in the connector (reusing its own ingestion filters);
    the framework only carries it.
    """

    included: bool
    excluded_by: Optional[str] = None

    @classmethod
    def include(cls) -> "Verdict":
        return cls(True, None)


_INCLUDED = Verdict.include()

# A level the source offers no filter for (e.g. Mode's datasets and queries).
# Distinct from pattern_field=None, which means "resolve the conventional
# <kind>_pattern field". Nodes at an UNFILTERED level report pattern_field=None
# and are always included.
UNFILTERED: str = "__unfiltered__"

# Stand-in for a node whose lister produced no usable name. Listers are declared
# to return Sequence[str], but real APIs break that contract — Mode hands back
# reports with name=null. Such a node can be neither filtered
# (AllowDenyPattern.allowed raises TypeError on a non-string) nor addressed as a
# --parent, but it does exist, and a probe is a diagnostic: "two unnamed reports
# live here" is more useful to a caller than a dropped row or a stack trace.
UNNAMED: str = "<unnamed>"

# Reported like the connectors' own structural exclusions ("system_object",
# "default_schema"): a statement about what the probe can address, not a
# prediction that ingestion would skip the object.
_UNNAMED_VERDICT = Verdict(False, "unnamed")


def _usable_name(name: object) -> TypeGuard[str]:
    """Whether a lister gave us a name we can filter on and descend into."""
    return isinstance(name, str) and bool(name.strip())


def _join_fqn(prefix: Optional[str], name: str) -> str:
    return f"{prefix}.{name}" if prefix else name


@dataclass(frozen=True)
class ClassifyContext:
    """Everything a level classifier needs to judge one child node."""

    config: Any
    name: str
    fqn: str
    pattern_field: Optional[str]
    # The container names already descended, top-first — the parent of this node.
    parent_path: Tuple[str, ...]
    # Report that this node's classification degraded rather than raised (e.g.
    # a connector couldn't resolve its exact ingestion identifier and matched
    # on a less-precise stand-in instead). Feeds the same ProbeResult.warnings
    # list ProbeSoftError does, deduplicated by list_children so a single
    # connector-wide reason isn't appended once per node it's classified for.
    warn: Callable[[str], None]


# classify(ctx) -> Verdict
LevelClassifier = Callable[[ClassifyContext], Verdict]


def pattern_verdict(config: Any, pattern_field: Optional[str], target: str) -> Verdict:
    """The standard allow/deny check: the config's *_pattern field against `target`.

    Exported so a custom level classifier can defer to it after its own
    structural exclusions.
    """
    if pattern_field is None:
        return _INCLUDED
    pattern = getattr(config, pattern_field)
    return _INCLUDED if pattern.allowed(target) else Verdict(False, pattern_field)
