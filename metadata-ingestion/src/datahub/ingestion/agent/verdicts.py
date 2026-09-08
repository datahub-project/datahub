from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional, Tuple


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
    # The string the connector matched, when it is not the node's own name.
    # Redshift matches "database.schema" once match_fully_qualified_names is on, and
    # reporting the bare name there tells a caller the opposite of what decided:
    # they see target='analytics' excluded by a pattern of '^analytics$' and conclude
    # the probe is broken. `target` is the one field probe filter exists to get right.
    matched_target: Optional[str] = None

    @classmethod
    def include(cls) -> "Verdict":
        return cls(True, None)


@dataclass(frozen=True)
class SchemaMatch:
    """A connector's own verdict for a container node, and the string it matched.

    Returned by probe_schema_verdict_override. Both facts travel together because
    only the override knows them: it runs the connector's own predicate (Redshift's
    is_schema_allowed over "database.schema"), so nothing else can say what decided.
    """

    included: bool
    target: str


_INCLUDED = Verdict.include()

# A level the source offers no filter for (e.g. Mode's datasets and queries).
# Distinct from pattern_field=None, which means "resolve the conventional
# <kind>_pattern field". Nodes at an UNFILTERED level report pattern_field=None
# and are always included.
UNFILTERED: str = "__unfiltered__"


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
    # on a less-precise stand-in instead). Feeds the same ProbeMethodResult.warnings
    # list ProbeSoftError does, deduplicated by check_filters so a single
    # connector-wide reason isn't appended once per node it's classified for.
    warn: Callable[[str], None]


def pattern_verdict(config: Any, pattern_field: Optional[str], target: str) -> Verdict:
    """The standard allow/deny check: the config's *_pattern field against `target`.

    Exported so a custom level classifier can defer to it after its own
    structural exclusions.
    """
    if pattern_field is None or pattern_field == UNFILTERED:
        # UNFILTERED is a sentinel, not a field name: looking it up would ask
        # the config for an attribute called "__unfiltered__". Its meaning is
        # "no filter at this level", which is the same include that None gets.
        # filter_check guards this before calling, but the sentinel and this
        # function are exported from the same module and read as composable.
        return _INCLUDED
    pattern = getattr(config, pattern_field)
    return _INCLUDED if pattern.allowed(target) else Verdict(False, pattern_field)


class ProbeSoftError(ValueError):
    """A connector raises this when one endpoint could not be read cleanly -- a
    404 on a resource deleted between listing and fetch, or a 403 on something
    this token cannot read -- and the connector wants to report that as an
    empty contribution rather than failing the whole command.

    **The connector that raises it must also catch it and record the reason.**
    run_probe_method catches only NotImplementedError; there is no
    framework-level catch for this. mode_probe's _listing is the worked example.
    Uncaught, it reaches the CLI and exits 2.

    An earlier version of this docstring described a mechanism that no longer
    exists -- ClientProbe.list_children catching per level, ProbeLevel.sources,
    LevelSource -- all from the declared hierarchy deleted within this branch.
    It also claimed run_probe_method records str(exc) on
    ProbeMethodResult.warnings and continues, which it does not. Recording the
    reason is the connector's job, and the two connectors that raise this
    disagreed about it: Mode caught it, Hex did not.

    Prefer a plain ValueError for "the caller named something that is not
    there" -- a nonexistent space or report is a bad argument, not a degraded
    read, and routing it through the soft path reports exit 0 with an empty
    result for what is really exit 2.

    Subclasses ValueError deliberately. When one does reach the CLI uncaught,
    what it reports is that the caller named something that isn't there ("no
    report named 'x'"), which is a bad argument, not an unreachable source --
    so it must exit 2, not 3, or the agent retries the connection instead of
    fixing the name. recipe_cli now classifies exceptions in one place
    (_USER_ERRORS), so that mapping no longer depends on remembering to add a
    clause to each of seven ladders.
    """


@contextmanager
def soft_on_status(*codes: int, context: str) -> Iterator[None]:
    """Treat the given HTTP statuses as expected absence, not failure.

    A probe must distinguish "nothing here" from "could not look" (see
    ProbeSoftError): the listed codes become a ProbeSoftError, anything else
    propagates. Duck-types on `.response.status_code` -- matches
    requests.HTTPError and similar shapes -- so the framework takes no
    HTTP-library dependency; any HTTP-based connector can reuse this instead
    of writing its own status-code split.
    """
    try:
        yield
    except Exception as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status in codes:
            raise ProbeSoftError(
                f"{context} returned HTTP {status}; treating it as empty."
            ) from exc
        raise
