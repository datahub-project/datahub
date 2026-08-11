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
    # list ProbeSoftError does, deduplicated by list_children so a single
    # connector-wide reason isn't appended once per node it's classified for.
    warn: Callable[[str], None]


def pattern_verdict(config: Any, pattern_field: Optional[str], target: str) -> Verdict:
    """The standard allow/deny check: the config's *_pattern field against `target`.

    Exported so a custom level classifier can defer to it after its own
    structural exclusions.
    """
    if pattern_field is None:
        return _INCLUDED
    pattern = getattr(config, pattern_field)
    return _INCLUDED if pattern.allowed(target) else Verdict(False, pattern_field)


class ProbeSoftError(Exception):
    """A connector's list_names raises this to report that one endpoint
    couldn't be read cleanly -- a 404 on a resource deleted between listing
    and fetch, or a 403 on something this token can't read -- and that
    ClientProbe.list_children should treat the contribution as empty rather
    than either:

    - letting the exception propagate and kill the whole list_children call,
      discarding sibling levels (e.g. Reports vs Datasets under a Mode Space)
      that already succeeded, or
    - silently swallowing it and returning [], which is indistinguishable
      from the level genuinely having no children.

    list_children catches this per LEVEL (not per endpoint): for a plain
    list_names/list_items level, that is the same thing, since one lister
    call produces the level's entire contribution. But a level assembled
    from several LevelSources (ProbeLevel.sources, e.g. tables + views) is
    fed by more than one lister, and the catch is still level-wide -- if the
    second source raises, whatever the first source already produced for
    this level is discarded too, not just the second source's share. No
    connector's sources= level raises ProbeSoftError today, so this hasn't
    manifested, but it is a real limitation of the current per-level
    granularity, not per-lister-within-a-level.

    run_probe_method records str(exc) on ProbeMethodResult.warnings and continues
    with the remaining sibling levels. Source-agnostic: any connector's
    lister may raise it, not just Mode's.
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
