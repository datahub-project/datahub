from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Collection,
    Iterator,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from datahub.ingestion.agent.pattern_path import require_pattern_at, unset_block_on


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

    @classmethod
    def exclude(
        cls, excluded_by: str, matched_target: Optional[str] = None
    ) -> "Verdict":
        """An exclusion, naming the field or rule that decided it.

        Prefer this to `Verdict(False, ...)` in a connector's selection module.
        Ingestion calls those functions directly, outside probe_verdict_override's
        consistency check, so an exclusion without a reason would never be
        caught there.
        """
        if not excluded_by.strip():
            raise ValueError("an excluded verdict must name what excluded it")
        return cls(False, excluded_by, matched_target)


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


@dataclass(frozen=True)
class VerdictContext:
    """What a connector's probe_verdict_override is told about one name.

    A context object rather than keyword arguments, unlike the older hooks:
    four connector plans proposed four signatures for this one hook, each a
    superset of the last, and every new field would otherwise break every
    implementer. Read only what you need.
    """

    kind: str
    name: str
    # The string the pattern would be matched against -- already qualified
    # the way the connector's other hooks say, so an override re-checking a
    # second pattern (table_pattern on a view) matches the same string.
    target: str
    parent_path: Tuple[str, ...]
    # The field that filters this kind, possibly a dotted path. None when the
    # kind is unfiltered or unresolved.
    pattern_field: Optional[str]
    # The exclusion a kind switch (probe_kind_switches) makes, or None when
    # no switch is off for this kind. Passed so an override can keep it or
    # overrule it.
    structural: Optional[Verdict]
    # Per-name facts the caller supplied with the name, such as the id a
    # workspace pattern matches (see `probe filter --from-run`). Empty when
    # only names were given, so an override must degrade, with a warning,
    # when a fact it needs is missing.
    attributes: Mapping[str, str]
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
    if unset_block_on(config, pattern_field) is not None:
        # A recipe that leaves an Optional block out filters nothing there, as
        # filter_check already reads it; raising would fail a classifier that
        # defers here on a recipe nothing is wrong with.
        return _INCLUDED
    pattern = require_pattern_at(config, pattern_field)
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

    Prefer ProbeArgumentError for "the caller named something that is not
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


class ProbeArgumentError(ValueError):
    """The caller named something that is wrong or does not exist.

    The way for a provider to say "fix your argument" (exit 2) with a message
    the caller should read. The framework shows exception text by type only
    (see agent.error_policy), so a plain ValueError keeps exit 2 but is
    reported by its class name, wherever it was raised.

    If the provider already recorded read failures before raising, the call
    reports ProbeReadFailed (exit 3) instead: the recorded failure is what
    explains the miss, not the argument.
    """


class ProbeReadFailed(Exception):
    """A command failed and the connector had already recorded why.

    Deliberately NOT a ValueError. A getter can call report.failure() and then
    raise something from the ValueError family -- Hex's _project_id_or_raise
    raises ProbeSoftError("no project titled 'x'") after its /projects fetch
    already failed and was recorded. Mapped to exit 2, that tells an agent its
    argument was wrong for what was an auth or transport error, and sends it to
    fix a title that was never the problem. This carries the recorded reason and
    maps to exit 3.
    """


class ProbeConnectionError(Exception):
    """The source could not be reached, or refused the session, while the probe
    was opening it.

    Not a ValueError, for the reason ProbeReadFailed is not: connectors wrap
    connect failures in exceptions the CLI otherwise reads as bad input --
    Snowflake raises ConfigurationError for DNS, network and auth failures
    alike -- and exit 2 sends an agent to edit a recipe that was never the
    problem. Maps to exit 3.
    """


class ProbeInternalError(Exception):
    """A getter failed with a programming error (TypeError, KeyError, ...)
    after its arguments had already been checked.

    Those exception types mean "your input was wrong" only before the call:
    once run_probe_method has coerced the arguments, a KeyError is the getter
    misreading a response, not the caller's mistake. Maps to exit 1.
    """


def soft_error_for(
    exc: BaseException, codes: Collection[int], context: str
) -> Optional[ProbeSoftError]:
    """The ProbeSoftError soft_on_status turns `exc` into, or None.

    Duck-types on `.response.status_code`, so the framework takes no
    HTTP-library dependency. The message names the context and the status
    only, never the exception's text.
    """
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if status in codes:
        return ProbeSoftError(
            f"{context} returned HTTP {status}; treating it as empty."
        )
    return None


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
        soft = soft_error_for(exc, codes, context)
        if soft is not None:
            raise soft from exc
        raise


def ancestors_in(
    chain: Sequence[str], kind: str, leaves: Collection[str]
) -> Optional[Tuple[str, ...]]:
    """The container kinds above `kind`, outermost first.

    `chain` is the source's containers outermost first. A container's
    ancestors are the ones before it; a leaf sits under the whole chain. None
    for a kind the chain does not describe, which `probe filter` reports rather
    than guessing at.
    """
    if kind in chain:
        return tuple(chain[: list(chain).index(kind)])
    if kind in leaves:
        return tuple(chain)
    return None
