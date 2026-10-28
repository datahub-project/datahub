from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Collection,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.pattern_path import (
    pattern_at,
    require_pattern_at,
    unset_block_on,
)


@dataclass(frozen=True)
class Verdict:
    """Would one node be ingested, given the recipe's filters and the source's
    own exclusions? excluded_by names the field or rule that drops it
    ("default_schema"), or None when included.
    """

    included: bool
    excluded_by: Optional[str] = None
    # The string the connector matched, when it is not the name the pattern was
    # given (a qualified schema): reported as the target, since it decided.
    matched_target: Optional[str] = None

    def __post_init__(self) -> None:
        # Checked here, where every verdict is built, so neither a connector's
        # probe_verdict_override nor a selection module can return one that
        # contradicts itself.
        if not self.included and not (
            isinstance(self.excluded_by, str) and self.excluded_by.strip()
        ):
            raise ValueError("an excluded verdict must name what excluded it")
        if self.included and self.excluded_by is not None:
            raise ValueError("an included verdict names nothing that excluded it")

    @classmethod
    def include(cls) -> "Verdict":
        return cls(True, None)

    @classmethod
    def exclude(
        cls, excluded_by: str, matched_target: Optional[str] = None
    ) -> "Verdict":
        """An exclusion, naming the field or rule that decided it."""
        return cls(False, excluded_by, matched_target)


_INCLUDED = Verdict.include()

# A level the source declares it does not filter. Its nodes report
# pattern_field=None and are always included.
UNFILTERED: str = "__unfiltered__"


@dataclass(frozen=True)
class ClassifyContext:
    """Everything a level classifier needs to judge one child node."""

    config: Any
    name: str
    fqn: str
    pattern_field: Optional[str]
    # The containers above this node, outermost first (--parent).
    parent_path: Tuple[str, ...]
    # Report a degraded judgement (a less precise target). Deduplicated by
    # message, so a connector-wide reason is reported once.
    warn: Callable[[str], None]
    # The kind being judged, canonicalised by check_filters.
    kind: str = ""


_NO_PARENT_WARNING = (
    "no parent given, so these were judged on their bare names; this "
    "source filters on a qualified identifier, so pass the containing "
    "schema/database to get the verdict ingestion actually makes"
)


def parent_required(ctx: ClassifyContext) -> bool:
    """True, after warning, when ctx has no parent to qualify its name with.

    For a probe_match_target that needs the container: without one it would
    build an identifier ingestion never builds (".orders"), so it returns None
    and the bare name is judged. The warning names no object, so it shows once.
    """
    if ctx.parent_path:
        return False
    ctx.warn(_NO_PARENT_WARNING)
    return True


def _hint_container(config: object, kind: str) -> Tuple[Optional[str], Optional[str]]:
    """(container kind, the command listing it) for a refusal's remedy, either
    of which may be None.

    Any failure is swallowed: this runs while a refusal is being built, and a
    connector whose hooks raise would otherwise replace an actionable
    "pass --parent" with an exit-1 defect report the caller cannot act on.
    """
    try:
        return _container_kind_for(config, kind), _listing_command_for(config, kind)
    except Exception:
        return None, None


def _container_kind_for(config: object, kind: str) -> Optional[str]:
    """The kind of the container a `kind` is qualified by, or None."""
    from datahub.ingestion.agent.probe_methods import config_hook

    ancestors_for = config_hook(config, "probe_ancestor_kinds")
    ancestors = ancestors_for(kind=kind) if ancestors_for else None
    if not isinstance(ancestors, (list, tuple)) or not ancestors:
        return None
    return str(ancestors[0])


def _listing_command_for(config: object, kind: str) -> Optional[str]:
    """A probe command that lists what contains a `kind`, or None.

    Looked up by kind rather than named per connector, so a source that gains a
    container listing earns the "run this first" sentence without the framework
    learning its name. Lazy import: probe_methods imports this module.
    """
    from datahub.ingestion.agent.probe_methods import (
        config_hook,
        listing_command_for_kind,
        provider_class_for_config,
    )

    ancestors_for = config_hook(config, "probe_ancestor_kinds")
    ancestors = ancestors_for(kind=kind) if ancestors_for else None
    # A hint only. A connector that returns an unusable shape loses the hint
    # rather than turning the refusal into a defect report -- and nothing
    # polices the shape on this path, because filter_check's check sits behind
    # `if not parent_path: return None` and this runs precisely when there is
    # no parent.
    if not isinstance(ancestors, (list, tuple)) or not ancestors:
        return None
    # The outermost ancestor, not the nearest: the container being asked for is
    # the one a Qualifier field pins (a database or project), and for a table
    # the nearer ancestor -- its schema -- may already have come from --parent.
    provider_cls = provider_class_for_config(type(config))
    if provider_cls is None:
        return None
    return listing_command_for_kind(config, provider_cls, str(ancestors[0]))


# Allow entries that admit every name, so a pattern built only from these
# filters nothing whatever it is matched against. AllowDenyPattern.is_allow_all
# recognises the literal `.*` alone, and reports False for "unknown" rather
# than "restricted" -- which would refuse `^.*$`, a recipe that filters
# nothing, for want of a container that could not change its verdict.
_MATCHES_ANYTHING = frozenset({".*", "^.*", ".*$", "^.*$", ".+", "^.+", ".+$", "^.+$"})


def filters_nothing(pattern: Optional[AllowDenyPattern]) -> bool:
    """Whether `pattern` admits every name, so no container can change the
    verdict it gives -- only the target reported beside it."""
    if pattern is None:
        return True
    if pattern.is_allow_all():
        return True
    return not pattern.deny and any(a in _MATCHES_ANYTHING for a in pattern.allow)


def require_container(
    container: Optional[str],
    *,
    config: object,
    kind: str,
    pattern_field: Optional[str],
    shape: str,
    pin_field: Optional[str],
    warn: Callable[[str], None],
) -> Optional[str]:
    """`container`, or None after warning, or a refusal.

    A source that matches a qualified identifier cannot be judged without the
    container: matching the bare name against a pattern written (or rewritten)
    for `shape` answers a question ingestion never asks, and the answer is
    usually "excluded" about something every run ingests. That verdict is the
    one failure `probe filter` exists to prevent, so it is not given.

    An allow-all pattern is the exception and answers normally: it includes
    everything whatever string it is matched against, so the missing container
    cannot change the verdict -- only the `target` reported alongside it. Said
    rather than assumed, because a reader comparing `target` with what
    ingestion logs would otherwise see a difference nothing explains.

    The deciding field is named rather than the pattern passed, so a caller
    cannot name one field while another decides. `pin_field` is passed in
    because declarations imports this module and the dependency cannot be
    inverted.
    """
    if container:
        return container
    pattern = pattern_at(config, pattern_field) if pattern_field else None
    if filters_nothing(pattern):
        where = f"nothing at `{pattern_field}`" if pattern_field else "nothing"
        warn(
            f"no container was given, so `target` below shows the bare name "
            f"rather than the '{shape}' this source matches; the verdict is "
            f"unaffected, as {where} filters these"
        )
        return None
    wanted, listing = _hint_container(config, kind)
    naming = f"the {wanted} as the outermost --parent" if wanted else "--parent"
    remedies = []
    if listing:
        remedies.append(f"run `probe run {listing}` to list them, then pass {naming}")
    else:
        # Named even with no listing to point at: a caller who passed
        # `--parent PUBLIC` and is told to "pass --parent" cannot tell that
        # what is missing is the database ABOVE it.
        remedies.append(f"pass {naming}")
    if pin_field:
        remedies.append(f"or pin a single value in `{pin_field}`")
    raise ProbeArgumentError(
        f"this source matches `{pattern_field}` against the qualified '{shape}', "
        f"and nothing here says which container to use. Judging the bare name "
        f"against that pattern would report a verdict ingestion does not make, "
        f"so none is given: " + ", ".join(remedies) + "."
    )


@dataclass(frozen=True)
class VerdictContext:
    """What probe_verdict_override is told about one name. A context object, so
    a new field breaks no implementer; read only what you need."""

    kind: str
    name: str
    # The match target (probe_match_target's), so an override re-checking a
    # second pattern matches the same string.
    target: str
    parent_path: Tuple[str, ...]
    # The field filtering this kind, possibly dotted; None when there is none.
    pattern_field: Optional[str]
    # A kind switch's exclusion (an Enables field set False), to keep or
    # overrule.
    structural: Optional[Verdict]
    # Per-name facts from `probe filter --from-run` (an id a pattern matches).
    # Empty for bare names: an override needing one degrades with a warning.
    attributes: Mapping[str, str]
    warn: Callable[[str], None]


def pattern_verdict(config: Any, pattern_field: Optional[str], target: str) -> Verdict:
    """The config's pattern field (a dotted path; an unset Optional block
    filters nothing) against `target`, for an override that defers to it."""
    if pattern_field is None or pattern_field == UNFILTERED:
        return _INCLUDED
    if unset_block_on(config, pattern_field) is not None:
        # An Optional block the recipe leaves out filters nothing.
        return _INCLUDED
    pattern = require_pattern_at(config, pattern_field)
    return _INCLUDED if pattern.allowed(target) else Verdict.exclude(pattern_field)


class ProbeSoftError(ValueError):
    """One sub-read could not be done cleanly (a 404 on something deleted
    between listing and fetch, a 403 the token cannot read), reported as an
    empty contribution rather than a failed command.

    The provider that raises it also catches it and records a warning
    (provider_helpers.soft_listing does both); the framework does not. A name
    the caller gave that does not exist is a ProbeArgumentError. A ValueError,
    so one that escapes exits 2.
    """


class ProbeArgumentError(ValueError):
    """The caller's argument is wrong or names nothing: exit 2, message shown.

    Text is shown by type only (agent.error_policy), so a plain ValueError
    keeps exit 2 but loses its message. After recorded read failures the call
    reports ProbeReadFailed (exit 3) instead.
    """


class ProbeReadFailed(Exception):
    """A command failed after the connector recorded why: exit 3, with the
    recorded reason. Not a ValueError: a "no such name" raised after a failed
    fetch reports the fetch, not a bad argument."""


class ProbeConnectionError(Exception):
    """The source could not be reached, or refused the session, while the probe
    opened it: exit 3. Not a ValueError: connectors wrap connect failures in
    types the CLI would otherwise read as bad input."""


class ProbeInternalError(Exception):
    """A defect in the probe or a provider: exit 1. Once the arguments are
    checked, a KeyError is the getter misreading a response, not the caller."""


def ancestors_in(
    chain: Sequence[str], kind: str, leaves: Collection[str]
) -> Optional[Tuple[str, ...]]:
    """The container kinds above `kind`, outermost first.

    `chain` is the source's containers, outermost first; a leaf sits under the
    whole chain. None for a kind the chain does not describe.
    """
    if kind in chain:
        return tuple(chain[: list(chain).index(kind)])
    if kind in leaves:
        return tuple(chain)
    return None
