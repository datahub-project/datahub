import re
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
    # Record that this target was built without the container ingestion
    # matches on. Asked after the verdict, never raised here: see
    # ContainerNeed. Defaulted, so a connector or test building a context by
    # hand needs no change.
    note_container: Callable[["ContainerNeed"], None] = lambda _need: None
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


# An entry of the form `^?.*<literal>$`, where the literal carries no dot or
# regex syntax, matches `<container>.<name>` exactly when it matches `<name>`:
# the suffix cannot reach back across the dot. So the question is not whether a
# pattern filters -- a deny list filters and is still blind to the container --
# but whether the container could change what it says.
_CONTAINER_BLIND = re.compile(r"\^?\.\*[A-Za-z0-9_\-]*\$")

# Allow entries that admit every name, so the container cannot change them.
_MATCHES_ANYTHING = frozenset({".*", "^.*", ".*$", "^.*$", ".+", "^.+", ".+$", "^.+$"})


def container_blind(pattern: Optional[AllowDenyPattern]) -> bool:
    """Whether the missing container could change what `pattern` says.

    False keeps the verdict unasked; True lets it stand on the bare name.
    Testing "filters nothing" instead refused recipes whose verdict no
    container could alter -- every Snowflake recipe, because
    `validate_legacy_schema_pattern` always appends a `.*INFORMATION_SCHEMA$`
    deny, and a `dataset_pattern` denying `.*_staging$` for the same reason.
    """
    if pattern is None:
        return True
    allow_blind = any(a in _MATCHES_ANYTHING for a in pattern.allow) or all(
        _CONTAINER_BLIND.fullmatch(a) for a in pattern.allow
    )
    return allow_blind and all(_CONTAINER_BLIND.fullmatch(d) for d in pattern.deny)


@dataclass(frozen=True)
class ContainerNeed:
    """A target built without the container ingestion matches on.

    Recorded while the target is built, and asked about after the verdict.
    By then the kind switch, the connector's own override and the --parent
    container have each had their say, and any of them can decide a name in
    every container -- which is what makes "this cannot be judged without the
    container" false. Raising where the target is built asks too early to
    know that.
    """

    config: object
    kind: str
    pattern_field: Optional[str]
    shape: str

    def refusal(self) -> "ProbeArgumentError":
        """The refusal to raise when nothing else decided the name."""
        from datahub.ingestion.agent.declarations import qualifier_field

        wanted, listing = _hint_container(self.config, self.kind)
        # Named even with no listing to point at: a caller who passed
        # `--parent PUBLIC` and is told to "pass --parent" cannot tell that
        # what is missing is the database ABOVE it. Not called "outermost":
        # a chain can have containers the source leaves out of it on purpose
        # (Unity's metastore), so this promises only the level, not the slot.
        # Not "outermost": a source can leave a level out of its chain on
        # purpose (Unity's metastore), so this promises the level, not the slot.
        naming = (
            f"the {wanted} as a --parent, before any already named"
            if wanted
            else "--parent"
        )
        remedies = []
        if listing:
            remedies.append(
                f"run `probe run {listing}` to list them, then pass {naming}"
            )
        else:
            remedies.append(f"pass {naming}")
        pin = qualifier_field(self.config)
        if pin:
            # Second, and qualified: pinning does not just answer the
            # question, it narrows what the recipe ingests.
            remedies.append(
                f"or narrow the recipe to one by pinning `{pin}`, which "
                f"changes what it reads"
            )
        return ProbeArgumentError(
            f"this source matches `{self.pattern_field}` against the "
            f"qualified '{self.shape}', and nothing here says which container "
            f"to use. Judging the bare name against that pattern would report "
            f"a verdict ingestion does not make, so none is given: "
            + ", ".join(remedies)
            + "."
        )


def require_container(
    container: Optional[str],
    *,
    config: object,
    kind: str,
    pattern_field: Optional[str],
    shape: str,
    warn: Callable[[str], None],
    note: Callable[["ContainerNeed"], None],
) -> Optional[str]:
    """`container`, or None -- after warning when the container cannot change
    the verdict, or after recording what a refusal would say when it can.

    Never raises. The decision belongs after the verdict, so this records and
    check_filters asks; see ContainerNeed.
    """
    if container:
        return container
    pattern = pattern_at(config, pattern_field) if pattern_field else None
    if container_blind(pattern):
        where = f"`{pattern_field}`" if pattern_field else "nothing here"
        warn(
            f"no container was given, so `target` below is not qualified with "
            f"the container this source matches on ('{shape}'); the verdict "
            f"stands because {where} cannot match one differently"
        )
        return None
    note(
        ContainerNeed(
            config=config, kind=kind, pattern_field=pattern_field, shape=shape
        )
    )
    return None


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
    # As ClassifyContext.note_container.
    note_container: Callable[["ContainerNeed"], None] = lambda _need: None


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
