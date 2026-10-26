"""`probe filter`: would the recipe's filters keep these names, and what decided.

Connection-free. Per name: find the kind's field (Filters, the name convention,
or a declared rule field), build the match target (probe_match_target), apply a
kind switch (the field marked Enables), ask the connector
(probe_verdict_override), else match the pattern. Then judge the immediate --parent the same way
(probe_ancestor_kinds): nothing inside an excluded container is ingested.
"""

import re
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from pydantic import BaseModel, ValidationError

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.config_validation import validate_source_config
from datahub.ingestion.agent.declarations import (
    declared_kind_enablers,
    declared_rule_filtered_kinds,
)
from datahub.ingestion.agent.error_policy import foreign_label
from datahub.ingestion.agent.introspect import pattern_field_for_config
from datahub.ingestion.agent.models import Filtering
from datahub.ingestion.agent.pattern_path import (
    copy_with_pattern_at,
    pattern_at,
    require_pattern_at,
    unset_block_on,
    validate_pattern_at,
)
from datahub.ingestion.agent.probe_methods import (
    config_hook,
    declared_kind_overrides,
    list_probe_methods,
    require_config_class,
)
from datahub.ingestion.agent.verdicts import (
    UNFILTERED,
    ClassifyContext,
    ProbeArgumentError,
    ProbeInternalError,
    Verdict,
    VerdictContext,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)

# The relational kinds, canonical for every source: `--kind schema` reaches the
# hooks, which compare kinds by identity, as `Schema`.
_STANDARD_KINDS = frozenset(
    {
        str(DatasetSubTypes.TABLE),
        str(DatasetSubTypes.VIEW),
        str(DatasetContainerSubTypes.SCHEMA),
        str(DatasetContainerSubTypes.DATABASE),
    }
)


@dataclass
class FilterVerdict:
    name: str
    # The string the pattern was matched against, often not the bare name
    # ("schema.table"): it explains a surprising verdict.
    target: str
    included: bool
    excluded_by: Optional[str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "target": self.target,
            "included": self.included,
            "excluded_by": self.excluded_by,
        }


@dataclass
class FilterCheckResult:
    source_type: str
    kind: str
    parent_path: List[str]
    # The config field that decided, so a caller edits the right recipe line.
    pattern_field: Optional[str]
    results: List[FilterVerdict]
    tried: Optional[Dict[str, List[str]]] = None
    warnings: List[str] = field(default_factory=list)
    filtering: Filtering = Filtering.BY_PATTERN
    # The verdicts come from an excluded --parent, so the level below does not
    # report the exclusion twice. Not serialized: `warnings` says it.
    excluded_by_container: bool = False

    def to_dict(self) -> Dict[str, object]:
        return {
            "source_type": self.source_type,
            "kind": self.kind,
            "parent_path": self.parent_path,
            "pattern_field": self.pattern_field,
            "filtering": str(self.filtering),
            "tried": self.tried,
            "results": [r.to_dict() for r in self.results],
            "warnings": self.warnings,
        }


def _match_target(config: object, ctx: ClassifyContext) -> str:
    """The string ingestion filters on for one node: probe_match_target's answer,
    or the bare name when it gives None or "" or is not declared."""
    hook = config_hook(config, "probe_match_target")
    if hook is None:
        return ctx.name
    target = hook(ctx=ctx)
    return target if isinstance(target, str) and target else ctx.name


def _switch_verdict(config: object, kind: str) -> Optional[Verdict]:
    """The exclusion a switched-off kind makes, or None: the field marked
    Enables(kind), when False, stops ingestion emitting the kind whatever the
    pattern says."""
    field = declared_kind_enablers(config).get(kind)
    if field is not None and getattr(config, field) is False:
        return Verdict.exclude(field)
    return None


def _parent_exclusion(
    source_type: str,
    config_dict: Dict[str, object],
    config: object,
    kind: str,
    parent_path: Sequence[str],
    warn: Callable[[str], None],
) -> Optional[str]:
    """The field that drops the immediate --parent container, if one does.

    Ingestion never reaches inside an excluded container. The parent is judged
    by check_filters itself, so its own parent is judged in turn.
    """
    if not parent_path:
        return None
    ancestors_for = config_hook(config, "probe_ancestor_kinds")
    ancestors = ancestors_for(kind=kind) if ancestors_for else None
    # A bare str is a sequence too, and would read as one kind per character.
    if ancestors is not None and (
        not isinstance(ancestors, (list, tuple))
        or not all(isinstance(ancestor, str) for ancestor in ancestors)
    ):
        raise ProbeInternalError(
            f"the connector is defective: {type(config).__name__}."
            f"probe_ancestor_kinds returned {type(ancestors).__name__}, not a "
            f"sequence of kind names"
        )
    if ancestors is None:
        warn(
            f"this source does not declare what contains a '{kind}', so the "
            f"--parent containers' own patterns were not checked; the verdict "
            f"covers the '{kind}' pattern only"
        )
        return None
    if not ancestors:
        return None

    parent_kind = ancestors[-1]
    parent = check_filters(
        source_type=source_type,
        config_dict=config_dict,
        kind=parent_kind,
        parent_path=parent_path[:-1],
        names=[parent_path[-1]],
    )
    if parent.filtering not in (Filtering.BY_PATTERN, Filtering.BY_RULE):
        # Nothing filters that level; its warnings would be noise here.
        return None
    for message in parent.warnings:
        warn(message)
    verdict = parent.results[0]
    if verdict.included:
        return None
    if parent.excluded_by_container:
        # Already reported at the level that excluded it.
        return verdict.excluded_by
    warn(
        f"the containing {parent_kind} '{verdict.name}' (matched as "
        f"'{verdict.target}') is excluded by {verdict.excluded_by}, so "
        f"ingestion never reaches anything inside it"
    )
    return verdict.excluded_by


def _canonical_kind(source_type: str, config: object, kind: str) -> str:
    """The declared spelling of a kind the caller may have cased differently:
    the hooks compare kinds by identity. A kind nothing declares is returned
    untouched, so the "declares no kind" warning can fire."""
    if kind in _STANDARD_KINDS:
        return kind
    lowered = kind.lower()
    for declared in sorted(_declared_kinds(source_type, config) | _STANDARD_KINDS):
        if declared.lower() == lowered:
            return declared
    return kind


def _declared_kinds(source_type: str, config: object) -> Set[str]:
    """The kinds the probe methods and probe_kind_overrides name, without a
    connection. Incomplete, so used only to canonicalise and to warn."""
    # Not introspect.declared_kinds_for_class, which swallows a provider that
    # fails to load so `describe` still answers: here that failure is the
    # connector's defect and must surface, not make every kind look unknown.
    kinds = {spec.kind for spec in list_probe_methods(source_type) if spec.kind}
    return kinds | set(declared_kind_overrides(config).values())


def _override_verdict(config: object, ctx: VerdictContext) -> Optional[Verdict]:
    """The connector's own verdict for one name (probe_verdict_override),
    checked for type: a returned `False` would read as "no opinion". Verdict
    refuses an inconsistent one as it is built."""
    override = config_hook(config, "probe_verdict_override")
    if override is None:
        return None
    verdict = override(ctx=ctx)
    if verdict is not None and not isinstance(verdict, Verdict):
        raise ProbeInternalError(
            f"{type(config).__name__}.probe_verdict_override returned "
            f"{type(verdict).__name__}; it must return a Verdict or None"
        )
    return verdict


@dataclass(frozen=True)
class _Resolution:
    pattern_field: Optional[str]
    filtering: Filtering


def _resolve_filtering(config: object, kind: str) -> _Resolution:
    rule_field = declared_rule_filtered_kinds(config).get(kind)
    if rule_field is not None:
        # Rules, not a pattern: the override judges each name.
        return _Resolution(rule_field, Filtering.BY_RULE)
    resolved = pattern_field_for_config(config, kind)
    # Both report every name included; `filtering` says whether that is the
    # source's design or a gap.
    if resolved == UNFILTERED:
        return _Resolution(None, Filtering.UNFILTERED)
    if resolved is None:
        return _Resolution(None, Filtering.UNRESOLVED)
    return _Resolution(resolved, Filtering.BY_PATTERN)


@dataclass(frozen=True)
class _Judged:
    """The config and pattern the names are judged against, and what was tried."""

    config: BaseModel
    pattern: AllowDenyPattern
    tried: Optional[Dict[str, List[str]]] = None


def _pattern_to_judge(
    config: BaseModel,
    pattern_field: Optional[str],
    filtering: Filtering,
    try_allow: Optional[Sequence[str]],
    try_deny: Optional[Sequence[str]],
    warn: Callable[[str], None],
) -> _Judged:
    """The recipe's pattern, or the --try-* hypothetical applied to a copy."""
    trying = bool(try_allow or try_deny)
    if filtering is Filtering.BY_RULE:
        if trying:
            warn(
                f"'{pattern_field}' holds rules, not an allow/deny pattern, so "
                f"--try-allow and --try-deny have nothing to replace and were "
                f"ignored; edit {pattern_field} in the recipe to test a change"
            )
        return _Judged(config, AllowDenyPattern.allow_all())
    if pattern_field is None:
        # No allow/deny list exists for a hypothetical to replace.
        if trying:
            warn(
                "--try-allow and --try-deny were ignored: this kind has no "
                "allow/deny pattern to replace"
            )
        return _Judged(config, AllowDenyPattern.allow_all())
    unset = unset_block_on(config, pattern_field)
    if unset is not None:
        # A valid recipe leaving an Optional block out: no filter applies.
        warn(
            f"`{unset}` is unset in this recipe, so nothing at "
            f"`{pattern_field}` filters these"
        )
        if trying:
            # Inventing a block would judge a recipe the caller did not write.
            warn(
                f"--try-allow and --try-deny were ignored: `{unset}` is unset, "
                f"so there is no pattern at `{pattern_field}` to replace; set "
                f"`{unset}` in the recipe to test a change"
            )
        return _Judged(config, AllowDenyPattern.allow_all())
    recipe_pattern = require_pattern_at(config, pattern_field)
    if not trying:
        return _Judged(config, recipe_pattern)
    pattern = _trial_pattern(recipe_pattern, try_allow, try_deny)
    tried = {"allow": list(pattern.allow), "deny": list(pattern.deny)}
    trial_config, judged_pattern = _with_trial_pattern(
        config, pattern_field, pattern, warn
    )
    return _Judged(trial_config, judged_pattern, tried)


def _trial_pattern(
    recipe_pattern: AllowDenyPattern,
    try_allow: Optional[Sequence[str]],
    try_deny: Optional[Sequence[str]],
) -> AllowDenyPattern:
    """The recipe's pattern with each --try-* flag replacing only its own
    half, every other field (`ignoreCase`) kept.

    Rebuilt rather than model_copy'd: the copy carries the compiled regexes
    the recipe's pattern cached on first use, and would match those.
    """
    # Compiled here, before any hook runs: AllowDenyPattern compiles lazily,
    # so a malformed regex would otherwise surface inside a connector's hook
    # and read as that connector's defect.
    for flag, regexes in (("--try-allow", try_allow), ("--try-deny", try_deny)):
        for regex in regexes or ():
            try:
                re.compile(regex)
            except re.error as exc:
                raise ProbeArgumentError(
                    f"{flag} {regex!r} is not a valid regular expression ({exc.msg})"
                ) from None
    fields = recipe_pattern.model_dump()
    if try_allow:
        fields["allow"] = list(try_allow)
    if try_deny:
        fields["deny"] = list(try_deny)
    return type(recipe_pattern).model_validate(fields)


def _with_trial_pattern(
    config: BaseModel,
    pattern_field: str,
    pattern: AllowDenyPattern,
    warn: Callable[[str], None],
) -> Tuple[BaseModel, AllowDenyPattern]:
    """`config` with `pattern` at `pattern_field`, and the pattern the
    verdicts use: as the source's validators normalize it, where they can.

    On the config too, so an override reading the pattern off the config
    judges the hypothetical. A shallow copy: a deep one would clone cached
    credentials (an IAM token manager and its token).
    """
    # Snapshotted before validators can rewrite `pattern` in place.
    requested = (list(pattern.allow), list(pattern.deny))
    config = copy_with_pattern_at(config, pattern_field, pattern)
    # Re-validated, since connectors normalize patterns in validators and
    # model_copy runs none; validate_assignment reruns them in place.
    try:
        validate_pattern_at(config, pattern_field, pattern)
    except ValidationError:
        # The recipe could not hold this pattern either.
        warn(
            "this source could not accept that pattern as written, so "
            "the verdicts below judge it exactly as given; the recipe "
            "may normalize it differently"
        )
        return config, pattern
    except Exception as exc:
        # A crashed validator judged nothing: degrade, named as the
        # connector's defect. Labelled, not quoted: its text can carry
        # config values.
        warn(
            f"this source's validator failed while checking that "
            f"pattern ({foreign_label(exc)}), so the verdicts "
            f"below judge it exactly as given; this is a defect in "
            f"the connector, not in the pattern"
        )
        return config, pattern
    # Re-read: a validator may have assigned a new pattern.
    effective = pattern_at(config, pattern_field)
    if effective is None:
        return config, pattern
    if (list(effective.allow), list(effective.deny)) != requested:
        # `tried` echoes what to write in the recipe; say that the verdicts
        # used the normalized form.
        warn(
            f"this source normalized that pattern before "
            f"matching: allow "
            f"{list(effective.allow)}, deny "
            f"{list(effective.deny)}. The verdicts below use "
            f"the normalized form, and `tried` shows what to "
            f"write in the recipe -- which this source would "
            f"normalize the same way."
        )
    return config, effective


def _warn_if_undeclared(
    source_type: str, config: object, kind: str, warn: Callable[[str], None]
) -> None:
    """An undeclared kind with no filter is likelier a typo than a level
    without one. A warning, not an error: the kinds are not fully
    enumerable here."""
    declared = _declared_kinds(source_type, config)
    if declared and kind not in declared:
        warn(
            f"'{source_type}' declares no kind '{kind}' and no filter for it, "
            f"so every name is reported included. Kinds it does declare: "
            f"{', '.join(sorted(declared))}"
        )


def _judge_name(
    judged: _Judged,
    resolution: _Resolution,
    kind: str,
    name: str,
    parent_path: Sequence[str],
    attributes: Mapping[str, str],
    warn: Callable[[str], None],
) -> FilterVerdict:
    """One name's verdict, in the order the module docstring gives (the
    --parent container is judged after, by check_filters)."""
    config = judged.config
    pattern_field = resolution.pattern_field
    prefix = ".".join(parent_path)
    ctx = ClassifyContext(
        config=config,
        name=name,
        fqn=f"{prefix}.{name}" if prefix else name,
        pattern_field=pattern_field,
        parent_path=tuple(parent_path),
        warn=warn,
        kind=kind,
    )
    structural = _switch_verdict(config, kind)
    by_rule = resolution.filtering is Filtering.BY_RULE
    target = name if by_rule else _match_target(config, ctx)
    # Told the switch verdict, to keep or overrule; None leaves the switch,
    # then the pattern, in charge.
    override = _override_verdict(
        config,
        VerdictContext(
            kind=kind,
            name=name,
            target=target,
            parent_path=tuple(parent_path),
            pattern_field=pattern_field,
            structural=structural,
            attributes=dict(attributes),
            warn=warn,
        ),
    )
    if by_rule and override is None and structural is None:
        raise ProbeInternalError(
            f"{type(config).__name__} declares '{kind}' decided by "
            f"{pattern_field}, but its probe_verdict_override gave no verdict "
            f"for '{name}'"
        )
    verdict = (
        override
        or structural
        or (
            # No pattern field means an allow-all pattern, which excludes nothing.
            Verdict.include()
            if pattern_field is None or judged.pattern.allowed(target)
            else Verdict.exclude(pattern_field)
        )
    )
    return FilterVerdict(
        name=name,
        target=verdict.matched_target or target,
        included=verdict.included,
        excluded_by=verdict.excluded_by,
    )


def check_filters(
    source_type: str,
    config_dict: Dict[str, object],
    kind: str,
    parent_path: Sequence[str],
    names: Sequence[str],
    try_allow: Optional[Sequence[str]] = None,
    try_deny: Optional[Sequence[str]] = None,
    attributes: Optional[Sequence[Mapping[str, str]]] = None,
) -> FilterCheckResult:
    """Would the recipe's filters keep these names, and what decided?

    Connection-free by construction: it judges names the caller already has
    (from `probe sql`, or from anywhere else). `try_allow`/`try_deny` answer
    the "what if I changed the pattern" question without editing the recipe.
    """
    config = validate_source_config(
        require_config_class(source_type), source_type, config_dict
    )
    if attributes is not None and len(attributes) != len(names):
        raise ValueError(
            f"{len(attributes)} attribute sets for {len(names)} names; each "
            f"name needs exactly one, even if empty"
        )
    per_name: List[Mapping[str, str]] = (
        list(attributes) if attributes is not None else [{} for _ in names]
    )

    warnings: List[str] = []
    seen: Set[str] = set()

    def warn(message: str) -> None:
        if message not in seen:
            seen.add(message)
            warnings.append(message)

    kind = _canonical_kind(source_type, config, kind)
    resolution = _resolve_filtering(config, kind)
    unresolved = resolution.filtering is Filtering.UNRESOLVED
    if unresolved:
        _warn_if_undeclared(source_type, config, kind, warn)
    judged = _pattern_to_judge(
        config,
        resolution.pattern_field,
        resolution.filtering,
        try_allow,
        try_deny,
        warn,
    )
    results = [
        _judge_name(judged, resolution, kind, name, parent_path, name_attrs, warn)
        for name, name_attrs in zip(names, per_name, strict=True)
    ]

    # Not for a kind nothing resolves: the warning above already says so.
    parent_excluded_by = (
        None
        if unresolved
        else _parent_exclusion(
            source_type, config_dict, judged.config, kind, parent_path, warn
        )
    )
    if parent_excluded_by is not None:
        for result in results:
            result.included = False
            result.excluded_by = parent_excluded_by

    return FilterCheckResult(
        source_type=source_type,
        kind=kind,
        parent_path=list(parent_path),
        pattern_field=resolution.pattern_field,
        filtering=resolution.filtering,
        results=results,
        tried=judged.tried,
        warnings=warnings,
        excluded_by_container=parent_excluded_by is not None,
    )
