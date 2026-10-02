from dataclasses import dataclass, field
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Set

from pydantic import BaseModel, ValidationError

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.config_validation import validate_source_config
from datahub.ingestion.agent.error_policy import foreign_label
from datahub.ingestion.agent.introspect import (
    declared_rule_filtered_kinds,
    pattern_field_for_config,
)
from datahub.ingestion.agent.pattern_path import (
    copy_with_pattern_at,
    pattern_at,
    require_pattern_at,
    unset_block_on,
    validate_pattern_at,
)
from datahub.ingestion.agent.probe_methods import (
    config_class_for,
    declared_kind_overrides,
    list_probe_methods,
)
from datahub.ingestion.agent.verdicts import (
    UNFILTERED,
    ClassifyContext,
    ProbeInternalError,
    Verdict,
    VerdictContext,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)

# The relational kinds. Canonical for every source, declared or not: a
# miscased `--kind schema` is echoed and warned about as `Schema`, and every
# config hook that compares kinds by identity sees that one spelling.
_STANDARD_KINDS = frozenset(
    {
        str(DatasetSubTypes.TABLE),
        str(DatasetSubTypes.VIEW),
        str(DatasetContainerSubTypes.SCHEMA),
        str(DatasetContainerSubTypes.DATABASE),
    }
)


def _kind_switches(config: object) -> Dict[str, str]:
    """kind -> the bool field that, when False, stops ingestion emitting it,
    as the config's probe_kind_switches declares.

    When one is off ingestion emits nothing of that kind, whatever the pattern
    says, so a pattern verdict for it is a verdict ingestion does not make.
    """
    declared = getattr(config, "probe_kind_switches", None)
    if not callable(declared):
        return {}
    return {str(kind): field for kind, field in declared().items()}


@dataclass
class FilterVerdict:
    name: str
    # The exact string the pattern was matched against. Reported because it is
    # usually NOT the bare name -- MySQL matches "schema.table", Postgres
    # "db.schema.table" -- and seeing it is what explains a surprising verdict.
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
    # The config field that decided, so a caller editing its recipe changes the
    # right line. Not always the one named after the kind: MySQL copies
    # table_pattern into view_pattern, and a view is decided by the latter.
    pattern_field: Optional[str]
    results: List[FilterVerdict]
    tried: Optional[Dict[str, List[str]]] = None
    warnings: List[str] = field(default_factory=list)
    # Why pattern_field is null, which the field alone cannot say:
    #   "by_pattern"  -- a field decided; pattern_field names it
    #   "unfiltered"  -- the source declares it filters nothing at this level
    #   "unresolved"  -- no field could be found, and none was declared absent
    # The last is the interesting one: it is what a dropped annotation looks
    # like. Teradata's database_pattern read exactly like Mode's genuinely
    # unfiltered datasets until this told them apart.
    filtering: str = "by_pattern"
    # Set when the verdicts come from an excluded --parent rather than this
    # level's own pattern, so a caller judging the level below does not
    # report the same exclusion twice. Not serialized: `warnings` says it.
    excluded_by_container: bool = False

    def to_dict(self) -> Dict[str, object]:
        return {
            "source_type": self.source_type,
            "kind": self.kind,
            "parent_path": self.parent_path,
            "pattern_field": self.pattern_field,
            "filtering": self.filtering,
            "tried": self.tried,
            "results": [r.to_dict() for r in self.results],
            "warnings": self.warnings,
        }


def _match_target(config: object, ctx: ClassifyContext) -> str:
    """The string this connector's ingestion would filter on for one node.

    The config's probe_match_target answers, for every kind; None or an empty
    string leaves the bare name, which is right for a source whose display
    name is its filter target (Kafka topics, Mode spaces).
    """
    hook = getattr(config, "probe_match_target", None)
    if not callable(hook):
        return ctx.name
    target = hook(ctx=ctx)
    return target if isinstance(target, str) and target else ctx.name


def _switch_verdict(config: object, kind: str) -> Optional[Verdict]:
    """The exclusion a switched-off kind makes, or None."""
    flag = _kind_switches(config).get(kind)
    # getattr, not a hard read: a config without the field never switches the
    # kind off.
    if flag is not None and getattr(config, flag, True) is False:
        return Verdict(False, flag)
    return None


def _parent_exclusion(
    source_type: str,
    config_dict: Dict[str, object],
    config: object,
    kind: str,
    parent_path: Sequence[str],
    warn: Callable[[str], None],
) -> Optional[str]:
    """The pattern that drops the immediate --parent container, if one does.

    Ingestion never reaches anything inside an excluded container: a table
    under a denied schema is not ingested whatever table_pattern says, and
    judging the table's own pattern alone reported it included. The parent is
    judged by check_filters itself, so its own parent is judged in turn and
    every rule the config declares for that kind applies to it.
    """
    if not parent_path:
        return None
    ancestors_for = getattr(config, "probe_ancestor_kinds", None)
    ancestors = ancestors_for(kind) if callable(ancestors_for) else None
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
    if parent.filtering not in ("by_pattern", "by_rule"):
        # Nothing filters that level. Its warnings would be about a kind this
        # source has no filter for, which is noise on this verdict.
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
    """The declared spelling of a kind the caller may have cased differently.

    The `<kind>_pattern` name convention lowercases, while kind switches and
    the config's hooks compare kinds by identity, so `--kind table` and
    `--kind Table` must reach them as one spelling or they answer
    differently. Canonicalised once, here, so everything downstream reads one
    spelling. A kind nothing declares is returned untouched, which keeps the
    "declares no kind" warning below able to fire.
    """
    if kind in _STANDARD_KINDS:
        return kind
    lowered = kind.lower()
    for declared in sorted(_declared_kinds(source_type, config) | _STANDARD_KINDS):
        if declared.lower() == lowered:
            return declared
    return kind


def _declared_kinds(source_type: str, config: object) -> Set[str]:
    """The kinds this source's probe methods name, and the kinds its config
    declares for them (probe_kind_overrides), as far as is knowable without a
    connection.

    Incomplete on purpose, and only ever used to canonicalise and to warn.
    """
    kinds = {spec.kind for spec in list_probe_methods(source_type) if spec.kind}
    return kinds | set(declared_kind_overrides(config).values())


def _override_verdict(config: object, ctx: VerdictContext) -> Optional[Verdict]:
    """The connector's own verdict for one name, when no pattern states it.

    Checked for type and for consistency because Python's truthiness would otherwise let a
    returned bool or tuple decide silently -- `False` reads as "no opinion"
    and the pattern answers instead of the connector.
    """
    override = getattr(config, "probe_verdict_override", None)
    if not callable(override):
        return None
    verdict = override(ctx=ctx)
    if verdict is not None and not isinstance(verdict, Verdict):
        raise ProbeInternalError(
            f"{type(config).__name__}.probe_verdict_override returned "
            f"{type(verdict).__name__}; it must return a Verdict or None"
        )
    if verdict is not None and verdict.included == (verdict.excluded_by is not None):
        # Reported as-is, an included name with a reason, or an excluded one
        # without, contradicts itself in the output and a caller cannot tell
        # which half to believe.
        raise ProbeInternalError(
            f"{type(config).__name__}.probe_verdict_override returned "
            f"included={verdict.included} with excluded_by="
            f"{verdict.excluded_by!r}; an included verdict has no excluded_by "
            f"and an excluded one must name it"
        )
    return verdict


@dataclass(frozen=True)
class _Resolution:
    resolved: Optional[str]
    pattern_field: Optional[str]
    filtering: str


def _resolve_filtering(config: object, kind: str) -> _Resolution:
    rule_field = declared_rule_filtered_kinds(config).get(kind)
    if rule_field is not None:
        # Rules, not a pattern: the connector's override judges each name,
        # and there is no allow/deny list for --try-* to replace.
        return _Resolution(rule_field, rule_field, "by_rule")
    resolved = pattern_field_for_config(config, kind)
    # Both of these report every name included, and the answer is right either
    # way -- the question is "would these be ingested", and where nothing
    # filters them the answer is yes. What differs is whether that is the
    # source's design or a gap, and `filtering` is what says which. They were
    # indistinguishable until a source could declare the first: a level whose
    # annotation had been dropped looked exactly like a level with no filter,
    # which is how Teradata's database_pattern went unnoticed.
    if resolved == UNFILTERED:
        return _Resolution(resolved, None, "unfiltered")
    if resolved is None:
        return _Resolution(None, None, "unresolved")
    return _Resolution(resolved, resolved, "by_pattern")


@dataclass(frozen=True)
class _Judged:
    """The config and pattern the names are judged against, and what was tried."""

    config: BaseModel
    pattern: AllowDenyPattern
    tried: Optional[Dict[str, List[str]]] = None


def _pattern_to_judge(
    config: BaseModel,
    pattern_field: Optional[str],
    filtering: str,
    try_allow: Optional[Sequence[str]],
    try_deny: Optional[Sequence[str]],
    warn: Callable[[str], None],
) -> _Judged:
    """The recipe's pattern, or the --try-* hypothetical applied to a copy."""
    if filtering == "by_rule":
        if try_allow or try_deny:
            warn(
                f"'{pattern_field}' holds rules, not an allow/deny pattern, so "
                f"--try-allow and --try-deny have nothing to replace and were "
                f"ignored; edit {pattern_field} in the recipe to test a change"
            )
        return _Judged(config, AllowDenyPattern.allow_all())
    if pattern_field is None:
        # "unfiltered" or "unresolved": no allow/deny list exists to replace.
        # Judging the hypothetical anyway reported exclusions ingestion can
        # never make, since it has no field to apply them from.
        if try_allow or try_deny:
            warn(
                "--try-allow and --try-deny were ignored: this kind has no "
                "allow/deny pattern to replace"
            )
        return _Judged(config, AllowDenyPattern.allow_all())
    unset = unset_block_on(config, pattern_field) if pattern_field is not None else None
    if unset is not None:
        # A valid recipe that leaves an Optional block out: ingestion applies
        # no filter from it. require_pattern_at would call that a resolution
        # bug and exit 2 on a recipe nothing is wrong with.
        warn(
            f"`{unset}` is unset in this recipe, so nothing at "
            f"`{pattern_field}` filters these"
        )
        if try_allow or try_deny:
            # No block to copy the hypothetical into, and inventing one would
            # judge a recipe the caller did not write.
            warn(
                f"--try-allow and --try-deny were ignored: `{unset}` is unset, "
                f"so there is no pattern at `{pattern_field}` to replace; set "
                f"`{unset}` in the recipe to test a change"
            )
        return _Judged(config, AllowDenyPattern.allow_all())
    recipe_pattern = (
        AllowDenyPattern.allow_all()
        if pattern_field is None
        else require_pattern_at(config, pattern_field)
    )
    if not (try_allow or try_deny):
        return _Judged(config, recipe_pattern)
    # Each half replaces only its own half. `allow=[".*"] if not try_allow`
    # threw the recipe's allow list away whenever only --try-deny was
    # given, so "what if I added this deny" was answered against an
    # allow-all nobody asked for: on a recipe with allow ['^analytics$'],
    # a --try-deny matching nothing flipped every other database from
    # excluded to included, and the caller reads that as "the deny I am
    # testing is harmless". The CLI documents --try-allow as replacing
    # allow and --try-deny as "as --try-allow, for deny"; this is what
    # that says.
    pattern = AllowDenyPattern(
        allow=list(try_allow) if try_allow else list(recipe_pattern.allow),
        deny=list(try_deny) if try_deny else list(recipe_pattern.deny),
    )
    tried = {"allow": list(pattern.allow), "deny": list(pattern.deny)}
    # Snapshotted before the connector's validators can touch `pattern`:
    # they may rewrite it in place, and this is what "as the caller wrote
    # it" has to mean when we compare afterwards.
    requested_allow = list(pattern.allow)
    requested_deny = list(pattern.deny)
    if pattern_field is not None:
        # The hypothetical has to reach the config's probe_verdict_override
        # too, not just the pattern comparison below: an override that reads
        # the pattern off the config (the SQL family's qualified schema match
        # does) and returns a verdict short-circuits the pattern branch, and
        # would otherwise judge the recipe's pattern while `tried` echoes the
        # hypothetical.
        #
        # A shallow copy on purpose: model_copy(deep=True) would clone the
        # cached RDS IAM token manager along with its minted token.
        config = copy_with_pattern_at(config, pattern_field, pattern)
        # ...and then re-validated, because model_copy does NOT rerun
        # validators and some connectors normalize the pattern there.
        # BigQuery's after-validator rewrites an unqualified
        # `dataset_pattern` entry to match `project.dataset`, so the
        # recipe's own `^analytics$` becomes `^.*\.analytics$` and
        # includes `analytics`, while the same string given to --try-allow
        # stayed raw and excluded it. The command answered the opposite of
        # the edit it exists to simulate, on BigQuery's default config.
        #
        # validate_assignment rather than a full model_validate: it reruns
        # the model's after-validators against the instance we already
        # have, so nothing is reconstructed and the token manager above is
        # still shared rather than cloned.
        try:
            validate_pattern_at(config, pattern_field, pattern)
        except ValidationError:
            # A connector whose validator REJECTS the hypothetical is
            # answering the question: the caller cannot write that in the
            # recipe either. Reported rather than silently judged against
            # the un-normalized pattern.
            warn(
                "this source could not accept that pattern as written, so "
                "the verdicts below judge it exactly as given; the recipe "
                "may normalize it differently"
            )
        except Exception as exc:
            # A validator that CRASHED is a different answer, and the
            # message above is the wrong one for it: it tells the caller
            # their pattern was rejected when nothing judged it. Same
            # degrade -- this is a diagnostic command and a hard failure
            # would be worse than a caveated answer -- but named for what
            # happened, so the caller is not sent to fix a pattern that
            # was never the problem. Labelled, not quoted: the validator's
            # message is the connector's text and can carry config values.
            warn(
                f"this source's validator failed while checking that "
                f"pattern ({foreign_label(exc)}), so the verdicts "
                f"below judge it exactly as given; this is a defect in "
                f"the connector, not in the pattern"
            )
        else:
            # Re-read the pattern the config actually ended up with.
            # Pydantic passes the same AllowDenyPattern instance through,
            # so an after-validator that rewrites it IN PLACE is already
            # visible on `pattern` -- but one that ASSIGNS a new pattern
            # would leave `pattern` stale, and the verdicts below are
            # computed from `pattern` rather than from the config.
            effective = pattern_at(config, pattern_field)
            if effective is not None:
                pattern = effective
                if (list(effective.allow), list(effective.deny)) != (
                    requested_allow,
                    requested_deny,
                ):
                    # `tried` deliberately keeps echoing what the caller
                    # asked for: that is the string to put in the recipe,
                    # which would be normalized the same way. But without
                    # saying so the result is unreadable -- BigQuery
                    # reports target `proj.analytics`, allow
                    # `['^analytics$']` and verdict INCLUDED, three facts
                    # that cannot all be true of the pattern as printed.
                    # Only the connector's log said a rewrite happened,
                    # and an agent reads `warnings`, not the log.
                    warn(
                        f"this source normalized that pattern before "
                        f"matching: allow "
                        f"{list(effective.allow)}, deny "
                        f"{list(effective.deny)}. The verdicts below use "
                        f"the normalized form, and `tried` shows what to "
                        f"write in the recipe -- which this source would "
                        f"normalize the same way."
                    )
    return _Judged(config, pattern, tried)


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
    config_cls = config_class_for(source_type)
    if config_cls is None:
        raise ValueError(f"unknown source type '{source_type}'")
    config = validate_source_config(config_cls, source_type, config_dict)
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
    resolved = resolution.resolved
    pattern_field = resolution.pattern_field
    filtering = resolution.filtering

    if resolved is None:
        # A kind the source never declares is more likely a typo than a level
        # without a filter, and answering "all included" for a misspelling would
        # be a wrong answer delivered confidently. It is a warning rather than
        # an error because the kinds are not fully enumerable here -- container
        # kinds are decided per recipe -- so a strict check would refuse valid
        # input, which is the failure being fixed.
        declared = _declared_kinds(source_type, config)
        if declared and kind not in declared:
            warn(
                f"'{source_type}' declares no kind '{kind}' and no filter for it, "
                f"so every name is reported included. Kinds it does declare: "
                f"{', '.join(sorted(declared))}"
            )

    judged = _pattern_to_judge(
        config, pattern_field, filtering, try_allow, try_deny, warn
    )
    config = judged.config
    pattern = judged.pattern
    tried = judged.tried

    prefix = ".".join(parent_path)
    results: List[FilterVerdict] = []
    for name, name_attributes in zip(names, per_name, strict=True):
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
        target = name if filtering == "by_rule" else _match_target(config, ctx)
        # The connector's word on what no single pattern states -- the SQL
        # family's system catalogs and qualified schema names, a view that
        # must also pass table_pattern, a pinned SQL Server database. Told the
        # switch verdict so it can keep or overrule it; None leaves the switch,
        # then the pattern, in charge. A verdict that matched on its own
        # string reports that string as the target.
        override = _override_verdict(
            config,
            VerdictContext(
                kind=kind,
                name=name,
                target=target,
                parent_path=tuple(parent_path),
                pattern_field=pattern_field,
                structural=structural,
                attributes=dict(name_attributes),
                warn=warn,
            ),
        )
        if filtering == "by_rule" and override is None and structural is None:
            raise ProbeInternalError(
                f"{type(config).__name__} declares '{kind}' decided by "
                f"{pattern_field}, but its probe_verdict_override gave no verdict "
                f"for '{name}'"
            )
        verdict = (
            override
            or structural
            or (
                Verdict.include()
                if pattern.allowed(target)
                else Verdict(False, pattern_field)
            )
        )
        results.append(
            FilterVerdict(
                name=name,
                target=verdict.matched_target or target,
                included=verdict.included,
                excluded_by=verdict.excluded_by,
            )
        )

    # Not for a kind nothing resolves: the warning above already says the
    # verdict means nothing, and this one would only repeat it.
    parent_excluded_by = (
        None
        if resolved is None
        else _parent_exclusion(
            source_type, config_dict, config, kind, parent_path, warn
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
        pattern_field=pattern_field,
        filtering=filtering,
        results=results,
        tried=tried,
        warnings=warnings,
        excluded_by_container=parent_excluded_by is not None,
    )
