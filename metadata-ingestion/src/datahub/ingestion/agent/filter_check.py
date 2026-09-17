from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Set

from pydantic import ValidationError

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.introspect import (
    declared_qualifier,
    pattern_field_for_config,
)
from datahub.ingestion.agent.verdicts import (
    UNFILTERED,
    ClassifyContext,
    SchemaMatch,
    Verdict,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)

# Kinds a recipe can switch off wholesale, and the flag that does it. When one
# is false ingestion emits nothing of that kind, whatever the pattern says --
# so reporting a pattern verdict for it is a verdict ingestion does not make,
# and `probe filter --kind View` answered "included: true" for a view that
# `include_views: false` guarantees will never appear.
#
# Only these two. The other include_* flags on these configs
# (include_view_lineage, include_usage_stats, include_table_location_lineage)
# govern what ELSE is emitted about an object, not whether the object itself
# is -- a verdict about a name has nothing to say about them.
# The kinds this module compares by identity -- the structural rules read
# them straight off the enums. Kept beside _INCLUDE_FLAG_FOR_KIND because
# both are the reason --kind has to be canonicalised before anything reads
# it (see _canonical_kind).
_STRUCTURAL_KINDS = frozenset(
    {
        str(DatasetSubTypes.TABLE),
        str(DatasetSubTypes.VIEW),
        str(DatasetContainerSubTypes.SCHEMA),
        str(DatasetContainerSubTypes.DATABASE),
    }
)

_INCLUDE_FLAG_FOR_KIND = {
    str(DatasetSubTypes.TABLE): "include_tables",
    str(DatasetSubTypes.VIEW): "include_views",
}


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


def _match_target(config: Any, kind: str, ctx: ClassifyContext) -> str:
    """The string this connector's ingestion would filter on for one node.

    Resolved by asking the config, never by re-deriving it here: the SQL family
    routes to its own get_identifier (see SQLCommonConfig.probe_match_target),
    and a connector whose display name IS its filter target -- Kafka topics,
    Mode spaces -- needs no hook and falls through to the bare name.
    """
    if kind in (DatasetContainerSubTypes.SCHEMA, DatasetContainerSubTypes.DATABASE):
        # The SQL shim resolves a *table's* identifier (db.schema.table); asked
        # about a container it would build "analytics..public". The hierarchy
        # attached it to Table/View levels only, and containers matched on the
        # bare name -- with Redshift's fully-qualified rule handled by the
        # schema override in _structural_verdict, not here.
        return ctx.name

    resolver = getattr(config, "probe_match_target", None)
    if not callable(resolver):
        # The display name IS what this source filters on -- Kafka topics, Mode
        # spaces. Checked before the parent, because warning here would tell an
        # agent to distrust a correct answer and go looking for a container that
        # does not exist.
        return ctx.name

    if not ctx.parent_path:
        # Without the container we cannot build the identifier ingestion uses:
        # the shim emits ".orders" for MySQL, "db..orders" for Postgres. A pattern
        # judged against that is judged against a string ingestion never sees.
        # Deliberately does not name the object: the reason is connector-wide,
        # and ClassifyContext.warn dedupes by message so one cause is reported
        # once rather than once per name judged.
        ctx.warn(
            "no parent given, so these were judged on their bare names; this "
            "source filters on a qualified identifier, so pass the containing "
            "schema/database to get the verdict ingestion actually makes"
        )
        return ctx.name
    target = resolver(ctx)
    if not isinstance(target, str) or not target:
        # The only degrade here that used to be silent, while the branches on
        # either side both warn -- and for the same reason they do: judging a
        # Postgres table on its bare name when ingestion matches
        # db.schema.table gives the wrong verdict, and a wrong verdict with
        # nothing marking it is indistinguishable from a right one.
        # Deliberately does not name the object, like the no-parent branch
        # above: check_filters' warn closure dedupes on the message string, so
        # embedding ctx.name emits one near-identical warning per table judged.
        # The branch below is the exception and keeps both name and target on
        # purpose -- it reports the malformed identifier it built, which is
        # per-object by nature and useless without them.
        ctx.warn(
            "the connector's identifier resolver returned nothing usable, so "
            "these were judged on their bare names; the verdict may not be the "
            "one ingestion makes"
        )
        return ctx.name
    if target.startswith(".") or ".." in target:
        # A missing component the connector expected. Match on the bare name and
        # flag it rather than report a verdict from an impossible identifier.
        ctx.warn(
            f"could not build a complete identifier for '{ctx.name}' (got "
            f"'{target}'); judged on its bare name instead"
        )
        return ctx.name
    return target


def _needs_parent_for_qualified_match(
    config: Any, kind: str, parent_path: Sequence[str]
) -> bool:
    """Whether this verdict was reached on a bare name that should be qualified.

    Asked of the config directly rather than through a hook. The previous
    version read `probe_schema_needs_parent`, which no config implemented any
    more -- the implementations were added with this warning, removed,
    restored, and removed again over the life of this PR, and the reader
    outlived them. So it always returned False and the warning below could
    never fire, leaving the inverted verdict it exists to explain silent:
    Snowflake with match_fully_qualified_names, and BigQuery by default, judge
    `PUBLIC` against `^MYDB\.PUBLIC$` and report excluded where ingestion
    includes.

    The two conditions are exactly the ones _qualified_schema_match gives up
    on, so they are read from the same place rather than restated by a
    connector.
    """
    if kind != DatasetContainerSubTypes.SCHEMA:
        # Schema only. Database was included here originally and should not
        # have been: database_pattern is matched on the bare database name
        # whatever match_fully_qualified_names says, so the verdict is already
        # the one ingestion makes -- and there is no container above a
        # database to name, so "pass --parent" pointed at nothing. The warning
        # reported correct verdicts as degraded.
        return False
    if not getattr(config, "match_fully_qualified_names", False):
        # The bare name is what ingestion matches too, so nothing is lost.
        return False
    return _qualified_container(config, parent_path) is None


def _structural_verdict(
    config: Any,
    kind: str,
    name: str,
    pattern_field: Optional[str],
    parent_path: Sequence[str] = (),
) -> Optional[Verdict]:
    """Exclusions the source applies before the user's pattern is consulted.

    Kept from the hierarchy's schema classifier rather than dropped with it: a
    system catalog is skipped whatever schema_pattern says, and Redshift's
    match_fully_qualified_names makes ingestion judge "database.schema" instead
    of the bare name. Reporting a plain pattern verdict for either would be a
    verdict ingestion does not make. None means "no structural rule applies --
    fall through to the pattern".
    """
    if kind == DatasetContainerSubTypes.DATABASE:
        default_databases = getattr(config, "default_databases", None)
        if callable(default_databases) and name.lower() in {
            d.lower() for d in default_databases()
        }:
            # Postgres templates, SQL Server's system databases: dropped
            # whatever database_pattern says.
            return Verdict(False, "default_database")
        return None

    flag = _INCLUDE_FLAG_FOR_KIND.get(kind)
    if flag is not None:
        # getattr, not a hard read: a non-SQL config has no such field, and a
        # kind it never emits should fall through to the pattern rather than
        # be reported excluded by a flag that does not exist.
        if getattr(config, flag, True) is False:
            return Verdict(False, flag)
        return None

    if kind != DatasetContainerSubTypes.SCHEMA:
        # Structural rules below are schema-level; a table named like a system
        # schema must not inherit them.
        return None

    default_schemas = getattr(config, "default_schemas", None)
    if callable(default_schemas) and name.lower() in {
        s.lower() for s in default_schemas()
    }:
        return Verdict(False, "default_schema")

    # The connector's own statement first, the shared convention second.
    # Running the convention first meant a connector that declared an override
    # AND enabled match_fully_qualified_names never had its override called --
    # the convention always had an answer, so the explicit declaration was
    # dead. That inverts the hook's own docstring ("checked before the generic
    # check") and the layering this module uses everywhere else:
    # _hinted_pattern_field wins over the name convention "because it is exact
    # by construction". Inert today, since only the base class defines the
    # hook and it returns None -- which is the right time to get the order
    # right, before a connector depends on it.
    override = getattr(config, "probe_schema_verdict_override", None)
    match = (
        override(schema=name, parent_path=tuple(parent_path))
        if callable(override)
        else None
    )
    if match is None:
        match = _qualified_schema_match(config, name, pattern_field, parent_path)
    if match is not None:
        # The override did the matching itself, so it is the only thing that knows
        # which string decided -- carry it out rather than reporting the bare name.
        return Verdict(
            included=match.included,
            excluded_by=None if match.included else pattern_field,
            matched_target=match.target,
        )
    return None


def _qualified_container(config: Any, parent_path: Sequence[str]) -> Optional[str]:
    """The container a qualified schema name is built from.

    The caller's wins: a recipe may span several databases or projects, and
    only the caller knows which one it is asking about. Falling back to a
    single configured container keeps `probe filter` answerable without a
    --parent for the common single-database recipe -- and a connector that
    implies none (Snowflake selects databases by pattern) gets None, which
    leaves the bare-name verdict plus a warning rather than a guess.
    """
    declared, authoritative = declared_qualifier(config)
    if authoritative and declared:
        # Redshift: one database per recipe, so honouring a different
        # --parent would answer about a database it does not read.
        return declared
    if parent_path:
        return parent_path[-1]
    return declared


def _qualified_schema_match(
    config: Any,
    name: str,
    pattern_field: Optional[str],
    parent_path: Sequence[str],
) -> Optional[SchemaMatch]:
    """The verdict for a source that matches schemas on `container.schema`.

    Snowflake, BigQuery and Redshift each declared this as their own
    probe_schema_verdict_override -- 108 lines of three near-identical
    implementations, one of which (Snowflake's) was simply missing for a
    while and gave inverted verdicts nobody noticed.

    Nothing in it was per-connector. `match_fully_qualified_names` is an
    existing *ingestion* field on all three, so the override was restating
    something the config already said; `pattern_field` is resolved above and
    already knows dataset_pattern from schema_pattern; and is_schema_allowed
    is the shared predicate all three were calling anyway. The one genuine
    difference -- which container to assume when the caller names none -- is
    now a one-line probe_default_container, and Snowflake needs none at all.
    """
    if pattern_field is None:
        return None
    if not getattr(config, "match_fully_qualified_names", False):
        # The bare name is what ingestion matches, so the generic classifier
        # above is already right.
        return None
    container = _qualified_container(config, parent_path)
    if container is None:
        return None
    pattern = getattr(config, pattern_field, None)
    if not isinstance(pattern, AllowDenyPattern):
        return None
    # lazy: pattern_utils is cheap, but this keeps the import next to its one use
    from datahub.configuration.pattern_utils import is_schema_allowed

    return SchemaMatch(
        included=is_schema_allowed(pattern, name, container, True),
        target=f"{container}.{name}",
    )


def _canonical_kind(source_type: str, config: Any, kind: str) -> str:
    """The declared spelling of a kind the caller may have cased differently.

    `--kind` was compared two ways at once. The `<kind>_pattern` name
    convention lowercases (introspect._pattern_field_candidates), so
    `--kind table` resolved table_pattern happily -- while every structural
    rule here compares against a StrEnum value and so matched only `Table`.
    The result was two opposite verdicts for the same question, with nothing
    reporting an unrecognised kind because pattern resolution had succeeded:

        --kind Table  (include_tables: false) -> excluded_by include_tables
        --kind table  (include_tables: false) -> included
        --kind Schema (redshift, qualified)   -> target dev.public, included
        --kind schema (redshift, qualified)   -> target public, excluded

    Canonicalised once, here, so everything downstream reads one spelling. A
    kind nothing declares is returned untouched, which is what keeps the
    "declares no kind" warning below able to fire.
    """
    if kind in _STRUCTURAL_KINDS:
        return kind
    lowered = kind.lower()
    for declared in sorted(_declared_kinds(source_type, config) | _STRUCTURAL_KINDS):
        if declared.lower() == lowered:
            return declared
    return kind


def _declared_kinds(source_type: str, config: Any) -> Set[str]:
    """The kinds this source's probe methods name, as far as is knowable without
    a connection.

    Incomplete on purpose, and only ever used to warn. `containers` takes its
    kind from the recipe (schema on a three-tier source, database on a two-tier
    one), which is why probe_container_kind is consulted here rather than read
    off a provider instance -- building one of those needs a connection.
    """
    from datahub.ingestion.agent.probe_methods import list_probe_methods

    kinds = {spec.kind for spec in list_probe_methods(source_type) if spec.kind}
    container_kind = getattr(config, "probe_container_kind", None)
    if callable(container_kind):
        try:
            kinds.add(str(container_kind()))
        except Exception:
            # A config that cannot answer it does not get a worse warning.
            pass
    return kinds


def check_filters(
    source_type: str,
    config_dict: Dict[str, object],
    kind: str,
    parent_path: Sequence[str],
    names: Sequence[str],
    try_allow: Optional[Sequence[str]] = None,
    try_deny: Optional[Sequence[str]] = None,
) -> FilterCheckResult:
    """Would the recipe's filters keep these names, and what decided?

    Connection-free by construction: it judges names the caller already has
    (from `probe sql`, or from anywhere else). `try_allow`/`try_deny` answer
    the "what if I changed the pattern" question without editing the recipe.
    """
    from datahub.ingestion.agent.probe_methods import config_class_for

    config_cls = config_class_for(source_type)
    if config_cls is None:
        raise ValueError(f"unknown source type '{source_type}'")
    config = config_cls.model_validate(config_dict)

    warnings: List[str] = []
    seen: Set[str] = set()

    def warn(message: str) -> None:
        if message not in seen:
            seen.add(message)
            warnings.append(message)

    kind = _canonical_kind(source_type, config, kind)
    resolved = pattern_field_for_config(config, kind)
    # Both of these report every name included, and the answer is right either
    # way -- the question is "would these be ingested", and where nothing
    # filters them the answer is yes. What differs is whether that is the
    # source's design or a gap, and `filtering` is what says which. They were
    # indistinguishable until a source could declare the first: a level whose
    # annotation had been dropped looked exactly like a level with no filter,
    # which is how Teradata's database_pattern went unnoticed.
    pattern_field = None if resolved == UNFILTERED else resolved
    if resolved == UNFILTERED:
        filtering = "unfiltered"
    elif resolved is None:
        filtering = "unresolved"
    else:
        filtering = "by_pattern"

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

    tried: Optional[Dict[str, List[str]]] = None
    recipe_pattern = (
        AllowDenyPattern.allow_all()
        if pattern_field is None
        else getattr(config, pattern_field)
    )
    if try_allow or try_deny:
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
        if pattern_field is not None:
            # The hypothetical has to reach the STRUCTURAL rules too, not just
            # the pattern comparison below. Redshift's and BigQuery's
            # probe_schema_verdict_override read the pattern off the config
            # themselves (they call is_schema_allowed with it), and the
            # verdict they return short-circuits the pattern branch -- so
            # --try-allow was silently ignored for every source that declares
            # one, which on BigQuery is every schema query, since
            # match_fully_qualified_names defaults True. `tried` still echoed
            # the hypothetical, so the result claimed to have applied it.
            #
            # A shallow copy on purpose: model_copy(deep=True) would clone the
            # cached RDS IAM token manager along with its minted token.
            config = config.model_copy(update={pattern_field: pattern})
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
                type(config).__pydantic_validator__.validate_assignment(
                    config, pattern_field, pattern
                )
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
                # was never the problem.
                warn(
                    f"this source's validator failed while checking that "
                    f"pattern ({type(exc).__name__}: {exc}), so the verdicts "
                    f"below judge it exactly as given; this is a defect in "
                    f"the connector, not in the pattern"
                )
    else:
        pattern = recipe_pattern

    prefix = ".".join(parent_path)
    results: List[FilterVerdict] = []
    for name in names:
        ctx = ClassifyContext(
            config=config,
            name=name,
            fqn=f"{prefix}.{name}" if prefix else name,
            pattern_field=pattern_field,
            parent_path=tuple(parent_path),
            warn=warn,
        )
        structural = _structural_verdict(config, kind, name, pattern_field, parent_path)
        if structural is None and _needs_parent_for_qualified_match(
            config, kind, parent_path
        ):
            # The connector has a qualified rule for this level but could not
            # apply it: it needs to know which container, and none was given.
            # Without this the caller sees every name excluded -- the bare name
            # judged against a qualified pattern -- and nothing saying why.
            # Deduped by message, so this reports once rather than per name.
            warn(
                "this source matches containers on a qualified name and could "
                "not tell which one you mean, so these were judged on their "
                "bare names and will mostly read as excluded; pass --parent to "
                "get the verdict ingestion actually makes"
            )
        # A structural verdict that matched on its own string reports that string;
        # otherwise the target is resolved the usual way and the pattern decides.
        target = (
            structural.matched_target
            if (structural and structural.matched_target)
            else _match_target(config, kind, ctx)
        )
        verdict = structural or (
            Verdict.include()
            if pattern.allowed(target)
            else Verdict(False, pattern_field)
        )
        results.append(
            FilterVerdict(
                name=name,
                target=target,
                included=verdict.included,
                excluded_by=verdict.excluded_by,
            )
        )

    return FilterCheckResult(
        source_type=source_type,
        kind=kind,
        parent_path=list(parent_path),
        pattern_field=pattern_field,
        filtering=filtering,
        results=results,
        tried=tried,
        warnings=warnings,
    )
