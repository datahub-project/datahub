from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Set

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.introspect import pattern_field_for_config
from datahub.ingestion.agent.verdicts import (
    UNFILTERED,
    ClassifyContext,
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


def _override_needs_parent(config: Any, kind: str) -> bool:
    """Whether this source would qualify a container name if it knew the parent.

    Asked outright. The first version inferred it by calling the verdict hook
    with a fake schema name and reading a None answer as "needs a parent" --
    but None also means "no override applies", which is Redshift's answer
    whenever match_fully_qualified_names is off, its default. Every default
    Redshift `probe filter --kind Schema` then warned that the source matches
    on a qualified name and the caller should pass --parent, when the bare-name
    verdict was already the one ingestion makes.

    A warning that fires when nothing is wrong is worse than none: it is the
    one on the screen when a real one appears.
    """
    if kind not in (DatasetContainerSubTypes.SCHEMA, DatasetContainerSubTypes.DATABASE):
        return False
    asks = getattr(config, "probe_schema_needs_parent", None)
    if not callable(asks):
        return False
    try:
        return bool(asks())
    except Exception:
        # A source that cannot answer this is not one to warn about.
        return False


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

    override = getattr(config, "probe_schema_verdict_override", None)
    # The container is passed because the qualified form needs one and only the
    # caller knows which was asked about. Redshift reads its single configured
    # database and ignores this; BigQuery cannot, because a recipe may name
    # several projects and the pattern is matched against "project.dataset".
    match = (
        override(schema=name, parent_path=tuple(parent_path))
        if callable(override)
        else None
    )
    if match is not None:
        # The override did the matching itself, so it is the only thing that knows
        # which string decided -- carry it out rather than reporting the bare name.
        return Verdict(
            included=match.included,
            excluded_by=None if match.included else pattern_field,
            matched_target=match.target,
        )
    return None


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
        if (
            structural is None
            and not parent_path
            and _override_needs_parent(config, kind)
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
