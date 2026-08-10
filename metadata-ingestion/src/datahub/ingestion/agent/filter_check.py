from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Set

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.introspect import pattern_field_for_config
from datahub.ingestion.agent.verdicts import (
    UNFILTERED,
    ClassifyContext,
    Verdict,
)
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes


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

    def to_dict(self) -> Dict[str, object]:
        return {
            "source_type": self.source_type,
            "kind": self.kind,
            "parent_path": self.parent_path,
            "pattern_field": self.pattern_field,
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
        return ctx.name
    target = resolver(ctx)
    return target if isinstance(target, str) and target else ctx.name


def _structural_verdict(
    config: Any, kind: str, name: str, pattern_field: Optional[str]
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
    allowed = override(schema=name) if callable(override) else None
    if allowed is not None:
        return Verdict.include() if allowed else Verdict(False, pattern_field)
    return None


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

    pattern_field = pattern_field_for_config(config, kind)
    if pattern_field is None:
        raise ValueError(
            f"'{source_type}' declares no filter for kind '{kind}'; "
            f"nothing would exclude these names at that level"
        )

    tried: Optional[Dict[str, List[str]]] = None
    if try_allow or try_deny:
        pattern = AllowDenyPattern(
            allow=list(try_allow) if try_allow else [".*"],
            deny=list(try_deny) if try_deny else [],
        )
        tried = {"allow": list(pattern.allow), "deny": list(pattern.deny)}
    elif pattern_field == UNFILTERED:
        pattern = AllowDenyPattern.allow_all()
    else:
        pattern = getattr(config, pattern_field)

    warnings: List[str] = []
    seen: Set[str] = set()

    def warn(message: str) -> None:
        if message not in seen:
            seen.add(message)
            warnings.append(message)

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
        target = _match_target(config, kind, ctx)
        verdict = _structural_verdict(config, kind, name, pattern_field) or (
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
        results=results,
        tried=tried,
        warnings=warnings,
    )
