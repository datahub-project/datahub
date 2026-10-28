"""The SQL family's probe verdicts that no single allow/deny pattern states:
system catalogs ingestion never lists, and schemas judged on their qualified
name.

Apart from sql_config so that ingestion does not import the probe framework:
SQLCommonConfig's hooks import this module only when the probe calls them, and
a connector calling these helpers from its own hooks imports them there too.
"""

from typing import Callable, Optional, Sequence

from datahub.configuration.common import ConfigModel
from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.ingestion.agent.declarations import declared_qualifier, qualifier_field
from datahub.ingestion.agent.pattern_path import pattern_at
from datahub.ingestion.agent.verdicts import (
    ProbeInternalError,
    Verdict,
    VerdictContext,
    require_container,
)
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes


def _in_defaults(config: ConfigModel, hook: str, name: str) -> bool:
    # Read by name: default_databases is declared only by sources that drop
    # databases, and callers outside SQLCommonConfig pass their own configs.
    defaults = getattr(config, hook, None)
    return callable(defaults) and name.lower() in {d.lower() for d in defaults()}


def qualifying_container(
    config: ConfigModel, parent_path: Sequence[str]
) -> Optional[str]:
    """The container a schema name is qualified with, or None.

    A Qualifier(authoritative=True) field wins (the recipe reads that one
    container whatever --parent says); then the caller's --parent, since a
    recipe may span several; then a Qualifier field pinning a single one.
    """
    declared, authoritative = declared_qualifier(config)
    if authoritative and declared:
        return declared
    if parent_path:
        return parent_path[-1]
    return declared


_NO_CONTAINER_WARNING = (
    "no parent container given, so these were judged on "
    "'schema.entity'; this source matches a fully qualified name, so "
    "pass the containing database/project to get the verdict "
    "ingestion actually makes"
)


def qualified_table_target(
    container: Optional[str],
    schema: str,
    entity: str,
    warn: Callable[[str], None],
    *,
    config: Optional[ConfigModel] = None,
    kind: str = "",
    pattern_field: Optional[str] = None,
    pin_field: Optional[str] = None,
) -> Optional[str]:
    """`container.schema.entity`: what a source whose tables live under a
    database or project matches table_pattern and view_pattern against.

    Without a container an invented one would judge another database's object,
    so there is no target to build. A caller that passes `config` and
    `pattern_field` gets require_container's rule instead: refuse rather than
    let the shim judge `schema.entity` against a pattern written for three
    parts. Omitting them keeps the warn-and-degrade this has always done, so
    an out-of-tree caller is unaffected; both in-tree callers opt in.
    """
    if container:
        return f"{container}.{schema}.{entity}"
    if config is None or pattern_field is None:
        # Not opted in. Refused rather than half-applied: a caller that passed
        # one and forgot the other would silently get the old wrong verdict.
        if config is not None or pattern_field is not None:
            raise ProbeInternalError(
                "qualified_table_target needs both `config` and "
                "`pattern_field` to judge whether a missing container matters; "
                "pass both or neither"
            )
        warn(_NO_CONTAINER_WARNING)
        return None
    # None after its own warning when the pattern filters nothing, so the two
    # levels say the same thing about the same situation; otherwise a refusal.
    require_container(
        container,
        config=config,
        kind=kind,
        pattern_field=pattern_field,
        shape="<container>.<schema>.<entity>",
        pin_field=pin_field,
        warn=warn,
    )
    return None


def _qualified_schema_verdict(
    config: ConfigModel, ctx: VerdictContext
) -> Optional[Verdict]:
    """The verdict on `container.schema`, which is what ingestion matches
    schema_pattern against once match_fully_qualified_names is on."""
    if not getattr(config, "match_fully_qualified_names", False):
        return None
    # Resolved before the container: a kind nothing filters must not be refused
    # for want of a container that would decide nothing.
    if ctx.pattern_field is None:
        return None
    pattern = pattern_at(config, ctx.pattern_field)
    if pattern is None:
        return None
    container = require_container(
        qualifying_container(config, ctx.parent_path),
        config=config,
        kind=ctx.kind,
        pattern_field=ctx.pattern_field,
        shape="<container>.<schema>",
        pin_field=qualifier_field(config),
        warn=ctx.warn,
    )
    if container is None:
        return None
    included = is_schema_allowed(pattern, ctx.name, container, True)
    return Verdict(
        included=included,
        excluded_by=None if included else ctx.pattern_field,
        matched_target=f"{container}.{ctx.name}",
    )


def sql_structural_verdict(
    config: ConfigModel, ctx: VerdictContext
) -> Optional[Verdict]:
    """The SQL family's verdicts that no single allow/deny pattern states.

    A database in default_databases() or a schema in default_schemas() is
    dropped whatever the pattern says: ingestion never lists those system
    catalogs. A schema on a source with match_fully_qualified_names on is
    judged as `container.schema`, and the verdict reports that string as its
    target. None leaves the pattern to decide.

    SQLFilterConfig's probe_verdict_override; a config with rules of its own
    returns this for the names those rules leave alone. A kind-switch
    exclusion already in ctx.structural stands, so this returns None for it.
    """
    if ctx.structural is not None:
        return None
    if ctx.kind == DatasetContainerSubTypes.DATABASE:
        if _in_defaults(config, "default_databases", ctx.name):
            return Verdict.exclude("default_database")
        return None
    if ctx.kind != DatasetContainerSubTypes.SCHEMA:
        return None
    if _in_defaults(config, "default_schemas", ctx.name):
        return Verdict.exclude("default_schema")
    return _qualified_schema_verdict(config, ctx)
