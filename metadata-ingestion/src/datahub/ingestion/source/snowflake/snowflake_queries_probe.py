"""Probe support for `snowflake-queries`: the `snowflake` provider over the
recipe's nested `connection`, and the verdicts this source's filters give.

The verdicts differ from the `snowflake` source's because this source never
lists the catalog. It judges each object a query-log row names, as the
`db.schema.table` identifier it puts in the URN, through
SnowflakeQueriesExtractor.is_temp_table and is_allowed_table. Both judge every
object as a table, so table_pattern applies to views too and view_pattern is
never read.
"""

from typing import TYPE_CHECKING, Optional, Sequence, Union

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.ingestion.agent.verdicts import (
    ClassifyContext,
    Verdict,
    VerdictContext,
    ancestors_in,
    parent_required,
    pattern_verdict,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnectionConfig,
)
from datahub.ingestion.source.snowflake.snowflake_probe import SnowflakeMetadataProbe
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
    _is_sys_table,
)
from datahub.ingestion.source.sql.sql_probe_verdicts import (
    qualified_table_target,
    sql_structural_verdict,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.snowflake.snowflake_queries import (
        SnowflakeQueriesSourceConfig,
    )

_CONTAINERS = (DatasetContainerSubTypes.DATABASE, DatasetContainerSubTypes.SCHEMA)
_DATASETS = (DatasetSubTypes.TABLE, DatasetSubTypes.VIEW)

# Ingestion's own rules beyond the patterns, named in `excluded_by`.
SYSTEM_TABLE_RULE = "system_table"
TEMPORARY_TABLES_RULE = "temporary_tables_pattern"

_VIEWS_JUDGED_AS_TABLES = (
    "snowflake-queries judges every object in the query log as a table, so "
    "views are judged against table_pattern and view_pattern has no effect; "
    "--try-allow/--try-deny with --kind View replace view_pattern and so "
    "change nothing here, use --kind Table to try a table_pattern change"
)


class SnowflakeQueriesMetadataProbe(SnowflakeMetadataProbe):
    """The `snowflake` provider, built from the recipe's `connection` block,
    so a probe authenticates exactly as this source's ingestion does."""

    @classmethod
    def for_config(
        cls,
        config: Union[SnowflakeConnectionConfig, "SnowflakeQueriesSourceConfig"],
    ) -> SnowflakeMetadataProbe:
        connection = (
            config
            if isinstance(config, SnowflakeConnectionConfig)
            else config.connection
        )
        return super().for_config(connection)


def _identifiers(config: "SnowflakeQueriesSourceConfig") -> SnowflakeIdentifierBuilder:
    # The report only collects warnings ingestion would log; the probe has no
    # run to attach them to.
    return SnowflakeIdentifierBuilder(
        identifier_config=config, structured_reporter=SourceReport()
    )


def ancestor_kinds(kind: str) -> Optional[Sequence[str]]:
    return ancestors_in(_CONTAINERS, kind, _DATASETS)


def match_target(
    config: "SnowflakeQueriesSourceConfig", ctx: ClassifyContext
) -> Optional[str]:
    """The string ingestion matches each level's pattern against: the dataset
    identifier it puts in the URN, or that identifier's database or schema
    part."""
    if ctx.kind == DatasetContainerSubTypes.DATABASE:
        return _spelled(config, config.database_pattern, ctx.name)
    if ctx.kind == DatasetContainerSubTypes.SCHEMA:
        # Qualified with the database, when match_fully_qualified_names asks
        # for it, by verdict().
        return _spelled(config, config.schema_pattern, ctx.name)
    if ctx.kind not in _DATASETS or parent_required(ctx):
        return None
    if len(ctx.parent_path) < 2:
        # Warns that the database is missing; None leaves the bare name.
        return qualified_table_target(None, ctx.parent_path[-1], ctx.name, ctx.warn)
    database, schema = ctx.parent_path[-2:]
    return _identifiers(config).get_dataset_identifier(
        table_name=ctx.name, schema_name=schema, db_name=database
    )


def _spelled(
    config: "SnowflakeQueriesSourceConfig", pattern: AllowDenyPattern, name: str
) -> str:
    """A container name as ingestion matches it: a part of the identifier it
    folds with convert_urns_to_lowercase. Only a case-sensitive pattern can
    tell the difference, so otherwise the caller's spelling is kept and the
    reported target reads as they gave it."""
    if pattern.ignoreCase:
        return name
    return _identifiers(config).snowflake_identifier(name)


def verdict(
    config: "SnowflakeQueriesSourceConfig", ctx: VerdictContext
) -> Optional[Verdict]:
    """Ingestion's decisions no single pattern states, in the order
    SqlParsingAggregator.is_allowed_table makes them: a temporary table first,
    then SnowflakeFilter.is_dataset_pattern_allowed with the TABLE domain."""
    if ctx.structural is not None:
        return None
    if ctx.kind == DatasetContainerSubTypes.SCHEMA:
        return _schema_verdict(config, ctx)
    if ctx.kind not in _DATASETS:
        return None
    if any(
        pattern.match(ctx.target)
        for pattern in config._compiled_temporary_tables_pattern
    ):
        return Verdict.exclude(TEMPORARY_TABLES_RULE)
    if _is_sys_table(ctx.target):
        return Verdict.exclude(SYSTEM_TABLE_RULE)
    if ctx.kind == DatasetSubTypes.VIEW:
        ctx.warn(_VIEWS_JUDGED_AS_TABLES)
        return pattern_verdict(config, "table_pattern", ctx.target)
    return None


def _schema_verdict(
    config: "SnowflakeQueriesSourceConfig", ctx: VerdictContext
) -> Optional[Verdict]:
    """is_schema_allowed against `database.schema`, the spelling
    SnowflakeFilter.is_dataset_pattern_allowed passes it."""
    if not config.match_fully_qualified_names or not ctx.parent_path:
        # Unqualified, or no database to qualify with, which this reports.
        return sql_structural_verdict(config, ctx)
    database = _spelled(config, config.schema_pattern, ctx.parent_path[-1])
    included = is_schema_allowed(config.schema_pattern, ctx.target, database, True)
    return Verdict(
        included=included,
        excluded_by=None if included else ctx.pattern_field,
        matched_target=f"{database}.{ctx.target}",
    )
