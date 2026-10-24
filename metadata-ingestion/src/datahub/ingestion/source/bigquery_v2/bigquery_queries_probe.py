"""Probe support for `bigquery-queries`: the `bigquery` provider over the
recipe's nested `connection`, and the verdicts this source's filters give.

The verdicts differ from the `bigquery` source's because this source never
lists the catalog. It judges each table a query names through
BigQueryQueriesExtractor.is_temp_table and is_allowed_table, on the
`project.dataset.table` name BigqueryTableIdentifier normalizes (shard
suffixes, partition and snapshot decorators). BigQueryFilter.is_allowed
judges every object as a table, so table_pattern applies to views too and
view_pattern is never read.
"""

from typing import TYPE_CHECKING, Optional, Sequence, Union

from datahub.ingestion.agent.verdicts import (
    ClassifyContext,
    Verdict,
    VerdictContext,
    ancestors_in,
    parent_required,
    pattern_verdict,
)
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigqueryTableIdentifier,
)
from datahub.ingestion.source.bigquery_v2.bigquery_connection import (
    BigQueryConnectionConfig,
)
from datahub.ingestion.source.bigquery_v2.bigquery_probe import BigQueryMetadataProbe
from datahub.ingestion.source.bigquery_v2.common import _SYSTEM_TABLES_ALLOW_DENY
from datahub.ingestion.source.common.gcp_project_filter import is_project_allowed
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sql.sql_probe_verdicts import (
    qualified_table_target,
    qualifying_container,
    sql_structural_verdict,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.bigquery_v2.bigquery_queries import (
        BigQueryQueriesSourceConfig,
    )

_CONTAINERS = (
    DatasetContainerSubTypes.BIGQUERY_PROJECT,
    DatasetContainerSubTypes.SCHEMA,
)
_DATASETS = (DatasetSubTypes.TABLE, DatasetSubTypes.VIEW)

# Ingestion's own rules beyond the patterns, named in `excluded_by`.
SYSTEM_TABLE_RULE = "system_table"
TEMP_DATASET_RULE = "temp_table_dataset_prefix"

_VIEWS_JUDGED_AS_TABLES = (
    "bigquery-queries judges every object in the query log as a table, so "
    "views are judged against table_pattern and view_pattern has no effect; "
    "--try-allow/--try-deny with --kind View replace view_pattern and so "
    "change nothing here, use --kind Table to try a table_pattern change"
)


class BigQueryQueriesMetadataProbe(BigQueryMetadataProbe):
    """The `bigquery` provider, built from the recipe's `connection` block,
    so credentials resolve exactly as this source's ingestion resolves them."""

    @classmethod
    def for_config(
        cls,
        config: Union[BigQueryConnectionConfig, "BigQueryQueriesSourceConfig"],
    ) -> BigQueryMetadataProbe:
        connection = (
            config
            if isinstance(config, BigQueryConnectionConfig)
            else config.connection
        )
        return super().for_config(connection)


def ancestor_kinds(kind: str) -> Optional[Sequence[str]]:
    return ancestors_in(_CONTAINERS, kind, _DATASETS)


def match_target(
    config: "BigQueryQueriesSourceConfig", ctx: ClassifyContext
) -> Optional[str]:
    """`project.dataset.table` as BigqueryTableIdentifier prints it, which
    is what table_pattern is matched against; None (the bare name) for the
    containers, whose qualified Schema verdict sql_structural_verdict gives."""
    if ctx.kind not in _DATASETS or parent_required(ctx):
        return None
    dataset = ctx.parent_path[-1]
    # project_ids is a Qualifier: one listed project pins the container when
    # the caller passes only the dataset.
    project = qualifying_container(config, ctx.parent_path[:-1])
    qualified = qualified_table_target(project, dataset, ctx.name, ctx.warn)
    if qualified is None:
        return None
    return _table_name(config, BigqueryTableIdentifier.from_string_name(qualified))


def _table_name(
    config: "BigQueryQueriesSourceConfig", table: BigqueryTableIdentifier
) -> str:
    """str(table) as ingestion prints it. BigqueryTableIdentifier.get_table_name
    appends a shard suffix held on the class, which BigQueryIdentifierBuilder
    sets process-wide from enable_legacy_sharded_table_support; the probe
    takes the suffix from this recipe instead of changing that shared state."""
    name = f"{table.project_id}.{table.dataset}.{table.get_table_display_name()}"
    if table.is_sharded_table() and not config.enable_legacy_sharded_table_support:
        name += BigqueryTableIdentifier.DEFAULT_BQ_SHARDED_TABLE_SUFFIX
    return name


def verdict(
    config: "BigQueryQueriesSourceConfig", ctx: VerdictContext
) -> Optional[Verdict]:
    """Ingestion's decisions no single pattern states, in the order
    SqlParsingAggregator.is_allowed_table makes them: a temporary table
    first, then BigQueryFilter.is_allowed."""
    if ctx.structural is not None:
        return None
    if ctx.kind == DatasetContainerSubTypes.BIGQUERY_PROJECT:
        return _project_verdict(config, ctx.name)
    if ctx.kind not in _DATASETS:
        return sql_structural_verdict(config, ctx)
    if len(ctx.target.split(".", maxsplit=2)) == 3:
        table = BigqueryTableIdentifier.from_string_name(ctx.target)
        if table.dataset.startswith(config.temp_table_dataset_prefix):
            return Verdict.exclude(TEMP_DATASET_RULE)
        # ctx.target is already the printed name BigQueryFilter.is_allowed
        # matches this pattern against.
        if not _SYSTEM_TABLES_ALLOW_DENY.allowed(ctx.target):
            return Verdict.exclude(SYSTEM_TABLE_RULE)
    if ctx.kind == DatasetSubTypes.VIEW:
        ctx.warn(_VIEWS_JUDGED_AS_TABLES)
        return pattern_verdict(config, "table_pattern", ctx.target)
    return None


def _project_verdict(
    config: "BigQueryQueriesSourceConfig", project_id: str
) -> Optional[Verdict]:
    # project_ids, when set, replaces project_id_pattern entirely
    # (is_project_allowed), so the pattern step alone would misjudge it.
    if not config.project_ids:
        return None
    if is_project_allowed(config, project_id):
        return Verdict.include()
    return Verdict.exclude("project_ids")
