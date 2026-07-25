from typing import Any, List, Sequence

from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.ingestion.agent.models import ProbeLeafKind, ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import (
    ClassifyContext,
    ClientProbe,
    LevelSource,
    ProbeLevel,
    Verdict,
)
from datahub.ingestion.source.common.gcp_project_filter import is_project_allowed
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)

_VIEW_TABLE_TYPES = ("VIEW", "MATERIALIZED_VIEW")


def _projects(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return [p.project_id for p in client.list_projects()]


def _datasets(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return [d.dataset_id for d in client.list_datasets(parent_path[0])]


def _tables(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    items = client.list_tables(f"{parent_path[0]}.{parent_path[1]}")
    return [t.table_id for t in items if t.table_type not in _VIEW_TABLE_TYPES]


def _views(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    items = client.list_tables(f"{parent_path[0]}.{parent_path[1]}")
    return [t.table_id for t in items if t.table_type in _VIEW_TABLE_TYPES]


def _columns(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    project, dataset, table = parent_path[0], parent_path[1], parent_path[2]
    schema = client.get_table(f"{project}.{dataset}.{table}").schema
    return [field.name for field in schema]


def _classify_project(ctx: ClassifyContext) -> Verdict:
    # Reuse ingestion's project gate (project_ids + project_id_pattern).
    if not is_project_allowed(ctx.config, ctx.name):
        return (False, "project_id_pattern")
    return (True, None)


def _classify_dataset(ctx: ClassifyContext) -> Verdict:
    # Same predicate BigQueryFilter.is_dataset_allowed uses.
    if not is_schema_allowed(
        ctx.config.dataset_pattern,
        ctx.name,
        ctx.parent_path[0],
        ctx.config.match_fully_qualified_names,
    ):
        return (False, "dataset_pattern")
    return (True, None)


def _classify_table(ctx: ClassifyContext) -> Verdict:
    # BigQuery ingestion matches table_pattern against the fully qualified
    # project.dataset.table for both tables and views.
    if not ctx.config.table_pattern.allowed(ctx.fqn):
        return (False, "table_pattern")
    return (True, None)


# BigQuery is a 3-level namespace: project -> dataset -> table -> column, reached
# through the BigQuery client (not SQLAlchemy). Its filters are project_id_pattern
# and dataset_pattern, so it needs its own kinds and labels.
BIGQUERY_PROBE = ClientProbe(
    client_factory=lambda config: config.get_bigquery_client(),
    close=lambda client: client.close(),
    levels=[
        # Name skew: the kind is "Project", but the field is project_id_pattern, not
        # project_pattern.
        ProbeLevel(
            DatasetContainerSubTypes.BIGQUERY_PROJECT,
            "project_id_pattern",
            _projects,
            classify=_classify_project,
        ),
        ProbeLevel(
            DatasetContainerSubTypes.BIGQUERY_DATASET,
            list_names=_datasets,
            classify=_classify_dataset,
        ),
        ProbeLevel(
            DatasetSubTypes.TABLE,
            sources=[
                LevelSource(_tables, DatasetSubTypes.TABLE),
                # Stays explicit: _classify_table always filters both tables and
                # views against config.table_pattern (BigQuery has no view-specific
                # verdict), so view_pattern here is purely a reporting label on the
                # node, not a field the verdict itself reads — test_bigquery_probe.py
                # (a guarding test) pins that quirk with a config fixture that never
                # sets a view_pattern attribute at all, so it can't resolve.
                LevelSource(_views, DatasetSubTypes.VIEW, "view_pattern"),
            ],
            classify=_classify_table,
        ),
        ProbeLevel(ProbeLeafKind.COLUMN, list_names=_columns),
    ],
)

BIGQUERY_PROBE_HIERARCHY: List[ProbeNodeKind] = BIGQUERY_PROBE.hierarchy()


def list_bigquery_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return BIGQUERY_PROBE.list_children(config, parent_path, limit)
