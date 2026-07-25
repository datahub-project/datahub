from typing import Any, List, Optional, Sequence

from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.ingestion.agent.models import ProbeLeafKind, ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import (
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


def _classify_project(
    config: Any, name: str, node_fqn: str, pattern_field: Optional[str]
) -> Verdict:
    # Reuse ingestion's project gate (project_ids + project_id_pattern).
    if not is_project_allowed(config, name):
        return (False, "project_id_pattern")
    return (True, None)


def _classify_dataset(
    config: Any, name: str, node_fqn: str, pattern_field: Optional[str]
) -> Verdict:
    # Same predicate BigQueryFilter.is_dataset_allowed uses.
    project = node_fqn.split(".")[0]
    if not is_schema_allowed(
        config.dataset_pattern, name, project, config.match_fully_qualified_names
    ):
        return (False, "dataset_pattern")
    return (True, None)


def _classify_table(
    config: Any, name: str, node_fqn: str, pattern_field: Optional[str]
) -> Verdict:
    # BigQuery ingestion matches table_pattern against the fully qualified
    # project.dataset.table for both tables and views.
    if not config.table_pattern.allowed(node_fqn):
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
        # dataset_pattern/table_pattern/view_pattern below all match their kind by
        # convention (BigQueryV2Config), but stay explicit here: test_bigquery_probe.py
        # is a guarding test whose config fixture is a plain SimpleNamespace, which
        # resolve_pattern_field can't introspect (no model_fields) — only a real
        # pydantic config resolves by convention.
        ProbeLevel(
            DatasetContainerSubTypes.BIGQUERY_DATASET,
            "dataset_pattern",
            _datasets,
            classify=_classify_dataset,
        ),
        ProbeLevel(
            DatasetSubTypes.TABLE,
            sources=[
                LevelSource(_tables, DatasetSubTypes.TABLE, "table_pattern"),
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
