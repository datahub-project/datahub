from typing import Any, List

from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.ingestion.agent.models import ProbeLeafKind, ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import (
    Verdict,
    column_nodes,
    container_nodes,
    table_nodes,
)
from datahub.ingestion.source.common.gcp_project_filter import is_project_allowed
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)

# BigQuery is a 3-level namespace: project -> dataset -> table -> column, reached
# through the BigQuery client (not SQLAlchemy). Its filters are project_id_pattern
# and dataset_pattern, so it needs its own kinds and labels.
BIGQUERY_PROBE_HIERARCHY: List[ProbeNodeKind] = [
    DatasetContainerSubTypes.BIGQUERY_PROJECT,
    DatasetContainerSubTypes.BIGQUERY_DATASET,
    DatasetSubTypes.TABLE,
    ProbeLeafKind.COLUMN,
]

_VIEW_TABLE_TYPES = ("VIEW", "MATERIALIZED_VIEW")


def list_bigquery_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    client = config.get_bigquery_client()
    try:
        if len(parent_path) == 0:

            def classify_project(name: str, node_fqn: str) -> Verdict:
                # Reuse ingestion's project gate (project_ids + project_id_pattern).
                if not is_project_allowed(config, name):
                    return (False, "project_id_pattern")
                return (True, None)

            names = [p.project_id for p in client.list_projects()]
            nodes, truncated = container_nodes(
                names,
                limit,
                DatasetContainerSubTypes.BIGQUERY_PROJECT,
                "project_id_pattern",
                classify=classify_project,
            )
        elif len(parent_path) == 1:
            project = parent_path[0]

            def classify_dataset(name: str, node_fqn: str) -> Verdict:
                # Same predicate BigQueryFilter.is_dataset_allowed uses.
                if not is_schema_allowed(
                    config.dataset_pattern,
                    name,
                    project,
                    config.match_fully_qualified_names,
                ):
                    return (False, "dataset_pattern")
                return (True, None)

            names = [d.dataset_id for d in client.list_datasets(project)]
            nodes, truncated = container_nodes(
                names,
                limit,
                DatasetContainerSubTypes.BIGQUERY_DATASET,
                "dataset_pattern",
                fqn_prefix=project,
                classify=classify_dataset,
            )
        elif len(parent_path) == 2:
            project, dataset = parent_path[0], parent_path[1]

            def classify_table(name: str, node_fqn: str, is_view: bool) -> Verdict:
                # BigQuery ingestion matches table_pattern against the fully
                # qualified project.dataset.table for both tables and views.
                if not config.table_pattern.allowed(node_fqn):
                    return (False, "table_pattern")
                return (True, None)

            items = list(client.list_tables(f"{project}.{dataset}"))
            views = [t.table_id for t in items if t.table_type in _VIEW_TABLE_TYPES]
            tables = [
                t.table_id for t in items if t.table_type not in _VIEW_TABLE_TYPES
            ]
            nodes, truncated = table_nodes(
                tables,
                views,
                limit,
                fqn_prefix=f"{project}.{dataset}",
                classify=classify_table,
            )
        else:
            project, dataset, table = parent_path[0], parent_path[1], parent_path[2]
            schema = client.get_table(f"{project}.{dataset}.{table}").schema
            cols = [{"name": field.name} for field in schema]
            nodes, truncated = column_nodes(
                cols, limit, fqn_prefix=f"{project}.{dataset}.{table}"
            )
        return ProbeResult(
            source_type="",
            supported=True,
            parent_path=parent_path,
            nodes=nodes,
            truncated=truncated,
        )
    finally:
        client.close()
