from typing import Any, Dict, List, Optional, Set, Tuple
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.bigquery.table import TableListItem

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.bigquery_v2.bigquery import BigqueryV2Source
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryDataset

PROJECT = "my-project"
DATASET = "my_dataset"

# One object of every type list_tables reports, as (table_id, API type).
OBJECTS = [
    ("orders", "TABLE"),
    ("orders_ext", "EXTERNAL"),
    ("orders_v", "VIEW"),
    ("orders_mv", "MATERIALIZED_VIEW"),
    ("orders_snap", "SNAPSHOT"),
]


def _item(table_id: str, table_type: str) -> TableListItem:
    return TableListItem(
        {
            "tableReference": {
                "projectId": PROJECT,
                "datasetId": DATASET,
                "tableId": table_id,
            },
            "type": table_type,
        }
    )


def _discovered(
    recipe: Dict[str, Any], objects: Optional[List[Tuple[str, str]]] = None
) -> Set[str]:
    """Run _process_schema on one dataset and return the table ids in table_refs.

    The full table/view/snapshot processors are fed empty results, so every ref
    found here came from list_tables discovery.
    """
    with (
        patch.object(BigQueryV2Config, "get_bigquery_client"),
        patch.object(BigQueryV2Config, "get_projects_client"),
    ):
        config = BigQueryV2Config.model_validate({"project_id": PROJECT, **recipe})
        source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
    schema_gen = source.bq_schema_extractor

    schema_api = MagicMock()
    schema_api.list_tables.return_value = [_item(*o) for o in objects or OBJECTS]
    schema_api.get_columns_for_dataset.return_value = {}
    schema_api.get_views_for_dataset.return_value = []
    schema_api.get_snapshots_for_dataset.return_value = []
    schema_gen.schema_api = schema_api

    with patch.object(schema_gen, "get_tables_for_dataset", return_value=[]):
        list(
            schema_gen._process_schema(
                project_id=PROJECT,
                bigquery_dataset=BigqueryDataset(name=DATASET),
                db_tables={},
                db_views={},
                db_snapshots={},
            )
        )
    return {ref.rsplit("/", 1)[-1] for ref in schema_gen.table_refs}


ALL = {table_id for table_id, _ in OBJECTS}
TABLES = {"orders", "orders_ext"}
VIEWS = {"orders_v", "orders_mv"}
SNAPSHOTS = {"orders_snap"}


@pytest.mark.parametrize(
    ("recipe", "expected"),
    [
        # Everything ingested: the full processors own every type, nothing is listed.
        ({}, set()),
        # A lineage-only recipe that skips table schemas still needs the tables.
        ({"include_tables": False}, TABLES),
        ({"include_views": False}, VIEWS),
        ({"include_table_snapshots": False}, SNAPSHOTS),
        (
            {"include_tables": False, "include_table_snapshots": False},
            TABLES | SNAPSHOTS,
        ),
        # Schema metadata off (also what tables + views off switches to): list everything.
        ({"include_schema_metadata": False}, ALL),
        ({"include_tables": False, "include_views": False}, ALL),
    ],
)
def test_table_refs_cover_types_the_recipe_does_not_ingest(
    recipe: Dict[str, Any], expected: Set[str]
) -> None:
    assert _discovered(recipe) == expected


def test_no_discovery_when_lineage_and_usage_are_off() -> None:
    assert (
        _discovered(
            {
                "include_tables": False,
                "include_table_lineage": False,
                "include_usage_statistics": False,
                "use_queries_v2": False,
            }
        )
        == set()
    )


def test_discovered_tables_still_honour_table_pattern() -> None:
    deny: List[str] = [f"{PROJECT}\\.{DATASET}\\.orders_ext"]
    assert _discovered({"include_tables": False, "table_pattern": {"deny": deny}}) == {
        "orders"
    }


def test_date_sharded_tables_collapse_to_one_ref() -> None:
    # Daily shards are one table to lineage, stored under the base name the
    # query log is matched against.
    shards = [(f"events_2026010{day}", "TABLE") for day in (1, 3, 2)]
    assert _discovered({"include_tables": False}, objects=shards) == {"events"}
