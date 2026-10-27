"""Integration tests for lineage-only upstream nodes and incremental lineage."""

import json
from pathlib import Path
from typing import Any, Dict, List, Set
from unittest.mock import MagicMock, Mock, patch

import time_machine

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.testing import mce_helpers
from tests.integration.dataplex.test_dataplex_integration import (
    FROZEN_TIME,
    create_mock_entry,
    create_mock_entry_group,
    create_mock_lineage_link,
)
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    mock_datahub_graph,  # noqa: F401  (pytest fixture)
)

BIGQUERY_TABLE_ENTRY_TYPE = (
    "projects/test-project/locations/us/entryTypes/bigquery-table"
)
DOWNSTREAM_FQN = "bigquery:test-project.analytics.daily_orders"
UPSTREAM_FQNS = [
    "gcs:raw-bucket/orders/2024/01/15.json",
    "hive_metastore:thrift-host.other_db.clicks",
    "bigquery:other-project.sales.orders",
    # Already in DataHub, owned by another connector.
    "bigquery:other-project.sales.customers",
]
EXISTING_URNS = {
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,other-project.sales.customers,PROD)"
}
DISABLED_WARNING_TITLE = "Dataplex lineage-only upstreams disabled"


def _fake_graph() -> MagicMock:
    """The pipeline's DataHubGraph, answering exists offline."""
    graph = MagicMock(spec=DataHubGraph)
    graph.__enter__.return_value = graph
    graph.server_id = None
    graph.exists.side_effect = lambda urn: urn in EXISTING_URNS
    return graph


def _recipe(output_path: str, **overrides: Any) -> Dict[str, Any]:
    return {
        "datahub_api": {"server": "http://localhost:8080"},
        "source": {
            "type": "dataplex",
            "config": {
                "project_ids": ["test-project"],
                "entries_locations": ["us"],
                "lineage_locations": ["us"],
                "include_lineage": True,
                "include_schema": False,
                "include_glossaries": False,
                **overrides,
            },
        },
        "sink": {"type": "file", "config": {"filename": output_path}},
    }


def _search_links(request: Any, upstream_fqns: List[str]) -> List[Mock]:
    if request.target.fully_qualified_name != DOWNSTREAM_FQN:
        return []
    return [create_mock_lineage_link(fqn, DOWNSTREAM_FQN) for fqn in upstream_fqns]


def _run_pipeline(
    recipe: Dict[str, Any], upstream_fqns: List[str] = UPSTREAM_FQNS
) -> Pipeline:
    with (
        patch("google.auth.default", return_value=(Mock(), "test-project")),
        patch("google.cloud.dataplex_v1.CatalogServiceClient") as catalog_class,
        patch(
            "datahub.ingestion.source.dataplex.dataplex.LineageClient"
        ) as lineage_class,
        patch(
            "datahub.ingestion.run.pipeline.DataHubGraph", return_value=_fake_graph()
        ),
    ):
        lineage_class.return_value.search_links.side_effect = lambda request: (
            _search_links(request, upstream_fqns)
        )
        entry = create_mock_entry(
            project_id="test-project",
            location="us",
            entry_group_id="@bigquery",
            entry_id="daily_orders",
            fqn=DOWNSTREAM_FQN,
            entry_type=BIGQUERY_TABLE_ENTRY_TYPE,
        )
        catalog = catalog_class.return_value
        catalog.list_entry_groups.return_value = [
            create_mock_entry_group("test-project", "us", "@bigquery")
        ]
        catalog.list_entries.return_value = [entry]
        catalog.get_entry.return_value = entry

        pipeline = Pipeline.create(recipe)
        pipeline.run()
        pipeline.raise_from_status()
    return pipeline


def _run(
    tmp_path: Path, golden_name: str, pytestconfig: Any, **overrides: Any
) -> Pipeline:
    output_path = tmp_path / f"{golden_name}.json"
    pipeline = _run_pipeline(_recipe(str(output_path), **overrides))
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(Path(__file__).parent / "golden" / f"{golden_name}.json"),
    )
    return pipeline


@time_machine.travel(FROZEN_TIME, tick=False)
def test_dataplex_lineage_only_nodes_golden(pytestconfig: Any, tmp_path: Path) -> None:
    pipeline = _run(
        tmp_path,
        "dataplex_lineage_only_nodes_golden",
        pytestconfig,
        include_lineage_only_upstreams=True,
    )

    warning_titles = {entry.title for entry in pipeline.source.get_report().warnings}
    assert DISABLED_WARNING_TITLE not in warning_titles


@time_machine.travel(FROZEN_TIME, tick=False)
def test_dataplex_incremental_lineage_golden(pytestconfig: Any, tmp_path: Path) -> None:
    _run(
        tmp_path,
        "dataplex_incremental_lineage_golden",
        pytestconfig,
        incremental_lineage=True,
    )


STUB_URNS = {
    "urn:li:dataset:(urn:li:dataPlatform:gcs,raw-bucket,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:hive,other_db.clicks,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,other-project.sales.orders,PROD)",
}


@time_machine.travel(FROZEN_TIME, tick=False)
def test_casing_resolved_platform_gets_no_node(tmp_path: Path) -> None:
    output_path = tmp_path / "out.json"
    recipe = _recipe(str(output_path), include_lineage_only_upstreams=True)
    recipe["flags"] = {
        "auto_resolve_lineage_urns": {
            "enabled": True,
            "upstream_platforms": [{"platform": "hive"}],
        }
    }

    _run_pipeline(recipe)

    emitted = {record["entityUrn"] for record in json.loads(output_path.read_text())}
    hive_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,other_db.clicks,PROD)"
    assert emitted & STUB_URNS == STUB_URNS - {hive_urn}


def _stateful_recipe(output_path: Path) -> Dict[str, Any]:
    recipe = _recipe(str(output_path), include_lineage_only_upstreams=True)
    recipe["run_id"] = "dataplex-stateful-run"
    recipe["pipeline_name"] = "dataplex-stateful-pipeline"
    recipe["source"]["config"]["stateful_ingestion"] = {
        "enabled": True,
        "remove_stale_metadata": True,
        "fail_safe_threshold": 100.0,
        "state_provider": {
            "type": "datahub",
            "config": {"datahub_api": {"server": "http://localhost:8080"}},
        },
    }
    return recipe


def _soft_deleted_urns(output_path: Path) -> Set[str]:
    return {
        record["entityUrn"]
        for record in json.loads(output_path.read_text())
        if record.get("aspectName") == "status"
        and record["aspect"]["json"].get("removed") is True
    }


@time_machine.travel(FROZEN_TIME, tick=False)
def test_lineage_only_nodes_are_never_soft_deleted(
    tmp_path: Path,
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    """Run 1 references the upstreams; run 2 no longer does. Stale-entity
    removal must not soft-delete them, because Dataplex does not own them."""
    out1 = tmp_path / "run1.json"
    out2 = tmp_path / "run2.json"
    with patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph
        pipeline1 = _run_pipeline(_stateful_recipe(out1))
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline1)
        pipeline2 = _run_pipeline(_stateful_recipe(out2), upstream_fqns=[])
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline2)

    emitted_in_run1 = {record["entityUrn"] for record in json.loads(out1.read_text())}
    assert emitted_in_run1 >= STUB_URNS
    assert checkpoint1 is not None and checkpoint2 is not None
    for checkpoint in (checkpoint1, checkpoint2):
        assert isinstance(checkpoint.state, GenericCheckpointState)
        assert not STUB_URNS & set(checkpoint.state.urns)
    assert not STUB_URNS & _soft_deleted_urns(out2)
