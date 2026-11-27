from contextlib import AbstractContextManager, ExitStack
from typing import Dict, List, Optional, Set, Tuple, Type
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.microstrategy.checkpoint_state import (
    MicroStrategyStateHandler,
)
from datahub.ingestion.source.microstrategy.client import (
    MicroStrategyAPIError,
    MicroStrategyClient,
)
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.constants import MSTR_PROP_FINGERPRINT
from datahub.ingestion.source.microstrategy.models import (
    Datasource,
    DatasourceConnection,
    MicroStrategyObject,
    Project,
    SqlView,
)
from datahub.ingestion.source.microstrategy.source import MicroStrategySource
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.metadata.schema_classes import (
    DashboardInfoClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
    StatusClass,
    _Aspect,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from tests.test_helpers.state_helpers import mock_datahub_graph  # noqa: F401

PROJECT_ID = "project-1"
DASH_1_URN = "urn:li:dashboard:(microstrategy,prod.project-1.dash-1)"
REPORT_DATASET_2_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:microstrategy,prod.project-1.ds-2,PROD)"
)
WAREHOUSE = {
    "id": "source-1",
    "name": "Sales Warehouse",
    "database": {
        "type": "snow_flake",
        "version": "snowflake_1x",
        "name": "SALES_DB",
        "schema": "ORDERS",
    },
    "dbms": {"name": "Snowflake"},
    "connection": {"id": "connection-1", "name": "Sales Connection"},
}
DATASETS = {
    "ds-1": {
        "id": "ds-1",
        "name": "Sales Cube",
        "sourceWarehouse": WAREHOUSE,
        "availableObjects": {
            "metrics": [{"id": "metric-1", "name": "Revenue", "dataType": "double"}],
            "attributes": [],
        },
    },
    "ds-2": {
        "id": "ds-2",
        "name": "Cost Cube",
        "availableObjects": {
            "metrics": [{"id": "metric-2", "name": "Cost", "dataType": "double"}],
            "attributes": [],
        },
    },
}
# dash-1 reads ds-1 and ds-2 (its compound grid adds the derived "Cost Pct" to
# ds-2) and links report-1; dash-2 reads only ds-2.
DASHBOARD_DATASETS = {"dash-1": ["ds-1", "ds-2"], "dash-2": ["ds-2"]}
DASHBOARD_VISUALIZATIONS: Dict[str, List[Dict[str, object]]] = {
    "dash-1": [
        {"key": "viz-1", "name": "Revenue Trend", "type": "line", "datasets": ["ds-1"]},
        {
            "key": "viz-2",
            "name": "Sales vs Cost",
            "visualizationType": "compound_grid",
            "columnSets": [
                {"key": "cs-sales", "name": "SALES"},
                {"key": "cs-cost", "name": "COST"},
            ],
        },
    ],
    "dash-2": [
        {"key": "viz-3", "name": "Cost Trend", "type": "line", "datasets": ["ds-2"]}
    ],
}
COMPOUND_GRID: Dict[str, object] = {
    "key": "viz-2",
    "name": "Sales vs Cost",
    "visualizationType": "compound_grid",
    "definition": {
        "grid": {
            "columnSets": [
                {
                    "key": "cs-sales",
                    "name": "SALES",
                    "columns": [
                        {
                            "type": "templateMetrics",
                            "elements": [
                                {"type": "metric", "id": "metric-1", "name": "Revenue"}
                            ],
                        }
                    ],
                },
                {
                    "key": "cs-cost",
                    "name": "COST",
                    "columns": [
                        {
                            "type": "templateMetrics",
                            "elements": [
                                {"type": "metric", "id": "metric-2", "name": "Cost"},
                                {
                                    "type": "metric",
                                    "id": "derived-2",
                                    "name": "Cost Pct",
                                    "derived": True,
                                    "dataType": "double",
                                },
                            ],
                        }
                    ],
                },
            ]
        }
    },
}
CONFIG: Dict[str, object] = {
    "base_url": "https://mstr.example.com/MicroStrategyLibrary",
    "platform_instance": "prod",
    "use_predefined_folder_names": False,
    "extract_reports": True,
    "extract_warehouse_lineage": False,
    "extract_report_sql_lineage": True,
    "extract_model_lineage": False,
    "extract_metric_expressions": False,
    "extract_usage_statistics": True,
    "stateful_ingestion": {"enabled": True, "remove_stale_metadata": True},
    "skip_unchanged_objects": True,
}


# A MicroStrategy server whose object versions tests can bump.
class _MicroStrategy:
    def __init__(self) -> None:
        self.dashboards: Dict[str, Dict[str, object]] = {
            "dash-1": {
                "id": "dash-1",
                "name": "Sales Performance",
                "type": "55",
                "subtype": "14081",
                "viewMedia": 1879072805,
                "owner": {"username": "sales_owner"},
                "version": "D1-V1",
                "dateModified": "2026-01-01T00:00:00.000+0000",
                "ancestors": [{"id": "folder-1", "name": "Finance"}],
            },
            "dash-2": {
                "id": "dash-2",
                "name": "Cost Review",
                "type": "55",
                "subtype": "14081",
                "viewMedia": 1879072805,
                "owner": {"username": "sales_owner"},
                "version": "D2-V1",
                "dateModified": "2026-01-01T00:00:00.000+0000",
                "ancestors": [{"id": "folder-1", "name": "Finance"}],
            },
        }
        self.objects: Dict[str, Dict[str, object]] = {
            "ds-1": {"id": "ds-1", "name": "Sales Cube", "subtype": "776"},
            "ds-2": {"id": "ds-2", "name": "Cost Cube", "subtype": "776"},
            "report-1": {"id": "report-1", "name": "Orders Report", "subtype": "768"},
        }
        for object_id, obj in self.objects.items():
            obj.update(
                type="3",
                version=f"{object_id.upper()}-V1",
                ancestors=[{"id": "folder-2", "name": "Data"}],
            )
        self.failing_definitions: Set[str] = set()
        self.definition_reads: List[str] = []
        self.dossier_instances = 0
        self.report_definition_reads: List[str] = []
        self.report_sql_views = 0
        self.unexpected_requests: List[str] = []

    def bump(self, object_id: str, field: str = "version") -> None:
        obj = self.dashboards.get(object_id) or self.objects[object_id]
        obj[field] = f"{obj[field]}-changed"

    def patches(self) -> List[AbstractContextManager[object]]:
        def fixed(name: str, value: object) -> AbstractContextManager[object]:
            return patch.object(MicroStrategyClient, name, return_value=value)

        def faked(name: str) -> AbstractContextManager[object]:
            return patch.object(
                MicroStrategyClient,
                name,
                autospec=True,
                side_effect=getattr(self, name),
            )

        return [
            fixed("login", None),
            fixed("close", None),
            fixed("list_projects", [Project(id=PROJECT_ID, name="Sales Analytics")]),
            fixed("list_project_datasources", [Datasource.model_validate(WAREHOUSE)]),
            fixed(
                "get_datasource_connection",
                DatasourceConnection.model_validate(
                    {
                        "id": "connection-1",
                        "name": "Sales Connection",
                        "database": {"type": "snow_flake"},
                        "connectionString": "DATABASE=SALES_DB;SCHEMA=ORDERS",
                    }
                ),
            ),
            fixed("get_predefined_folders", []),
            fixed("get_model_document", {}),
            fixed("delete_dossier_instance", True),
            fixed("delete_report_instance", True),
            fixed("create_report_instance", "ri-1"),
            fixed("create_document_instance", "i-1"),
            fixed("delete_document_instance", True),
            faked("search_dashboards"),
            faked("get_object_info"),
            faked("get_object_dependencies"),
            faked("get_dossier_definition"),
            faked("create_dossier_instance"),
            faked("get_dossier_visualization"),
            faked("get_report_definition"),
            faked("get_report_sql_view"),
            patch.object(
                MicroStrategyClient,
                "get_document_definition",
                side_effect=MicroStrategyAPIError("not a document"),
            ),
            faked("_request"),
            patch.object(
                MicroStrategySource, "_process_usage_statistics", return_value=[]
            ),
        ]

    def search_dashboards(
        self, _client: MicroStrategyClient, project_id: str
    ) -> List[MicroStrategyObject]:
        return [
            MicroStrategyObject.model_validate(obj) for obj in self.dashboards.values()
        ]

    def get_object_info(
        self,
        _client: MicroStrategyClient,
        project_id: str,
        object_id: str,
        object_type: int,
    ) -> Optional[MicroStrategyObject]:
        obj = self.objects.get(object_id.lower())
        return MicroStrategyObject.model_validate(obj) if obj else None

    def get_object_dependencies(
        self,
        _client: MicroStrategyClient,
        project_id: str,
        object_id: str,
        object_type: str,
    ) -> List[MicroStrategyObject]:
        if object_id != "dash-1":
            return []
        return [MicroStrategyObject.model_validate(self.objects["report-1"])]

    def get_dossier_definition(
        self, _client: MicroStrategyClient, project_id: str, dossier_id: str
    ) -> Dict[str, object]:
        self.definition_reads.append(dossier_id)
        if dossier_id in self.failing_definitions:
            raise MicroStrategyAPIError("definition unavailable")
        return {
            "result": {
                "definition": {
                    "datasets": [DATASETS[ds] for ds in DASHBOARD_DATASETS[dossier_id]],
                    "chapters": [
                        {
                            "key": "chapter-1",
                            "pages": [
                                {
                                    "key": "page-1",
                                    "name": "SUMMARY",
                                    "visualizations": DASHBOARD_VISUALIZATIONS[
                                        dossier_id
                                    ],
                                }
                            ],
                        }
                    ],
                }
            }
        }

    def create_dossier_instance(
        self, _client: MicroStrategyClient, project_id: str, dossier_id: str
    ) -> str:
        self.dossier_instances += 1
        return "i-1"

    def get_dossier_visualization(
        self,
        _client: MicroStrategyClient,
        project_id: str,
        dossier_id: str,
        instance_id: str,
        chapter_key: str,
        visualization_key: str,
    ) -> Dict[str, object]:
        if visualization_key == "viz-2":
            return COMPOUND_GRID
        return {"key": visualization_key}

    def get_report_definition(
        self, _client: MicroStrategyClient, project_id: str, report_id: str
    ) -> Dict[str, object]:
        self.report_definition_reads.append(report_id)
        return {
            "id": report_id,
            "name": "Orders Report",
            "availableObjects": {"metrics": [{"id": "metric-1", "name": "Revenue"}]},
        }

    def get_report_sql_view(
        self,
        _client: MicroStrategyClient,
        project_id: str,
        report_id: str,
        instance_id: str,
    ) -> SqlView:
        self.report_sql_views += 1
        return SqlView(sqlStatement="select revenue from SALES_DB.ORDERS.ORDERS")

    def _request(
        self, _client: MicroStrategyClient, method: str, path: str, **_: object
    ) -> None:
        self.unexpected_requests.append(f"{method} {path}")
        raise MicroStrategyAPIError(f"unexpected request {method} {path}")


# Aspects written by previous runs, served back through the graph.
class _DataHub:
    def __init__(self, graph: MagicMock) -> None:
        self.aspects: Dict[Tuple[str, str], _Aspect] = {}
        self.soft_deleted: Set[str] = set()
        graph.get_aspect.side_effect = self.get_aspect
        graph.get_entities.side_effect = self.get_entities
        graph._make_schema_resolver.side_effect = (
            lambda platform, platform_instance, env, **_: SchemaResolver(
                platform=platform, platform_instance=platform_instance, env=env
            )
        )

    def get_aspect(self, urn: str, aspect_type: Type[_Aspect]) -> Optional[_Aspect]:
        return self.aspects.get((urn, aspect_type.ASPECT_NAME))

    def get_entities(
        self, _entity_name: str, urns: List[str], aspects: List[str]
    ) -> Dict[str, Dict[str, Tuple[_Aspect, None]]]:
        assert aspects == [StatusClass.ASPECT_NAME]
        found: Dict[str, Dict[str, Tuple[_Aspect, None]]] = {}
        for urn in urns:
            status = self.aspects.get((urn, StatusClass.ASPECT_NAME))
            if isinstance(status, StatusClass):
                removed = status.removed or urn in self.soft_deleted
                found[urn] = {
                    StatusClass.ASPECT_NAME: (StatusClass(removed=removed), None)
                }
        return found

    def write(self, workunits: List[MetadataWorkUnit]) -> None:
        for wu in workunits:
            if isinstance(wu.metadata, MetadataChangeProposalWrapper):
                assert wu.metadata.entityUrn and wu.metadata.aspect
                self.aspects[
                    (wu.metadata.entityUrn, wu.metadata.aspect.ASPECT_NAME)
                ] = wu.metadata.aspect


def _run(
    graph: Optional[MagicMock],
    server: _MicroStrategy,
    run_id: str,
    **overrides: object,
) -> Tuple[MicroStrategySource, List[MetadataWorkUnit]]:
    with ExitStack() as stack:
        for context in server.patches():
            stack.enter_context(context)
        ctx = PipelineContext(
            run_id=run_id, pipeline_name="microstrategy-test", graph=graph
        )
        source = MicroStrategySource(
            MicroStrategyConfig.model_validate({**CONFIG, **overrides}), ctx
        )
        workunits = list(source.get_workunits())
        source.close()
        for _, committable in ctx.get_committables():
            committable.commit()
    assert server.unexpected_requests == []
    return source, workunits


def _stale_urns(source: MicroStrategySource) -> Set[str]:
    assert source.stale_entity_removal_handler is not None
    checkpoint = source.state_provider.get_current_checkpoint(
        source.stale_entity_removal_handler.job_id
    )
    assert checkpoint is not None
    assert isinstance(checkpoint.state, GenericCheckpointState)
    return set(checkpoint.state.urns)


def _soft_deleted_urns(workunits: List[MetadataWorkUnit]) -> Set[str]:
    return {wu.get_urn() for wu in workunits if wu.id.startswith("soft-delete-")}


def _aspects(
    workunits: List[MetadataWorkUnit], urn: str, aspect_type: Type[_Aspect]
) -> List[_Aspect]:
    return [
        wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.entityUrn == urn
        and isinstance(wu.metadata.aspect, aspect_type)
    ]


def _first_run(
    graph: MagicMock, **overrides: object
) -> Tuple[_MicroStrategy, _DataHub, MicroStrategySource]:
    server = _MicroStrategy()
    datahub = _DataHub(graph)
    source, workunits = _run(graph, server, "run-1", **overrides)
    datahub.write(workunits)
    server.definition_reads.clear()
    server.report_definition_reads.clear()
    return server, datahub, source


def test_unchanged_objects_are_skipped_and_kept(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    server = _MicroStrategy()
    datahub = _DataHub(mock_datahub_graph)

    first, first_wus = _run(mock_datahub_graph, server, "run-1")
    datahub.write(first_wus)
    assert server.definition_reads == ["dash-1", "dash-2"]
    assert server.report_definition_reads == ["report-1"]
    assert server.report_sql_views == 1
    (dashboard_info,) = _aspects(first_wus, DASH_1_URN, DashboardInfoClass)
    assert isinstance(dashboard_info, DashboardInfoClass)
    assert dashboard_info.customProperties[MSTR_PROP_FINGERPRINT]
    instances = server.dossier_instances

    second, second_wus = _run(mock_datahub_graph, server, "run-2")

    assert server.definition_reads == ["dash-1", "dash-2"]
    assert server.dossier_instances == instances
    assert server.report_definition_reads == ["report-1"]
    assert server.report_sql_views == 1
    assert second.report.dashboards_skipped_unchanged == 2
    assert second.report.reports_skipped_unchanged == 1
    assert _soft_deleted_urns(second_wus) == set()
    assert _stale_urns(second) == _stale_urns(first)
    assert (
        set(second._usage_targets)
        == set(first._usage_targets)
        == {
            "DASH-1",
            "DASH-2",
            "REPORT-1",
        }
    )
    assert second._usage_targets == first._usage_targets


@pytest.mark.parametrize("field", ["version", "dateModified"])
def test_changed_dashboard_alone_is_read_again(
    mock_datahub_graph: MagicMock,  # noqa: F811
    field: str,
) -> None:
    server, _, first = _first_run(mock_datahub_graph)
    server.bump("dash-2", field)

    second, second_wus = _run(mock_datahub_graph, server, "run-2")

    assert server.definition_reads == ["dash-2"]
    assert second.report.dashboards_skipped_unchanged == 1
    assert _soft_deleted_urns(second_wus) == set()
    assert _stale_urns(second) == _stale_urns(first)


def test_changed_report_reads_the_dashboard_using_it(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    server, _, _ = _first_run(mock_datahub_graph)
    server.bump("report-1")

    second, _ = _run(mock_datahub_graph, server, "run-2")

    assert server.definition_reads == ["dash-1"]
    assert server.report_definition_reads == ["report-1"]
    assert second.report.reports_skipped_unchanged == 0


def test_changed_dataset_reads_only_the_dashboards_using_it(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    server, _, _ = _first_run(mock_datahub_graph)
    server.bump("ds-1")

    _run(mock_datahub_graph, server, "run-2")

    assert server.definition_reads == ["dash-1"]


@pytest.mark.parametrize("change", ["soft_deleted", "restamped", "missing"])
def test_dashboard_is_read_again_when_datahub_disagrees(
    mock_datahub_graph: MagicMock,  # noqa: F811
    change: str,
) -> None:
    server, datahub, _ = _first_run(mock_datahub_graph)
    info_key = (DASH_1_URN, DashboardInfoClass.ASPECT_NAME)
    if change == "soft_deleted":
        datahub.soft_deleted.add(DASH_1_URN)
    elif change == "restamped":
        info = datahub.aspects[info_key]
        assert isinstance(info, DashboardInfoClass)
        info.customProperties[MSTR_PROP_FINGERPRINT] = "another-run"
    else:
        del datahub.aspects[info_key]

    _run(mock_datahub_graph, server, "run-2")

    assert server.definition_reads == ["dash-1"]


def test_recipe_change_reads_everything(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    server, _, _ = _first_run(mock_datahub_graph)

    second, _ = _run(mock_datahub_graph, server, "run-2", ingest_owner=False)

    assert server.definition_reads == ["dash-1", "dash-2"]
    assert server.report_definition_reads == ["report-1"]
    assert second.report.dashboards_skipped_unchanged == 0


def test_runtime_only_option_change_still_skips(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    server, _, _ = _first_run(mock_datahub_graph)

    _run(mock_datahub_graph, server, "run-2", progress_log_interval_dashboards=7)

    assert server.definition_reads == []


def test_untrusted_checkpoint_reads_everything(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    server, datahub, _ = _first_run(mock_datahub_graph)
    # Run 2 commits stale-entity state but no object checkpoint, so run 1's
    # object checkpoint no longer describes what DataHub holds.
    with patch.object(
        MicroStrategyStateHandler, "create_checkpoint", return_value=None
    ):
        _, second_wus = _run(mock_datahub_graph, server, "run-2")
    datahub.write(second_wus)
    server.definition_reads.clear()

    third, _ = _run(mock_datahub_graph, server, "run-3")

    assert server.definition_reads == ["dash-1", "dash-2"]
    assert third.report.dashboards_skipped_unchanged == 0


def test_failed_dashboard_keeps_its_last_read(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    server, datahub, first = _first_run(mock_datahub_graph)
    server.bump("dash-2")
    server.failing_definitions.add("dash-2")

    second, second_wus = _run(mock_datahub_graph, server, "run-2")
    datahub.write(second_wus)

    assert _soft_deleted_urns(second_wus) == set()
    assert _stale_urns(second) == _stale_urns(first)
    server.failing_definitions.clear()
    server.definition_reads.clear()

    _run(mock_datahub_graph, server, "run-3")

    assert server.definition_reads == ["dash-2"]


def test_per_report_shared_report_keeps_every_dashboard(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    server, _, first = _first_run(mock_datahub_graph, dataset_identity="per_report")

    unchanged, _ = _run(
        mock_datahub_graph, server, "run-2", dataset_identity="per_report"
    )
    assert server.definition_reads == []
    assert unchanged.report.report_datasets_skipped_unchanged == 3
    assert _stale_urns(unchanged) == _stale_urns(first)

    server.bump("dash-2")
    changed, changed_wus = _run(
        mock_datahub_graph, server, "run-3", dataset_identity="per_report"
    )

    # ds-2 is re-emitted from dash-2, so dash-1 is read too for its share.
    assert server.definition_reads == ["dash-1", "dash-2"]
    assert changed.report.dashboards_skipped_unchanged == 0
    assert changed.report.report_datasets_skipped_unchanged == 2
    (properties,) = _aspects(changed_wus, REPORT_DATASET_2_URN, DatasetPropertiesClass)
    assert isinstance(properties, DatasetPropertiesClass)
    dashboard_ids = properties.customProperties["microstrategyDashboardIds"]
    assert sorted(dashboard_ids.split(",")) == ["dash-1", "dash-2"]
    (schema,) = _aspects(changed_wus, REPORT_DATASET_2_URN, SchemaMetadataClass)
    assert isinstance(schema, SchemaMetadataClass)
    assert "Cost Pct" in {field.fieldPath for field in schema.fields}
    assert _soft_deleted_urns(changed_wus) == set()
    assert _stale_urns(changed) == _stale_urns(first)


def test_default_run_does_not_stamp_or_checkpoint() -> None:
    server = _MicroStrategy()

    source, workunits = _run(
        None, server, "run-1", stateful_ingestion={"enabled": False}
    )

    stamped = [
        wu.get_urn()
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and MSTR_PROP_FINGERPRINT
        in (getattr(wu.metadata.aspect, "customProperties", None) or {})
    ]
    assert stamped == []
    assert source.report.dashboards_skipped_unchanged == 0
    assert any(
        "skip_unchanged_objects is ignored" in str(warning)
        for warning in source.report.warnings
    )
