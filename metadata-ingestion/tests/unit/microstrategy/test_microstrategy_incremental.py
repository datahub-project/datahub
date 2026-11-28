from contextlib import AbstractContextManager, ExitStack
from typing import Dict, List, Optional, Set, Tuple, Type
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, ConfigDict

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.microstrategy.checkpoint_state import (
    MicroStrategyCheckpointState,
    MicroStrategyStateHandler,
)
from datahub.ingestion.source.microstrategy.client import (
    MicroStrategyAPIError,
    MicroStrategyClient,
)
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.constants import (
    FINGERPRINT_EXCLUDED_CONFIG_FIELDS,
    MSTR_PROP_FINGERPRINT,
)
from datahub.ingestion.source.microstrategy.models import (
    Datasource,
    DatasourceConnection,
    MicroStrategyObject,
    ModelTablesResponse,
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
REPORT_CHART_URN = "urn:li:chart:(microstrategy,prod.project-1.report-1)"
REPORT_DATASET_1_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:microstrategy,prod.project-1.ds-1,PROD)"
)
REPORT_DATASET_2_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:microstrategy,prod.project-1.ds-2,PROD)"
)
REPORT_1_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:microstrategy,prod.project-1.report-1,PROD)"
)
WAREHOUSE: Dict[str, object] = {
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
        self.dashboard_datasets = {
            dashboard_id: list(dataset_ids)
            for dashboard_id, dataset_ids in DASHBOARD_DATASETS.items()
        }
        self.dashboard_visualizations = dict(DASHBOARD_VISUALIZATIONS)
        self.dependencies: Dict[str, List[str]] = {"dash-1": ["report-1"]}
        self.failing_search = False
        self.failing_report_definitions: Set[str] = set()
        self.warehouses: List[Dict[str, object]] = [WAREHOUSE]
        self.model_tables: List[Dict[str, object]] = [
            {"id": "table-1", "physicalTable": {"tableName": "ORDERS"}}
        ]
        self.failing_definitions: Set[str] = set()
        self.failing_visualizations: Set[str] = set()
        self.failing_sql_views = False
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
            fixed(
                "list_project_datasources",
                [Datasource.model_validate(warehouse) for warehouse in self.warehouses],
            ),
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
            fixed("get_dossier_datasets_sql", []),
            fixed("get_model_document", {}),
            fixed("delete_dossier_instance", True),
            fixed("delete_report_instance", True),
            fixed("create_report_instance", "ri-1"),
            fixed("create_document_instance", "i-1"),
            fixed("delete_document_instance", True),
            faked("search_dashboards"),
            faked("search_reports"),
            faked("get_object_info"),
            faked("get_object_dependencies"),
            faked("get_dossier_definition"),
            faked("create_dossier_instance"),
            faked("get_dossier_visualization"),
            faked("get_report_definition"),
            faked("get_report_sql_view"),
            faked("list_model_tables"),
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
        if self.failing_search:
            raise MicroStrategyAPIError("search unavailable")
        return [
            MicroStrategyObject.model_validate(obj) for obj in self.dashboards.values()
        ]

    def search_reports(
        self, _client: MicroStrategyClient, project_id: str
    ) -> List[MicroStrategyObject]:
        return [MicroStrategyObject.model_validate(self.objects["report-1"])]

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
        return [
            MicroStrategyObject.model_validate(self.objects[dependency_id])
            for dependency_id in self.dependencies.get(object_id, [])
        ]

    def get_dossier_definition(
        self, _client: MicroStrategyClient, project_id: str, dossier_id: str
    ) -> Dict[str, object]:
        self.definition_reads.append(dossier_id)
        if dossier_id in self.failing_definitions:
            raise MicroStrategyAPIError("definition unavailable")
        return {
            "result": {
                "definition": {
                    "datasets": [
                        DATASETS[ds] for ds in self.dashboard_datasets[dossier_id]
                    ],
                    "chapters": [
                        {
                            "key": "chapter-1",
                            "pages": [
                                {
                                    "key": "page-1",
                                    "name": "SUMMARY",
                                    "visualizations": self.dashboard_visualizations[
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
        if dossier_id in self.failing_visualizations:
            raise MicroStrategyAPIError("visualization unavailable")
        if visualization_key == "viz-2":
            return COMPOUND_GRID
        return {"key": visualization_key}

    def get_report_definition(
        self, _client: MicroStrategyClient, project_id: str, report_id: str
    ) -> Dict[str, object]:
        self.report_definition_reads.append(report_id)
        if report_id in self.failing_report_definitions:
            raise RuntimeError("unexpected report definition")
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
        if self.failing_sql_views:
            raise MicroStrategyAPIError("sql view unavailable")
        return SqlView(sqlStatement="select revenue from SALES_DB.ORDERS.ORDERS")

    def list_model_tables(
        self, _client: MicroStrategyClient, project_id: str, **_: object
    ) -> ModelTablesResponse:
        return ModelTablesResponse(
            tables=self.model_tables, total=len(self.model_tables)
        )

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


class _Run(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    source: MicroStrategySource
    workunits: List[MetadataWorkUnit]


class _FirstRun(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    server: _MicroStrategy
    datahub: _DataHub
    source: MicroStrategySource


def _run(
    graph: Optional[MagicMock],
    server: _MicroStrategy,
    run_id: str,
    **overrides: object,
) -> _Run:
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
    return _Run(source=source, workunits=workunits)


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


def _stamped_urns(workunits: List[MetadataWorkUnit]) -> List[str]:
    return [
        wu.get_urn()
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and MSTR_PROP_FINGERPRINT
        in (getattr(wu.metadata.aspect, "customProperties", None) or {})
    ]


def _first_run(graph: MagicMock, **overrides: object) -> _FirstRun:
    server = _MicroStrategy()
    datahub = _DataHub(graph)
    first = _run(graph, server, "run-1", **overrides)
    datahub.write(first.workunits)
    server.definition_reads.clear()
    server.report_definition_reads.clear()
    return _FirstRun(server=server, datahub=datahub, source=first.source)


def test_unchanged_objects_are_skipped_and_kept(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    server = _MicroStrategy()
    datahub = _DataHub(mock_datahub_graph)

    first = _run(mock_datahub_graph, server, "run-1")
    datahub.write(first.workunits)
    assert server.definition_reads == ["dash-1", "dash-2"]
    assert server.report_definition_reads == ["report-1"]
    assert server.report_sql_views == 1
    (dashboard_info,) = _aspects(first.workunits, DASH_1_URN, DashboardInfoClass)
    assert isinstance(dashboard_info, DashboardInfoClass)
    assert dashboard_info.customProperties[MSTR_PROP_FINGERPRINT]
    instances = server.dossier_instances

    second = _run(mock_datahub_graph, server, "run-2")

    assert server.definition_reads == ["dash-1", "dash-2"]
    assert server.dossier_instances == instances
    assert server.report_definition_reads == ["report-1"]
    assert server.report_sql_views == 1
    assert second.source.report.dashboards_skipped_unchanged == 2
    assert second.source.report.reports_skipped_unchanged == 1
    assert _soft_deleted_urns(second.workunits) == set()
    assert _stale_urns(second.source) == _stale_urns(first.source)
    assert (
        set(second.source._usage_targets)
        == set(first.source._usage_targets)
        == {
            "DASH-1",
            "DASH-2",
            "REPORT-1",
        }
    )
    assert second.source._usage_targets == first.source._usage_targets


@pytest.mark.parametrize("field", ["version", "dateModified"])
def test_changed_dashboard_alone_is_read_again(
    mock_datahub_graph: MagicMock,  # noqa: F811
    field: str,
) -> None:
    first = _first_run(mock_datahub_graph)
    first.server.bump("dash-2", field)

    second = _run(mock_datahub_graph, first.server, "run-2")

    assert first.server.definition_reads == ["dash-2"]
    assert second.source.report.dashboards_skipped_unchanged == 1
    assert _soft_deleted_urns(second.workunits) == set()
    assert _stale_urns(second.source) == _stale_urns(first.source)


def test_changed_report_reads_the_dashboard_using_it(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)
    first.server.bump("report-1")

    second = _run(mock_datahub_graph, first.server, "run-2")

    assert first.server.definition_reads == ["dash-1"]
    assert first.server.report_definition_reads == ["report-1"]
    assert second.source.report.reports_skipped_unchanged == 0


def test_changed_dataset_reads_only_the_dashboards_using_it(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)
    first.server.bump("ds-1")

    _run(mock_datahub_graph, first.server, "run-2")

    assert first.server.definition_reads == ["dash-1"]


@pytest.mark.parametrize("change", ["soft_deleted", "restamped", "missing"])
def test_dashboard_is_read_again_when_datahub_disagrees(
    mock_datahub_graph: MagicMock,  # noqa: F811
    change: str,
) -> None:
    first = _first_run(mock_datahub_graph)
    info_key = (DASH_1_URN, DashboardInfoClass.ASPECT_NAME)
    if change == "soft_deleted":
        first.datahub.soft_deleted.add(DASH_1_URN)
    elif change == "restamped":
        info = first.datahub.aspects[info_key]
        assert isinstance(info, DashboardInfoClass)
        info.customProperties[MSTR_PROP_FINGERPRINT] = "another-run"
    else:
        del first.datahub.aspects[info_key]

    _run(mock_datahub_graph, first.server, "run-2")

    assert first.server.definition_reads == ["dash-1"]


def test_warehouse_order_does_not_change_the_fingerprint(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    other_warehouse = {**WAREHOUSE, "id": "source-2", "name": "Other Warehouse"}
    server = _MicroStrategy()
    server.warehouses = [WAREHOUSE, other_warehouse]
    datahub = _DataHub(mock_datahub_graph)
    datahub.write(_run(mock_datahub_graph, server, "run-1").workunits)
    server.definition_reads.clear()
    server.warehouses = [other_warehouse, WAREHOUSE]

    second = _run(mock_datahub_graph, server, "run-2")

    assert server.definition_reads == []
    assert second.source.report.dashboards_skipped_unchanged == 2


def test_model_table_change_reads_everything(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph, extract_model_lineage=True)
    first.server.model_tables[0]["physicalTable"] = {"tableName": "ORDERS_V2"}

    second = _run(mock_datahub_graph, first.server, "run-2", extract_model_lineage=True)

    assert first.server.definition_reads == ["dash-1", "dash-2"]
    assert second.source.report.dashboards_skipped_unchanged == 0


def test_recipe_change_reads_everything(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)

    second = _run(mock_datahub_graph, first.server, "run-2", ingest_owner=False)

    assert first.server.definition_reads == ["dash-1", "dash-2"]
    assert first.server.report_definition_reads == ["report-1"]
    assert second.source.report.dashboards_skipped_unchanged == 0


def test_runtime_only_option_change_still_skips(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)

    _run(mock_datahub_graph, first.server, "run-2", progress_log_interval_dashboards=7)

    assert first.server.definition_reads == []


def test_untrusted_checkpoint_reads_everything(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)
    # Run 2 commits stale-entity state but no object checkpoint, so run 1's
    # object checkpoint no longer describes what DataHub holds.
    with patch.object(
        MicroStrategyStateHandler, "create_checkpoint", return_value=None
    ):
        second = _run(mock_datahub_graph, first.server, "run-2")
    first.datahub.write(second.workunits)
    first.server.definition_reads.clear()

    third = _run(mock_datahub_graph, first.server, "run-3")

    assert first.server.definition_reads == ["dash-1", "dash-2"]
    assert third.source.report.dashboards_skipped_unchanged == 0


def test_failed_dashboard_keeps_its_last_read(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)
    first.server.bump("dash-2")
    first.server.failing_definitions.add("dash-2")

    second = _run(mock_datahub_graph, first.server, "run-2")
    first.datahub.write(second.workunits)

    assert _soft_deleted_urns(second.workunits) == set()
    assert _stale_urns(second.source) == _stale_urns(first.source)
    first.server.failing_definitions.clear()
    first.server.definition_reads.clear()

    _run(mock_datahub_graph, first.server, "run-3")

    assert first.server.definition_reads == ["dash-2"]


def test_failed_change_check_reads_the_object(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)
    mock_datahub_graph.get_entities.side_effect = RuntimeError("graph unavailable")

    second = _run(mock_datahub_graph, first.server, "run-2")

    assert first.server.definition_reads == ["dash-1", "dash-2"]
    assert second.source.report.dashboards_skipped_unchanged == 0
    assert second.source.report.change_check_failures > 0
    assert [
        warning.title
        for warning in second.source.report.warnings
        if warning.title == "Unchanged-object check failed"
    ] == ["Unchanged-object check failed"]
    assert _soft_deleted_urns(second.workunits) == set()


@pytest.mark.parametrize("dataset_identity", ["per_dashboard", "per_report"])
def test_report_read_without_its_sql_view_is_read_again(
    mock_datahub_graph: MagicMock,  # noqa: F811
    dataset_identity: str,
) -> None:
    server = _MicroStrategy()
    server.failing_sql_views = True
    datahub = _DataHub(mock_datahub_graph)
    first = _run(mock_datahub_graph, server, "run-1", dataset_identity=dataset_identity)
    datahub.write(first.workunits)
    assert REPORT_CHART_URN not in _stamped_urns(first.workunits)
    server.failing_sql_views = False
    server.report_definition_reads.clear()
    server.report_sql_views = 0

    second = _run(
        mock_datahub_graph, server, "run-2", dataset_identity=dataset_identity
    )

    assert server.report_definition_reads == ["report-1"]
    assert server.report_sql_views == 1
    assert second.source.report.reports_skipped_unchanged == 0
    assert _soft_deleted_urns(second.workunits) == set()


def test_dashboard_read_without_visualization_details_is_read_again(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)
    first.server.bump("dash-2")
    first.server.failing_visualizations.add("dash-2")

    second = _run(mock_datahub_graph, first.server, "run-2")
    first.datahub.write(second.workunits)

    assert first.server.definition_reads == ["dash-2"]
    assert _stamped_urns(second.workunits) == []
    assert _soft_deleted_urns(second.workunits) == set()
    assert _stale_urns(second.source) == _stale_urns(first.source)
    first.server.failing_visualizations.clear()
    first.server.definition_reads.clear()

    _run(mock_datahub_graph, first.server, "run-3")

    assert first.server.definition_reads == ["dash-2"]


def test_per_report_shared_report_keeps_every_dashboard(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph, dataset_identity="per_report")
    server = first.server

    unchanged = _run(mock_datahub_graph, server, "run-2", dataset_identity="per_report")
    assert server.definition_reads == []
    assert unchanged.source.report.report_datasets_skipped_unchanged == 3
    assert _stale_urns(unchanged.source) == _stale_urns(first.source)

    server.bump("dash-2")
    changed = _run(mock_datahub_graph, server, "run-3", dataset_identity="per_report")

    # ds-2 is re-emitted from dash-2, so dash-1 is read too for its share.
    assert server.definition_reads == ["dash-1", "dash-2"]
    assert changed.source.report.dashboards_skipped_unchanged == 0
    assert changed.source.report.report_datasets_skipped_unchanged == 2
    (properties,) = _aspects(
        changed.workunits, REPORT_DATASET_2_URN, DatasetPropertiesClass
    )
    assert isinstance(properties, DatasetPropertiesClass)
    dashboard_ids = properties.customProperties["microstrategyDashboardIds"]
    assert sorted(dashboard_ids.split(",")) == ["dash-1", "dash-2"]
    (schema,) = _aspects(changed.workunits, REPORT_DATASET_2_URN, SchemaMetadataClass)
    assert isinstance(schema, SchemaMetadataClass)
    assert "Cost Pct" in {field.fieldPath for field in schema.fields}
    assert _soft_deleted_urns(changed.workunits) == set()
    assert _stale_urns(changed.source) == _stale_urns(first.source)


def test_per_report_dataset_gaining_a_dashboard_reads_its_other_dashboards(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    server = _MicroStrategy()
    server.dashboard_datasets["dash-1"] = ["ds-1"]
    server.dashboard_visualizations["dash-1"] = DASHBOARD_VISUALIZATIONS["dash-1"][:1]
    datahub = _DataHub(mock_datahub_graph)
    first = _run(mock_datahub_graph, server, "run-1", dataset_identity="per_report")
    datahub.write(first.workunits)
    server.definition_reads.clear()
    server.bump("dash-2")
    server.dashboard_datasets["dash-2"] = ["ds-2", "ds-1"]

    second = _run(mock_datahub_graph, server, "run-2", dataset_identity="per_report")

    # ds-1 only turns out to change once dash-2 is read.
    assert server.definition_reads == ["dash-2", "dash-1"]
    assert second.source.report.dashboards_skipped_unchanged == 0
    (properties,) = _aspects(
        second.workunits, REPORT_DATASET_1_URN, DatasetPropertiesClass
    )
    assert isinstance(properties, DatasetPropertiesClass)
    dashboard_ids = properties.customProperties["microstrategyDashboardIds"]
    assert sorted(dashboard_ids.split(",")) == ["dash-1", "dash-2"]
    assert _soft_deleted_urns(second.workunits) == set()


def test_per_report_report_leaving_the_report_pass_is_dropped(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph, dataset_identity="per_report")
    assert REPORT_1_DATASET_URN in _stale_urns(first.source)
    first.server.bump("dash-1")
    first.server.dependencies = {}

    second = _run(
        mock_datahub_graph, first.server, "run-2", dataset_identity="per_report"
    )
    full = _run(
        mock_datahub_graph,
        first.server,
        "run-3",
        dataset_identity="per_report",
        skip_unchanged_objects=False,
    )

    assert REPORT_1_DATASET_URN not in _stale_urns(second.source)
    assert _stale_urns(second.source) == _stale_urns(full.source)


def test_per_report_without_dashboards_skips_unchanged_reports(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    options: Dict[str, object] = {
        "dataset_identity": "per_report",
        "extract_dashboards": False,
    }
    first = _first_run(mock_datahub_graph, **options)

    unchanged = _run(mock_datahub_graph, first.server, "run-2", **options)
    assert first.server.report_definition_reads == []
    assert unchanged.source.report.report_datasets_skipped_unchanged == 1
    assert _stale_urns(unchanged.source) == _stale_urns(first.source)

    first.server.bump("report-1")
    _run(mock_datahub_graph, first.server, "run-3", **options)
    assert first.server.report_definition_reads == ["report-1"]


def test_failed_report_keeps_its_last_read(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)
    first.server.bump("report-1")
    first.server.failing_report_definitions.add("report-1")

    second = _run(mock_datahub_graph, first.server, "run-2")
    first.datahub.write(second.workunits)

    assert _soft_deleted_urns(second.workunits) == set()
    assert _stale_urns(second.source) == _stale_urns(first.source)
    first.server.failing_report_definitions.clear()
    first.server.report_definition_reads.clear()

    _run(mock_datahub_graph, first.server, "run-3")

    assert first.server.report_definition_reads == ["report-1"]


def test_failed_run_keeps_objects_it_did_not_reach(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)
    first.server.failing_search = True

    second = _run(mock_datahub_graph, first.server, "run-2")
    assert second.source.report.failures
    first.server.failing_search = False

    third = _run(mock_datahub_graph, first.server, "run-3")

    assert first.server.definition_reads == []
    assert third.source.report.dashboards_skipped_unchanged == 2
    assert _stale_urns(third.source) == _stale_urns(first.source)


def test_default_run_does_not_stamp_or_checkpoint() -> None:
    server = _MicroStrategy()

    run = _run(None, server, "run-1", stateful_ingestion={"enabled": False})

    assert _stamped_urns(run.workunits) == []
    assert run.source.report.dashboards_skipped_unchanged == 0
    assert any(
        "skip_unchanged_objects is ignored" in str(warning)
        for warning in run.source.report.warnings
    )


@pytest.mark.parametrize("dataset_identity", ["per_dashboard", "per_report"])
def test_stale_removal_without_skipping_does_not_stamp(
    mock_datahub_graph: MagicMock,  # noqa: F811
    dataset_identity: str,
) -> None:
    _DataHub(mock_datahub_graph)

    run = _run(
        mock_datahub_graph,
        _MicroStrategy(),
        "run-1",
        skip_unchanged_objects=False,
        dataset_identity=dataset_identity,
    )

    assert _stamped_urns(run.workunits) == []
    # Still checkpointed, so a failed read keeps its last entities.
    current = run.source.state_provider.get_current_checkpoint(
        run.source._object_state.job_id
    )
    assert current is not None
    assert isinstance(current.state, MicroStrategyCheckpointState)
    assert current.state.dashboards


def test_fingerprint_excluded_fields_are_config_fields() -> None:
    assert set(MicroStrategyConfig.model_fields) >= FINGERPRINT_EXCLUDED_CONFIG_FIELDS
