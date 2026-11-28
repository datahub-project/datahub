from contextlib import AbstractContextManager, ExitStack
from typing import Callable, Dict, Iterator, List, Optional, Set, Tuple, Type
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, ConfigDict

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.microstrategy.checkpoint_state import (
    MicroStrategyCheckpointState,
    MicroStrategyStateHandler,
    ObjectCheckpoint,
    ReportDatasetCheckpoint,
)
from datahub.ingestion.source.microstrategy.client import (
    MicroStrategyAPIError,
    MicroStrategyAuthError,
    MicroStrategyClient,
)
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.constants import (
    ENTITY_CHECK_BATCH_SIZE,
    FINGERPRINT_EXCLUDED_CONFIG_FIELDS,
    MSTR_PROP_FINGERPRINT,
    MSTR_UNCHANGED_OBJECTS_JOB_ID,
)
from datahub.ingestion.source.microstrategy.incremental import (
    ReportDatasetPlan,
    entities_present,
)
from datahub.ingestion.source.microstrategy.models import (
    Datasource,
    DatasourceConnection,
    MicroStrategyObject,
    ModelTablesResponse,
    Project,
    SqlView,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.ingestion.source.microstrategy.source import MicroStrategySource
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stateful_ingestion_base import StateProviderWrapper
from datahub.metadata.schema_classes import (
    ChartInfoClass,
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
DASH_2_URN = "urn:li:dashboard:(microstrategy,prod.project-1.dash-2)"
DASH_1_VIZ_URN = "urn:li:chart:(microstrategy,prod.project-1.dash-1.viz-1)"
DASH_1_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:microstrategy,prod.project-1.dash-1.ds-1,PROD)"
)
REPORT_SOURCE_URN = "urn:li:dataset:(urn:li:dataPlatform:microstrategy,prod.project-1.report-1.source,PROD)"
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
MALFORMED_GRID: Dict[str, object] = {
    "key": "viz-2",
    "definition": {
        "grid": {
            "columnSets": [
                {
                    "key": "cs-cost",
                    "name": "COST",
                    "columns": [
                        {
                            "type": "templateMetrics",
                            "elements": [{"type": "metric", "id": "m", "derived": [1]}],
                        }
                    ],
                }
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
        self.malformed_visualizations: Set[str] = set()
        self.failing_report_api: Set[str] = set()
        self.failing_dependencies = False
        self.failing_model_tables = False
        self.report_source_id: Optional[str] = None
        self.failing_sql_views = False
        # Fields only the search result carries, e.g. a date the object endpoint disagrees with.
        self.search_report_fields: Dict[str, object] = {}
        self.failing_connection = False
        self.failing_metric_models: Dict[str, int] = {}
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
            faked("get_datasource_connection"),
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
            fixed("get_model_report", {}),
            faked("get_report_sql_view"),
            faked("list_model_tables"),
            faked("get_metric_model"),
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
        report = {**self.objects["report-1"], **self.search_report_fields}
        return [MicroStrategyObject.model_validate(report)]

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
        if self.failing_dependencies:
            raise MicroStrategyAPIError("dependencies unavailable")
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
        if dossier_id in self.malformed_visualizations:
            return MALFORMED_GRID
        if visualization_key == "viz-2":
            return COMPOUND_GRID
        return {"key": visualization_key}

    def get_report_definition(
        self, _client: MicroStrategyClient, project_id: str, report_id: str
    ) -> Dict[str, object]:
        self.report_definition_reads.append(report_id)
        if report_id in self.failing_report_definitions:
            raise RuntimeError("unexpected report definition")
        if report_id in self.failing_report_api:
            raise MicroStrategyAPIError("report definition unavailable")
        definition: Dict[str, object] = {
            "id": report_id,
            "name": "Orders Report",
            "availableObjects": {"metrics": [{"id": "metric-1", "name": "Revenue"}]},
        }
        if self.report_source_id:
            definition["sourceId"] = self.report_source_id
        return definition

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

    def get_datasource_connection(
        self, _client: MicroStrategyClient, connection_id: str, **_: object
    ) -> DatasourceConnection:
        if self.failing_connection:
            raise MicroStrategyAPIError("connection unavailable")
        return DatasourceConnection.model_validate(
            {
                "id": connection_id,
                "name": "Sales Connection",
                "database": {"type": "snow_flake"},
                "connectionString": "DATABASE=SALES_DB;SCHEMA=ORDERS",
            }
        )

    def list_model_tables(
        self, _client: MicroStrategyClient, project_id: str, **_: object
    ) -> ModelTablesResponse:
        if self.failing_model_tables:
            raise MicroStrategyAPIError("model tables unavailable")
        return ModelTablesResponse(
            tables=self.model_tables, total=len(self.model_tables)
        )

    def get_metric_model(
        self, _client: MicroStrategyClient, project_id: str, metric_id: str
    ) -> Dict[str, object]:
        status = self.failing_metric_models.get(metric_id)
        if status is not None:
            raise MicroStrategyAPIError("metric model unavailable", status_code=status)
        return {"id": metric_id}

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
        self,
        entity_name: str,
        urns: List[str],
        aspects: List[str],
        with_system_metadata: bool = False,
    ) -> Dict[str, Dict[str, Tuple[_Aspect, None]]]:
        if aspects != [StatusClass.ASPECT_NAME]:
            return {}
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
    return {
        wu.get_urn()
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, StatusClass)
        and wu.metadata.aspect.removed
    }


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
    assert sorted(server.definition_reads) == ["dash-1", "dash-2"]
    assert server.report_definition_reads == ["report-1"]
    assert server.report_sql_views == 1
    (dashboard_info,) = _aspects(first.workunits, DASH_1_URN, DashboardInfoClass)
    assert isinstance(dashboard_info, DashboardInfoClass)
    assert dashboard_info.customProperties[MSTR_PROP_FINGERPRINT]
    instances = server.dossier_instances

    second = _run(mock_datahub_graph, server, "run-2")

    assert sorted(server.definition_reads) == ["dash-1", "dash-2"]
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

    assert sorted(first.server.definition_reads) == ["dash-1", "dash-2"]
    assert second.source.report.dashboards_skipped_unchanged == 0


def test_recipe_change_reads_everything(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)

    second = _run(mock_datahub_graph, first.server, "run-2", ingest_owner=False)

    assert sorted(first.server.definition_reads) == ["dash-1", "dash-2"]
    assert first.server.report_definition_reads == ["report-1"]
    assert second.source.report.dashboards_skipped_unchanged == 0


def test_runtime_only_option_change_still_skips(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)

    _run(
        mock_datahub_graph,
        first.server,
        "run-2",
        stateful_ingestion={
            "enabled": True,
            "remove_stale_metadata": True,
            "fail_safe_threshold": 50.0,
        },
    )

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

    assert sorted(first.server.definition_reads) == ["dash-1", "dash-2"]
    assert third.source.report.dashboards_skipped_unchanged == 0


def test_failed_change_check_reads_the_object(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)
    mock_datahub_graph.get_entities.side_effect = RuntimeError("graph unavailable")

    second = _run(mock_datahub_graph, first.server, "run-2")

    assert sorted(first.server.definition_reads) == ["dash-1", "dash-2"]
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


@pytest.mark.parametrize("dataset_identity", ["per_dashboard", "per_report"])
def test_report_read_without_its_connection_is_read_again(
    mock_datahub_graph: MagicMock,  # noqa: F811
    dataset_identity: str,
) -> None:
    server = _MicroStrategy()
    server.failing_connection = True
    datahub = _DataHub(mock_datahub_graph)
    first = _run(mock_datahub_graph, server, "run-1", dataset_identity=dataset_identity)
    datahub.write(first.workunits)
    assert REPORT_CHART_URN not in _stamped_urns(first.workunits)
    assert REPORT_1_DATASET_URN not in _stamped_urns(first.workunits)
    server.failing_connection = False
    server.report_definition_reads.clear()

    second = _run(
        mock_datahub_graph, server, "run-2", dataset_identity=dataset_identity
    )

    assert server.report_definition_reads == ["report-1"]
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


@pytest.mark.parametrize("status, read_again", [(503, True), (500, False)])
def test_dashboard_with_a_transient_metric_model_failure_is_read_again(
    mock_datahub_graph: MagicMock,  # noqa: F811
    status: int,
    read_again: bool,
) -> None:
    first = _first_run(mock_datahub_graph, extract_metric_expressions=True)
    first.server.bump("dash-2")
    first.server.failing_metric_models["metric-2"] = status

    second = _run(
        mock_datahub_graph, first.server, "run-2", extract_metric_expressions=True
    )
    first.datahub.write(second.workunits)
    first.server.failing_metric_models.clear()
    first.server.definition_reads.clear()

    _run(mock_datahub_graph, first.server, "run-3", extract_metric_expressions=True)

    assert first.server.definition_reads == (["dash-2"] if read_again else [])


@pytest.mark.parametrize("status, read_again", [(503, True), (500, False)])
def test_report_with_a_transient_metric_model_failure_is_read_again(
    mock_datahub_graph: MagicMock,  # noqa: F811
    status: int,
    read_again: bool,
) -> None:
    first = _first_run(mock_datahub_graph, extract_metric_expressions=True)
    first.server.bump("report-1")
    first.server.failing_metric_models["metric-1"] = status

    second = _run(
        mock_datahub_graph, first.server, "run-2", extract_metric_expressions=True
    )
    first.datahub.write(second.workunits)
    first.server.failing_metric_models.clear()
    first.server.report_definition_reads.clear()

    _run(mock_datahub_graph, first.server, "run-3", extract_metric_expressions=True)

    assert first.server.report_definition_reads == (["report-1"] if read_again else [])


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
    assert sorted(server.definition_reads) == ["dash-1", "dash-2"]
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


def test_per_report_dataset_skips_when_search_and_object_dates_differ(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    server = _MicroStrategy()
    server.objects["report-1"]["dateModified"] = "2026-01-01T00:00:01.000+0000"
    server.search_report_fields = {"dateModified": "2026-01-01T00:00:00.000+0000"}
    options: Dict[str, object] = {
        "dataset_identity": "per_report",
        "extract_dashboards": False,
    }
    datahub = _DataHub(mock_datahub_graph)
    first = _run(mock_datahub_graph, server, "run-1", **options)
    datahub.write(first.workunits)
    assert server.report_definition_reads == ["report-1"]
    server.report_definition_reads.clear()

    second = _run(mock_datahub_graph, server, "run-2", **options)

    assert server.report_definition_reads == []
    assert second.source.report.report_datasets_skipped_unchanged == 1


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
        JobId(MSTR_UNCHANGED_OBJECTS_JOB_ID)
    )
    assert current is not None
    assert isinstance(current.state, MicroStrategyCheckpointState)
    assert current.state.dashboards


def test_fingerprint_excluded_fields_are_config_fields() -> None:
    assert set(MicroStrategyConfig.model_fields) >= FINGERPRINT_EXCLUDED_CONFIG_FIELDS


def _break_read(server: _MicroStrategy, failure: str) -> None:
    if failure == "dashboard":
        server.bump("dash-2")
        server.failing_definitions.add("dash-2")
        return
    server.bump("report-1")
    if failure == "report_unexpected":
        server.failing_report_definitions.add("report-1")
    else:
        server.failing_report_api.add("report-1")


def _repair_reads(server: _MicroStrategy) -> None:
    server.failing_definitions.clear()
    server.failing_report_definitions.clear()
    server.failing_report_api.clear()
    server.definition_reads.clear()
    server.report_definition_reads.clear()


@pytest.mark.parametrize("dataset_identity", ["per_dashboard", "per_report"])
@pytest.mark.parametrize("failure", ["dashboard", "report_unexpected", "report_api"])
def test_failed_read_keeps_its_last_read_and_is_retried(
    mock_datahub_graph: MagicMock,  # noqa: F811
    dataset_identity: str,
    failure: str,
) -> None:
    first = _first_run(mock_datahub_graph, dataset_identity=dataset_identity)
    _break_read(first.server, failure)

    second = _run(
        mock_datahub_graph, first.server, "run-2", dataset_identity=dataset_identity
    )
    first.datahub.write(second.workunits)

    assert _soft_deleted_urns(second.workunits) == set()
    assert _stale_urns(second.source) == _stale_urns(first.source)
    _repair_reads(first.server)

    third = _run(
        mock_datahub_graph, first.server, "run-3", dataset_identity=dataset_identity
    )
    full = _run(
        mock_datahub_graph,
        first.server,
        "run-4",
        dataset_identity=dataset_identity,
        skip_unchanged_objects=False,
    )

    if failure == "dashboard":
        assert "dash-2" in first.server.definition_reads
    else:
        assert "report-1" in first.server.report_definition_reads
    assert _stale_urns(third.source) == _stale_urns(full.source)


def _fail_after_emitting(method: str) -> AbstractContextManager[object]:
    original = getattr(MicroStrategySource, method)

    def failing(*args: object, **kwargs: object) -> Iterator[MetadataWorkUnit]:
        yield from original(*args, **kwargs)
        raise RuntimeError("failed after the stamp was written")

    return patch.object(MicroStrategySource, method, failing)


@pytest.mark.parametrize(
    "method, urn, aspect_type, reads",
    [
        ("_process_dashboard", DASH_1_URN, DashboardInfoClass, "definition_reads"),
        (
            "_process_report_object",
            REPORT_CHART_URN,
            ChartInfoClass,
            "report_definition_reads",
        ),
    ],
)
def test_object_whose_read_failed_after_writing_its_stamp_is_read_again(
    mock_datahub_graph: MagicMock,  # noqa: F811
    method: str,
    urn: str,
    aspect_type: Type[_Aspect],
    reads: str,
) -> None:
    first = _first_run(mock_datahub_graph)
    del first.datahub.aspects[(urn, aspect_type.ASPECT_NAME)]

    with _fail_after_emitting(method):
        second = _run(mock_datahub_graph, first.server, "run-2")
    first.datahub.write(second.workunits)

    assert _soft_deleted_urns(second.workunits) == set()
    assert _stale_urns(second.source) == _stale_urns(first.source)
    getattr(first.server, reads).clear()

    third = _run(mock_datahub_graph, first.server, "run-3")

    assert getattr(first.server, reads)
    assert not third.source.report.failures
    assert _stale_urns(third.source) == _stale_urns(first.source)


def test_per_report_failed_only_embedder_keeps_the_dataset(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    server = _MicroStrategy()
    server.dashboard_datasets["dash-1"] = ["ds-1"]
    server.dashboard_visualizations["dash-1"] = DASHBOARD_VISUALIZATIONS["dash-1"][:1]
    datahub = _DataHub(mock_datahub_graph)
    first = _run(mock_datahub_graph, server, "run-1", dataset_identity="per_report")
    datahub.write(first.workunits)
    server.bump("dash-2")
    server.failing_definitions.add("dash-2")

    second = _run(mock_datahub_graph, server, "run-2", dataset_identity="per_report")

    assert REPORT_DATASET_2_URN in _stale_urns(first.source)
    assert _soft_deleted_urns(second.workunits) == set()
    assert _stale_urns(second.source) == _stale_urns(first.source)
    assert REPORT_DATASET_2_URN not in _stamped_urns(second.workunits)


def test_per_report_dataset_with_a_failed_embedder_is_not_stamped(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph, dataset_identity="per_report")
    _break_read(first.server, "dashboard")

    second = _run(
        mock_datahub_graph, first.server, "run-2", dataset_identity="per_report"
    )

    assert REPORT_DATASET_2_URN not in _stamped_urns(second.workunits)


def test_per_report_enrichment_error_keeps_the_last_read(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph, dataset_identity="per_report")
    first.server.bump("report-1")

    with patch.object(
        MicroStrategySource, "_enrich_report_dataset", side_effect=RuntimeError("x")
    ):
        second = _run(
            mock_datahub_graph, first.server, "run-2", dataset_identity="per_report"
        )
    first.datahub.write(second.workunits)

    assert _soft_deleted_urns(second.workunits) == set()
    assert _stale_urns(second.source) == _stale_urns(first.source)
    first.server.report_definition_reads.clear()

    _run(mock_datahub_graph, first.server, "run-3", dataset_identity="per_report")

    assert "report-1" in first.server.report_definition_reads


def test_per_report_emit_failure_keeps_the_last_read(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph, dataset_identity="per_report")
    first.server.bump("dash-2")

    with patch.object(
        MicroStrategySource,
        "_report_dataset_entry_workunits",
        side_effect=RuntimeError("emit failed"),
    ):
        second = _run(
            mock_datahub_graph, first.server, "run-2", dataset_identity="per_report"
        )

    assert second.source.report.failures
    assert _stale_urns(second.source) == _stale_urns(first.source)


@pytest.mark.parametrize("removal", ["deleted", "denied"])
def test_missing_dashboard_is_soft_deleted_and_the_rest_kept(
    mock_datahub_graph: MagicMock,  # noqa: F811
    removal: str,
) -> None:
    first = _first_run(mock_datahub_graph)
    overrides: Dict[str, object] = {}
    if removal == "deleted":
        del first.server.dashboards["dash-2"]
    else:
        overrides["dashboard_pattern"] = {"deny": ["Cost Review"]}

    second = _run(mock_datahub_graph, first.server, "run-2", **overrides)

    soft_deleted = _soft_deleted_urns(second.workunits)
    assert DASH_2_URN in soft_deleted
    assert DASH_1_URN not in soft_deleted
    assert REPORT_CHART_URN not in soft_deleted
    assert DASH_2_URN not in _stale_urns(second.source)
    assert DASH_1_URN in _stale_urns(second.source)


@pytest.mark.parametrize("change", ["soft_deleted", "no_entry"])
@pytest.mark.parametrize(
    ("urn", "reads"),
    [
        (DASH_1_VIZ_URN, "dash-1"),
        (DASH_1_DATASET_URN, "dash-1"),
        (REPORT_CHART_URN, "report-1"),
        (REPORT_SOURCE_URN, "report-1"),
    ],
)
def test_object_is_read_again_when_a_tracked_entity_is_gone(
    mock_datahub_graph: MagicMock,  # noqa: F811
    urn: str,
    reads: str,
    change: str,
) -> None:
    first = _first_run(mock_datahub_graph)
    if change == "soft_deleted":
        first.datahub.soft_deleted.add(urn)
    else:
        del first.datahub.aspects[(urn, StatusClass.ASPECT_NAME)]

    _run(mock_datahub_graph, first.server, "run-2")

    if reads == "dash-1":
        assert first.server.definition_reads == ["dash-1"]
    else:
        assert first.server.report_definition_reads == ["report-1"]


def test_per_report_dataset_is_read_again_when_its_entity_is_gone(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph, dataset_identity="per_report")
    first.datahub.soft_deleted.add(REPORT_DATASET_1_URN)

    second = _run(
        mock_datahub_graph, first.server, "run-2", dataset_identity="per_report"
    )

    assert first.server.definition_reads == ["dash-1"]
    assert second.source.report.report_datasets_skipped_unchanged == 2


def test_ignore_old_state_reads_everything(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)

    second = _run(
        mock_datahub_graph,
        first.server,
        "run-2",
        stateful_ingestion={
            "enabled": True,
            "remove_stale_metadata": True,
            "ignore_old_state": True,
        },
    )

    assert sorted(first.server.definition_reads) == ["dash-1", "dash-2"]
    assert second.source.report.dashboards_skipped_unchanged == 0


def test_skipping_works_without_stale_removal(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    options: Dict[str, object] = {
        "stateful_ingestion": {"enabled": True, "remove_stale_metadata": False}
    }
    first = _first_run(mock_datahub_graph, **options)

    second = _run(mock_datahub_graph, first.server, "run-2", **options)

    assert first.server.definition_reads == []
    assert second.source.report.dashboards_skipped_unchanged == 2


def test_run_that_did_not_skip_leaves_nothing_to_skip(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph, skip_unchanged_objects=False)

    second = _run(mock_datahub_graph, first.server, "run-2")

    assert sorted(first.server.definition_reads) == ["dash-1", "dash-2"]
    assert second.source.report.dashboards_skipped_unchanged == 0


def _rename_dash_2(server: _MicroStrategy) -> None:
    server.dashboards["dash-2"]["name"] = "Renamed Review"


def _reown_dash_2(server: _MicroStrategy) -> None:
    server.dashboards["dash-2"]["owner"] = {"username": "other_owner"}


def _certify_dash_2(server: _MicroStrategy) -> None:
    server.dashboards["dash-2"]["certifiedInfo"] = {"certified": True}


def _move_dash_2(server: _MicroStrategy) -> None:
    server.dashboards["dash-2"]["ancestors"] = [{"id": "folder-9", "name": "Archive"}]


@pytest.mark.parametrize(
    "mutate", [_rename_dash_2, _reown_dash_2, _certify_dash_2, _move_dash_2]
)
def test_dashboard_inputs_read_the_dashboard_again(
    mock_datahub_graph: MagicMock,  # noqa: F811
    mutate: Callable[[_MicroStrategy], None],
) -> None:
    first = _first_run(mock_datahub_graph)
    mutate(first.server)

    _run(mock_datahub_graph, first.server, "run-2")

    assert first.server.definition_reads == ["dash-2"]


def test_moved_dependency_reads_the_dashboard_using_it(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)
    first.server.objects["report-1"]["ancestors"] = [
        {"id": "folder-9", "name": "Archive"}
    ]

    _run(mock_datahub_graph, first.server, "run-2")

    assert first.server.definition_reads == ["dash-1"]
    assert first.server.report_definition_reads == ["report-1"]


def test_warehouse_database_name_change_reads_everything(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)
    first.server.warehouses = [
        {**WAREHOUSE, "database": {**WAREHOUSE["database"], "name": "OTHER_DB"}}  # type: ignore[dict-item]
    ]

    _run(mock_datahub_graph, first.server, "run-2")

    assert sorted(first.server.definition_reads) == ["dash-1", "dash-2"]


def test_connector_version_change_reads_everything(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)

    with patch(
        "datahub.ingestion.source.microstrategy.incremental.__version__", "other"
    ):
        _run(mock_datahub_graph, first.server, "run-2")

    assert sorted(first.server.definition_reads) == ["dash-1", "dash-2"]


def test_source_cube_of_a_report_reads_the_report_again(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    server = _MicroStrategy()
    server.report_source_id = "ds-1"
    datahub = _DataHub(mock_datahub_graph)
    datahub.write(_run(mock_datahub_graph, server, "run-1").workunits)
    server.report_definition_reads.clear()
    server.bump("ds-1")

    _run(mock_datahub_graph, server, "run-2")
    assert server.report_definition_reads == ["report-1"]

    server.report_definition_reads.clear()
    del server.objects["ds-1"]

    _run(mock_datahub_graph, server, "run-3")
    assert server.report_definition_reads == ["report-1"]


@pytest.mark.parametrize("api", ["get_aspect", "get_entities"])
def test_change_check_failures_read_the_objects(
    mock_datahub_graph: MagicMock,  # noqa: F811
    api: str,
) -> None:
    first = _first_run(mock_datahub_graph)
    getattr(mock_datahub_graph, api).side_effect = RuntimeError("graph unavailable")

    second = _run(mock_datahub_graph, first.server, "run-2")

    assert sorted(first.server.definition_reads) == ["dash-1", "dash-2"]
    assert second.source.report.change_check_failures > 0
    assert _soft_deleted_urns(second.workunits) == set()


def test_report_dataset_check_failure_emits_the_dataset(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph, dataset_identity="per_report")

    def fail_for_report_dataset(urn: str, aspect_type: Type[_Aspect]) -> object:
        if urn == REPORT_DATASET_1_URN:
            raise RuntimeError("graph unavailable")
        return first.datahub.get_aspect(urn, aspect_type)

    mock_datahub_graph.get_aspect.side_effect = fail_for_report_dataset

    second = _run(
        mock_datahub_graph, first.server, "run-2", dataset_identity="per_report"
    )

    assert second.source.report.change_check_failures == 1
    assert _aspects(second.workunits, REPORT_DATASET_1_URN, DatasetPropertiesClass)


def test_lost_authentication_during_a_change_check_aborts_the_run(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)
    mock_datahub_graph.get_aspect.side_effect = MicroStrategyAuthError("expired")

    second = _run(mock_datahub_graph, first.server, "run-2")

    assert second.source.report.failures
    assert first.server.definition_reads == []


@pytest.mark.parametrize("skip_unchanged_objects", [True, False])
def test_oversized_checkpoint_warns(
    mock_datahub_graph: MagicMock,  # noqa: F811
    skip_unchanged_objects: bool,
) -> None:
    _DataHub(mock_datahub_graph)

    with patch.object(MicroStrategyStateHandler, "is_too_large", return_value=True):
        run = _run(
            mock_datahub_graph,
            _MicroStrategy(),
            "run-1",
            skip_unchanged_objects=skip_unchanged_objects,
        )

    assert "Unchanged-object checkpoint too large" in [
        warning.title for warning in run.source.report.warnings
    ]


@pytest.mark.parametrize(("max_size", "too_large"), [(100, True), (10**6, False)])
def test_checkpoint_size_limit(max_size: int, too_large: bool) -> None:
    provider = MagicMock()
    provider.is_stateful_ingestion_configured.return_value = True
    provider.stateful_ingestion_config.max_checkpoint_state_size = max_size
    handler = MicroStrategyStateHandler(
        provider,
        MicroStrategyConfig.model_validate(CONFIG),
        MicroStrategyReport(),
        "pipeline",
        "run-1",
        None,
    )
    state = MicroStrategyCheckpointState()
    for index in range(50):
        state.dashboards.setdefault("P", {})[f"D-{index}"] = ObjectCheckpoint(
            fingerprint="f" * 64, urns=[f"urn:li:dashboard:(microstrategy,d-{index})"]
        )
    provider.get_current_checkpoint.return_value = Checkpoint(
        job_name="microstrategy_unchanged_objects",
        pipeline_name="pipeline",
        run_id="run-1",
        state=state,
    )

    assert handler.is_too_large() is too_large


def test_model_tables_unavailable_reads_the_objects_again(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    server = _MicroStrategy()
    server.failing_model_tables = True
    datahub = _DataHub(mock_datahub_graph)
    first = _run(mock_datahub_graph, server, "run-1", extract_model_lineage=True)
    datahub.write(first.workunits)
    assert _stamped_urns(first.workunits) == []
    server.failing_model_tables = False
    server.definition_reads.clear()
    server.report_definition_reads.clear()

    second = _run(mock_datahub_graph, server, "run-2", extract_model_lineage=True)

    assert sorted(server.definition_reads) == ["dash-1", "dash-2"]
    assert server.report_definition_reads == ["report-1"]
    assert second.source.report.dashboards_skipped_unchanged == 0


def test_malformed_visualization_detail_reads_the_dashboard_again(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    server = _MicroStrategy()
    server.malformed_visualizations.add("dash-2")
    datahub = _DataHub(mock_datahub_graph)
    first = _run(mock_datahub_graph, server, "run-1")
    datahub.write(first.workunits)
    assert DASH_2_URN not in _stamped_urns(first.workunits)
    assert DASH_1_URN in _stamped_urns(first.workunits)
    server.malformed_visualizations.clear()
    server.definition_reads.clear()

    _run(mock_datahub_graph, server, "run-2")

    assert server.definition_reads == ["dash-2"]


def test_dependencies_unavailable_reads_the_dashboards_again(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    server = _MicroStrategy()
    server.failing_dependencies = True
    datahub = _DataHub(mock_datahub_graph)
    first = _run(mock_datahub_graph, server, "run-1")
    datahub.write(first.workunits)
    assert DASH_1_URN not in _stamped_urns(first.workunits)
    server.failing_dependencies = False
    server.definition_reads.clear()

    _run(mock_datahub_graph, server, "run-2")

    assert sorted(server.definition_reads) == ["dash-1", "dash-2"]


def test_unreadable_checkpoint_reads_everything_and_warns_once(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first = _first_run(mock_datahub_graph)
    original = StateProviderWrapper.get_last_checkpoint

    def unreadable(
        self: StateProviderWrapper,
        job_id: JobId,
        checkpoint_state_class: Type[CheckpointStateBase],
    ) -> object:
        if checkpoint_state_class is MicroStrategyCheckpointState:
            raise ValueError("saved by another version")
        return original(self, job_id, checkpoint_state_class)

    with patch.object(StateProviderWrapper, "get_last_checkpoint", unreadable):
        second = _run(mock_datahub_graph, first.server, "run-2")

    assert sorted(first.server.definition_reads) == ["dash-1", "dash-2"]
    assert [warning.title for warning in second.source.report.warnings].count(
        "Unchanged-object checkpoint unreadable"
    ) == 1
    assert _soft_deleted_urns(second.workunits) == set()


@pytest.mark.parametrize(
    ("stale_job", "stale_run_id", "trusted"),
    [
        (None, None, True),
        ("stale", None, False),
        ("stale", "other-run", False),
        ("stale", "run-1", True),
    ],
)
def test_saved_state_is_trusted_only_with_matching_stale_state(
    stale_job: Optional[str], stale_run_id: Optional[str], trusted: bool
) -> None:
    handler = MicroStrategyStateHandler(
        MagicMock(),
        MicroStrategyConfig.model_validate(CONFIG),
        MicroStrategyReport(),
        "pipeline",
        "run-2",
        JobId(stale_job) if stale_job else None,
    )
    last = MagicMock(run_id="run-1", state=MicroStrategyCheckpointState())
    stale = MagicMock(run_id=stale_run_id) if stale_run_id else None

    assert (handler._trusted(last, stale) is not None) is trusted


def test_checkpoint_state_normalizes_ids_and_urns() -> None:
    state = MicroStrategyCheckpointState.model_validate(
        {
            "dashboards": {
                "Project-1": {
                    "Dash-1": {"fingerprint": "f", "urns": ["urn:b", "urn:a", "urn:b"]}
                }
            }
        }
    )

    record = state.dashboards["PROJECT-1"]["DASH-1"]
    assert record.urns == ["urn:a", "urn:b"]


def test_report_dataset_plan_marks_what_must_be_emitted() -> None:
    plan = ReportDatasetPlan(
        previous_dashboards={
            "DASH-1": ObjectCheckpoint(dependency_ids=["REPORT-1"]),
            "DASH-2": ObjectCheckpoint(),
        },
        previous_reports={
            "REPORT-1": ReportDatasetCheckpoint(
                dashboard_ids=["DASH-1", "DASH-2"], from_report_pass=False
            ),
            "REPORT-2": ReportDatasetCheckpoint(from_report_pass=True),
            "REPORT-3": ReportDatasetCheckpoint(dashboard_ids=["DASH-2"]),
        },
        candidates=["DASH-1", "DASH-2"],
        unchanged={"DASH-1"},
    )

    plan.mark_reports_of_changed_dashboards()
    assert plan.dirty == {"REPORT-1", "REPORT-3"}
    assert plan.dashboards_to_read() == {"DASH-1"}
    assert plan.embedders("REPORT-1") == {"DASH-1", "DASH-2"}

    assert plan.read_in_report_pass("REPORT-2") is False
    assert plan.read_in_report_pass("REPORT-4") is True
    plan.mark_reports_left_report_pass()
    assert "REPORT-2" not in plan.dirty
    assert plan.emits("REPORT-4")
    assert not plan.emits("REPORT-2")
    assert set(plan.kept_reports()) == {"REPORT-2"}


def test_report_dataset_plan_drops_a_report_that_left_the_report_pass() -> None:
    plan = ReportDatasetPlan(
        previous_dashboards={},
        previous_reports={"REPORT-2": ReportDatasetCheckpoint(from_report_pass=True)},
        candidates=[],
        unchanged=set(),
    )

    plan.mark_reports_left_report_pass()

    assert plan.emits("REPORT-2")
    assert plan.kept_reports() == {}


def test_entities_present_checks_every_batch_and_skips_shared_references() -> None:
    urns = [
        f"urn:li:dataset:(urn:li:dataPlatform:microstrategy,ds-{index},PROD)"
        for index in range(ENTITY_CHECK_BATCH_SIZE + 5)
    ]
    present = {StatusClass.ASPECT_NAME: (StatusClass(removed=False), None)}
    graph = MagicMock()
    graph.get_entities.side_effect = lambda _type, batch, aspects: {
        urn: present for urn in batch
    }

    assert entities_present(graph, [*urns, "urn:li:tag:Measure"])
    assert graph.get_entities.call_count == 2

    last_missing = MagicMock()
    last_missing.get_entities.side_effect = lambda _type, batch, aspects: {
        urn: present for urn in batch if urn != urns[-1]
    }
    assert not entities_present(last_missing, urns)
