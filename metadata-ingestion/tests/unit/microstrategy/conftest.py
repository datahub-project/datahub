import json
import threading
from typing import Dict, Iterator, List, Optional, Set

import pytest
from pydantic import BaseModel, ConfigDict

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.microstrategy.client import MicroStrategyAPIError
from datahub.ingestion.source.microstrategy.models import (
    Datasource,
    DatasourceConnection,
    MicroStrategyObject,
    ModelTablesResponse,
    PredefinedFolder,
    Project,
    SqlView,
)
from datahub.ingestion.source.microstrategy.source import MicroStrategySource


@pytest.fixture
def mixed_datasources() -> List[Datasource]:
    return [
        Datasource.model_validate(
            {
                "id": "AAA111",
                "name": "oracle_ds",
                "database": {"type": "oracle", "name": "ORA_DB", "schema": "ORA"},
            }
        ),
        Datasource.model_validate(
            {
                "id": "BBB222",
                "name": "snowflake_ds",
                "database": {"type": "snow_flake", "name": "SNOW_DB", "schema": "SNOW"},
            }
        ),
    ]


def install_client(source: MicroStrategySource, client: object) -> None:
    source.client = client  # type: ignore[assignment]  # duck-typed fake


def model_table(
    name: str, fact_id: str, datasource_id: Optional[str]
) -> Dict[str, object]:
    table: Dict[str, object] = {
        "information": {"objectId": f"logical-{fact_id}", "name": name},
        "physicalTable": {"tableName": name},
        "facts": [
            {"information": {"objectId": fact_id}, "expression": {"text": "AMOUNT"}}
        ],
        "attributes": [],
    }
    if datasource_id:
        table["primaryDataSource"] = {"objectId": datasource_id, "name": "ignored"}
    return table


class ModelTablesClient:
    def __init__(self, tables: List[Dict[str, object]]) -> None:
        self.tables = tables
        self.fields_requested: List[str] = []
        self.offsets_requested: List[int] = []

    def list_model_tables(
        self,
        project_id: str,
        limit: Optional[int] = None,
        offset: int = 0,
        fields: str = "",
    ) -> ModelTablesResponse:
        self.fields_requested.append(fields)
        self.offsets_requested.append(offset)
        page = self.tables[offset : offset + limit] if limit else self.tables[offset:]
        return ModelTablesResponse(tables=page, total=len(self.tables))


class ConnectionModelTablesClient(ModelTablesClient):
    def __init__(
        self,
        tables: List[Dict[str, object]],
        fail_connection: bool = False,
        connections: Optional[Dict[str, Dict[str, object]]] = None,
    ) -> None:
        super().__init__(tables)
        self.fail_connection = fail_connection
        self.connections = connections or {}
        self.connection_requests: List[str] = []

    def get_datasource_connection(
        self, connection_id: str, project_id: Optional[str] = None
    ) -> DatasourceConnection:
        self.connection_requests.append(connection_id)
        if self.fail_connection:
            raise MicroStrategyAPIError("connection lookup failed")
        return DatasourceConnection.model_validate(
            {
                "id": connection_id,
                "name": "oracle_connection",
                "database": {"type": "oracle"},
                "connectionString": "SCHEMA=CONN_SCHEMA",
                **self.connections.get(connection_id, {}),
            }
        )


class ReportSqlModelTablesClient(ConnectionModelTablesClient):
    def create_report_instance(self, project_id: str, report_id: str) -> str:
        return f"ri-{report_id}"

    def get_report_sql_view(
        self, project_id: str, report_id: str, instance_id: str
    ) -> SqlView:
        return SqlView(sql_statement="select amount from orders")

    def delete_report_instance(self, *args: object) -> bool:
        return True


class WorkunitSignature(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    payload: str


def workunit_signatures(workunits: List[MetadataWorkUnit]) -> List[WorkunitSignature]:
    return [
        WorkunitSignature(
            id=wu.id,
            payload=json.dumps(wu.metadata.to_obj(), sort_keys=True, default=str),
        )
        for wu in workunits
    ]


_SHARED_DATASET_ID = "ds-shared-rpt"
_SHARED_DATASET_NAME = "Shared Orders Report"
_SHARED_FOLDER = {"id": "folder-shared", "name": "Shared Reports"}
_ORDERS_SQL = (
    "select a11.REGION as REGION, sum(a11.REVENUE) as TOTAL_REVENUE "
    "from RETAIL_DB.CORE.ORDERS a11 group by a11.REGION"
)
_MARGINS_SQL = (
    "select a11.REGION as REGION, sum(a11.MARGIN) as TOTAL_MARGIN "
    "from RETAIL_DB.CORE.MARGINS a11 group by a11.REGION"
)

_DERIVED_METRICS: Dict[str, Dict[str, object]] = {
    "doc-alpha": {
        "information": {"objectId": "1" * 32, "name": "Alpha Margin"},
        "expression": {"text": "[Total Revenue] * 0.25"},
    },
    "doc-beta": {
        "information": {"objectId": "2" * 32, "name": "Beta Margin"},
        "expression": {"text": "[Total Revenue] * 0.40"},
    },
}


def _shared_dataset_definition() -> Dict[str, object]:
    return {
        "id": _SHARED_DATASET_ID,
        "name": _SHARED_DATASET_NAME,
        "availableObjects": {
            "attributes": [{"id": "ATTR-REGION", "name": "Region"}],
            "metrics": [
                {"id": "MET-REV", "name": "Total Revenue", "dataType": "double"}
            ],
        },
    }


def _visualizations(prefix: str, count: int) -> List[Dict[str, object]]:
    return [
        {"key": f"viz-{prefix}-{idx}", "name": f"{prefix} Grid {idx}"}
        for idx in range(1, count + 1)
    ]


class ConcurrentFakeClient:
    """Two documents and a dossier sharing one report dataset, plus a standalone
    report. Thread-safe call logs; definitions named in `failures` raise."""

    def __init__(
        self,
        modified_overrides: Optional[Dict[str, str]] = None,
        failures: Optional[Dict[str, Exception]] = None,
    ) -> None:
        self._lock = threading.Lock()
        self._modified_overrides = dict(modified_overrides or {})
        self._failures = dict(failures or {})
        self.http_thread_names: Set[str] = set()
        self.dataset_lookup_calls: List[str] = []
        self.report_sql_calls: List[str] = []
        self.metric_model_calls: List[str] = []
        self.definition_reads: List[str] = []

    def _record(self, log: Optional[List[str]] = None, value: str = "") -> None:
        with self._lock:
            self.http_thread_names.add(threading.current_thread().name)
            if log is not None:
                log.append(value)

    def _modified_at(self, object_id: str) -> str:
        return self._modified_overrides.get(
            object_id.lower(), "2026-01-01T00:00:00.000+0000"
        )

    def _read_definition(self, object_id: str) -> None:
        self._record(self.definition_reads, object_id)
        failure = self._failures.get(object_id)
        if failure is not None:
            raise failure

    def login(self) -> None:
        return None

    def close(self) -> None:
        return None

    def list_projects(self) -> List[Project]:
        return [Project.model_validate({"id": "proj-1", "name": "Retail Analytics"})]

    def list_project_datasources(self, project_id: str) -> List[Datasource]:
        self._record()
        return [
            Datasource.model_validate(
                {
                    "id": "wh-1",
                    "name": "Analytics Snowflake",
                    "dbms": {"name": "Snowflake"},
                    "database": {
                        "type": "snowflake",
                        "connection": {"database": "RETAIL_DB", "schema": "CORE"},
                    },
                }
            )
        ]

    def list_datasources(self, project_id: Optional[str] = None) -> List[Datasource]:
        return self.list_project_datasources(project_id or "proj-1")

    def list_model_tables(
        self,
        project_id: str,
        *,
        limit: int = 1000,
        offset: int = 0,
        fields: Optional[str] = None,
    ) -> ModelTablesResponse:
        self._record()
        if offset > 0:
            return ModelTablesResponse(tables=[], total=1)
        return ModelTablesResponse(
            tables=[
                {
                    "information": {"objectId": "LGT-ORDERS", "name": "ORDERS"},
                    "physicalTable": {
                        "tableName": "ORDERS",
                        "information": {"objectId": "TBL-ORDERS", "name": "ORDERS"},
                    },
                    "attributes": [
                        {
                            "information": {
                                "objectId": "ATTR-REGION",
                                "name": "Region",
                            },
                            "forms": [],
                        }
                    ],
                    "facts": [
                        {"information": {"objectId": "FACT-REV", "name": "Revenue"}}
                    ],
                }
            ],
            total=1,
        )

    def get_predefined_folders(
        self, project_id: str, folder_types: List[int]
    ) -> List[PredefinedFolder]:
        if 7 not in folder_types:
            return []
        return [PredefinedFolder.model_validate({**_SHARED_FOLDER, "folderType": 7})]

    def _dashboard_object(
        self, object_id: str, name: str, view_media: int
    ) -> MicroStrategyObject:
        return MicroStrategyObject.model_validate(
            {
                "id": object_id,
                "name": name,
                "type": 55,
                "subtype": "14081",
                "viewMedia": view_media,
                "dateModified": self._modified_at(object_id),
                "ancestors": [_SHARED_FOLDER],
            }
        )

    def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            [
                self._dashboard_object("doc-alpha", "Alpha Document", 1),
                self._dashboard_object("doc-beta", "Beta Document", 1),
                self._dashboard_object("dossier-gamma", "Gamma Dossier", 1879072805),
            ]
        )

    def search_reports(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            [
                self._report_object(_SHARED_DATASET_ID),
                self._report_object("rpt-standalone"),
            ]
        )

    def _report_object(self, report_id: str) -> MicroStrategyObject:
        canonical_id = report_id.lower()
        name = (
            "Standalone Margin Report"
            if canonical_id == "rpt-standalone"
            else _SHARED_DATASET_NAME
        )
        return MicroStrategyObject.model_validate(
            {
                "id": canonical_id,
                "name": name,
                "type": 3,
                "subtype": 768,
                "dateModified": self._modified_at(canonical_id),
                "ancestors": [_SHARED_FOLDER],
            }
        )

    def create_document_instance(self, project_id: str, document_id: str) -> str:
        self._record()
        return f"inst-{document_id}"

    def create_dossier_instance(self, project_id: str, dossier_id: str) -> str:
        self._record()
        return f"inst-{dossier_id}"

    def delete_dossier_instance(self, *args: object) -> bool:
        return True

    def delete_document_instance(self, *args: object) -> bool:
        return True

    def get_document_definition(
        self, project_id: str, document_id: str
    ) -> Dict[str, object]:
        self._read_definition(document_id)
        suffix = "ALPHA" if document_id == "doc-alpha" else "BETA"
        return {
            "id": document_id,
            "datasets": [_shared_dataset_definition()],
            "chapters": [
                {
                    "key": "ch-1",
                    "name": "Overview",
                    "pages": [
                        {
                            "key": "pg-1",
                            "name": "Main",
                            "visualizations": _visualizations(suffix, 2),
                        }
                    ],
                }
            ],
        }

    def get_dossier_definition(
        self, project_id: str, dossier_id: str
    ) -> Dict[str, object]:
        self._read_definition(dossier_id)
        return {
            "result": {
                "definition": {
                    "datasets": [_shared_dataset_definition()],
                    "chapters": [
                        {
                            "key": "ch-g",
                            "name": "Summary",
                            "pages": [
                                {
                                    "key": "pg-g",
                                    "name": "Summary Page",
                                    "visualizations": _visualizations("GAMMA", 3),
                                }
                            ],
                        }
                    ],
                }
            }
        }

    def get_dossier_visualization(
        self,
        project_id: str,
        dossier_id: str,
        instance_id: str,
        chapter_key: str,
        visualization_key: str,
    ) -> Dict[str, object]:
        self._record()
        return {
            "datasets": [{"id": _SHARED_DATASET_ID, "name": _SHARED_DATASET_NAME}],
            "definition": {
                "grid": {
                    "rows": [
                        {"id": "ATTR-REGION", "name": "Region", "type": "attribute"}
                    ],
                    "columns": [
                        {"id": "MET-REV", "name": "Total Revenue", "type": "metric"}
                    ],
                }
            },
        }

    def get_object_dependencies(
        self, project_id: str, object_id: str, object_type: int
    ) -> List[MicroStrategyObject]:
        self._record()
        return [
            MicroStrategyObject.model_validate(
                {
                    "id": _SHARED_DATASET_ID,
                    "name": _SHARED_DATASET_NAME,
                    "type": 3,
                    "subtype": 768,
                    "dateModified": self._modified_at(_SHARED_DATASET_ID),
                }
            )
        ]

    def get_object_info(
        self, project_id: str, object_id: str, object_type: int
    ) -> Optional[MicroStrategyObject]:
        if object_type != 3:
            return None
        self._record(self.dataset_lookup_calls, object_id)
        return self._report_object(object_id)

    def get_report_object(
        self, project_id: str, report_id: str
    ) -> Optional[MicroStrategyObject]:
        self._record()
        return self._report_object(report_id)

    def get_model_document(
        self,
        project_id: str,
        document_id: str,
        instance_id: Optional[str] = None,
    ) -> Dict[str, object]:
        # Distinct formulas per document show workers do not overwrite each other.
        self._record()
        derived = _DERIVED_METRICS.get(document_id)
        if derived is None:
            return {"datasets": []}
        return {
            "datasets": [
                {
                    "information": {
                        "objectId": _SHARED_DATASET_ID,
                        "name": _SHARED_DATASET_NAME,
                    },
                    "derivedMetrics": [derived],
                }
            ]
        }

    def create_model_report_instance(self, project_id: str, report_id: str) -> str:
        return f"minst-{report_id}"

    def delete_model_report_instance(self, *args: object) -> bool:
        return True

    def get_model_report(
        self,
        project_id: str,
        report_id: str,
        instance_id: Optional[str] = None,
    ) -> Dict[str, object]:
        self._record()
        return {
            "information": {"objectId": report_id, "name": _SHARED_DATASET_NAME},
            "sourceType": "normal",
            "dataSource": {"dataTemplate": {"units": []}},
        }

    def get_metric_model(
        self, project_id: str, metric_id: str, stop: object = None
    ) -> Dict[str, object]:
        self._record(self.metric_model_calls, metric_id)
        return {
            "expression": {
                "text": "Sum(Revenue)",
                "tokens": [{"objectId": "FACT-REV", "subType": "fact"}],
            }
        }

    def get_dossier_datasets_sql(
        self, project_id: str, dossier_id: str, instance_id: str
    ) -> List[Dict[str, object]]:
        self._record()
        return [
            {
                "id": _SHARED_DATASET_ID,
                "name": _SHARED_DATASET_NAME,
                "sqlStatement": _ORDERS_SQL,
            }
        ]

    def create_report_instance(self, project_id: str, report_id: str) -> str:
        return f"rinst-{report_id}"

    def delete_report_instance(self, *args: object) -> None:
        return None

    def get_report_sql_view(
        self, project_id: str, report_id: str, instance_id: str
    ) -> SqlView:
        self._record(self.report_sql_calls, report_id)
        sql = _MARGINS_SQL if report_id.lower() == "rpt-standalone" else _ORDERS_SQL
        return SqlView.model_validate({"sqlStatement": sql})

    def get_report_definition(
        self, project_id: str, report_id: str
    ) -> Dict[str, object]:
        self._record()
        return {
            "id": report_id,
            "name": _SHARED_DATASET_NAME,
            "definition": {
                "grid": {
                    "rows": [
                        {
                            "name": "Attributes",
                            "elements": [
                                {
                                    "id": "ATTR-REGION",
                                    "name": "Region",
                                    "type": "attribute",
                                }
                            ],
                        }
                    ],
                    "columns": [
                        {
                            "name": "Metrics",
                            "elements": [
                                {
                                    "id": "MET-REV",
                                    "name": "Total Revenue",
                                    "type": "metric",
                                }
                            ],
                        }
                    ],
                }
            },
        }
