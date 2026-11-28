import json
import pathlib
import threading
import time
from typing import Dict, Iterator, List, Optional, Set, Type
from unittest import mock

import pytest
import requests
import time_machine
from pydantic import BaseModel, ConfigDict
from requests.adapters import HTTPAdapter

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.microstrategy.client import (
    MicroStrategyAuthError,
    MicroStrategyClient,
    ensure_session_pool_size,
)
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.constants import (
    MSTR_AUTH_TOKEN_HEADER,
    MSTR_IO_THREAD_NAME_PREFIX,
    MicroStrategyDatasetIdentity,
)
from datahub.ingestion.source.microstrategy.lineage import MicroStrategyLineageExtractor
from datahub.ingestion.source.microstrategy.microstrategy_semantic_model import (
    MicroStrategySemanticModelMapper,
)
from datahub.ingestion.source.microstrategy.models import (
    Datasource,
    MicroStrategyObject,
    ModelTablesResponse,
    PredefinedFolder,
    Project,
    SqlView,
)
from datahub.ingestion.source.microstrategy.parallel import (
    SingleFlight,
    ordered_parallel_map,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.ingestion.source.microstrategy.source import MicroStrategySource
from datahub.metadata.schema_classes import (
    SchemaMetadataClass,
    StatusClass,
    _Aspect,
)
from datahub.sql_parsing import sqlglot_lineage as sqlglot_lineage_module
from datahub.sql_parsing.schema_resolver import SchemaResolver


class _ProjectKey(ContainerKey):
    project_id: str


class _WorkunitSignature(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    payload: str


class _MappedItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    item: int
    value: int


class _AspectKey(BaseModel):
    model_config = ConfigDict(frozen=True)

    urn: str
    aspect_name: str


def _workunit_signatures(
    workunits: List[MetadataWorkUnit],
) -> List[_WorkunitSignature]:
    return [
        _WorkunitSignature(
            id=wu.id,
            payload=json.dumps(wu.metadata.to_obj(), sort_keys=True, default=str),
        )
        for wu in workunits
    ]


def test_ordered_parallel_map_preserves_order_and_inline_serial_mode() -> None:
    caller_thread = threading.current_thread()
    serial_threads: Set[Optional[int]] = set()

    def serial_fn(x: int) -> int:
        serial_threads.add(threading.current_thread().ident)
        return x * 10

    serial_results = [
        _MappedItem(item=outcome.item, value=outcome.unwrap())
        for outcome in ordered_parallel_map(serial_fn, [1, 2, 3], max_workers=1)
    ]
    assert serial_results == [
        _MappedItem(item=1, value=10),
        _MappedItem(item=2, value=20),
        _MappedItem(item=3, value=30),
    ]
    assert serial_threads == {caller_thread.ident}

    worker_threads: Set[str] = set()
    lock = threading.Lock()

    def staggered_fn(x: int) -> int:
        with lock:
            worker_threads.add(threading.current_thread().name)
        # Earlier items sleep longer so completions finish out of input order.
        time.sleep((5 - x) * 0.015)
        return x * 100

    parallel_results = [
        _MappedItem(item=outcome.item, value=outcome.unwrap())
        for outcome in ordered_parallel_map(
            staggered_fn, [0, 1, 2, 3, 4, 5], max_workers=4
        )
    ]
    assert parallel_results == [
        _MappedItem(item=0, value=0),
        _MappedItem(item=1, value=100),
        _MappedItem(item=2, value=200),
        _MappedItem(item=3, value=300),
        _MappedItem(item=4, value=400),
        _MappedItem(item=5, value=500),
    ]
    assert any(name.startswith(MSTR_IO_THREAD_NAME_PREFIX) for name in worker_threads)


def test_ordered_parallel_map_caps_max_pending_and_cancels_on_abort() -> None:
    lock = threading.Lock()
    in_flight = 0
    max_observed_in_flight = 0
    started_items: List[int] = []
    release_wave = threading.Event()
    wave_started = threading.Event()

    def work(item: int) -> int:
        nonlocal in_flight, max_observed_in_flight
        with lock:
            in_flight += 1
            max_observed_in_flight = max(max_observed_in_flight, in_flight)
            started_items.append(item)
            if len(started_items) >= 3:
                wave_started.set()
        try:
            if item >= 1:
                release_wave.wait(timeout=2.0)
            return item
        finally:
            with lock:
                in_flight -= 1

    iterator = ordered_parallel_map(work, range(20), max_workers=2, max_pending=3)
    try:
        first = next(iterator)
        assert first.unwrap() == 0
        assert wave_started.wait(timeout=2.0)
        time.sleep(0.03)
        with lock:
            assert max_observed_in_flight <= 2
            assert started_items == [0, 1, 2]
    finally:
        release_wave.set()
        iterator.close()

    with lock:
        assert len(started_items) < 20

    executed_after_auth: List[int] = []

    def auth_failing(item: int) -> int:
        if item == 0:
            raise MicroStrategyAuthError("401 session expired")
        time.sleep(0.05)
        with lock:
            executed_after_auth.append(item)
        return item

    with pytest.raises(MicroStrategyAuthError):
        stream = ordered_parallel_map(
            auth_failing, range(30), max_workers=2, max_pending=4
        )
        try:
            for outcome in stream:
                outcome.unwrap()
        finally:
            stream.close()

    assert len(executed_after_auth) < 10


def test_single_flight_deduplicates_and_propagates_errors() -> None:
    flight: SingleFlight[str, str] = SingleFlight()
    barrier = threading.Barrier(4)
    call_count = 0
    lock = threading.Lock()

    def fetch_once() -> str:
        nonlocal call_count
        with lock:
            call_count += 1
        time.sleep(0.03)
        return "shared-payload"

    results: List[str] = []

    def caller() -> None:
        barrier.wait(timeout=2.0)
        res = flight.do("same-key", fetch_once)
        with lock:
            results.append(res)

    threads = [threading.Thread(target=caller) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=3.0)

    assert call_count == 1
    assert results == ["shared-payload"] * 4

    err_barrier = threading.Barrier(3)
    err_calls = 0
    caught: List[str] = []

    def failing_fetch() -> str:
        nonlocal err_calls
        with lock:
            err_calls += 1
        time.sleep(0.02)
        raise RuntimeError("boom")

    def err_caller() -> None:
        err_barrier.wait(timeout=2.0)
        try:
            flight.do("failing-key", failing_fetch)
        except RuntimeError as exc:
            with lock:
                caught.append(str(exc))

    err_threads = [threading.Thread(target=err_caller) for _ in range(3)]
    for t in err_threads:
        t.start()
    for t in err_threads:
        t.join(timeout=3.0)

    assert err_calls == 1
    assert caught == ["boom"] * 3
    assert flight.do("failing-key", lambda: "recovered") == "recovered"


def test_client_pool_size_and_coalesced_concurrent_401_reauthentication() -> None:
    config = MicroStrategyConfig.model_validate(
        {
            "base_url": "https://mstr.example.com/MicroStrategyLibrary",
            "auth": {
                "type": "password",
                "username": "svc_user",
                "password": "secret",
            },
            "max_workers": 10,
        }
    )
    report = MicroStrategyReport()
    client = MicroStrategyClient(config, report)
    try:
        https_adapter = client.session.get_adapter("https://")
        assert isinstance(https_adapter, HTTPAdapter)
        assert https_adapter.poolmanager.connection_pool_kw["maxsize"] == 20

        extra_session = requests.Session()
        try:
            ensure_session_pool_size(extra_session, 24)
            mounted = extra_session.get_adapter("https://")
            assert isinstance(mounted, HTTPAdapter)
            assert mounted.poolmanager.connection_pool_kw["maxsize"] == 24
        finally:
            extra_session.close()

        client.session.headers[MSTR_AUTH_TOKEN_HEADER] = "stale-token"

        barrier = threading.Barrier(4)
        lock = threading.Lock()
        login_calls = 0

        def fake_response(
            status_code: int, payload: Dict[str, object]
        ) -> requests.Response:
            resp = requests.Response()
            resp.status_code = status_code
            resp._content = json.dumps(payload).encode("utf-8")
            resp.headers["Content-Type"] = "application/json"
            if status_code == 204:
                resp._content = b""
            return resp

        def fake_request(
            method: str,
            url: str,
            headers: Optional[Dict[str, str]] = None,
            **kwargs: object,
        ) -> requests.Response:
            nonlocal login_calls
            if url.endswith("/api/auth/login"):
                with lock:
                    login_calls += 1
                    token_num = login_calls
                resp = fake_response(204, {})
                resp.headers[MSTR_AUTH_TOKEN_HEADER] = f"fresh-token-{token_num}"
                return resp
            if url.endswith("/api/auth/logout"):
                return fake_response(204, {})

            sent = (headers or {}).get(MSTR_AUTH_TOKEN_HEADER)
            if sent == "stale-token":
                barrier.wait(timeout=2.0)
                return fake_response(401, {"message": "Auth token expired"})
            assert sent == "fresh-token-1"
            return fake_response(200, {"ok": True, "token": sent})

        results: List[requests.Response] = []
        errors: List[BaseException] = []

        def worker(idx: int) -> None:
            try:
                res = client._request("GET", f"/projects/p-{idx}")
                with lock:
                    results.append(res)
            except BaseException as exc:
                with lock:
                    errors.append(exc)

        with mock.patch.object(client.session, "request", side_effect=fake_request):
            threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=3.0)
            client.close()

        assert errors == []
        assert login_calls == 1
        assert len(results) == 4
        assert all(r.json()["token"] == "fresh-token-1" for r in results)
    finally:
        client.session.close()


def test_report_per_thread_phase_stack_isolates_concurrent_api_attribution() -> None:
    report = MicroStrategyReport()
    barrier = threading.Barrier(4)
    phase_names = [
        "visualization_details",
        "report_sql_fetch",
        "metric_models",
        "dataset_object_lookup",
    ]

    def worker(idx: int) -> None:
        phase_name = phase_names[idx]
        duration = float(idx + 1) * 0.1
        with report.phase(phase_name):
            barrier.wait(timeout=2.0)
            for _ in range(3):
                report.report_api_call("GET", f"/api/v2/{phase_name}", 200, duration)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=3.0)

    assert report._current_phase_stack() == []
    assert report.api_calls == 12
    for idx, phase_name in enumerate(phase_names):
        expected_secs = round(3 * float(idx + 1) * 0.1, 3)
        assert report.api_seconds_by_phase[phase_name] == pytest.approx(
            expected_secs, abs=1e-3
        )
        assert report.phase_calls[phase_name] == 1


class _ConcurrentFakeClient:
    def __init__(self, modified_overrides: Optional[Dict[str, str]] = None) -> None:
        self._lock = threading.Lock()
        self._modified_overrides: Dict[str, str] = dict(modified_overrides or {})
        self.http_thread_names: Set[str] = set()
        self._active_http = 0
        self.max_concurrent_http = 0
        self.dataset_lookup_calls: List[str] = []
        self.report_sql_calls: List[str] = []
        self.metric_model_calls: List[str] = []
        self.definition_reads: List[str] = []

    def _modified_at(self, object_id: str) -> str:
        return self._modified_overrides.get(
            object_id.lower(), "2026-01-01T00:00:00.000+0000"
        )

    def _enter_http(self, delay: float = 0.015) -> None:
        with self._lock:
            self.http_thread_names.add(threading.current_thread().name)
            self._active_http += 1
            self.max_concurrent_http = max(self.max_concurrent_http, self._active_http)
        try:
            time.sleep(delay)
        finally:
            with self._lock:
                self._active_http -= 1

    def login(self) -> None:
        return None

    def close(self) -> None:
        return None

    def list_projects(self) -> List[Project]:
        return [Project.model_validate({"id": "proj-1", "name": "Retail Analytics"})]

    def list_project_datasources(self, project_id: str) -> List[Datasource]:
        self._enter_http(0.005)
        return [
            Datasource.model_validate(
                {
                    "id": "wh-1",
                    "name": "Analytics Snowflake",
                    "dbms": {"name": "Snowflake"},
                    "database": {
                        "type": "snowflake",
                        "connection": {
                            "database": "RETAIL_DB",
                            "schema": "CORE",
                        },
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
        self._enter_http(0.005)
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
                        {
                            "information": {
                                "objectId": "FACT-REV",
                                "name": "Revenue",
                            }
                        }
                    ],
                }
            ],
            total=1,
        )

    def get_predefined_folders(
        self, project_id: str, folder_types: List[int]
    ) -> List[PredefinedFolder]:
        if 7 in folder_types:
            return [
                PredefinedFolder.model_validate(
                    {"id": "folder-shared", "name": "Shared Reports", "folderType": 7}
                )
            ]
        return []

    def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            [
                MicroStrategyObject.model_validate(
                    {
                        "id": "doc-alpha",
                        "name": "Alpha Document",
                        "type": 55,
                        "subtype": "14081",
                        "viewMedia": 1,
                        "dateModified": self._modified_at("doc-alpha"),
                        "ancestors": [
                            {"id": "folder-shared", "name": "Shared Reports"}
                        ],
                    }
                ),
                MicroStrategyObject.model_validate(
                    {
                        "id": "doc-beta",
                        "name": "Beta Document",
                        "type": 55,
                        "subtype": "14081",
                        "viewMedia": 1,
                        "dateModified": self._modified_at("doc-beta"),
                        "ancestors": [
                            {"id": "folder-shared", "name": "Shared Reports"}
                        ],
                    }
                ),
                MicroStrategyObject.model_validate(
                    {
                        "id": "dossier-gamma",
                        "name": "Gamma Dossier",
                        "type": 55,
                        "subtype": "14081",
                        "viewMedia": 1879072805,
                        "dateModified": self._modified_at("dossier-gamma"),
                        "ancestors": [
                            {"id": "folder-shared", "name": "Shared Reports"}
                        ],
                    }
                ),
            ]
        )

    def search_reports(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            [
                self._build_report_object("ds-shared-rpt"),
                self._build_report_object("rpt-standalone"),
            ]
        )

    def create_document_instance(self, project_id: str, document_id: str) -> str:
        self._enter_http(0.005)
        return f"inst-{document_id}"

    def create_dossier_instance(self, project_id: str, dossier_id: str) -> str:
        self._enter_http(0.005)
        return f"inst-{dossier_id}"

    def delete_dossier_instance(
        self, project_id: str, dossier_id: str, instance_id: str
    ) -> bool:
        return True

    def delete_document_instance(
        self, project_id: str, document_id: str, instance_id: str
    ) -> bool:
        return True

    def get_document_definition(
        self,
        project_id: str,
        document_id: str,
    ) -> Dict[str, object]:
        self._enter_http(0.02)
        with self._lock:
            self.definition_reads.append(document_id)
        suffix = "ALPHA" if document_id == "doc-alpha" else "BETA"
        return {
            "id": document_id,
            "datasets": [
                {
                    "id": "ds-shared-rpt",
                    "name": "Shared Orders Report",
                    "availableObjects": {
                        "attributes": [{"id": "ATTR-REGION", "name": "Region"}],
                        "metrics": [
                            {
                                "id": "MET-REV",
                                "name": "Total Revenue",
                                "dataType": "double",
                            }
                        ],
                    },
                }
            ],
            "chapters": [
                {
                    "key": "ch-1",
                    "name": "Overview",
                    "pages": [
                        {
                            "key": "pg-1",
                            "name": "Main",
                            "visualizations": [
                                {
                                    "key": f"viz-{suffix}-1",
                                    "name": f"{suffix} Grid 1",
                                    "visualizationType": "grid",
                                },
                                {
                                    "key": f"viz-{suffix}-2",
                                    "name": f"{suffix} Grid 2",
                                    "visualizationType": "grid",
                                },
                            ],
                        }
                    ],
                }
            ],
        }

    def get_dossier_definition(
        self,
        project_id: str,
        dossier_id: str,
    ) -> Dict[str, object]:
        self._enter_http(0.02)
        with self._lock:
            self.definition_reads.append(dossier_id)
        return {
            "result": {
                "definition": {
                    "datasets": [
                        {
                            "id": "ds-shared-rpt",
                            "name": "Shared Orders Report",
                            "availableObjects": {
                                "attributes": [{"id": "ATTR-REGION", "name": "Region"}],
                                "metrics": [
                                    {
                                        "id": "MET-REV",
                                        "name": "Total Revenue",
                                        "dataType": "double",
                                    }
                                ],
                            },
                        }
                    ],
                    "chapters": [
                        {
                            "key": "ch-g",
                            "name": "Summary",
                            "pages": [
                                {
                                    "key": "pg-g",
                                    "name": "Summary Page",
                                    "visualizations": [
                                        {
                                            "key": "viz-g-1",
                                            "name": "Gamma Chart 1",
                                            "visualizationType": "bar",
                                        },
                                        {
                                            "key": "viz-g-2",
                                            "name": "Gamma Chart 2",
                                            "visualizationType": "line",
                                        },
                                        {
                                            "key": "viz-g-3",
                                            "name": "Gamma Chart 3",
                                            "visualizationType": "kpi",
                                        },
                                    ],
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
        self._enter_http(0.015)
        return {
            "datasets": [{"id": "ds-shared-rpt", "name": "Shared Orders Report"}],
            "definition": {
                "grid": {
                    "rows": [
                        {
                            "id": "ATTR-REGION",
                            "name": "Region",
                            "type": "attribute",
                        }
                    ],
                    "columns": [
                        {
                            "id": "MET-REV",
                            "name": "Total Revenue",
                            "type": "metric",
                        }
                    ],
                }
            },
        }

    def get_object_dependencies(
        self, project_id: str, object_id: str, object_type: int
    ) -> List[MicroStrategyObject]:
        self._enter_http(0.005)
        return [
            MicroStrategyObject.model_validate(
                {
                    "id": "ds-shared-rpt",
                    "name": "Shared Orders Report",
                    "type": 3,
                    "subtype": 768,
                    "dateModified": self._modified_at("ds-shared-rpt"),
                }
            )
        ]

    def _build_report_object(self, report_id: str) -> MicroStrategyObject:
        canonical_id = report_id.lower()
        name = (
            "Standalone Margin Report"
            if canonical_id == "rpt-standalone"
            else "Shared Orders Report"
        )
        return MicroStrategyObject.model_validate(
            {
                "id": canonical_id,
                "name": name,
                "type": 3,
                "subtype": 768,
                "dateModified": self._modified_at(canonical_id),
                "ancestors": [{"id": "folder-shared", "name": "Shared Reports"}],
            }
        )

    def get_object_info(
        self, project_id: str, object_id: str, object_type: int
    ) -> Optional[MicroStrategyObject]:
        self._enter_http(0.01)
        if object_type != 3:
            return None
        with self._lock:
            self.dataset_lookup_calls.append(object_id)
        return self._build_report_object(object_id)

    def get_report_object(
        self, project_id: str, report_id: str
    ) -> Optional[MicroStrategyObject]:
        self._enter_http(0.01)
        return self._build_report_object(report_id)

    def get_model_document(
        self,
        project_id: str,
        document_id: str,
        instance_id: Optional[str] = None,
    ) -> Dict[str, object]:
        # Distinct formulas per document verify concurrent workers do not overwrite slots.
        self._enter_http(0.025)
        if document_id == "doc-alpha":
            metric_id = "1" * 32
            metric_name = "Alpha Margin"
            formula = "[Total Revenue] * 0.25"
        elif document_id == "doc-beta":
            metric_id = "2" * 32
            metric_name = "Beta Margin"
            formula = "[Total Revenue] * 0.40"
        else:
            return {"datasets": []}
        return {
            "datasets": [
                {
                    "information": {
                        "objectId": "ds-shared-rpt",
                        "name": "Shared Orders Report",
                    },
                    "derivedMetrics": [
                        {
                            "information": {
                                "objectId": metric_id,
                                "name": metric_name,
                            },
                            "expression": {"text": formula},
                        }
                    ],
                }
            ]
        }

    def create_model_report_instance(self, project_id: str, report_id: str) -> str:
        return f"minst-{report_id}"

    def delete_model_report_instance(
        self, project_id: str, report_id: str, instance_id: str
    ) -> bool:
        return True

    def get_model_report(
        self,
        project_id: str,
        report_id: str,
        instance_id: Optional[str] = None,
    ) -> Dict[str, object]:
        self._enter_http(0.01)
        return {
            "information": {"objectId": report_id, "name": "Shared Orders Report"},
            "sourceType": "normal",
            "dataSource": {"dataTemplate": {"units": []}},
        }

    def get_metric_model(self, project_id: str, metric_id: str) -> Dict[str, object]:
        self._enter_http(0.01)
        with self._lock:
            self.metric_model_calls.append(metric_id)
        return {
            "expression": {
                "text": "Sum(Revenue)",
                "tokens": [{"objectId": "FACT-REV", "subType": "fact"}],
            }
        }

    def get_dossier_datasets_sql(
        self, project_id: str, dossier_id: str, instance_id: str
    ) -> List[Dict[str, object]]:
        self._enter_http(0.015)
        return [
            {
                "id": "ds-shared-rpt",
                "name": "Shared Orders Report",
                "sqlStatement": (
                    "select a11.REGION as REGION, sum(a11.REVENUE) as TOTAL_REVENUE "
                    "from RETAIL_DB.CORE.ORDERS a11 group by a11.REGION"
                ),
            }
        ]

    def create_report_instance(self, project_id: str, report_id: str) -> str:
        return f"rinst-{report_id}"

    def delete_report_instance(
        self, project_id: str, report_id: str, instance_id: str
    ) -> None:
        return None

    def get_report_sql_view(
        self, project_id: str, report_id: str, instance_id: str
    ) -> SqlView:
        self._enter_http(0.015)
        with self._lock:
            self.report_sql_calls.append(report_id)
        if report_id.lower() == "rpt-standalone":
            return SqlView.model_validate(
                {
                    "sqlStatement": (
                        "select a11.REGION as REGION, sum(a11.MARGIN) as TOTAL_MARGIN "
                        "from RETAIL_DB.CORE.MARGINS a11 group by a11.REGION"
                    )
                }
            )
        return SqlView.model_validate(
            {
                "sqlStatement": (
                    "select a11.REGION as REGION, sum(a11.REVENUE) as TOTAL_REVENUE "
                    "from RETAIL_DB.CORE.ORDERS a11 group by a11.REGION"
                )
            }
        )

    def get_report_definition(
        self, project_id: str, report_id: str
    ) -> Dict[str, object]:
        self._enter_http(0.015)
        return {
            "id": report_id,
            "name": "Shared Orders Report",
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


class _SourceRunResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    workunits: List[MetadataWorkUnit]
    client: _ConcurrentFakeClient
    sql_parse_threads: Set[Optional[int]]


@pytest.mark.parametrize(
    "dataset_identity",
    [
        MicroStrategyDatasetIdentity.PER_DASHBOARD,
        MicroStrategyDatasetIdentity.PER_REPORT,
    ],
)
def test_end_to_end_parallel_workers_keep_sqlglot_on_main_thread_and_match_serial(
    dataset_identity: str,
) -> None:
    def run_source(max_workers: int) -> _SourceRunResult:
        config = MicroStrategyConfig.model_validate(
            {
                "base_url": "https://mstr.example.com/MicroStrategyLibrary",
                "extract_lineage": True,
                "extract_warehouse_lineage": True,
                "extract_document_dataset_sql_lineage": True,
                "extract_report_sql_lineage": True,
                "extract_reports": True,
                "extract_derived_metrics": True,
                "resolve_report_metrics_via_instance": True,
                "dataset_identity": dataset_identity,
                "max_workers": max_workers,
            }
        )
        source = MicroStrategySource(config, PipelineContext(run_id=f"w-{max_workers}"))
        fake_client = _ConcurrentFakeClient()
        source.client.close()
        source.client = fake_client  # type: ignore[assignment]

        sql_parse_threads: Set[Optional[int]] = set()
        original_sql_parse = sqlglot_lineage_module.create_lineage_from_sql_statements

        def tracked_sql_parse(*args: object, **kwargs: object) -> object:
            sql_parse_threads.add(threading.current_thread().ident)
            return original_sql_parse(*args, **kwargs)  # type: ignore[arg-type]

        with mock.patch.object(
            sqlglot_lineage_module,
            "create_lineage_from_sql_statements",
            side_effect=tracked_sql_parse,
        ):
            workunits = list(source.get_workunits_internal())
        assert source._model_documents_in_flight == {}
        return _SourceRunResult(
            workunits=workunits,
            client=fake_client,
            sql_parse_threads=sql_parse_threads,
        )

    main_ident = threading.current_thread().ident
    serial_run = run_source(max_workers=1)
    parallel_run = run_source(max_workers=4)

    assert any(
        name.startswith(MSTR_IO_THREAD_NAME_PREFIX)
        for name in parallel_run.client.http_thread_names
    )
    assert parallel_run.client.max_concurrent_http >= 2
    assert serial_run.sql_parse_threads == {main_ident}
    assert parallel_run.sql_parse_threads == {main_ident}

    assert parallel_run.client.dataset_lookup_calls == ["ds-shared-rpt"]
    assert parallel_run.client.metric_model_calls == ["MET-REV"]
    assert parallel_run.client.report_sql_calls.count("ds-shared-rpt") == 1

    assert _workunit_signatures(parallel_run.workunits) == _workunit_signatures(
        serial_run.workunits
    )

    schemas_by_urn: Dict[str, SchemaMetadataClass] = {}
    for wu in parallel_run.workunits:
        schema = wu.get_aspect_of_type(SchemaMetadataClass)
        if schema is not None:
            schemas_by_urn[wu.get_urn()] = schema

    if dataset_identity == MicroStrategyDatasetIdentity.PER_DASHBOARD:
        alpha_ds = next(u for u in schemas_by_urn if "doc-alpha.ds-shared-rpt" in u)
        beta_ds = next(u for u in schemas_by_urn if "doc-beta.ds-shared-rpt" in u)
        alpha_fields = {
            f.fieldPath: f.description or "" for f in schemas_by_urn[alpha_ds].fields
        }
        beta_fields = {
            f.fieldPath: f.description or "" for f in schemas_by_urn[beta_ds].fields
        }
        assert "0.25" in alpha_fields["Alpha Margin"]
        assert "Beta Margin" not in alpha_fields
        assert "0.40" in beta_fields["Beta Margin"]
        assert "Alpha Margin" not in beta_fields
    else:
        shared_ds = next(u for u in schemas_by_urn if "proj-1.ds-shared-rpt" in u)
        shared_fields = {
            f.fieldPath: f.description or "" for f in schemas_by_urn[shared_ds].fields
        }
        assert "0.25" in shared_fields["Alpha Margin"]
        assert "0.40" in shared_fields["Beta Margin"]


class _InMemoryGraphStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._aspects: Dict[_AspectKey, _Aspect] = {}
        self.session = requests.Session()

    def close(self) -> None:
        self.session.close()

    def build_graph(self) -> mock.MagicMock:
        graph = mock.MagicMock(spec=DataHubGraph)
        graph._session = self.session
        graph.get_aspect.side_effect = self.get_aspect
        graph.get_entities.side_effect = self.get_entities
        graph._make_schema_resolver.side_effect = (
            lambda platform, platform_instance, env, **_: SchemaResolver(
                platform=platform, platform_instance=platform_instance, env=env
            )
        )
        return graph

    def get_aspect(self, urn: str, aspect_type: Type[_Aspect]) -> Optional[_Aspect]:
        with self._lock:
            return self._aspects.get(
                _AspectKey(urn=urn, aspect_name=aspect_type.ASPECT_NAME)
            )

    def get_entities(
        self,
        entity_name: str,
        urns: List[str],
        aspects: List[str],
        with_system_metadata: bool = True,
    ) -> Dict[str, object]:
        if aspects != [StatusClass.ASPECT_NAME]:
            return {}
        found: Dict[str, object] = {}
        with self._lock:
            for urn in urns:
                status = self._aspects.get(
                    _AspectKey(urn=urn, aspect_name=StatusClass.ASPECT_NAME)
                )
                if isinstance(status, StatusClass):
                    found[urn] = {
                        StatusClass.ASPECT_NAME: (
                            StatusClass(removed=status.removed),
                            None,
                        )
                    }
        return found

    def write(self, workunits: List[MetadataWorkUnit]) -> None:
        with self._lock:
            for wu in workunits:
                if isinstance(wu.metadata, MetadataChangeProposalWrapper):
                    assert wu.metadata.entityUrn and wu.metadata.aspect
                    self._aspects[
                        _AspectKey(
                            urn=wu.metadata.entityUrn,
                            aspect_name=wu.metadata.aspect.ASPECT_NAME,
                        )
                    ] = wu.metadata.aspect


class _IncrementalPassResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    workunits: List[MetadataWorkUnit]
    report: MicroStrategyReport
    client: _ConcurrentFakeClient


@pytest.mark.parametrize(
    "dataset_identity",
    [
        MicroStrategyDatasetIdentity.PER_DASHBOARD,
        MicroStrategyDatasetIdentity.PER_REPORT,
    ],
)
def test_incremental_skip_unchanged_parallel_matches_serial(
    tmp_path: pathlib.Path,
    dataset_identity: str,
) -> None:
    def execute_series(max_workers: int) -> List[_IncrementalPassResult]:
        state_file = tmp_path / f"state-{dataset_identity}-{max_workers}.json"
        store = _InMemoryGraphStore()
        try:
            graph = store.build_graph()
            schedules: List[Dict[str, str]] = [
                {},
                {"doc-beta": "2026-02-01T00:00:00.000+0000"},
                {
                    "doc-beta": "2026-02-01T00:00:00.000+0000",
                    "rpt-standalone": "2026-03-01T00:00:00.000+0000",
                },
                {
                    "doc-beta": "2026-02-01T00:00:00.000+0000",
                    "rpt-standalone": "2026-03-01T00:00:00.000+0000",
                },
            ]
            passes: List[_IncrementalPassResult] = []
            for idx, modified_overrides in enumerate(schedules, start=1):
                with time_machine.travel(f"2026-06-0{idx}T00:00:00Z", tick=False):
                    config = MicroStrategyConfig.model_validate(
                        {
                            "base_url": "https://mstr.example.com/MicroStrategyLibrary",
                            "extract_lineage": True,
                            "extract_warehouse_lineage": True,
                            "extract_document_dataset_sql_lineage": True,
                            "extract_report_sql_lineage": True,
                            "extract_reports": True,
                            "extract_independent_reports": True,
                            "extract_derived_metrics": True,
                            "resolve_report_metrics_via_instance": True,
                            "dataset_identity": dataset_identity,
                            "skip_unchanged_objects": True,
                            "max_workers": max_workers,
                            "stateful_ingestion": {
                                "enabled": True,
                                "remove_stale_metadata": True,
                                "state_provider": {
                                    "type": "file",
                                    "config": {"filename": str(state_file)},
                                },
                            },
                        }
                    )
                    ctx = PipelineContext(
                        run_id=f"inc-{idx}",
                        pipeline_name=f"mstr-inc-{dataset_identity}",
                        graph=graph,
                    )
                    source = MicroStrategySource(config, ctx)
                    fake_client = _ConcurrentFakeClient(
                        modified_overrides=modified_overrides
                    )
                    source.client.close()
                    source.client = fake_client  # type: ignore[assignment]
                    workunits = list(source.get_workunits())
                    source.close()
                    for _, committable in ctx.get_committables():
                        committable.commit()
                    store.write(workunits)
                    passes.append(
                        _IncrementalPassResult(
                            workunits=workunits,
                            report=source.report,
                            client=fake_client,
                        )
                    )
            return passes
        finally:
            store.close()

    serial_passes = execute_series(max_workers=1)
    parallel_passes = execute_series(max_workers=4)

    for serial_pass, parallel_pass in zip(serial_passes, parallel_passes, strict=True):
        assert _workunit_signatures(parallel_pass.workunits) == _workunit_signatures(
            serial_pass.workunits
        )
        assert (
            parallel_pass.report.dashboards_skipped_unchanged
            == serial_pass.report.dashboards_skipped_unchanged
        )
        assert (
            parallel_pass.report.reports_skipped_unchanged
            == serial_pass.report.reports_skipped_unchanged
        )
        assert (
            parallel_pass.report.report_datasets_skipped_unchanged
            == serial_pass.report.report_datasets_skipped_unchanged
        )
        assert {
            wu.get_urn()
            for wu in parallel_pass.workunits
            if wu.id.startswith("soft-delete-")
        } == set()

    if dataset_identity == MicroStrategyDatasetIdentity.PER_DASHBOARD:
        assert parallel_passes[1].report.dashboards_skipped_unchanged == 2
        assert parallel_passes[1].report.reports_skipped_unchanged == 2
        assert parallel_passes[1].client.definition_reads == ["doc-beta"]
        assert parallel_passes[2].report.dashboards_skipped_unchanged == 3
        assert parallel_passes[2].report.reports_skipped_unchanged == 1
        assert parallel_passes[2].client.definition_reads == []
        assert parallel_passes[3].report.dashboards_skipped_unchanged == 3
        assert parallel_passes[3].report.reports_skipped_unchanged == 2
        assert parallel_passes[3].client.definition_reads == []
    else:
        assert parallel_passes[1].report.dashboards_skipped_unchanged == 0
        assert parallel_passes[1].report.report_datasets_skipped_unchanged == 1
        assert parallel_passes[2].report.dashboards_skipped_unchanged == 3
        assert parallel_passes[2].report.report_datasets_skipped_unchanged == 1
        assert parallel_passes[2].client.definition_reads == []
        assert parallel_passes[3].report.dashboards_skipped_unchanged == 3
        assert parallel_passes[3].report.report_datasets_skipped_unchanged == 2
        assert parallel_passes[3].client.definition_reads == []


class _ParallelSemanticClient:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.metric_threads: Set[str] = set()
        self.consolidation_calls: List[str] = []
        self._active = 0
        self.max_concurrent = 0

    def _enter(self) -> None:
        with self._lock:
            self.metric_threads.add(threading.current_thread().name)
            self._active += 1
            self.max_concurrent = max(self.max_concurrent, self._active)
        try:
            time.sleep(0.02)
        finally:
            with self._lock:
                self._active -= 1

    def search_metrics(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            [
                MicroStrategyObject.model_validate(
                    {"id": f"MET-{idx}", "name": f"Metric {idx}", "type": 4}
                )
                for idx in range(6)
            ]
        )

    def get_metric_model(self, project_id: str, metric_id: str) -> Dict[str, object]:
        self._enter()
        idx = int(metric_id.split("-")[1])
        if idx % 2 == 0:
            return {
                "expression": {
                    "text": f"Sum(Revenue) * {idx + 1}",
                    "tokens": [{"objectId": "FACT-1", "subType": "fact"}],
                }
            }
        return {
            "expression": {
                "text": f"Consolidated({idx})",
                "tokens": [{"objectId": "CONSOL-SHARED", "subType": "consolidation"}],
            }
        }

    def get_attribute_relationships(
        self, project_id: str, attribute_id: str
    ) -> Dict[str, object]:
        self._enter()
        if attribute_id == "ATTR-1":
            return {
                "children": [
                    {
                        "attribute": {"objectId": "ATTR-2", "name": "Store"},
                        "relationshipTable": {
                            "objectId": "LGT-STORE",
                            "name": "STORE",
                        },
                        "relationshipType": "one_to_many",
                    }
                ]
            }
        return {"children": []}

    def get_consolidation_model(
        self, project_id: str, consolidation_id: str
    ) -> Dict[str, object]:
        self._enter()
        with self._lock:
            self.consolidation_calls.append(consolidation_id)
        return {
            "elements": [
                {
                    "expression": {
                        "tree": {
                            "type": "elements_object",
                            "elements": [
                                {
                                    "attribute": {
                                        "objectId": "ATTR-1",
                                        "subType": "attribute",
                                    }
                                }
                            ],
                        }
                    }
                }
            ]
        }


class _SemanticEmitResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    workunits: List[MetadataWorkUnit]
    client: _ParallelSemanticClient


def test_semantic_model_parallel_emit_matches_serial_and_coalesces_consolidations() -> (
    None
):
    model_tables: List[Dict[str, object]] = [
        {
            "information": {"objectId": "LGT-SALES", "name": "SALES"},
            "physicalTable": {
                "tableName": "SALES",
                "information": {"objectId": "TBL-SALES", "name": "SALES"},
            },
            "attributes": [
                {"information": {"objectId": "ATTR-1", "name": "Region"}, "forms": []}
            ],
            "facts": [{"information": {"objectId": "FACT-1", "name": "Revenue"}}],
        },
        {
            "information": {"objectId": "LGT-STORE", "name": "STORE"},
            "physicalTable": {
                "tableName": "STORE",
                "information": {"objectId": "TBL-STORE", "name": "STORE"},
            },
            "attributes": [
                {"information": {"objectId": "ATTR-2", "name": "Store"}, "forms": []}
            ],
            "facts": [],
        },
    ]

    def run_emit(max_workers: int) -> _SemanticEmitResult:
        config = MicroStrategyConfig.model_validate(
            {
                "base_url": "https://mstr.example.com/MicroStrategyLibrary",
                "emit_semantic_model_entities": True,
                "max_workers": max_workers,
            }
        )
        report = MicroStrategyReport()
        client = _ParallelSemanticClient()
        mapper = MicroStrategySemanticModelMapper(
            config=config,
            report=report,
            client=client,  # type: ignore[arg-type]
            lineage=MicroStrategyLineageExtractor(config, report),
        )
        wus = list(
            mapper.emit(
                project_id="proj-1",
                project_name="Retail Project",
                model_tables=model_tables,
                warehouse_context=None,
                project_container_key=_ProjectKey(
                    platform="microstrategy",
                    instance=None,
                    env="PROD",
                    project_id="proj-1",
                ),
            )
        )
        return _SemanticEmitResult(workunits=wus, client=client)

    serial_emit = run_emit(max_workers=1)
    parallel_emit = run_emit(max_workers=4)

    assert any(
        name.startswith(MSTR_IO_THREAD_NAME_PREFIX)
        for name in parallel_emit.client.metric_threads
    )
    assert parallel_emit.client.max_concurrent >= 2
    assert parallel_emit.client.consolidation_calls == ["CONSOL-SHARED"]
    assert _workunit_signatures(parallel_emit.workunits) == _workunit_signatures(
        serial_emit.workunits
    )
