import json
import logging
import pathlib
import threading
from typing import Callable, Dict, Iterator, List, Optional, Set, Type
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
from datahub.ingestion.source.microstrategy import lineage as lineage_module
from datahub.ingestion.source.microstrategy.client import (
    MicroStrategyAPIError,
    MicroStrategyAuthError,
    MicroStrategyClient,
    MicroStrategyRequestStopped,
    http_pool_size,
)
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.constants import (
    MSTR_API_METADATA_SEARCHES,
    MSTR_AUTH_TOKEN_HEADER,
    MSTR_IO_THREAD_NAME_PREFIX,
    MSTR_METRIC_MODEL_CONSECUTIVE_FAILURE_LIMIT,
    MicroStrategyDatasetIdentity,
)
from datahub.ingestion.source.microstrategy.lineage import MicroStrategyLineageExtractor
from datahub.ingestion.source.microstrategy.microstrategy_semantic_model import (
    MicroStrategySemanticModelMapper,
)
from datahub.ingestion.source.microstrategy.models import (
    DatasourceConnection,
    MicroStrategyObject,
    PredefinedFolder,
)
from datahub.ingestion.source.microstrategy.parallel import (
    TaskFailure,
    ordered_parallel_map,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.ingestion.source.microstrategy.source import MicroStrategySource
from datahub.ingestion.source.microstrategy.spill_cache import SpillConnection
from datahub.metadata.schema_classes import (
    SchemaMetadataClass,
    StatusClass,
    _Aspect,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from tests.unit.microstrategy.conftest import (
    ConcurrentFakeClient,
    install_client,
    workunit_signatures,
)

_BASE_URL = "https://mstr.example.com/MicroStrategyLibrary"
_WAIT_SECONDS = 5.0


class _ProjectKey(ContainerKey):
    project_id: str


class _AspectKey(BaseModel):
    model_config = ConfigDict(frozen=True)

    urn: str
    aspect_name: str


def _run_threads(count: int, target: Callable[[int], None]) -> None:
    threads = [threading.Thread(target=target, args=(idx,)) for idx in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=_WAIT_SECONDS)
        assert not thread.is_alive()


def _live_worker_threads() -> List[str]:
    return [
        thread.name
        for thread in threading.enumerate()
        if thread.name.startswith(MSTR_IO_THREAD_NAME_PREFIX)
    ]


@pytest.mark.parametrize("max_workers", [1, 4])
def test_outcomes_keep_input_order(max_workers: int) -> None:
    last_done = threading.Event()

    def work(item: int) -> int:
        # The first item finishes last whenever it can run concurrently.
        if item == 0 and max_workers > 1:
            assert last_done.wait(_WAIT_SECONDS)
        if item == 3:
            last_done.set()
        return item * 10

    with ordered_parallel_map(work, range(4), max_workers=max_workers) as outcomes:
        assert [outcome.unwrap() for outcome in outcomes] == [0, 10, 20, 30]


def test_inline_mode_runs_on_the_calling_thread() -> None:
    threads: Set[Optional[int]] = set()

    def work(item: int) -> int:
        threads.add(threading.current_thread().ident)
        return item

    with ordered_parallel_map(work, range(3), max_workers=1) as outcomes:
        list(outcomes)
    assert threads == {threading.current_thread().ident}


def test_items_pulled_ahead_of_the_consumer_stay_within_max_pending() -> None:
    pulled = 0

    def items() -> Iterator[int]:
        nonlocal pulled
        for item in range(20):
            pulled += 1
            yield item

    with ordered_parallel_map(
        lambda item: item, items(), max_workers=2, max_pending=3
    ) as outcomes:
        for consumed, _ in enumerate(outcomes, start=1):
            assert pulled <= consumed + 3
    assert pulled == 20


def test_a_task_error_is_isolated_to_its_outcome() -> None:
    def work(item: int) -> int:
        if item == 1:
            raise MicroStrategyAPIError("boom")
        return item

    with ordered_parallel_map(work, range(3), max_workers=2) as outcomes:
        results = list(outcomes)

    assert [isinstance(outcome, TaskFailure) for outcome in results] == [
        False,
        True,
        False,
    ]
    with pytest.raises(MicroStrategyAPIError):
        results[1].unwrap()


def test_close_cancels_work_that_has_not_started() -> None:
    started: List[int] = []
    lock = threading.Lock()

    def work(item: int) -> int:
        with lock:
            started.append(item)
        return item

    with ordered_parallel_map(
        work, range(50), max_workers=2, max_pending=4
    ) as outcomes:
        assert next(outcomes).unwrap() == 0

    assert len(started) <= 1 + 4
    assert _live_worker_threads() == []


@pytest.mark.parametrize("max_workers", [1, 3])
def test_upstream_error_is_raised_after_submitted_items(max_workers: int) -> None:
    def items() -> Iterator[int]:
        yield from range(3)
        raise ValueError("search failed")

    seen: List[int] = []
    with pytest.raises(ValueError, match="search failed"):
        with ordered_parallel_map(
            lambda item: item, items(), max_workers=max_workers
        ) as outcomes:
            for outcome in outcomes:
                seen.append(outcome.unwrap())

    assert seen == [0, 1, 2]


@pytest.mark.parametrize("max_workers", [1, 3])
def test_close_closes_the_upstream_generator(max_workers: int) -> None:
    closed = threading.Event()

    def items() -> Iterator[int]:
        try:
            yield from range(10)
        finally:
            closed.set()

    with ordered_parallel_map(
        lambda item: item, items(), max_workers=max_workers
    ) as outcomes:
        next(outcomes)

    assert closed.is_set()


def test_max_workers_below_one_is_rejected() -> None:
    with pytest.raises(ValueError):
        ordered_parallel_map(lambda item: item, [1], max_workers=0)


def _client(max_workers: int = 4) -> MicroStrategyClient:
    config = MicroStrategyConfig.model_validate(
        {
            "base_url": _BASE_URL,
            "auth": {"type": "password", "username": "svc_user", "password": "pw"},
            "max_workers": max_workers,
        }
    )
    return MicroStrategyClient(config, MicroStrategyReport())


def _response(
    status_code: int,
    token: Optional[str] = None,
    payload: Optional[Dict[str, object]] = None,
) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = (
        b"" if status_code == 204 else json.dumps(payload or {}).encode()
    )
    response.headers["Content-Type"] = "application/json"
    if token:
        response.headers[MSTR_AUTH_TOKEN_HEADER] = token
    return response


def _sent_token(headers: Optional[Dict[str, str]]) -> Optional[str]:
    return (headers or {}).get(MSTR_AUTH_TOKEN_HEADER)


def test_client_pool_fits_every_worker_and_keeps_session_defaults() -> None:
    client = _client(max_workers=10)
    try:
        adapter = client.session.get_adapter("https://")
        assert isinstance(adapter, HTTPAdapter)
        assert adapter.poolmanager.connection_pool_kw["maxsize"] == http_pool_size(10)
        assert http_pool_size(10) >= 10 * 2
        for header in requests.utils.default_headers():
            assert header in client.session.headers
        assert client.session.headers["Accept"] == "application/json"
    finally:
        client.session.close()


def test_source_leaves_the_datahub_graph_session_alone() -> None:
    graph_session = requests.Session()
    adapter = graph_session.get_adapter("https://")
    graph = mock.MagicMock(spec=DataHubGraph)
    graph._session = graph_session
    config = MicroStrategyConfig.model_validate(
        {"base_url": _BASE_URL, "max_workers": 16}
    )
    try:
        MicroStrategySource(config, PipelineContext(run_id="graph", graph=graph))
        assert graph_session.get_adapter("https://") is adapter
    finally:
        graph_session.close()


def test_concurrent_401s_relogin_once() -> None:
    client = _client()
    client.session.headers[MSTR_AUTH_TOKEN_HEADER] = "stale"
    barrier = threading.Barrier(4)
    lock = threading.Lock()
    logins: List[str] = []

    def fake_request(
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs: object,
    ) -> requests.Response:
        if url.endswith("/api/auth/login"):
            with lock:
                logins.append(url)
            return _response(204, token="fresh")
        if _sent_token(headers) == "stale":
            barrier.wait(timeout=_WAIT_SECONDS)
            return _response(401)
        assert _sent_token(headers) == "fresh"
        return _response(200)

    errors: List[BaseException] = []

    def worker(idx: int) -> None:
        try:
            client._request("GET", f"/projects/p-{idx}")
        except BaseException as error:
            errors.append(error)

    with mock.patch.object(client.session, "request", side_effect=fake_request):
        _run_threads(4, worker)

    assert errors == []
    assert len(logins) == 1


def test_401_after_another_workers_relogin_replays_with_the_new_token() -> None:
    # A replays with token-1 and is rejected only because B's re-login replaced
    # token-1 meanwhile; A must replay with token-2, not declare auth dead.
    client = _client()
    client.session.headers[MSTR_AUTH_TOKEN_HEADER] = "stale"
    first_login_done = threading.Event()
    second_login_done = threading.Event()
    lock = threading.Lock()
    logins: List[str] = []

    def fake_request(
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs: object,
    ) -> requests.Response:
        sent = _sent_token(headers)
        if url.endswith("/api/auth/login"):
            with lock:
                logins.append(url)
                token = f"token-{len(logins)}"
            (first_login_done if token == "token-1" else second_login_done).set()
            return _response(204, token=token)
        if sent == "token-2":
            return _response(200)
        if url.endswith("/a") and sent == "token-1":
            assert second_login_done.wait(_WAIT_SECONDS)
        return _response(401)

    errors: List[BaseException] = []

    def worker(idx: int) -> None:
        try:
            if idx == 1:
                assert first_login_done.wait(_WAIT_SECONDS)
            client._request("GET", "/a" if idx == 0 else "/b")
        except BaseException as error:
            errors.append(error)

    with mock.patch.object(client.session, "request", side_effect=fake_request):
        _run_threads(2, worker)

    assert errors == []
    assert len(logins) == 2


def test_failed_relogin_runs_login_once_and_every_waiter_raises() -> None:
    client = _client()
    client.session.headers[MSTR_AUTH_TOKEN_HEADER] = "stale"
    barrier = threading.Barrier(4)
    lock = threading.Lock()
    logins: List[str] = []

    def fake_request(
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs: object,
    ) -> requests.Response:
        if url.endswith("/api/auth/login"):
            with lock:
                logins.append(url)
            return _response(401)
        barrier.wait(timeout=_WAIT_SECONDS)
        return _response(401)

    errors: List[BaseException] = []

    def worker(idx: int) -> None:
        try:
            client._request("GET", f"/projects/p-{idx}")
        except BaseException as error:
            with lock:
                errors.append(error)

    with mock.patch.object(client.session, "request", side_effect=fake_request):
        _run_threads(4, worker)

    assert len(logins) == 1
    assert len(errors) == 4
    assert all(isinstance(error, MicroStrategyAuthError) for error in errors)


def test_stop_event_ends_a_retry_wait() -> None:
    client = _client()
    stop = threading.Event()

    def unavailable(*args: object, **kwargs: object) -> requests.Response:
        stop.set()
        response = _response(503)
        response.headers["Retry-After"] = "60"
        return response

    with (
        mock.patch.object(
            client.session, "request", side_effect=unavailable
        ) as request,
        pytest.raises(MicroStrategyRequestStopped),
    ):
        client._request("GET", "/model/metrics/M-1", stop=stop)
    assert request.call_count == 1


def test_concurrent_metadata_searches_do_not_clobber_active_search() -> None:
    client = _client()
    client.session.headers[MSTR_AUTH_TOKEN_HEADER] = "token-1"
    barrier = threading.Barrier(4)
    second_post = threading.Event()
    lock = threading.Lock()
    active_search_id: Optional[str] = None
    search_targets: Dict[str, str] = {}

    def fake_request(
        method: str,
        url: str,
        params: Optional[Dict[str, object]] = None,
        **kwargs: object,
    ) -> requests.Response:
        nonlocal active_search_id
        assert url.endswith(MSTR_API_METADATA_SEARCHES)
        assert params is not None
        if method == "POST":
            used_by = str(params["usedByObject"]).split(";")[0]
            with lock:
                search_id = f"search-{len(search_targets)}"
                search_targets[search_id] = used_by
                active_search_id = search_id
                is_first = len(search_targets) == 1
                if not is_first:
                    second_post.set()
            if is_first:
                second_post.wait(timeout=0.05)
            return _response(200, payload={"id": search_id})
        search_id = str(params["searchId"])
        with lock:
            current = active_search_id
            target = search_targets[search_id]
        if search_id != current:
            return _response(404)
        return _response(
            200,
            payload={"result": [{"id": f"dep-{target}", "name": f"Dep {target}"}]},
        )

    results: Dict[int, List[str]] = {}
    errors: List[BaseException] = []

    def worker(idx: int) -> None:
        barrier.wait(timeout=_WAIT_SECONDS)
        try:
            deps = client.get_object_dependencies("proj-1", f"doc-{idx}", "55")
            with lock:
                results[idx] = [dep.id for dep in deps]
        except BaseException as error:
            with lock:
                errors.append(error)

    with mock.patch.object(client.session, "request", side_effect=fake_request):
        _run_threads(4, worker)

    assert errors == []
    assert results == {idx: [f"dep-doc-{idx}"] for idx in range(4)}


def _source(
    max_workers: int,
    dataset_identity: str = MicroStrategyDatasetIdentity.PER_DASHBOARD,
    client: Optional[ConcurrentFakeClient] = None,
) -> MicroStrategySource:
    config = MicroStrategyConfig.model_validate(
        {
            "base_url": _BASE_URL,
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
    source.client.close()
    install_client(source, client or ConcurrentFakeClient())
    return source


class _SourceRun(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    workunits: List[MetadataWorkUnit]
    source: MicroStrategySource
    client: ConcurrentFakeClient
    sql_parse_threads: Set[Optional[int]]


def _run_source(
    max_workers: int,
    dataset_identity: str = MicroStrategyDatasetIdentity.PER_DASHBOARD,
    failures: Optional[Dict[str, Exception]] = None,
) -> _SourceRun:
    client = ConcurrentFakeClient(failures=failures)
    source = _source(max_workers, dataset_identity, client)
    sql_parse_threads: Set[Optional[int]] = set()
    original_sql_parse = lineage_module.create_lineage_from_sql_statements

    def tracked_sql_parse(*args: object, **kwargs: object) -> object:
        sql_parse_threads.add(threading.current_thread().ident)
        return original_sql_parse(*args, **kwargs)  # type: ignore[arg-type]  # pass-through

    with mock.patch.object(
        lineage_module,
        "create_lineage_from_sql_statements",
        side_effect=tracked_sql_parse,
    ):
        workunits = list(source.get_workunits_internal())
    return _SourceRun(
        workunits=workunits,
        source=source,
        client=client,
        sql_parse_threads=sql_parse_threads,
    )


def _schema_fields(workunits: List[MetadataWorkUnit], urn_part: str) -> Set[str]:
    for wu in workunits:
        schema = wu.get_aspect_of_type(SchemaMetadataClass)
        if schema is not None and urn_part in wu.get_urn():
            return {field.fieldPath for field in schema.fields}
    raise AssertionError(f"no schema for {urn_part}")


@pytest.mark.parametrize(
    "dataset_identity",
    [
        MicroStrategyDatasetIdentity.PER_DASHBOARD,
        MicroStrategyDatasetIdentity.PER_REPORT,
    ],
)
def test_parallel_run_matches_serial_and_parses_sql_on_the_main_thread(
    dataset_identity: str,
) -> None:
    serial = _run_source(1, dataset_identity)
    parallel = _run_source(4, dataset_identity)

    assert any(
        name.startswith(MSTR_IO_THREAD_NAME_PREFIX)
        for name in parallel.client.http_thread_names
    )
    main_thread = {threading.current_thread().ident}
    assert serial.sql_parse_threads == parallel.sql_parse_threads == main_thread
    assert parallel.client.dataset_lookup_calls == ["ds-shared-rpt"]
    assert parallel.client.metric_model_calls == ["MET-REV"]
    assert parallel.client.report_sql_calls.count("ds-shared-rpt") == 1
    assert workunit_signatures(parallel.workunits) == workunit_signatures(
        serial.workunits
    )
    if dataset_identity == MicroStrategyDatasetIdentity.PER_DASHBOARD:
        alpha = _schema_fields(parallel.workunits, "doc-alpha.ds-shared-rpt")
        beta = _schema_fields(parallel.workunits, "doc-beta.ds-shared-rpt")
        assert "Alpha Margin" in alpha and "Beta Margin" not in alpha
        assert "Beta Margin" in beta and "Alpha Margin" not in beta
    else:
        shared = _schema_fields(parallel.workunits, "proj-1.ds-shared-rpt")
        assert {"Alpha Margin", "Beta Margin"} <= shared


def test_api_error_in_one_worker_leaves_the_other_dashboards_as_serial() -> None:
    failures: Dict[str, Exception] = {"doc-beta": MicroStrategyAPIError("boom")}
    serial = _run_source(1, failures=failures)
    parallel = _run_source(4, failures=failures)

    assert workunit_signatures(parallel.workunits) == workunit_signatures(
        serial.workunits
    )
    assert [w.title for w in parallel.source.report.warnings] == [
        w.title for w in serial.source.report.warnings
    ]
    assert parallel.source.report.failures == serial.source.report.failures == []
    assert any("doc-alpha" in wu.get_urn() for wu in parallel.workunits)


def test_auth_error_in_a_worker_stops_the_run_with_one_failure() -> None:
    run = _run_source(4, failures={"doc-beta": MicroStrategyAuthError("lost")})

    assert len(run.source.report.failures) == 1
    assert _live_worker_threads() == []


def _timeout_error() -> MicroStrategyAPIError:
    error = MicroStrategyAPIError("Read timed out")
    error.__cause__ = requests.ReadTimeout()
    return error


class _TimeoutStreakClient(ConcurrentFakeClient):
    def __init__(
        self,
        timed_out_ids: Set[str],
        *,
        reports_only: bool = False,
        reorder_completions: bool,
    ) -> None:
        super().__init__()
        self._timed_out_ids = timed_out_ids
        self._reports_only = reports_only
        self._reorder = reorder_completions
        self._first_event = threading.Event()
        self._second_event = threading.Event()

    def search_dashboards(self, project_id: str) -> Iterator[MicroStrategyObject]:
        if self._reports_only:
            return iter([])
        return iter(
            [
                self._dashboard_object(f"dossier-{idx}", f"Dossier {idx}", 1879072805)
                for idx in range(5)
            ]
        )

    def search_reports(self, project_id: str) -> Iterator[MicroStrategyObject]:
        if self._reports_only:
            return iter([self._report_object(f"rpt-{idx}") for idx in range(10)])
        return iter([self._report_object("rpt-standalone")])

    def _create_instance(self, object_id: str, prefix: str) -> str:
        self._record()
        if self._reorder:
            if self._timed_out_ids == {f"{prefix}-0", f"{prefix}-2"}:
                # Let item 1 succeed first so both timeouts finish back-to-back.
                if object_id == f"{prefix}-1":
                    self._first_event.set()
                elif object_id in self._timed_out_ids:
                    assert self._first_event.wait(_WAIT_SECONDS)
            elif self._timed_out_ids == {f"{prefix}-0", f"{prefix}-1"}:
                # Place item 2's fast success between the two consecutive timeouts.
                if object_id == f"{prefix}-0":
                    self._first_event.set()
                elif object_id == f"{prefix}-2":
                    assert self._first_event.wait(_WAIT_SECONDS)
                    self._second_event.set()
                elif object_id == f"{prefix}-1":
                    assert self._second_event.wait(_WAIT_SECONDS)
        if object_id in self._timed_out_ids:
            raise _timeout_error()
        return f"inst-{object_id}"

    def create_document_instance(self, project_id: str, document_id: str) -> str:
        return self._create_instance(document_id, "dossier")

    def create_dossier_instance(self, project_id: str, dossier_id: str) -> str:
        return self._create_instance(dossier_id, "dossier")

    def create_report_instance(self, project_id: str, report_id: str) -> str:
        return self._create_instance(report_id, "rpt")

    def get_report_definition(
        self, project_id: str, report_id: str
    ) -> Dict[str, object]:
        return {
            **super().get_report_definition(project_id, report_id),
            "dataSource": {"id": f"src-{report_id}", "name": f"Source {report_id}"},
        }


def _run_timeout_streak(
    max_workers: int,
    timed_out_ids: Set[str],
    *,
    reports_only: bool = False,
) -> _SourceRun:
    client = _TimeoutStreakClient(
        timed_out_ids,
        reports_only=reports_only,
        reorder_completions=max_workers > 1,
    )
    config = MicroStrategyConfig.model_validate(
        {
            "base_url": _BASE_URL,
            "extract_lineage": True,
            "extract_warehouse_lineage": True,
            "extract_report_sql_lineage": True,
            "extract_reports": True,
            "extract_independent_reports": True,
            "max_consecutive_execution_timeouts": 2,
            "max_workers": max_workers,
        }
    )
    source = MicroStrategySource(config, PipelineContext(run_id=f"t-{max_workers}"))
    source.client.close()
    install_client(source, client)
    workunits = list(source.get_workunits_internal())
    return _SourceRun(
        workunits=workunits,
        source=source,
        client=client,
        sql_parse_threads=set(),
    )


@pytest.mark.parametrize("reports_only", [False, True], ids=["dashboards", "reports"])
@pytest.mark.parametrize("max_workers", [1, 4])
def test_execution_timeout_limiter_trips_in_input_order(
    max_workers: int,
    reports_only: bool,
) -> None:
    prefix = "rpt" if reports_only else "dossier"
    interleaved = _run_timeout_streak(
        max_workers, {f"{prefix}-0", f"{prefix}-2"}, reports_only=reports_only
    )
    consecutive = _run_timeout_streak(
        max_workers, {f"{prefix}-0", f"{prefix}-1"}, reports_only=reports_only
    )

    assert interleaved.source.report.execution_timeouts == 2
    assert list(interleaved.source.report.execution_timeout_limit_projects) == []
    assert interleaved.source.report.executions_skipped_after_timeouts == 0

    assert consecutive.source.report.execution_timeouts == 2
    assert list(consecutive.source.report.execution_timeout_limit_projects) == [
        "proj-1"
    ]
    assert consecutive.source.report.executions_skipped_after_timeouts >= 1
    if not reports_only:
        assert interleaved.client.report_sql_calls == ["rpt-standalone"]
        assert consecutive.client.report_sql_calls == []
    else:
        assert len(interleaved.client.report_sql_calls) == 8
        assert len(consecutive.client.report_sql_calls) < 8


def test_interleaved_parallel_timeouts_emit_same_workunits_as_serial() -> None:
    serial = _run_timeout_streak(1, {"dossier-0", "dossier-2"})
    parallel = _run_timeout_streak(4, {"dossier-0", "dossier-2"})

    assert workunit_signatures(parallel.workunits) == workunit_signatures(
        serial.workunits
    )


class _FailingLookupClient:
    def __init__(self, workers: int) -> None:
        self._lock = threading.Lock()
        self._barrier = threading.Barrier(workers)
        self.calls: List[str] = []

    def _fail(self, project_id: str, object_id: str) -> None:
        with self._lock:
            self.calls.append(f"{project_id}:{object_id}")
        raise MicroStrategyAPIError("lookup failed")

    def get_object_info(
        self, project_id: str, object_id: str, object_type: int
    ) -> Optional[MicroStrategyObject]:
        self._fail(project_id, object_id)
        return None

    def get_report_object(
        self, project_id: str, report_id: str
    ) -> Optional[MicroStrategyObject]:
        self._fail(project_id, report_id)
        return None

    def wait(self) -> None:
        self._barrier.wait(timeout=_WAIT_SECONDS)


class _LookupRun(BaseModel):
    model_config = ConfigDict(frozen=True)

    calls: List[str]
    lookup_failures: int
    warning_contexts: Dict[str, List[str]]


def _failed_lookups(projects: List[str], concurrent: bool) -> _LookupRun:
    source = _source(len(projects))
    client = _FailingLookupClient(len(projects) if concurrent else 1)
    install_client(source, client)

    def lookup(idx: int) -> None:
        client.wait()
        source._dataset_object_info(projects[idx], "ds-1")
        client.wait()
        source._linked_report_object(projects[idx], "ds-1")

    if concurrent:
        _run_threads(len(projects), lookup)
    else:
        for idx in range(len(projects)):
            lookup(idx)
    return _LookupRun(
        calls=sorted(client.calls),
        lookup_failures=source.report.dataset_object_lookup_failures,
        warning_contexts={
            str(w.title): sorted(w.context) for w in source.report.warnings
        },
    )


@pytest.mark.parametrize(
    "projects", [["p1"] * 4, ["p1", "p2", "p1", "p2"]], ids=["one", "two"]
)
def test_concurrent_failed_lookups_fetch_and_warn_once_per_project(
    projects: List[str],
) -> None:
    serial = _failed_lookups(projects, concurrent=False)
    concurrent = _failed_lookups(projects, concurrent=True)

    assert concurrent == serial
    assert len(concurrent.calls) == 2 * len(set(projects))


def test_concurrent_personal_folder_lookups_fetch_once() -> None:
    source = _source(4)
    calls: List[str] = []
    barrier = threading.Barrier(4)

    def predefined_folders(
        project_id: str, folder_types: List[int]
    ) -> List[PredefinedFolder]:
        calls.append(project_id)
        return []

    def lookup(idx: int) -> None:
        barrier.wait(timeout=_WAIT_SECONDS)
        source._personal_folders("proj-1")

    with mock.patch.object(
        source.client, "get_predefined_folders", side_effect=predefined_folders
    ):
        _run_threads(4, lookup)

    assert calls == ["proj-1"]


def test_slow_connection_lookup_blocks_only_callers_of_that_connection() -> None:
    source = _source(4)
    release_slow = threading.Event()
    slow_started = threading.Event()
    calls: List[str] = []

    def get_datasource_connection(
        connection_id: str, project_id: Optional[str] = None
    ) -> DatasourceConnection:
        calls.append(connection_id)
        if connection_id == "conn-slow":
            slow_started.set()
            assert release_slow.wait(2 * _WAIT_SECONDS)
        return DatasourceConnection.model_validate(
            {"id": connection_id, "name": connection_id}
        )

    with mock.patch.object(
        source.client,
        "get_datasource_connection",
        side_effect=get_datasource_connection,
        create=True,
    ):
        waiters = [
            threading.Thread(
                target=source._datasource_connection, args=("proj-1", "conn-slow")
            )
            for _ in range(3)
        ]
        for waiter in waiters:
            waiter.start()
        assert slow_started.wait(_WAIT_SECONDS)
        fast = threading.Thread(
            target=source._datasource_connection, args=("proj-1", "conn-fast")
        )
        fast.start()
        fast.join(timeout=_WAIT_SECONDS)
        fast_finished_first = not fast.is_alive()
        release_slow.set()
        fast.join(timeout=_WAIT_SECONDS)
        assert fast_finished_first
        for waiter in waiters:
            waiter.join(timeout=_WAIT_SECONDS)
            assert not waiter.is_alive()

    assert sorted(calls) == ["conn-fast", "conn-slow"]


def test_report_counters_and_first_failure_flag_are_exact_under_threads(
    caplog: pytest.LogCaptureFixture,
) -> None:
    report = MicroStrategyReport()
    barrier = threading.Barrier(8)
    firsts: List[bool] = []

    def worker(idx: int) -> None:
        barrier.wait(timeout=_WAIT_SECONDS)
        for _ in range(500):
            report.report_dashboard_scanned()
            report.report_datasource_skipped_without_database()
        firsts.append(report.report_metric_presence_check_failure())
        report.warning(title="Worker warning", message="m", context=f"worker={idx}")

    _run_threads(8, worker)

    assert report.dashboards_scanned == 8 * 500
    assert report.datasources_skipped_without_database == 8 * 500
    assert firsts.count(True) == 1
    assert report.metric_presence_check_failures == 8

    with caplog.at_level(logging.WARNING):
        report.warning(title="Located", message="m", context="c")
    assert caplog.records[-1].funcName == (
        "test_report_counters_and_first_failure_flag_are_exact_under_threads"
    )


class _InMemoryGraphStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._aspects: Dict[_AspectKey, _Aspect] = {}

    def build_graph(self) -> mock.MagicMock:
        graph = mock.MagicMock(spec=DataHubGraph)
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


class _IncrementalPass(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    workunits: List[MetadataWorkUnit]
    report: MicroStrategyReport
    client: ConcurrentFakeClient


_MODIFIED_SCHEDULE: List[Dict[str, str]] = [
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


def _incremental_series(
    state_file: pathlib.Path, dataset_identity: str, worker_counts: List[int]
) -> List[_IncrementalPass]:
    store = _InMemoryGraphStore()
    graph = store.build_graph()
    passes: List[_IncrementalPass] = []
    for idx, (max_workers, modified) in enumerate(
        zip(worker_counts, _MODIFIED_SCHEDULE, strict=True), start=1
    ):
        with time_machine.travel(f"2026-06-0{idx}T00:00:00Z", tick=False):
            config = MicroStrategyConfig.model_validate(
                {
                    "base_url": _BASE_URL,
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
            client = ConcurrentFakeClient(modified_overrides=modified)
            source.client.close()
            install_client(source, client)
            workunits = list(source.get_workunits())
            source.close()
            for _, committable in ctx.get_committables():
                committable.commit()
            store.write(workunits)
            passes.append(
                _IncrementalPass(
                    workunits=workunits, report=source.report, client=client
                )
            )
    return passes


@pytest.mark.parametrize(
    "dataset_identity",
    [
        MicroStrategyDatasetIdentity.PER_DASHBOARD,
        MicroStrategyDatasetIdentity.PER_REPORT,
    ],
)
def test_checkpoint_written_with_one_worker_still_skips_with_four(
    tmp_path: pathlib.Path, dataset_identity: str
) -> None:
    serial = _incremental_series(tmp_path / "serial.json", dataset_identity, [1] * 4)
    mixed = _incremental_series(tmp_path / "mixed.json", dataset_identity, [1, 4, 1, 4])

    for serial_pass, mixed_pass in zip(serial, mixed, strict=True):
        assert workunit_signatures(mixed_pass.workunits) == workunit_signatures(
            serial_pass.workunits
        )
        for counter in (
            "dashboards_skipped_unchanged",
            "reports_skipped_unchanged",
            "report_datasets_skipped_unchanged",
        ):
            assert getattr(mixed_pass.report, counter) == getattr(
                serial_pass.report, counter
            )
        assert not any(wu.id.startswith("soft-delete-") for wu in mixed_pass.workunits)

    if dataset_identity == MicroStrategyDatasetIdentity.PER_DASHBOARD:
        assert mixed[1].report.dashboards_skipped_unchanged == 2
        assert mixed[1].client.definition_reads == ["doc-beta"]
        assert mixed[3].report.dashboards_skipped_unchanged == 3
        assert mixed[3].report.reports_skipped_unchanged == 2
    else:
        assert mixed[1].report.report_datasets_skipped_unchanged == 1
        assert mixed[3].report.dashboards_skipped_unchanged == 3
        assert mixed[3].report.report_datasets_skipped_unchanged == 2
    assert mixed[3].client.definition_reads == []


class _SemanticClient:
    def __init__(self, fail: bool = False) -> None:
        self._lock = threading.Lock()
        self._fail = fail
        self.metric_threads: Set[str] = set()
        self.metric_calls: List[str] = []
        self.consolidation_calls: List[str] = []

    def search_metrics(self, project_id: str) -> Iterator[MicroStrategyObject]:
        return iter(
            [
                MicroStrategyObject.model_validate(
                    {"id": f"MET-{idx}", "name": f"Metric {idx}", "type": 4}
                )
                for idx in range(100 if self._fail else 6)
            ]
        )

    def get_metric_model(
        self, project_id: str, metric_id: str, stop: object = None
    ) -> Dict[str, object]:
        with self._lock:
            self.metric_threads.add(threading.current_thread().name)
            self.metric_calls.append(metric_id)
        if self._fail:
            raise MicroStrategyAPIError("metric model unavailable")
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
        if attribute_id != "ATTR-1":
            return {"children": []}
        return {
            "children": [
                {
                    "attribute": {"objectId": "ATTR-2", "name": "Store"},
                    "relationshipTable": {"objectId": "LGT-STORE", "name": "STORE"},
                    "relationshipType": "one_to_many",
                }
            ]
        }

    def get_consolidation_model(
        self, project_id: str, consolidation_id: str
    ) -> Dict[str, object]:
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


_SEMANTIC_MODEL_TABLES: List[Dict[str, object]] = [
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


def _emit_semantic_model(
    client: _SemanticClient, max_workers: int
) -> List[MetadataWorkUnit]:
    config = MicroStrategyConfig.model_validate(
        {
            "base_url": _BASE_URL,
            "emit_semantic_model_entities": True,
            "max_workers": max_workers,
        }
    )
    report = MicroStrategyReport()
    mapper = MicroStrategySemanticModelMapper(
        config=config,
        report=report,
        client=client,  # type: ignore[arg-type]  # duck-typed fake
        lineage=MicroStrategyLineageExtractor(config, report, SpillConnection()),
    )
    return list(
        mapper.emit(
            project_id="proj-1",
            project_name="Retail Project",
            model_tables=_SEMANTIC_MODEL_TABLES,
            table_context_resolver=lambda _: None,
            project_container_key=_ProjectKey(
                platform="microstrategy", instance=None, env="PROD", project_id="proj-1"
            ),
        )
    )


def test_semantic_model_parallel_emit_matches_serial_and_resolves_once() -> None:
    serial_client = _SemanticClient()
    parallel_client = _SemanticClient()
    serial = _emit_semantic_model(serial_client, max_workers=1)
    parallel = _emit_semantic_model(parallel_client, max_workers=4)

    assert any(
        name.startswith(MSTR_IO_THREAD_NAME_PREFIX)
        for name in parallel_client.metric_threads
    )
    assert parallel_client.consolidation_calls == ["CONSOL-SHARED"]
    assert workunit_signatures(parallel) == workunit_signatures(serial)


def test_metric_breaker_bounds_requests_past_the_limit() -> None:
    client = _SemanticClient(fail=True)
    _emit_semantic_model(client, max_workers=4)

    assert (
        MSTR_METRIC_MODEL_CONSECUTIVE_FAILURE_LIMIT
        <= len(client.metric_calls)
        <= MSTR_METRIC_MODEL_CONSECUTIVE_FAILURE_LIMIT + 4
    )
