from __future__ import annotations

import json
import logging
import re
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple, Union

from google.cloud.datacatalog_lineage import (
    BatchSearchLinkProcessesRequest,
    GetProcessRequest,
    LineageClient,
    Process,
    ProcessLinks,
)
from google.oauth2 import service_account

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import PerLocationRateLimiter
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)

# API limit; more returns INVALID_ARGUMENT.
BATCH_SEARCH_LINK_PROCESSES_MAX_LINKS = 100

MAX_CUSTOM_PROPERTY_VALUE_LEN = 1000

# After this many failed batch calls, a parent is skipped for the rest of the run.
BATCH_FAILURE_BREAKER_THRESHOLD = 3

BIGQUERY_ORIGIN = "BIGQUERY"
UNSPECIFIED_ORIGIN = "SOURCE_TYPE_UNSPECIFIED"
SQL_ATTRIBUTE = "sql"

_PROCESS_NAME_REGEX = re.compile(
    r"^projects/(?P<project>[^/]+)/locations/(?P<location>[^/]+)/processes/(?P<process_id>[^/]+)$"
)

# e.g. /* {"app": "dbt", "node_id": "..."} */ SELECT ...
_DBT_HEADER_REGEX = re.compile(r"^\s*/\*\s*(\{.*?\})\s*\*/", re.DOTALL)


def ts_millis(timestamp: Any) -> Optional[int]:
    """proto Timestamp / datetime -> epoch millis, None-safe."""
    if timestamp is None:
        return None
    try:
        return int(timestamp.timestamp() * 1000)
    except (AttributeError, ValueError, OSError):
        return None


@dataclass(frozen=True)
class ProcessInfo:
    """The subset of a Data Lineage API Process this connector uses."""

    name: str
    origin_source_type: str
    attributes: Tuple[Tuple[str, str], ...]
    # origin.name: the producing resource (e.g. the Dataproc cluster path).
    origin_name: str = ""

    @property
    def project(self) -> Optional[str]:
        match = _PROCESS_NAME_REGEX.match(self.name)
        return match.group("project") if match else None

    @property
    def location(self) -> Optional[str]:
        match = _PROCESS_NAME_REGEX.match(self.name)
        return match.group("location") if match else None

    def attribute(self, key: str) -> Optional[str]:
        for attribute_key, attribute_value in self.attributes:
            if attribute_key == key:
                return attribute_value
        return None

    @property
    def bigquery_job_id(self) -> Optional[str]:
        # The API docs do not pin the key; live processes use bigquery_job_id.
        return self.attribute("bigquery_job_id") or self.attribute("job_id")


@dataclass(frozen=True)
class LinkProcessChoice:
    """The single process chosen for a link (latest end time wins)."""

    process_name: str
    end_time_ms: Optional[int] = None


@dataclass
class ProcessQuery:
    """SQL text and properties for a process that has real SQL."""

    sql: str
    custom_properties: Dict[str, str] = field(default_factory=dict)


class OperationsReporter(Protocol):
    def report_lineage_api_call(
        self, api_name: str, elapsed_seconds: float
    ) -> None: ...

    def report_link_process_batch_call(self) -> None: ...

    def report_link_process_batch_failure(self, parent: str) -> None: ...

    def report_multi_process_links_collapsed(self, count: int) -> None: ...

    def report_process_fetched(self) -> None: ...

    def report_process_cache_hit(self) -> None: ...

    def report_process_cache_negative_hit(self) -> None: ...

    def report_process_lookup_failed(self, process_name: str) -> None: ...

    def report_operation_sql_fetched(self) -> None: ...

    def report_operation_sql_fetch_failed(self, context: str) -> bool: ...


class DataplexOperationResolver:
    """Resolves link names to processes, sharing the extractor's limiter and retries."""

    def __init__(
        self,
        report: OperationsReporter,
        lineage_client: LineageClient,
        rate_limiter: PerLocationRateLimiter,
        retry_decorator_factory: Callable[[], Callable],
    ) -> None:
        self.report = report
        self.lineage_client = lineage_client
        self._rate_limiter = rate_limiter
        self._retry_decorator_factory = retry_decorator_factory
        self._process_cache: Dict[str, Optional[ProcessInfo]] = {}
        self._cache_lock = threading.Lock()
        # Single-flight: racing workers wait on the event, then read the cache.
        self._inflight: Dict[str, threading.Event] = {}
        self._parent_failures: Dict[str, int] = {}
        self._parent_lock = threading.Lock()

    def _batch_search_impl(
        self, parent: str, link_names: List[str]
    ) -> List[ProcessLinks]:
        # Inside the retry loop: every attempt spends quota.
        self._rate_limiter.acquire(parent)
        request = BatchSearchLinkProcessesRequest(
            parent=parent,
            links=link_names,
            page_size=BATCH_SEARCH_LINK_PROCESSES_MAX_LINKS,
        )
        return list(self.lineage_client.batch_search_link_processes(request=request))

    def _parent_breaker_open(self, parent: str) -> bool:
        with self._parent_lock:
            return (
                self._parent_failures.get(parent, 0) >= BATCH_FAILURE_BREAKER_THRESHOLD
            )

    def _record_parent_failure(self, parent: str, exc: Exception) -> None:
        with self._parent_lock:
            failures = self._parent_failures.get(parent, 0) + 1
            self._parent_failures[parent] = failures
        self.report.report_link_process_batch_failure(parent)
        logger.debug(
            "batch_search_link_processes failed for parent %s (%d time(s)): %s",
            parent,
            failures,
            exc,
        )

    def resolve_links_to_processes(
        self, parent: str, link_names: List[str]
    ) -> Dict[str, LinkProcessChoice]:
        """One producing process per link; a failing chunk only loses its own links."""
        if self._parent_breaker_open(parent):
            return {}
        choices: Dict[str, LinkProcessChoice] = {}
        multi_process_links = 0
        retrying = self._retry_decorator_factory()(self._batch_search_impl)
        for start in range(0, len(link_names), BATCH_SEARCH_LINK_PROCESSES_MAX_LINKS):
            chunk = link_names[start : start + BATCH_SEARCH_LINK_PROCESSES_MAX_LINKS]
            if self._parent_breaker_open(parent):
                break
            self.report.report_link_process_batch_call()
            try:
                with PerfTimer() as timer:
                    process_links_pages = retrying(parent, chunk)
                self.report.report_lineage_api_call(
                    "batch_search_link_processes", timer.elapsed_seconds()
                )
            except Exception as exc:
                self._record_parent_failure(parent, exc)
                continue

            for process_links in process_links_pages:
                for link_info in process_links.links:
                    end_ms = ts_millis(link_info.end_time)
                    existing = choices.get(link_info.link)
                    if existing is not None:
                        multi_process_links += 1
                        if (existing.end_time_ms or 0) >= (end_ms or 0):
                            continue
                    choices[link_info.link] = LinkProcessChoice(
                        process_name=process_links.process, end_time_ms=end_ms
                    )
        if multi_process_links:
            self.report.report_multi_process_links_collapsed(multi_process_links)
        return choices

    def _get_process_impl(self, process_name: str) -> Process:
        # Inside the retry loop: every attempt spends quota.
        self._rate_limiter.acquire(process_name)
        return self.lineage_client.get_process(
            request=GetProcessRequest(name=process_name)
        )

    def get_process_info(self, process_name: str) -> Optional[ProcessInfo]:
        """Fetch a process once per run; failures are cached as None."""
        while True:
            with self._cache_lock:
                if process_name in self._process_cache:
                    cached = self._process_cache[process_name]
                    if cached is None:
                        self.report.report_process_cache_negative_hit()
                    else:
                        self.report.report_process_cache_hit()
                    return cached
                fetch_event = self._inflight.get(process_name)
                if fetch_event is None:
                    fetch_event = threading.Event()
                    self._inflight[process_name] = fetch_event
                    break
            fetch_event.wait()

        info: Optional[ProcessInfo] = None
        try:
            retrying = self._retry_decorator_factory()(self._get_process_impl)
            try:
                with PerfTimer() as timer:
                    process = retrying(process_name)
                self.report.report_lineage_api_call(
                    "get_process", timer.elapsed_seconds()
                )
                # Sorted: map order is unstable and ends up in query properties.
                attributes = tuple(
                    sorted(
                        (str(key), _stringify_attribute(value))
                        for key, value in dict(process.attributes).items()
                    )
                )
                info = ProcessInfo(
                    name=process.name or process_name,
                    origin_source_type=(
                        process.origin.source_type.name
                        if process.origin and process.origin.source_type
                        else UNSPECIFIED_ORIGIN
                    ),
                    attributes=attributes,
                    origin_name=(process.origin.name if process.origin else "") or "",
                )
                self.report.report_process_fetched()
            except Exception as exc:
                self.report.report_process_lookup_failed(process_name)
                logger.debug("get_process failed for %s: %s", process_name, exc)
        finally:
            with self._cache_lock:
                self._process_cache[process_name] = info
                self._inflight.pop(process_name, None)
            fetch_event.set()
        return info


def _json_composite(value: Any) -> Union[Dict[str, Any], List[Any]]:
    """json.dumps default for proto-plus struct and list Values."""
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


def _stringify_attribute(value: Any) -> str:
    """Coerce a map<string, Value> attribute to a string."""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, default=_json_composite, sort_keys=True)
    except (TypeError, ValueError):
        return str(value)


def _parse_dbt_header(sql: Optional[str]) -> Dict[str, str]:
    """Hoist a dbt JSON header comment into dbt_-prefixed properties."""
    if not sql:
        return {}
    match = _DBT_HEADER_REGEX.match(sql)
    if not match:
        return {}
    try:
        header = json.loads(match.group(1))
    except (ValueError, TypeError):
        return {}
    if not isinstance(header, dict):
        return {}
    return {
        f"dbt_{key}": str(value)[:MAX_CUSTOM_PROPERTY_VALUE_LEN]
        for key, value in header.items()
    }


@dataclass
class JobDetails:
    """The subset of a BigQuery job copied onto a query."""

    query: Optional[str] = None
    custom_properties: Dict[str, str] = field(default_factory=dict)


class BigQueryJobSqlFetcher:
    """Best-effort SQL fetch for BigQuery-origin processes; failures are cached."""

    def __init__(
        self,
        report: OperationsReporter,
        source_report: SourceReport,
        credentials: Optional[service_account.Credentials] = None,
    ) -> None:
        self.report = report
        self.source_report = source_report
        self._credentials = credentials
        self._cache: Dict[Tuple[str, str, str], Optional[JobDetails]] = {}
        self._client: Optional[Any] = None
        self._disabled = False
        self._lock = threading.Lock()

    def _get_client(self, project: str) -> Optional[Any]:
        """Must be called with ``self._lock`` held."""
        if self._disabled:
            return None
        if self._client is None:
            try:
                # Lazy: this optional feature must not break the import.
                from google.cloud import bigquery
            except ImportError as exc:
                self._disable(
                    "The google-cloud-bigquery package is not installed, so "
                    "operation queries have no BigQuery SQL this run.",
                    exc,
                )
                return None
            try:
                # Client() fails without a default project; get_job passes its own.
                self._client = bigquery.Client(
                    project=project, credentials=self._credentials
                )
            except Exception as exc:
                self._disable(
                    "The BigQuery client could not be created, so operation "
                    "queries have no BigQuery SQL this run.",
                    exc,
                )
                return None
        return self._client

    def _disable(self, message: str, exc: Exception) -> None:
        self.source_report.warning(
            message,
            title="Dataplex BigQuery client unavailable",
            exc=exc,
        )
        self._disabled = True

    def close(self) -> None:
        with self._lock:
            if self._client is not None:
                self._client.close()
                self._client = None

    def fetch(self, process: ProcessInfo) -> Optional[JobDetails]:
        job_id = process.bigquery_job_id
        project = process.project
        location = process.location
        if not job_id or not project or not location:
            return None
        cache_key = (project, location, job_id)
        with self._lock:
            if cache_key in self._cache:
                return self._cache[cache_key]
            client = self._get_client(project)

        details: Optional[JobDetails] = None
        if client is not None:
            try:
                job = client.get_job(job_id, project=project, location=location)
                details = self._map_job(job)
                self.report.report_operation_sql_fetched()
            except Exception as exc:
                context = f"project={project}, location={location}, job_id={job_id}"
                if self.report.report_operation_sql_fetch_failed(context):
                    self.source_report.warning(
                        "A BigQuery job's SQL could not be fetched, so its lineage "
                        "edges have no operation node. Fetching job SQL needs "
                        "bigquery.jobs.get, for example from "
                        "roles/bigquery.resourceViewer. Later failures are counted "
                        "under num_operation_sql_fetch_failed.",
                        title="Dataplex BigQuery job SQL fetch failed",
                        context=context,
                        exc=exc,
                    )
        with self._lock:
            self._cache[cache_key] = details
        return details

    @staticmethod
    def _map_job(job: Any) -> JobDetails:
        details = JobDetails(query=getattr(job, "query", None))
        properties = details.custom_properties
        if getattr(job, "job_type", None):
            properties["bigquery_job_type"] = str(job.job_type)
        total_bytes = getattr(job, "total_bytes_processed", None)
        if total_bytes is not None:
            properties["total_bytes_processed"] = str(total_bytes)
        labels = getattr(job, "labels", None) or {}
        for key, value in labels.items():
            properties[f"job_label_{key}"] = str(value)[:MAX_CUSTOM_PROPERTY_VALUE_LEN]
        return details


def build_process_query(
    process: ProcessInfo,
    config: DataplexConfig,
    sql_fetcher: Optional[BigQueryJobSqlFetcher],
) -> Optional[ProcessQuery]:
    """SQL and properties for a process, or None when it has no real SQL."""
    sql = process.attribute(SQL_ATTRIBUTE)
    custom_properties: Dict[str, str] = {
        "gcp_process_name": process.name,
        "gcp_origin_source_type": process.origin_source_type,
    }
    if process.origin_name:
        custom_properties["gcp_origin_name"] = process.origin_name
    for key in config.lineage_operation_attribute_allowlist:
        value = process.attribute(key)
        if value is not None and key != SQL_ATTRIBUTE:
            custom_properties[key] = value[:MAX_CUSTOM_PROPERTY_VALUE_LEN]

    if process.origin_source_type == BIGQUERY_ORIGIN and process.bigquery_job_id:
        custom_properties.setdefault("bigquery_job_id", process.bigquery_job_id)
        if sql_fetcher is not None:
            job = sql_fetcher.fetch(process)
            if job is not None:
                custom_properties.update(job.custom_properties)
                sql = sql or job.query

    if not sql:
        return None
    custom_properties.update(_parse_dbt_header(sql))
    return ProcessQuery(sql=sql, custom_properties=custom_properties)
