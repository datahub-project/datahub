from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
)

from google.cloud.datacatalog_lineage import (
    BatchSearchLinkProcessesRequest,
    GetProcessRequest,
    LineageClient,
    ListRunsRequest,
)
from google.oauth2 import service_account

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataPlatformInstanceClass,
    QueryLanguageClass,
    QueryPropertiesClass,
    QuerySourceClass,
    QueryStatementClass,
    QuerySubjectClass,
    QuerySubjectsClass,
)
from datahub.metadata.urns import QueryUrn
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.ratelimiter import TokenBucket

logger = logging.getLogger(__name__)

# API limit; more returns INVALID_ARGUMENT.
BATCH_SEARCH_LINK_PROCESSES_MAX_LINKS = 100

MAX_CUSTOM_PROPERTY_VALUE_LEN = 1000

# After this many failed batch calls, a parent is skipped for the rest of the run.
BATCH_FAILURE_BREAKER_THRESHOLD = 3

# One process can fan out over thousands of tables; the backend rejects huge aspects.
MAX_QUERY_SUBJECTS = 1000

MAX_RUN_ATTRIBUTES = 100

# Process origin -> query node platform; others use the downstream's platform.
ORIGIN_PLATFORM: Dict[str, str] = {
    "BIGQUERY": "bigquery",
    "VERTEX_AI": "vertexai",
    "COMPOSER": "airflow",
    "DATAPROC": "dataproc-metastore",
}

BIGQUERY_ORIGIN = "BIGQUERY"
UNSPECIFIED_ORIGIN = "SOURCE_TYPE_UNSPECIFIED"
DEFAULT_ACTOR = "urn:li:corpuser:datahub"
SQL_ATTRIBUTE = "sql"

_PROCESS_NAME_REGEX = re.compile(
    r"^projects/(?P<project>[^/]+)/locations/(?P<location>[^/]+)/processes/(?P<process_id>[^/]+)$"
)

# e.g. /* {"app": "dbt", "dag_id": "..."} */ SELECT ...
_DBT_HEADER_REGEX = re.compile(r"^\s*/\*\s*(\{.*?\})\s*\*/", re.DOTALL)


def query_urn_for_process(process_name: str) -> str:
    """Stable Query URN for a GCP lineage process (identity = Process.name)."""
    digest = hashlib.sha256(process_name.encode("utf-8")).hexdigest()
    return QueryUrn(f"dataplex_{digest}").urn()


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
    display_name: str
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

    @property
    def process_id(self) -> str:
        match = _PROCESS_NAME_REGEX.match(self.name)
        return match.group("process_id") if match else self.name

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
    start_time_ms: Optional[int] = None
    end_time_ms: Optional[int] = None


@dataclass(frozen=True)
class RunInfo:
    """The subset of a Data Lineage API Run hoisted onto the Query entity."""

    name: str
    display_name: str
    state: str
    start_ms: Optional[int]
    end_ms: Optional[int]
    attributes: Tuple[Tuple[str, str], ...]


class OperationsReporter(Protocol):
    def report_lineage_api_call(
        self, api_name: str, elapsed_seconds: float
    ) -> None: ...

    def report_link_process_batch_call(self) -> None: ...

    def report_link_process_batch_failure(self, parent: str) -> None: ...

    def report_multi_process_links_collapsed(self, count: int) -> None: ...

    def report_process_fetched(self, process_name: str) -> None: ...

    def report_process_cache_hit(self) -> None: ...

    def report_process_cache_negative_hit(self) -> None: ...

    def report_process_lookup_failed(self, process_name: str) -> None: ...

    def report_run_fetched(self, process_name: str) -> None: ...

    def report_run_lookup_failed(self, process_name: str) -> None: ...

    def report_run_attributes_truncated(
        self, process_name: str, dropped: int
    ) -> None: ...

    def report_query_subjects_truncated(self, dropped: int) -> None: ...

    def report_query_entity_emitted(self, query_urn: str) -> None: ...

    def report_process_unknown_origin(self, process_name: str) -> None: ...

    def report_operation_sql_fetched(self) -> None: ...

    def report_operation_sql_fetch_failed(self, context: str) -> None: ...


class DataplexOperationResolver:
    """Resolves link names to processes, sharing the extractor's limiter and retries."""

    def __init__(
        self,
        config: DataplexConfig,
        report: OperationsReporter,
        lineage_client: LineageClient,
        rate_limiter: TokenBucket,
        retry_decorator_factory: Callable[[], Callable],
    ) -> None:
        self.config = config
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

    # -- batch_search_link_processes --------------------------------------

    def _batch_search_impl(self, parent: str, link_names: List[str]) -> list:
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
        if failures == 1:
            logger.warning(
                "batch_search_link_processes failed for parent %s; affected "
                "lineage edges will have no operation node: %s",
                parent,
                exc,
            )
        if failures == BATCH_FAILURE_BREAKER_THRESHOLD:
            logger.warning(
                "batch_search_link_processes failed %d times for parent %s; "
                "disabling operation resolution for this parent for the rest "
                "of the run.",
                failures,
                parent,
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
            # Once per chunk, outside the retry loop.
            self._rate_limiter.acquire()
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
                    start_ms = ts_millis(link_info.start_time)
                    existing = choices.get(link_info.link)
                    if existing is not None:
                        multi_process_links += 1
                        if (existing.end_time_ms or 0) >= (end_ms or 0):
                            continue
                    choices[link_info.link] = LinkProcessChoice(
                        process_name=process_links.process,
                        start_time_ms=start_ms,
                        end_time_ms=end_ms,
                    )
        if multi_process_links:
            self.report.report_multi_process_links_collapsed(multi_process_links)
        return choices

    # -- get_process -------------------------------------------------------

    def _get_process_impl(self, process_name: str) -> Any:
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
            self._rate_limiter.acquire()
            retrying = self._retry_decorator_factory()(self._get_process_impl)
            try:
                with PerfTimer() as timer:
                    process = retrying(process_name)
                self.report.report_lineage_api_call(
                    "get_process", timer.elapsed_seconds()
                )
                # Sorted: map order is unstable and ends up in the query statement.
                attributes = tuple(
                    sorted(
                        (str(key), _stringify_attribute(value))
                        for key, value in dict(process.attributes).items()
                    )
                )
                info = ProcessInfo(
                    name=process.name or process_name,
                    display_name=process.display_name or "",
                    origin_source_type=(
                        process.origin.source_type.name
                        if process.origin and process.origin.source_type
                        else UNSPECIFIED_ORIGIN
                    ),
                    attributes=attributes,
                    origin_name=(
                        getattr(process.origin, "name", "") if process.origin else ""
                    )
                    or "",
                )
                self.report.report_process_fetched(process_name)
            except Exception as exc:
                self.report.report_process_lookup_failed(process_name)
                logger.debug("get_process failed for %s: %s", process_name, exc)
        finally:
            with self._cache_lock:
                self._process_cache[process_name] = info
                self._inflight.pop(process_name, None)
            fetch_event.set()
        return info

    # -- list_runs ---------------------------------------------------------

    def _list_latest_run_impl(self, process_name: str) -> List:
        """First row of the first page: ListRuns is ordered by start_time desc.

        Reading further pages would issue RPCs outside the retry and the limiter.
        """
        pager = self.lineage_client.list_runs(
            request=ListRunsRequest(parent=process_name, page_size=1)
        )
        first_page = next(iter(pager.pages), None)
        return list(first_page.runs) if first_page is not None else []

    def get_latest_run(self, process_name: str) -> Optional[RunInfo]:
        """Latest run of a process, or None. Called once per process, after the pool."""
        self._rate_limiter.acquire()
        retrying = self._retry_decorator_factory()(self._list_latest_run_impl)
        try:
            with PerfTimer() as timer:
                runs = retrying(process_name)
            self.report.report_lineage_api_call("list_runs", timer.elapsed_seconds())
        except Exception as exc:
            self.report.report_run_lookup_failed(process_name)
            logger.debug("list_runs failed for %s: %s", process_name, exc)
            return None
        if not runs:
            return None
        latest = runs[0]
        state = getattr(latest, "state", None)
        attributes = sorted(
            _flatten_attributes(getattr(latest, "attributes", None) or {})
        )
        if len(attributes) > MAX_RUN_ATTRIBUTES:
            self.report.report_run_attributes_truncated(
                process_name, len(attributes) - MAX_RUN_ATTRIBUTES
            )
        self.report.report_run_fetched(process_name)
        return RunInfo(
            name=getattr(latest, "name", "") or "",
            display_name=getattr(latest, "display_name", "") or "",
            state=getattr(state, "name", None) or (str(state) if state else ""),
            start_ms=ts_millis(getattr(latest, "start_time", None)),
            end_ms=ts_millis(getattr(latest, "end_time", None)),
            attributes=tuple(attributes[:MAX_RUN_ATTRIBUTES]),
        )


def _stringify_attribute(value: Any) -> str:
    """Coerce a map<string, Value> attribute to a string."""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value)
    except (TypeError, ValueError):
        return str(value)


def _to_plain(value: Any) -> Any:
    """proto-plus collection -> plain dict / list / scalar.

    Without this, ``str()`` on a RepeatedComposite embeds object ids.
    """
    if isinstance(value, (str, bytes)) or value is None:
        return value
    if hasattr(value, "items"):
        return {str(key): _to_plain(item) for key, item in value.items()}
    if hasattr(value, "__iter__"):
        return [_to_plain(item) for item in value]
    return value


def _flatten_attributes(attributes: Any, prefix: str = "") -> List[Tuple[str, str]]:
    """map<string, Value> -> dotted-key string leaves; an empty struct stays ``{}``."""
    flattened: List[Tuple[str, str]] = []
    for key, value in dict(attributes).items():
        dotted = f"{prefix}{key}"
        plain = _to_plain(value)
        if isinstance(plain, dict) and plain:
            flattened.extend(_flatten_attributes(plain, prefix=f"{dotted}."))
        else:
            flattened.append((str(dotted), _stringify_attribute(plain)))
    return flattened


@dataclass
class _QueryEntityState:
    process_name: str
    downstream_platform: str
    subjects: Set[str] = field(default_factory=set)
    first_start_ms: Optional[int] = None
    last_end_ms: Optional[int] = None


class QueryEntityAccumulator:
    """Collects per-query subjects across workers; emitted once, after the pool drains."""

    def __init__(self, config: DataplexConfig, report: OperationsReporter) -> None:
        self.config = config
        self.report = report
        self._by_query_urn: Dict[str, _QueryEntityState] = {}
        self._lock = threading.Lock()

    def add(
        self,
        query_urn: str,
        process_name: str,
        downstream_platform: str,
        subject_urns: Iterable[str],
        start_time_ms: Optional[int],
        end_time_ms: Optional[int],
    ) -> None:
        with self._lock:
            state = self._by_query_urn.get(query_urn)
            if state is None:
                state = _QueryEntityState(
                    process_name=process_name,
                    downstream_platform=downstream_platform,
                )
                self._by_query_urn[query_urn] = state
            elif downstream_platform < state.downstream_platform:
                # Deterministic regardless of thread arrival order.
                state.downstream_platform = downstream_platform
            # Capped at emission, after sorting, so thread order cannot change it.
            state.subjects.update(subject_urns)
            if start_time_ms is not None and (
                state.first_start_ms is None or start_time_ms < state.first_start_ms
            ):
                state.first_start_ms = start_time_ms
            if end_time_ms is not None and (
                state.last_end_ms is None or end_time_ms > state.last_end_ms
            ):
                state.last_end_ms = end_time_ms

    def gen_workunits(
        self,
        resolver: DataplexOperationResolver,
        sql_fetcher: Optional[BigQueryJobSqlFetcher],
    ) -> Iterable[MetadataWorkUnit]:
        for query_urn, state in self._by_query_urn.items():
            process = resolver.get_process_info(state.process_name)
            if process is None:
                continue
            dropped = len(state.subjects) - MAX_QUERY_SUBJECTS
            if dropped > 0:
                self.report.report_query_subjects_truncated(dropped)
            run = resolver.get_latest_run(state.process_name)
            yield from self._gen_query_entity(
                query_urn, state, process, run, sql_fetcher
            )
            self.report.report_query_entity_emitted(query_urn)

    def _gen_query_entity(
        self,
        query_urn: str,
        state: _QueryEntityState,
        process: ProcessInfo,
        run: Optional[RunInfo],
        sql_fetcher: Optional[BigQueryJobSqlFetcher],
    ) -> Iterable[MetadataWorkUnit]:
        origin = process.origin_source_type
        custom_properties: Dict[str, str] = {
            "gcp_process_name": process.name,
            "gcp_origin_source_type": origin,
        }
        if process.origin_name:
            custom_properties["gcp_origin_name"] = process.origin_name
        for key in self.config.lineage_operation_attribute_allowlist:
            value = process.attribute(key)
            if value is not None:
                custom_properties[key] = value[:MAX_CUSTOM_PROPERTY_VALUE_LEN]
        if state.first_start_ms is not None:
            custom_properties["link_start_time_ms"] = str(state.first_start_ms)
        if state.last_end_ms is not None:
            custom_properties["link_end_time_ms"] = str(state.last_end_ms)
        if run is not None:
            if run.name:
                custom_properties["gcp_run_name"] = run.name
            if run.display_name:
                custom_properties["run_display_name"] = run.display_name
            if run.state:
                custom_properties["run_state"] = run.state
            if run.start_ms is not None:
                custom_properties["run_start_time_ms"] = str(run.start_ms)
            if run.end_ms is not None:
                custom_properties["run_end_time_ms"] = str(run.end_ms)
            for key, value in run.attributes:
                custom_properties.setdefault(key, value[:MAX_CUSTOM_PROPERTY_VALUE_LEN])

        statement_text, language = self._statement_for(process, run)
        actor = DEFAULT_ACTOR

        job = None
        if (
            sql_fetcher is not None
            and origin == BIGQUERY_ORIGIN
            and process.bigquery_job_id
        ):
            job = sql_fetcher.fetch(process)
        if job is not None:
            if job.query:
                statement_text = job.query
                language = QueryLanguageClass.SQL
            if job.user_email:
                # Full email, matching the BigQuery connector's corpuser URNs.
                actor = builder.make_user_urn(job.user_email)
                custom_properties["job_user_email"] = job.user_email
            custom_properties.update(job.custom_properties)
            custom_properties.update(_parse_dbt_header(job.query))

        if origin == BIGQUERY_ORIGIN and process.bigquery_job_id:
            custom_properties.setdefault("bigquery_job_id", process.bigquery_job_id)
            if process.project and process.location:
                custom_properties["console_job_url"] = (
                    f"https://console.cloud.google.com/bigquery?project={process.project}"
                    f"&j=bq:{process.location}:{process.bigquery_job_id}"
                    "&page=queryresults"
                )

        audit_created = AuditStampClass(time=state.first_start_ms or 0, actor=actor)
        audit_modified = AuditStampClass(
            time=state.last_end_ms or state.first_start_ms or 0, actor=actor
        )
        aspects = [
            QueryPropertiesClass(
                statement=QueryStatementClass(value=statement_text, language=language),
                source=QuerySourceClass.SYSTEM,
                name=process.display_name or process.process_id,
                created=audit_created,
                lastModified=audit_modified,
                customProperties=custom_properties,
            ),
            QuerySubjectsClass(
                subjects=[
                    QuerySubjectClass(entity=urn)
                    for urn in sorted(state.subjects)[:MAX_QUERY_SUBJECTS]
                ]
            ),
            DataPlatformInstanceClass(
                platform=builder.make_data_platform_urn(
                    ORIGIN_PLATFORM.get(origin, state.downstream_platform)
                )
            ),
        ]
        for change_proposal in MetadataChangeProposalWrapper.construct_many(
            entityUrn=query_urn, aspects=aspects
        ):
            yield change_proposal.as_workunit()

    def _statement_for(
        self, process: ProcessInfo, run: Optional[RunInfo]
    ) -> Tuple[str, str]:
        """(statement text, language) before any BigQuery SQL enrichment."""
        origin = process.origin_source_type
        sql_attribute = process.attribute(SQL_ATTRIBUTE)
        if sql_attribute:
            return sql_attribute, QueryLanguageClass.SQL
        if origin == BIGQUERY_ORIGIN:
            job_id = process.bigquery_job_id or "unknown"
            return (
                f"-- BigQuery job: {job_id}\n"
                "-- SQL statement not stored in the Data Lineage API.\n"
                "-- Enable include_lineage_operation_sql to fetch it from the "
                "BigQuery Jobs API.",
                QueryLanguageClass.SQL,
            )
        if origin == UNSPECIFIED_ORIGIN:
            self.report.report_process_unknown_origin(process.name)
        # Always SQL: the GraphQL enum knows no other value, and anything else
        # breaks searchAcrossLineage for every path through the node.
        lines = [f"-- {origin} process: {process.display_name or process.process_id}"]
        if process.origin_name:
            lines.append(f"-- origin = {process.origin_name}")
        lines.extend(f"-- {key} = {value}" for key, value in process.attributes)
        if run is not None:
            lines.append(
                f"-- latest run: {run.display_name or run.name}"
                + (f" ({run.state})" if run.state else "")
            )
            for milliseconds, label in ((run.start_ms, "start"), (run.end_ms, "end")):
                if milliseconds is not None:
                    stamp = datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)
                    lines.append(f"-- run_{label} = {stamp.isoformat()}")
            lines.extend(f"-- {key} = {value}" for key, value in run.attributes)
        return "\n".join(lines), QueryLanguageClass.SQL


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
    """The subset of a BigQuery job hoisted onto a Query entity."""

    query: Optional[str] = None
    user_email: Optional[str] = None
    custom_properties: Dict[str, str] = field(default_factory=dict)


class BigQueryJobSqlFetcher:
    """Best-effort SQL fetch for BigQuery-origin processes; failures are cached."""

    def __init__(
        self,
        report: OperationsReporter,
        credentials: Optional[service_account.Credentials] = None,
    ) -> None:
        self.report = report
        self._credentials = credentials
        self._cache: Dict[Tuple[str, str, str], Optional[JobDetails]] = {}
        self._client: Optional[Any] = None
        self._disabled = False

    def _get_client(self) -> Optional[Any]:
        if self._disabled:
            return None
        if self._client is None:
            try:
                # Lazy: this optional feature must not break the import.
                from google.cloud import bigquery

                self._client = bigquery.Client(credentials=self._credentials)
            except Exception as exc:
                logger.warning(
                    "BigQuery client unavailable; SQL enrichment disabled for "
                    "this run: %s",
                    exc,
                )
                self._disabled = True
                return None
        return self._client

    def fetch(self, process: ProcessInfo) -> Optional[JobDetails]:
        job_id = process.bigquery_job_id
        project = process.project
        location = process.location
        if not job_id or not project or not location:
            return None
        cache_key = (project, location, job_id)
        if cache_key in self._cache:
            return self._cache[cache_key]

        client = self._get_client()
        details: Optional[JobDetails] = None
        if client is not None:
            try:
                job = client.get_job(job_id, project=project, location=location)
                details = self._map_job(job)
                self.report.report_operation_sql_fetched()
            except Exception as exc:
                self.report.report_operation_sql_fetch_failed(
                    f"project={project}, location={location}, job_id={job_id}"
                )
                logger.debug("BigQuery jobs.get failed for %s: %s", cache_key, exc)
        self._cache[cache_key] = details
        return details

    @staticmethod
    def _map_job(job: Any) -> JobDetails:
        details = JobDetails(
            query=getattr(job, "query", None),
            user_email=getattr(job, "user_email", None),
        )
        properties = details.custom_properties
        if getattr(job, "job_type", None):
            properties["bigquery_job_type"] = str(job.job_type)
        for stamp_attribute in ("created", "started", "ended"):
            stamp = getattr(job, stamp_attribute, None)
            if stamp is not None:
                properties[f"job_{stamp_attribute}"] = stamp.isoformat()
        total_bytes = getattr(job, "total_bytes_processed", None)
        if total_bytes is not None:
            properties["total_bytes_processed"] = str(total_bytes)
        labels = getattr(job, "labels", None) or {}
        for key, value in labels.items():
            properties[f"job_label_{key}"] = str(value)[:MAX_CUSTOM_PROPERTY_VALUE_LEN]
        return details
