import concurrent.futures
import json
import logging
import warnings
from collections import defaultdict
from datetime import datetime
from enum import Enum
from itertools import product
from time import sleep, time
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from urllib3.exceptions import InsecureRequestWarning

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.request_helper import make_curl_command
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_datahub_source_mapping import (
    DremioToDataHubSourceTypeMapping,
)
from datahub.ingestion.source.dremio.dremio_models import (
    DremioContainerResponse,
    DremioEntityContainerType,
    DremioJobState,
)
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport
from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)

DREMIO_SYSTEM_TABLES_PATTERN = [
    r"^information_schema$",
    r"^sys$",
    r"^information_schema\..*",
    r"^sys\..*",
]


class DremioAPIException(Exception):
    pass


class DremioEdition(Enum):
    CLOUD = "CLOUD"
    ENTERPRISE = "ENTERPRISE"
    COMMUNITY = "COMMUNITY"


class DremioFilter:
    """
    Dremio-specific filtering logic that follows the standard pattern used by other SQL platforms.
    """

    def __init__(
        self, config: "DremioSourceConfig", structured_reporter: "DremioSourceReport"
    ) -> None:
        self.config = config
        self.structured_reporter = structured_reporter

    def is_schema_allowed(
        self, schema_path: List[str], container_name: str = ""
    ) -> bool:
        """
        Check if a schema (container path) is allowed based on schema_pattern and system schema filtering.

        Args:
            schema_path: List of path components (e.g., ['source', 'folder', 'subfolder'])
            container_name: Name of the final container (optional, can be included in schema_path)

        Returns:
            True if the schema should be included, False otherwise
        """
        if container_name:
            full_path_components = schema_path + [container_name]
        else:
            full_path_components = schema_path

        if not full_path_components:
            return True

        full_schema_name = ".".join(full_path_components).lower()

        if not self.config.include_system_tables and not AllowDenyPattern(
            deny=DREMIO_SYSTEM_TABLES_PATTERN
        ).allowed(full_schema_name):
            return False

        if len(full_path_components) == 1:
            container_name = full_path_components[0]

            if self.config.schema_pattern.allowed(container_name):
                return True

            # Allow root containers that are prefixes of hierarchical patterns
            for pattern in self.config.schema_pattern.allow:
                if "." in pattern and pattern.lower().startswith(
                    container_name.lower() + "."
                ):
                    deny_only = AllowDenyPattern(
                        allow=[".*"],
                        deny=self.config.schema_pattern.deny,
                    )
                    if deny_only.allowed(container_name):
                        return True
            return False
        else:
            current_path = full_schema_name

            if self.config.schema_pattern.allowed(current_path):
                return True

            # Allow intermediate paths for open-ended patterns (ending with .*)
            for pattern in self.config.schema_pattern.allow:
                if pattern.endswith(".*"):
                    pattern_prefix = pattern[:-2]
                    if pattern_prefix.lower().startswith(current_path.lower() + "."):
                        deny_only = AllowDenyPattern(
                            allow=[".*"],
                            deny=self.config.schema_pattern.deny,
                        )
                        if deny_only.allowed(current_path):
                            return True
            return False

    def is_dataset_allowed(
        self, dataset_name: str, schema_path: List[str], dataset_type: str = "table"
    ) -> bool:
        """
        Check if a dataset (table/view) is allowed based on dataset_pattern and system table filtering.

        Args:
            dataset_name: Name of the dataset
            schema_path: List of schema path components
            dataset_type: Type of dataset ('table', 'view', etc.)

        Returns:
            True if the dataset should be included, False otherwise
        """
        if schema_path:
            full_dataset_name = f"{'.'.join(schema_path)}.{dataset_name}".lower()
        else:
            full_dataset_name = dataset_name.lower()

        if not self.config.include_system_tables and not AllowDenyPattern(
            deny=DREMIO_SYSTEM_TABLES_PATTERN
        ).allowed(full_dataset_name):
            return False

        return self.config.dataset_pattern.allowed(full_dataset_name)

    def should_include_container(self, path: List[str], name: str) -> bool:
        """
        Backward compatibility method that uses the new filtering logic.
        """
        return self.is_schema_allowed(path, name)


class DremioAPIOperations:
    _retry_count: int = 5
    _timeout: int = 1800

    def __init__(
        self, connection_args: "DremioSourceConfig", report: "DremioSourceReport"
    ) -> None:
        self.filter = DremioFilter(connection_args, report)
        self.source_type_mapper = DremioToDataHubSourceTypeMapping(
            extra_mappings=connection_args.source_type_mappings,
        )
        self.allow_schema_pattern: List[str] = connection_args.schema_pattern.allow
        self.deny_schema_pattern: List[str] = connection_args.schema_pattern.deny
        self._max_workers: int = connection_args.max_workers
        self.is_dremio_cloud = connection_args.is_dremio_cloud
        self.start_time = connection_args.start_time
        self.end_time = connection_args.end_time
        self.report = report
        self._chunk_size = 1000  # Sensible default to prevent OOM
        # /catalog/{id} responses are id-keyed and immutable for a run, so
        # we reuse them across container walk + dataset fetch.
        self._catalog_cache: Dict[str, Any] = {}
        # Path-keyed Community Edition lookups (`/catalog/by-path/...`) live
        # outside the id-keyed cache, so memoise them separately.
        self._dataset_id_cache: Dict[Tuple[str, str], Optional[str]] = {}
        self.session = requests.Session()
        if connection_args.is_dremio_cloud:
            self.base_url = self._get_cloud_base_url(
                connection_args,
            )
        else:
            self.base_url = self._get_on_prem_base_url(connection_args)

        self.ui_url = self._get_ui_url(connection_args)

        self.authenticate(connection_args)
        self.edition = self.get_dremio_edition()
        # Lazily probed on first call to _supports_array_queried_datasets.
        self._queried_datasets_is_array: Optional[bool] = None

    def get_dremio_edition(self):
        if self.is_dremio_cloud:
            return DremioEdition.CLOUD
        else:
            return (
                DremioEdition.ENTERPRISE
                if self.test_for_enterprise_edition()
                else DremioEdition.COMMUNITY
            )

    def _get_cloud_base_url(self, connection_args: "DremioSourceConfig") -> str:
        """Return the base URL for Dremio Cloud."""
        if connection_args.dremio_cloud_project_id:
            project_id = connection_args.dremio_cloud_project_id
        else:
            self.report.failure(
                "Project ID must be provided for Dremio Cloud environments."
            )
            raise DremioAPIException(
                "Project ID must be provided for Dremio Cloud environments."
            )

        if connection_args.dremio_cloud_region == "US":
            return f"https://api.dremio.cloud:443/v0/projects/{project_id}"
        return f"https://api.{connection_args.dremio_cloud_region.lower()}.dremio.cloud:443/v0/projects/{project_id}"

    def _get_on_prem_base_url(self, connection_args: "DremioSourceConfig") -> str:
        """Return the base URL for on-prem Dremio."""
        host = connection_args.hostname
        port = connection_args.port
        protocol = "https" if connection_args.tls else "http"
        if not host:
            self.report.failure(
                "Hostname must be provided for on-premises Dremio instances."
            )
            raise DremioAPIException(
                "Hostname must be provided for on-premises Dremio instances."
            )
        return f"{protocol}://{host}:{port}/api/v3"

    def _get_ui_url(self, connection_args: "DremioSourceConfig") -> str:
        """Return the UI URL for Dremio."""
        if connection_args.is_dremio_cloud:
            if connection_args.dremio_cloud_project_id:
                project_id = connection_args.dremio_cloud_project_id
            else:
                self.report.failure(
                    "Project ID must be provided for Dremio Cloud environments."
                )
                raise DremioAPIException(
                    "Project ID must be provided for Dremio Cloud environments."
                )
            cloud_region = connection_args.dremio_cloud_region
            if cloud_region == "US":
                return f"https://app.dremio.cloud/sonar/{project_id}"
            return f"https://app.{cloud_region.lower()}.dremio.cloud/sonar/{project_id}"

        else:
            host = connection_args.hostname
            port = connection_args.port
            protocol = "https" if connection_args.tls else "http"
            if not host:
                self.report.failure(
                    "Hostname must be provided for on-premises Dremio instances."
                )
                raise DremioAPIException(
                    "Hostname must be provided for on-premises Dremio instances."
                )
            return f"{protocol}://{host}:{port}"

    def _setup_session(self) -> None:
        """Setup the session for retries and connection handling."""
        retry_strategy = Retry(
            total=self._retry_count,
            status_forcelist=[429, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
            backoff_factor=1,
        )
        # Pool size matches max_workers to prevent "Connection pool is full" warnings
        # that serialise parallel catalog traversal into sequential requests.
        pool_size = max(self._max_workers, 15)
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=pool_size,
            pool_maxsize=pool_size,
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update({"Content-Type": "application/json"})

    def authenticate(self, connection_args: "DremioSourceConfig") -> None:
        """Authenticate the session for Dremio, handling both cloud and on-prem cases."""
        self._setup_session()

        self._verify = (
            True
            if connection_args.is_dremio_cloud
            else (
                connection_args.tls
                and not connection_args.disable_certificate_verification
            )
        )
        if not self._verify:
            warnings.simplefilter("ignore", InsecureRequestWarning)

        # Cloud Dremio authentication using PAT
        if self.is_dremio_cloud:
            if not connection_args.password:
                self.report.failure(
                    "Personal Access Token (PAT) is missing for cloud authentication."
                )
                raise DremioAPIException(
                    "Personal Access Token (PAT) is missing for cloud authentication."
                )
            self.session.headers.update(
                {
                    "Authorization": f"Bearer {connection_args.password.get_secret_value()}"
                }
            )
            logger.debug("Configured Dremio cloud API session to use PAT")
            return

        # On-prem auth (PAT or Basic). Only retry transient errors —
        # bad credentials/config aren't going to recover.
        for attempt in range(1, self._retry_count + 1):
            try:
                if connection_args.authentication_method == "PAT":
                    self.session.headers.update(
                        {
                            "Authorization": f"Bearer {connection_args.password.get_secret_value() if connection_args.password else ''}",
                        }
                    )
                    logger.debug("Configured Dremio API session to use PAT")
                    return
                else:
                    assert connection_args.username and connection_args.password, (
                        "Username and password are required for authentication"
                    )
                    host = connection_args.hostname
                    port = connection_args.port
                    protocol = "https" if connection_args.tls else "http"
                    login_url = f"{protocol}://{host}:{port}/apiv2/login"
                    response = self.session.post(
                        url=login_url,
                        data=json.dumps(
                            {
                                "userName": connection_args.username,
                                "password": connection_args.password.get_secret_value(),
                            }
                        ),
                        verify=self._verify,
                        timeout=self._timeout,
                    )
                    response.raise_for_status()
                    token = response.json().get("token")
                    if token:
                        logger.debug("Exchanged username and password for Dremio token")
                        self.session.headers.update(
                            {"Authorization": f"_dremio{token}"}
                        )
                        return
                    else:
                        self.report.failure("Failed to authenticate", login_url)
                        raise DremioAPIException("Failed to authenticate with Dremio")
            except (AssertionError, DremioAPIException):
                # Credential / config errors: fail fast instead of looping.
                raise
            except Exception as e:
                self.report.failure("Failed to authenticate", str(e))
                if attempt < self._retry_count:
                    sleep(1)

        self.report.failure(
            "Credentials cannot be refreshed. Please check your username and password."
        )
        raise DremioAPIException(
            "Credentials cannot be refreshed. Please check your username and password."
        )

    def _request(self, method: str, url: str, data: Union[str, None] = None) -> Dict:
        """Send a request to the Dremio API."""

        logger.debug(f"{method} request to {self.base_url + url}")
        self.report.api_calls_total += 1
        self.report.api_calls_by_method_and_path[f"{method} {url}"] += 1

        with PerfTimer() as timer:
            response = self.session.request(
                method=method,
                url=(self.base_url + url),
                data=data,
                verify=self._verify,
                timeout=self._timeout,
            )
            self.report.api_call_secs_by_method_and_path[f"{method} {url}"] += (
                timer.elapsed_seconds()
            )
            # response.raise_for_status()  # Enabling this line, makes integration tests to fail
            try:
                return response.json()
            except requests.exceptions.JSONDecodeError as e:
                logger.info(
                    f"On {method} request to {url}, failed to parse JSON from response (status {response.status_code}): {response.text}"
                )
                logger.debug(
                    f"Request curl equivalent: {make_curl_command(self.session, method, url, data)}"
                )
                raise DremioAPIException(
                    f"Failed to parse JSON from response (status {response.status_code}): {response.text}"
                ) from e

    def get(self, url: str) -> Dict:
        """Send a GET request to the Dremio API.

        Responses for /catalog/{id} (no sub-path) are cached in memory because
        the catalog tree traversal and subsequent dataset fetch both walk the same
        nodes, producing 4-6 redundant round-trips per catalog entry.

        The cache is best-effort under the get_all_containers ThreadPoolExecutor:
        two threads can both miss the check-then-write and refetch the same URL.
        That's tolerable here — Dremio's /catalog/{id} GET is idempotent, the
        worst case is one extra HTTP round-trip per racing pair, and a single-
        key dict access is atomic under the GIL so we won't corrupt the cache.
        Adding a Lock would serialise all hot-path GETs and erase the win.
        """
        _cacheable = url.startswith("/catalog/") and url.count("/") == 2
        if _cacheable and url in self._catalog_cache:
            return self._catalog_cache[url]
        result = self._request("GET", url)
        if _cacheable:
            self._catalog_cache[url] = result
        return result

    def post(self, url: str, data: str) -> Dict:
        """Send a POST request to the Dremio API."""
        return self._request("POST", url, data=data)

    def _wait_for_job(self, job_id: str, deadline: float) -> None:
        """Poll until a Dremio job reaches a terminal state.

        ``deadline`` is an absolute ``time.time()`` instant shared with
        the fetch phase so poll + fetch share one end-to-end budget.

        Uses adaptive backoff (1 s → 10 s) so fast queries aren't penalised
        by a fixed sleep and slow queries don't hammer the status endpoint.
        """
        wait = 1.0
        while True:
            status = self.get_job_status(job_id)
            # Tolerate malformed responses (rate-limit HTML, partial JSON,
            # transient proxy errors) — the wall-clock deadline below is the
            # safety net, not a KeyError mid-query.
            state = status.get("jobState")
            if state is None:
                logger.warning(
                    f"Dremio job {job_id} status response missing 'jobState'; "
                    f"keys present: {list(status.keys())}"
                )
            if state == DremioJobState.COMPLETED:
                return
            elif state == DremioJobState.FAILED:
                raise RuntimeError(
                    f"Query failed: {status.get('errorMessage', 'Unknown error')}"
                )
            elif state == DremioJobState.CANCELED:
                raise RuntimeError("Query was canceled")

            if time() >= deadline:
                self.cancel_query(job_id)
                raise DremioAPIException(
                    "Query execution timed out at the shared poll/fetch deadline"
                )

            sleep(wait)
            wait = min(wait * 1.5, 10.0)

    def execute_query(self, query: str, timeout: int = 3600) -> List[Dict[str, Any]]:
        """Execute a SQL query. `timeout` covers both poll and fetch phases."""
        try:
            with PerfTimer() as timer:
                logger.info(f"Executing query: {query}")
                response = self.post(url="/sql", data=json.dumps({"sql": query}))

                if "errorMessage" in response:
                    self.report.failure(
                        message="SQL Error", context=f"{response['errorMessage']}"
                    )
                    raise DremioAPIException(f"SQL Error: {response['errorMessage']}")

                job_id = response["id"]
                deadline = time() + timeout
                try:
                    self._wait_for_job(job_id, deadline=deadline)
                except RuntimeError as e:
                    raise DremioAPIException(str(e)) from e
                result = self._fetch_all_results(job_id, deadline=deadline)
                logger.info(
                    f"Query executed in {timer.elapsed_seconds()} seconds with {len(result)} results"
                )
                return result

        except requests.RequestException as e:
            raise DremioAPIException("Error executing query") from e

    def _fetch_all_results(
        self, job_id: str, deadline: Optional[float] = None
    ) -> List[Dict]:
        # Eager wrapper around the streaming iterator so deadline / error
        # handling live in one place.
        return list(self._fetch_results_iter(job_id, deadline=deadline))

    def _fetch_results_iter(
        self, job_id: str, deadline: Optional[float] = None
    ) -> Iterator[Dict]:
        """Stream job result pages. `deadline` (wall-clock epoch seconds) is
        checked before each page request to enforce the caller's timeout."""
        limit = 500
        offset = 0
        total = 0

        while True:
            if deadline is not None and time() > deadline:
                self.cancel_query(job_id)
                raise DremioAPIException(
                    f"Query result fetch timed out for job {job_id}"
                )

            result = self.get_job_result(job_id, offset, limit)

            if "rows" not in result:
                logger.warning(
                    f"API response for job {job_id} missing 'rows' key. "
                    f"Response keys: {list(result.keys())}"
                )
                if "errorMessage" in result:
                    raise DremioAPIException(f"Query error: {result['errorMessage']}")
                elif "message" in result:
                    logger.warning(
                        f"Query warning for job {job_id}: {result['message']}"
                    )
                break

            result_rows = result["rows"]
            if not result_rows:
                break

            for row in result_rows:
                yield row
                total += 1

            actual = len(result_rows)
            offset += actual
            if actual < limit:
                break

        logger.info(f"Streamed {total} total rows for job {job_id}")

    def execute_query_iter(
        self, query: str, timeout: int = 3600
    ) -> Iterator[Dict[str, Any]]:
        """Execute SQL query and return results as a streaming iterator"""
        try:
            with PerfTimer() as timer:
                logger.info(f"Executing streaming query: {query}")
                response = self.post(url="/sql", data=json.dumps({"sql": query}))

                if "errorMessage" in response:
                    self.report.failure(
                        message="SQL Error", context=f"{response['errorMessage']}"
                    )
                    raise DremioAPIException(f"SQL Error: {response['errorMessage']}")

                job_id = response["id"]
                deadline = time() + timeout
                try:
                    self._wait_for_job(job_id, deadline=deadline)
                except RuntimeError as e:
                    raise DremioAPIException(str(e)) from e
                logger.info(
                    f"Query job completed in {timer.elapsed_seconds()} seconds, starting streaming"
                )
                return self._fetch_results_iter(job_id, deadline=deadline)

        except requests.RequestException as e:
            raise DremioAPIException("Error executing streaming query") from e

    def cancel_query(self, job_id: str) -> None:
        """Cancel a running query"""
        try:
            self.post(url=f"/job/{job_id}/cancel", data=json.dumps({}))
        except Exception as e:
            logger.error(f"Failed to cancel query {job_id}: {str(e)}")

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Check job status"""
        return self.get(
            url=f"/job/{job_id}/",
        )

    def get_job_result(
        self, job_id: str, offset: int = 0, limit: int = 500
    ) -> Dict[str, Any]:
        """Get job results in batches"""
        return self.get(
            url=f"/job/{job_id}/results?offset={offset}&limit={limit}",
        )

    def get_dataset_id(self, schema: str, dataset: str) -> Optional[str]:
        """Resolve the catalog id for `schema.dataset`. Memoised because
        Community Edition uses path-keyed lookups that bypass the id-keyed
        catalog cache (RESOURCE_ID + LOCATION_ID both call here per table)."""
        cache_key = (schema, dataset)
        if cache_key in self._dataset_id_cache:
            return self._dataset_id_cache[cache_key]

        schema_split = schema.split(".")
        schema_str = ""
        last_val = 0

        for increment_val in range(1, len(schema_split) + 1):
            current_path = ".".join(schema_split[last_val:increment_val])
            url_encoded = quote(current_path, safe="")
            response = self.get(url=f"/catalog/by-path/{schema_str}/{url_encoded}")

            if not response.get("errorMessage"):
                last_val = increment_val
                schema_str = (
                    f"{schema_str}/{url_encoded}" if schema_str else url_encoded
                )

        dataset_response = self.get(
            url=f"/catalog/by-path/{schema_str}/{quote(dataset, safe='')}",
        )
        dataset_id = dataset_response.get("id")
        if not dataset_id:
            logger.error(f"Dataset ID not found for {schema}.{dataset}")

        self._dataset_id_cache[cache_key] = dataset_id
        return dataset_id

    def community_get_formatted_tables(
        self, tables_and_columns: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        schema_list = []
        schema_dict_lookup = []
        dataset_list = []
        column_dictionary: Dict[str, List[Dict]] = defaultdict(list)

        ordinal_position = 0
        for record in tables_and_columns:
            if not record.get("COLUMN_NAME"):
                continue

            table_full_path = record.get("FULL_TABLE_PATH")
            if not table_full_path:
                continue

            # Ensure ordinal_position is always an integer for proper sorting
            raw_ordinal = record.get("ORDINAL_POSITION", ordinal_position)
            try:
                ordinal_pos = (
                    int(raw_ordinal) if raw_ordinal is not None else ordinal_position
                )
            except (ValueError, TypeError):
                ordinal_pos = ordinal_position

            column_dictionary[table_full_path].append(
                {
                    "name": record["COLUMN_NAME"],
                    "ordinal_position": ordinal_pos,
                    "is_nullable": record["IS_NULLABLE"],
                    "data_type": record["DATA_TYPE"],
                    "column_size": record["COLUMN_SIZE"],
                }
            )

            ordinal_position += 1

            if record.get("TABLE_SCHEMA") not in schema_list:
                schema_list.append(record.get("TABLE_SCHEMA"))

        distinct_tables_list = list(
            {
                tuple(
                    dictionary[key]
                    for key in (
                        "TABLE_SCHEMA",
                        "TABLE_NAME",
                        "FULL_TABLE_PATH",
                        "VIEW_DEFINITION",
                    )
                    if key in dictionary
                ): dictionary
                for dictionary in tables_and_columns
            }.values()
        )

        for schema in schema_list:
            schema_dict_lookup.append(self.validate_schema_format(schema))

        for table, schemas in product(distinct_tables_list, schema_dict_lookup):
            if table.get("TABLE_SCHEMA") == schemas.get("original_path"):
                dataset_list.append(
                    {
                        "TABLE_SCHEMA": "["
                        + ", ".join(
                            schemas.get("formatted_path") + [table.get("TABLE_NAME")]
                        )
                        + "]",
                        "TABLE_NAME": table.get("TABLE_NAME"),
                        "COLUMNS": column_dictionary.get(
                            table.get("FULL_TABLE_PATH", "")
                        ),
                        "VIEW_DEFINITION": table.get("VIEW_DEFINITION"),
                        "RESOURCE_ID": self.get_dataset_id(
                            schema=".".join(schemas.get("formatted_path")),
                            dataset=table.get("TABLE_NAME", ""),
                        ),
                        "LOCATION_ID": self.get_dataset_id(
                            schema=".".join(schemas.get("formatted_path")),
                            dataset="",
                        ),
                    }
                )

        return dataset_list

    def get_pattern_condition(
        self, patterns: Union[str, List[str]], field: str, allow: bool = True
    ) -> str:
        if not patterns:
            return ""

        if isinstance(patterns, str):
            patterns = [patterns.upper()]

        if ".*" in patterns and allow:
            return ""

        patterns = [p.upper() for p in patterns if p != ".*"]
        if not patterns:
            return ""

        operator = "REGEXP_LIKE" if allow else "NOT REGEXP_LIKE"
        pattern_str = "|".join(f"({p})" for p in patterns)
        return f"AND {operator}({field}, '{pattern_str}')"

    def get_all_tables_and_columns(self) -> Iterator[Dict]:
        """
        Fetch all tables and columns using a single global query.

        Runs one SQL query across the entire catalog rather than one per container.
        This eliminates the ~10s Dremio planning overhead that was incurred for each
        of thousands of containers (e.g. 10,000 containers × 10s = ~30 hours).
        Results are still chunked with LIMIT/OFFSET to prevent Dremio OOM errors.
        """
        if self.edition == DremioEdition.ENTERPRISE:
            query_template = DremioSQLQueries.QUERY_DATASETS_EE_GLOBAL
        elif self.edition == DremioEdition.CLOUD:
            query_template = DremioSQLQueries.QUERY_DATASETS_CLOUD_GLOBAL
        else:
            query_template = DremioSQLQueries.QUERY_DATASETS_CE_GLOBAL

        schema_field = "CONCAT(REPLACE(REPLACE(REPLACE(UPPER(TABLE_SCHEMA), ', ', '.'), '[', ''), ']', ''))"

        schema_condition = self.get_pattern_condition(
            self.allow_schema_pattern, schema_field
        )
        deny_schema_condition = self.get_pattern_condition(
            self.deny_schema_pattern, schema_field, allow=False
        )

        yield from self._get_all_tables_global_chunked(
            query_template, schema_condition, deny_schema_condition
        )

    def _get_all_tables_global_chunked(
        self,
        query_template: str,
        schema_condition: str,
        deny_schema_condition: str,
    ) -> Iterator[Dict]:
        """Yield tables from the global dataset query in LIMIT/OFFSET chunks.

        Chunking guards against Dremio OOM on huge result sets. The query
        is ordered by (schema, name, ordinal), so EE/Cloud accumulates the
        in-flight table across chunks and flushes on path change — the
        final table is flushed after the loop. Community uses
        `community_get_formatted_tables`, which is per-chunk complete.
        """
        chunk_size = self._chunk_size
        offset = 0

        column_dictionary: Dict[str, List[Dict]] = defaultdict(list)
        table_metadata: Dict[str, Dict] = {}
        last_table_path: Optional[str] = None

        def _build_emit(path: str) -> Dict:
            info = table_metadata.pop(path)
            columns = sorted(
                column_dictionary.pop(path, []),
                key=lambda col: col.get("ordinal_position", 0),
            )
            return {
                "TABLE_NAME": info.get("TABLE_NAME"),
                "TABLE_SCHEMA": info.get("TABLE_SCHEMA"),
                "COLUMNS": columns,
                "VIEW_DEFINITION": info.get("VIEW_DEFINITION"),
                "RESOURCE_ID": info.get("RESOURCE_ID"),
                "LOCATION_ID": info.get("LOCATION_ID"),
                "OWNER": info.get("OWNER"),
                "OWNER_TYPE": info.get("OWNER_TYPE"),
                "CREATED": info.get("CREATED"),
                "FORMAT_TYPE": info.get("FORMAT_TYPE"),
            }

        while True:
            limit_clause = f"LIMIT {chunk_size} OFFSET {offset}"
            formatted_query = query_template.format(
                schema_pattern=schema_condition,
                deny_schema_pattern=deny_schema_condition,
                limit_clause=limit_clause,
            )

            logger.info(
                f"Fetching global dataset chunk (offset: {offset}, limit: {chunk_size})"
            )

            try:
                chunk_results = list(self.execute_query_iter(query=formatted_query))

                if not chunk_results:
                    logger.info("No more datasets found in global query")
                    break

                if self.edition == DremioEdition.COMMUNITY:
                    for table in self.community_get_formatted_tables(chunk_results):
                        yield table
                else:
                    for record in chunk_results:
                        if not record.get("COLUMN_NAME"):
                            continue

                        table_full_path = record.get("FULL_TABLE_PATH")
                        if not table_full_path:
                            continue

                        # Flush on table-path change — that's the first
                        # point we know we've seen every column for the
                        # previous table (rows are ORDER BY contiguous).
                        if (
                            last_table_path is not None
                            and last_table_path != table_full_path
                            and last_table_path in table_metadata
                        ):
                            yield _build_emit(last_table_path)

                        raw_ordinal = record.get("ORDINAL_POSITION")
                        try:
                            ordinal_pos = (
                                int(raw_ordinal) if raw_ordinal is not None else 0
                            )
                        except (ValueError, TypeError):
                            ordinal_pos = 0

                        column_dictionary[table_full_path].append(
                            {
                                "name": record["COLUMN_NAME"],
                                "ordinal_position": ordinal_pos,
                                "is_nullable": record["IS_NULLABLE"],
                                "data_type": record["DATA_TYPE"],
                                "column_size": record["COLUMN_SIZE"],
                            }
                        )

                        if table_full_path not in table_metadata:
                            table_metadata[table_full_path] = {
                                "TABLE_NAME": record.get("TABLE_NAME"),
                                "TABLE_SCHEMA": record.get("TABLE_SCHEMA"),
                                "VIEW_DEFINITION": record.get("VIEW_DEFINITION"),
                                "RESOURCE_ID": record.get("RESOURCE_ID"),
                                "LOCATION_ID": record.get("LOCATION_ID"),
                                "OWNER": record.get("OWNER"),
                                "OWNER_TYPE": record.get("OWNER_TYPE"),
                                "CREATED": record.get("CREATED"),
                                "FORMAT_TYPE": record.get("FORMAT_TYPE"),
                            }

                        last_table_path = table_full_path

                if len(chunk_results) < chunk_size:
                    break

                offset += chunk_size

            except DremioAPIException as e:
                logger.error(f"Error in global dataset query at offset {offset}: {e}")
                if "'rows'" in str(e):
                    self.report.report_warning(
                        f"Dremio crash detected during global dataset fetch "
                        f"(KeyError: 'rows' - likely OOM). Current chunk_size: {chunk_size}. "
                        f"Consider reducing chunk size if this persists.",
                        context="global_dataset_fetch",
                    )
                break

        # Final flush for the EE/Cloud path (Community already emitted).
        if last_table_path is not None and last_table_path in table_metadata:
            yield _build_emit(last_table_path)

    def validate_schema_format(self, schema):
        if "." in schema:
            schema_path = self.get(
                url=f"/catalog/{self.get_dataset_id(schema=schema, dataset='')}"
            ).get("path")
            return {"original_path": schema, "formatted_path": schema_path}
        return {"original_path": schema, "formatted_path": [schema]}

    def test_for_enterprise_edition(self):
        response = self.session.get(
            url=f"{self.base_url}/catalog/privileges",
            verify=self._verify,
            timeout=self._timeout,
        )

        if response.status_code == 200:
            return True

        return False

    def get_view_parents(self, dataset_id: str) -> List:
        parents_list = []

        if self.edition == DremioEdition.ENTERPRISE:
            parents = self.get(
                url=f"/catalog/{dataset_id}/graph",
            ).get("parents")

            if not parents:
                return []

            for parent in parents:
                parents_list.append(".".join(parent.get("path")))

        return parents_list

    def _supports_array_queried_datasets(self) -> bool:
        """Detect whether sys.jobs_recent.queried_datasets is ARRAY<VARCHAR>
        (Dremio Software 26.1.0+) vs VARCHAR. Probes once with ARRAY_SIZE —
        the function only resolves on array columns, so success → array,
        any failure → legacy form. Cloud is always array."""
        if self._queried_datasets_is_array is not None:
            return self._queried_datasets_is_array

        if self.edition == DremioEdition.CLOUD:
            self._queried_datasets_is_array = True
            return True

        probe_query = (
            "SELECT ARRAY_SIZE(queried_datasets) AS sz FROM SYS.JOBS_RECENT LIMIT 1"
        )
        try:
            list(self.execute_query_iter(query=probe_query))
            self._queried_datasets_is_array = True
            logger.info(
                "Detected ARRAY<VARCHAR> queried_datasets column "
                "(Dremio Software 26.1.0+); using array-aware jobs query."
            )
        except Exception as exc:
            # execute_query_iter can raise DremioAPIException, RuntimeError, or
            # transport errors — any failure means we don't have the array
            # column type. Real problems surface later from the actual jobs query.
            self._queried_datasets_is_array = False
            logger.info(
                f"queried_datasets probe failed; assuming legacy VARCHAR. "
                f"Probe error: {exc}"
            )

        return self._queried_datasets_is_array

    def extract_all_queries(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Iterator[Dict[str, Any]]:
        """
        Memory-efficient streaming version for extracting query results.

        Optional start_time/end_time override the instance-level window (used by
        the stateful time-window handler to advance start_time to the previous
        run's end_time, avoiding redundant re-processing).
        """
        effective_start = start_time if start_time is not None else self.start_time
        effective_end = end_time if end_time is not None else self.end_time

        start_timestamp_str = (
            effective_start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            if effective_start
            else None
        )
        end_timestamp_str = (
            effective_end.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            if effective_end
            else None
        )

        if self.edition == DremioEdition.CLOUD:
            jobs_query = DremioSQLQueries.get_query_all_jobs_cloud(
                start_timestamp_millis=start_timestamp_str,
                end_timestamp_millis=end_timestamp_str,
            )
        elif self._supports_array_queried_datasets():
            jobs_query = DremioSQLQueries.get_query_all_jobs_array(
                start_timestamp_millis=start_timestamp_str,
                end_timestamp_millis=end_timestamp_str,
            )
        else:
            jobs_query = DremioSQLQueries.get_query_all_jobs(
                start_timestamp_millis=start_timestamp_str,
                end_timestamp_millis=end_timestamp_str,
            )

        # Use chunked query execution for large query result sets
        return self._get_queries_chunked(jobs_query)

    def _get_queries_chunked(self, base_query: str) -> Iterator[Dict[str, Any]]:
        """Fetch queries using chunked LIMIT/OFFSET execution to prevent Dremio OOM."""
        chunk_size = self._chunk_size
        offset = 0

        while True:
            chunked_query = base_query.format(
                limit_clause=f"LIMIT {chunk_size} OFFSET {offset}"
            )

            logger.info(f"Fetching chunk of {chunk_size} queries (offset: {offset})")

            try:
                chunk_results = list(self.execute_query_iter(query=chunked_query))

                if not chunk_results:
                    logger.info("No more queries to fetch")
                    break

                yield from chunk_results

                if len(chunk_results) < chunk_size:
                    break

                offset += chunk_size

            except DremioAPIException as e:
                logger.error(
                    f"Error in chunked query extraction at offset {offset}: {e}"
                )
                if "'rows'" in str(e):
                    self.report.report_warning(
                        f"Dremio crash detected during query extraction "
                        f"(KeyError: 'rows' - likely OOM). Current chunk_size: {chunk_size}. "
                        f"Consider reducing chunk size if this persists.",
                        context="query_extraction",
                    )
                break

    def get_tags_for_resource(self, resource_id: str) -> Optional[List[str]]:
        """
        Get Dremio tags for a given resource_id.
        """

        try:
            tags = self.get(
                url=f"/catalog/{resource_id}/collaboration/tag",
            )
            return tags.get("tags")
        except Exception as exc:
            logger.info(
                "Resource ID {} has no tags: {}".format(
                    resource_id,
                    exc,
                )
            )
        return None

    def get_description_for_resource(self, resource_id: str) -> Optional[str]:
        """
        Get Dremio wiki entry for a given resource_id.
        """

        try:
            tags = self.get(
                url=f"/catalog/{resource_id}/collaboration/wiki",
            )
            return tags.get("text")
        except Exception as exc:
            logger.info(
                "Resource ID {} has no wiki entry: {}".format(
                    resource_id,
                    exc,
                )
            )
        return None

    def get_all_containers(self):
        """
        Query the Dremio sources API and return filtered source information.
        """
        containers = []
        response = self.get(url="/catalog")

        def process_source(source):
            container_type = source.get("containerType")

            if container_type == DremioEntityContainerType.SOURCE.value:
                source_resp = {}
                source_config = {}
                if source.get("id"):
                    try:
                        source_resp = self.get(
                            url=f"/catalog/{source.get('id')}",
                        )
                        source_config = source_resp.get("config", {})
                    except Exception as exc:
                        # /catalog/{id} can fail for sources the auth user can't
                        # see the config of (e.g. cross-tenant in Dremio Cloud).
                        # We still want to emit the source as a container with
                        # the basic info from /catalog; surface the failure so
                        # users can spot it but don't abort the whole walk.
                        logger.warning(
                            f"Failed to fetch source detail for "
                            f"{source.get('path', source.get('name', source.get('id')))}; "
                            f"falling back to basic catalog info: {exc}"
                        )

                db = source_config.get(
                    "database", source_config.get("databaseName", "")
                )

                source_name = (
                    source.get("path", [""])[0]
                    if source.get("path")
                    else source.get("name", "")
                )

                if source_name:
                    if self.filter.should_include_container([], source_name):
                        container_data = {
                            **source,  # Original source data
                            **source_resp,  # Source details (may be empty)
                            "name": source_name,  # Preserve the source name
                            "container_type": DremioEntityContainerType.SOURCE,
                            "root_path": source_config.get("rootPath"),
                            "database_name": db,
                            "path": [],  # Root sources should have empty path for proper browse paths
                        }

                        self.report.report_container_scanned(source_name)
                        return DremioContainerResponse.model_validate(container_data)

                    self.report.report_container_filtered(source_name)
            elif container_type in (
                DremioEntityContainerType.SPACE.value,
                DremioEntityContainerType.HOME.value,
            ):
                space_name = (
                    source.get("path", [""])[0]
                    if source.get("path")
                    else source.get("name", "")
                )

                if space_name:
                    if self.filter.should_include_container([], space_name):
                        # HOME is modelled as a SPACE in DataHub.
                        mapped_container_type = DremioEntityContainerType.SPACE

                        container_data = {
                            **source,  # Original source data
                            "name": space_name,  # Preserve the space name
                            "containerType": mapped_container_type.value,  # Use alias and value
                            "path": [],  # Root spaces should have empty path for proper browse paths
                        }

                        self.report.report_container_scanned(space_name)
                        return DremioContainerResponse.model_validate(container_data)

                    self.report.report_container_filtered(space_name)
            return None

        def process_source_and_containers(source):
            container = process_source(source)
            if not container:
                return []

            sub_containers = []
            # Fall back to name if no ID is present (some sources have one but not the other).
            resource_id = container.id or container.name
            if resource_id:
                try:
                    sub_containers = self.get_containers_for_location(
                        resource_id=resource_id,
                        path=[container.name],
                        root_container_type=container.container_type,
                    )
                except Exception as exc:
                    logger.debug(
                        f"Failed to get sub-containers for {container.name}: {exc}"
                    )

            return [container] + sub_containers

        # Use ThreadPoolExecutor to parallelize the processing of sources
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._max_workers
        ) as executor:
            future_to_source = {
                executor.submit(process_source_and_containers, source): source
                for source in response.get("data", [])
            }

            for future in concurrent.futures.as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    containers.extend(future.result())
                except Exception as exc:
                    logger.error(f"Error processing source: {exc}")
                    self.report.warning(
                        message="Failed to process source",
                        context=f"{source}",
                        exc=exc,
                    )

        return containers

    def get_context_for_vds(self, resource_id: str) -> str:
        context_array = self.get(
            url=f"/catalog/{resource_id}",
        ).get("sqlContext")
        if context_array:
            return ".".join(
                f'"{part}"' if "." in part else f"{part}" for part in context_array
            )
        else:
            return ""

    def get_containers_for_location(
        self,
        resource_id: str,
        path: List[str],
        root_container_type: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        containers = []

        def traverse_path(location_id: str, entity_path: List[str]) -> List:
            nonlocal containers, root_container_type
            try:
                response = self.get(url=f"/catalog/{location_id}")

                # Check if current folder should be included
                if (
                    response.get("entityType")
                    == DremioEntityContainerType.FOLDER.lower()
                ):
                    folder_name = entity_path[-1]
                    folder_path = entity_path[:-1]
                    folder_full_name = ".".join(folder_path + [folder_name])

                    if self.filter.should_include_container(folder_path, folder_name):
                        folder_data = {
                            "id": location_id,
                            "name": folder_name,
                            "path": folder_path,
                            "container_type": DremioEntityContainerType.FOLDER,
                            # Passed down so the folder inherits its root's browse-path subtype.
                            "root_container_type": root_container_type,
                        }

                        self.report.report_container_scanned(folder_full_name)
                        containers.append(
                            DremioContainerResponse.model_validate(folder_data)
                        )
                    else:
                        self.report.report_container_filtered(folder_full_name)

                # Recursively process child containers
                for container in response.get("children", []):
                    if container.get("type") == DremioEntityContainerType.CONTAINER:
                        traverse_path(container.get("id"), container.get("path"))

            except Exception as exc:
                logger.info(
                    "Location {} contains no tables or views. Skipping...".format(
                        location_id
                    )
                )
                self.report.warning(
                    message="Failed to get tables or views",
                    context=f"{location_id}",
                    exc=exc,
                )

            return containers

        return traverse_path(location_id=resource_id, entity_path=path)
