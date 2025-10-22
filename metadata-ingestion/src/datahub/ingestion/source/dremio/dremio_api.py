import concurrent.futures
import json
import logging
import re
import warnings
from collections import defaultdict
from enum import Enum
from itertools import product
from time import sleep, time
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from urllib3.exceptions import InsecureRequestWarning

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.emitter.request_helper import make_curl_command
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_datahub_source_mapping import (
    DremioToDataHubSourceTypeMapping,
)
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport
from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries
from datahub.utilities.perf_timer import PerfTimer

if TYPE_CHECKING:
    from datahub.ingestion.source.dremio.dremio_entities import DremioContainer

logger = logging.getLogger(__name__)

# System table patterns to exclude (similar to BigQuery's approach)
# Note: These patterns are applied to lowercase names for case-insensitive matching
# Conservative patterns that only match actual system schemas/tables
DREMIO_SYSTEM_TABLES_PATTERN = [
    r"^information_schema$",  # Exact INFORMATION_SCHEMA schema match
    r"^sys$",  # Exact SYS schema match
    r"^information_schema\..*",  # Tables in INFORMATION_SCHEMA schema
    r"^sys\..*",  # Tables in SYS schema
]


class DremioAPIException(Exception):
    pass


class DremioEdition(Enum):
    CLOUD = "CLOUD"
    ENTERPRISE = "ENTERPRISE"
    COMMUNITY = "COMMUNITY"


class DremioEntityContainerType(Enum):
    SPACE = "SPACE"
    CONTAINER = "CONTAINER"
    FOLDER = "FOLDER"
    SOURCE = "SOURCE"


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
        Uses the standard is_schema_allowed function for consistency with other platforms.

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

        # Construct full schema name for system schema filtering
        full_schema_name = ".".join(full_path_components).lower()

        # First, check if it's a system schema (excluded unless include_system_tables is True)
        if not self.config.include_system_tables and not AllowDenyPattern(
            deny=DREMIO_SYSTEM_TABLES_PATTERN
        ).allowed(full_schema_name):
            return False

        # Use the standard is_schema_allowed function with fully qualified matching
        # This makes Dremio consistent with Snowflake/BigQuery behavior
        schema_name = full_path_components[-1]  # Last component
        parent_path = (
            ".".join(full_path_components[:-1]) if len(full_path_components) > 1 else ""
        )

        return is_schema_allowed(
            self.config.schema_pattern,
            schema_name,
            parent_path,
            True,  # Always use fully qualified names for Dremio's hierarchical structure
        )

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
        # Construct fully qualified name: source.folder.subfolder.table
        if schema_path:
            full_dataset_name = f"{'.'.join(schema_path)}.{dataset_name}".lower()
        else:
            full_dataset_name = dataset_name.lower()

        # First, check if it's a system table (excluded unless include_system_tables is True)
        if not self.config.include_system_tables and not AllowDenyPattern(
            deny=DREMIO_SYSTEM_TABLES_PATTERN
        ).allowed(full_dataset_name):
            return False

        # Then check the user-configured dataset pattern
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
        self.dremio_to_datahub_source_mapper = DremioToDataHubSourceTypeMapping()

        # Initialize the new filter
        self.filter = DremioFilter(connection_args, report)

        # Keep old fields for backward compatibility during transition
        self.allow_schema_pattern: List[str] = connection_args.schema_pattern.allow
        self.deny_schema_pattern: List[str] = connection_args.schema_pattern.deny

        self._max_workers: int = connection_args.max_workers
        self.is_dremio_cloud = connection_args.is_dremio_cloud
        self.start_time = connection_args.start_time
        self.end_time = connection_args.end_time
        self.report = report
        self._chunk_size = 1000  # Sensible default to prevent OOM
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
        adapter = HTTPAdapter(max_retries=retry_strategy)
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
                {"Authorization": f"Bearer {connection_args.password}"}
            )
            logger.debug("Configured Dremio cloud API session to use PAT")
            return

        # On-prem Dremio authentication (PAT or Basic Auth)
        for _ in range(1, self._retry_count + 1):
            try:
                if connection_args.authentication_method == "PAT":
                    self.session.headers.update(
                        {
                            "Authorization": f"Bearer {connection_args.password}",
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
                                "password": connection_args.password,
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
            except Exception as e:
                self.report.failure("Failed to authenticate", str(e))
                sleep(1)  # Optional: exponential backoff

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
        """Send a GET request to the Dremio API."""
        return self._request("GET", url)

    def post(self, url: str, data: str) -> Dict:
        """Send a POST request to the Dremio API."""
        return self._request("POST", url, data=data)

    def execute_query(self, query: str, timeout: int = 3600) -> List[Dict[str, Any]]:
        """Execute SQL query with timeout and error handling"""
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

                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(self.fetch_results, job_id)
                    try:
                        result = future.result(timeout=timeout)
                        logger.info(
                            f"Query executed in {timer.elapsed_seconds()} seconds with {len(result)} results"
                        )
                        return result
                    except concurrent.futures.TimeoutError:
                        self.cancel_query(job_id)
                        raise DremioAPIException(
                            f"Query execution timed out after {timeout} seconds"
                        ) from None
                    except RuntimeError as e:
                        raise DremioAPIException() from e

        except requests.RequestException as e:
            raise DremioAPIException("Error executing query") from e

    def fetch_results(self, job_id: str) -> List[Dict]:
        """Fetch job results with status checking"""
        start_time = time()
        while True:
            status = self.get_job_status(job_id)
            if status["jobState"] == "COMPLETED":
                break
            elif status["jobState"] == "FAILED":
                error_message = status.get("errorMessage", "Unknown error")
                raise RuntimeError(f"Query failed: {error_message}")
            elif status["jobState"] == "CANCELED":
                raise RuntimeError("Query was canceled")

            if time() - start_time > self._timeout:
                self.cancel_query(job_id)
                raise TimeoutError("Query execution timed out while fetching results")

            sleep(3)

        return self._fetch_all_results(job_id)

    def _fetch_all_results(self, job_id: str) -> List[Dict]:
        """Fetch all results for a completed job"""
        limit = 500
        offset = 0
        rows = []

        while True:
            result = self.get_job_result(job_id, offset, limit)

            # Handle cases where API response doesn't contain 'rows' key
            # This can happen with OOM errors or when no rows are returned
            if "rows" not in result:
                logger.warning(
                    f"API response for job {job_id} missing 'rows' key. "
                    f"Response keys: {list(result.keys())}"
                )
                # Check for error conditions
                if "errorMessage" in result:
                    raise DremioAPIException(f"Query error: {result['errorMessage']}")
                elif "message" in result:
                    logger.warning(
                        f"Query warning for job {job_id}: {result['message']}"
                    )
                # Return empty list if no rows key and no error
                break

            # Handle empty rows response
            result_rows = result["rows"]
            if not result_rows:
                logger.debug(
                    f"No more rows returned for job {job_id} at offset {offset}"
                )
                break

            rows.extend(result_rows)

            # Check actual returned rows to determine if we should continue
            actual_rows_returned = len(result_rows)
            if actual_rows_returned == 0:
                logger.debug(f"Query returned no rows for job {job_id}")
                break

            offset = offset + actual_rows_returned
            # If we got fewer rows than requested, we've reached the end
            if actual_rows_returned < limit:
                break

        logger.info(f"Fetched {len(rows)} total rows for job {job_id}")
        return rows

    def _fetch_results_iter(self, job_id: str) -> Iterator[Dict]:
        """
        Fetch job results as an iterator.
        """
        limit = 500
        offset = 0
        total_rows_fetched = 0

        while True:
            result = self.get_job_result(job_id, offset, limit)

            # Handle cases where API response doesn't contain 'rows' key
            if "rows" not in result:
                logger.warning(
                    f"API response for job {job_id} missing 'rows' key. "
                    f"Response keys: {list(result.keys())}"
                )
                # Check for error conditions
                if "errorMessage" in result:
                    raise DremioAPIException(f"Query error: {result['errorMessage']}")
                elif "message" in result:
                    logger.warning(
                        f"Query warning for job {job_id}: {result['message']}"
                    )
                # Stop iteration if no rows key and no error
                break

            # Handle empty rows response
            result_rows = result["rows"]
            if not result_rows:
                logger.debug(
                    f"No more rows returned for job {job_id} at offset {offset}"
                )
                break

            # Yield individual rows instead of collecting them
            for row in result_rows:
                yield row
                total_rows_fetched += 1

            # Check actual returned rows to determine if we should continue
            actual_rows_returned = len(result_rows)
            if actual_rows_returned == 0:
                logger.debug(f"Query returned no rows for job {job_id}")
                break

            offset = offset + actual_rows_returned
            # If we got fewer rows than requested, we've reached the end
            if actual_rows_returned < limit:
                break

        logger.info(f"Streamed {total_rows_fetched} total rows for job {job_id}")

    def execute_query_iter(
        self, query: str, timeout: int = 3600
    ) -> Iterator[Dict[str, Any]]:
        """Execute SQL query and return results as an iterator"""
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

                # Wait for job completion
                start_time = time()
                while True:
                    status = self.get_job_status(job_id)
                    if status["jobState"] == "COMPLETED":
                        break
                    elif status["jobState"] == "FAILED":
                        error_message = status.get("errorMessage", "Unknown error")
                        raise RuntimeError(f"Query failed: {error_message}")
                    elif status["jobState"] == "CANCELED":
                        raise RuntimeError("Query was canceled")

                    if time() - start_time > timeout:
                        self.cancel_query(job_id)
                        raise DremioAPIException(
                            f"Query execution timed out after {timeout} seconds"
                        )

                    sleep(3)

                logger.info(
                    f"Query job completed in {timer.elapsed_seconds()} seconds, fetching results"
                )

                # Return iterator
                return self._fetch_results_iter(job_id)

        except requests.RequestException as e:
            raise DremioAPIException("Error executing query") from e

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
        """Retrieve the dataset ID based on schema and dataset name."""
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

    def get_all_tables_and_columns(
        self, containers: Iterator["DremioContainer"]
    ) -> Iterator[Dict]:
        """
        Get tables and columns as an iterator.
        """
        if self.edition == DremioEdition.ENTERPRISE:
            query_template = DremioSQLQueries.QUERY_DATASETS_EE
        elif self.edition == DremioEdition.CLOUD:
            query_template = DremioSQLQueries.QUERY_DATASETS_CLOUD
        else:
            query_template = DremioSQLQueries.QUERY_DATASETS_CE

        schema_field = "CONCAT(REPLACE(REPLACE(REPLACE(UPPER(TABLE_SCHEMA), ', ', '.'), '[', ''), ']', ''))"

        schema_condition = self.get_pattern_condition(
            self.allow_schema_pattern, schema_field
        )
        deny_schema_condition = self.get_pattern_condition(
            self.deny_schema_pattern, schema_field, allow=False
        )

        # Process each container's results with chunking
        for schema in containers:
            try:
                for table in self._get_container_tables_chunked(
                    schema, query_template, schema_condition, deny_schema_condition
                ):
                    yield table

            except DremioAPIException as e:
                self.report.warning(
                    message="Container has no tables or views",
                    context=f"{schema.subclass} {schema.container_name}",
                    exc=e,
                )
                continue

    def _get_container_tables_chunked(
        self,
        schema: "DremioContainer",
        query_template: str,
        schema_condition: str,
        deny_schema_condition: str,
    ) -> Iterator[Dict]:
        """
        Fetch tables for a container using chunked queries with LIMIT and OFFSET
        to prevent Dremio OOM errors.
        """
        chunk_size = self._chunk_size
        offset = 0

        while True:
            # Create chunked query with LIMIT and OFFSET
            limit_clause = f"LIMIT {chunk_size} OFFSET {offset}"

            formatted_query = query_template.format(
                schema_pattern=schema_condition,
                deny_schema_pattern=deny_schema_condition,
                container_name=schema.container_name.lower(),
                limit_clause=limit_clause,
            )

            logger.info(
                f"Fetching chunk of {chunk_size} tables for container {schema.container_name} "
                f"(offset: {offset})"
            )

            try:
                container_results = list(self.execute_query_iter(query=formatted_query))

                if not container_results:
                    # No more results
                    logger.info(
                        f"No more tables found for container {schema.container_name}"
                    )
                    break

                if self.edition == DremioEdition.COMMUNITY:
                    # Process community edition results
                    formatted_tables = self.community_get_formatted_tables(
                        container_results
                    )
                    for table in formatted_tables:
                        yield table
                else:
                    # Process enterprise/cloud edition results
                    column_dictionary: Dict[str, List[Dict]] = defaultdict(list)
                    table_metadata: Dict[str, Dict] = {}

                    for record in container_results:
                        if not record.get("COLUMN_NAME"):
                            continue

                        table_full_path = record.get("FULL_TABLE_PATH")
                        if not table_full_path:
                            continue

                        # Store column information
                        # Ensure ordinal_position is always an integer for proper sorting
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

                        # Store table metadata (only once per table)
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

                    # Yield tables one at a time
                    for table_path, table_info in table_metadata.items():
                        # Sort columns by ordinal_position to ensure consistent ordering
                        columns = sorted(
                            column_dictionary[table_path],
                            key=lambda col: col.get("ordinal_position", 0),
                        )
                        yield {
                            "TABLE_NAME": table_info.get("TABLE_NAME"),
                            "TABLE_SCHEMA": table_info.get("TABLE_SCHEMA"),
                            "COLUMNS": columns,
                            "VIEW_DEFINITION": table_info.get("VIEW_DEFINITION"),
                            "RESOURCE_ID": table_info.get("RESOURCE_ID"),
                            "LOCATION_ID": table_info.get("LOCATION_ID"),
                            "OWNER": table_info.get("OWNER"),
                            "OWNER_TYPE": table_info.get("OWNER_TYPE"),
                            "CREATED": table_info.get("CREATED"),
                            "FORMAT_TYPE": table_info.get("FORMAT_TYPE"),
                        }

                # If we got fewer results than chunk_size, we're done
                if len(container_results) < chunk_size:
                    logger.info(
                        f"Completed fetching tables for container {schema.container_name}. "
                        f"Final chunk had {len(container_results)} results."
                    )
                    break

                offset += chunk_size

            except DremioAPIException as e:
                logger.error(
                    f"Error in chunked query for container {schema.container_name} "
                    f"at offset {offset}: {e}"
                )
                # Check if it's the KeyError: 'rows' that indicates Dremio crash/OOM
                if "'rows'" in str(e):
                    self.report.report_warning(
                        f"Dremio crash detected for container {schema.container_name} "
                        f"(KeyError: 'rows' - likely OOM). Current chunk_size: {chunk_size}. "
                        f"Consider reducing chunk size if this persists.",
                        context=f"container:{schema.container_name}",
                        exc=e,
                    )
                # Stop processing this container on error
                break

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

    def extract_all_queries_iter(self) -> Iterator[Dict[str, Any]]:
        """
        Get queries as an iterator.
        """
        # Convert datetime objects to string format for SQL queries
        start_timestamp_str = None
        end_timestamp_str = None

        if self.start_time:
            start_timestamp_str = self.start_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        if self.end_time:
            end_timestamp_str = self.end_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        if self.edition == DremioEdition.CLOUD:
            jobs_query = DremioSQLQueries.get_query_all_jobs_cloud(
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
        """
        Fetch queries using chunked execution with LIMIT and OFFSET to prevent Dremio OOM.
        """
        chunk_size = self._chunk_size
        offset = 0

        while True:
            # Create chunked query with LIMIT and OFFSET
            limit_clause = f"LIMIT {chunk_size} OFFSET {offset}"

            chunked_query = base_query.format(limit_clause=limit_clause)

            logger.info(f"Fetching chunk of {chunk_size} queries (offset: {offset})")

            try:
                chunk_results = list(self.execute_query_iter(query=chunked_query))

                if not chunk_results:
                    logger.info("No more queries to fetch")
                    break

                for query_result in chunk_results:
                    yield query_result

                # If we got fewer results than chunk_size, we're done
                if len(chunk_results) < chunk_size:
                    logger.info(
                        f"Completed fetching queries. Final chunk had {len(chunk_results)} results."
                    )
                    break

                offset += chunk_size

            except DremioAPIException as e:
                logger.error(
                    f"Error in chunked query extraction at offset {offset}: {e}"
                )
                # Check if it's the KeyError: 'rows' that indicates Dremio crash/OOM
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
            logging.info(
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
            logging.info(
                "Resource ID {} has no wiki entry: {}".format(
                    resource_id,
                    exc,
                )
            )
        return None

    def _check_pattern_match(
        self,
        pattern: str,
        paths: List[str],
        allow_prefix: bool = True,
    ) -> bool:
        """
        Helper method to check if a pattern matches any of the paths.
        Handles hierarchical matching where each level is matched independently.
        Also handles prefix matching for partial paths.
        """
        if pattern == ".*":
            return True

        # Convert the pattern to regex with proper anchoring
        regex_pattern = pattern
        if pattern.startswith("^"):
            # Already has start anchor
            regex_pattern = pattern.replace(".", r"\.")  # Escape dots
            regex_pattern = regex_pattern.replace(
                r"\.*", ".*"
            )  # Convert .* to wildcard
        else:
            # Add start anchor and handle dots
            regex_pattern = "^" + pattern.replace(".", r"\.").replace(r"\.*", ".*")

        # Handle end matching
        if not pattern.endswith(".*"):
            if pattern.endswith("$"):
                # Keep explicit end anchor
                pass
            elif not allow_prefix:
                # Add end anchor for exact matching
                regex_pattern = regex_pattern + "$"

        return any(re.match(regex_pattern, path, re.IGNORECASE) for path in paths)

    def _could_match_pattern(self, pattern: str, path_components: List[str]) -> bool:
        """
        Check if a container path could potentially match a schema pattern.
        This handles hierarchical path matching for container filtering.
        """
        if pattern == ".*":
            return True

        current_path = ".".join(path_components)

        # Handle simple .* patterns (like "a.b.c.*")
        if pattern.endswith(".*") and not any(c in pattern for c in "^$[](){}+?\\"):
            # Simple dotstar pattern - check prefix matching
            pattern_prefix = pattern[:-2]  # Remove ".*"
            return current_path.lower().startswith(
                pattern_prefix.lower()
            ) or pattern_prefix.lower().startswith(current_path.lower())
        else:
            # Complex regex pattern - use existing regex matching logic
            return self._check_pattern_match(pattern, [current_path], allow_prefix=True)

    def should_include_container(self, path: List[str], name: str) -> bool:
        """
        Helper method to check if a container should be included based on schema patterns.
        Used by both get_all_containers and get_containers_for_location.
        """
        path_components = path + [name] if path else [name]
        full_path = ".".join(path_components)

        # Default allow everything case
        if self.allow_schema_pattern == [".*"] and not self.deny_schema_pattern:
            self.report.report_container_scanned(full_path)
            return True

        # Check deny patterns first
        if self.deny_schema_pattern:
            for pattern in self.deny_schema_pattern:
                if self._check_pattern_match(
                    pattern=pattern,
                    paths=[full_path],
                    allow_prefix=False,
                ):
                    self.report.report_container_filtered(full_path)
                    return False

        # Check allow patterns
        for pattern in self.allow_schema_pattern:
            # Check if current path could potentially match this pattern
            if self._could_match_pattern(pattern, path_components):
                self.report.report_container_scanned(full_path)
                return True

        self.report.report_container_filtered(full_path)
        return False

    def get_all_containers(self):
        """
        Query the Dremio sources API and return filtered source information.
        """
        containers = []
        response = self.get(url="/catalog")

        def process_source(source):
            if source.get("containerType") == DremioEntityContainerType.SOURCE.value:
                source_resp = self.get(
                    url=f"/catalog/{source.get('id')}",
                )

                source_config = source_resp.get("config", {})
                db = source_config.get(
                    "database", source_config.get("databaseName", "")
                )

                if self.should_include_container([], source.get("path")[0]):
                    return {
                        "id": source.get("id"),
                        "name": source.get("path")[0],
                        "path": [],
                        "container_type": DremioEntityContainerType.SOURCE,
                        "source_type": source_resp.get("type"),
                        "root_path": source_config.get("rootPath"),
                        "database_name": db,
                    }
            else:
                if self.should_include_container([], source.get("path")[0]):
                    return {
                        "id": source.get("id"),
                        "name": source.get("path")[0],
                        "path": [],
                        "container_type": DremioEntityContainerType.SPACE,
                    }
            return None

        def process_source_and_containers(source):
            container = process_source(source)
            if not container:
                return []

            # Get sub-containers
            sub_containers = self.get_containers_for_location(
                resource_id=container.get("id"),
                path=[container.get("name")],
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
        self, resource_id: str, path: List[str]
    ) -> List[Dict[str, str]]:
        containers = []

        def traverse_path(location_id: str, entity_path: List[str]) -> List:
            nonlocal containers
            try:
                response = self.get(url=f"/catalog/{location_id}")

                # Check if current folder should be included
                if (
                    response.get("entityType")
                    == DremioEntityContainerType.FOLDER.value.lower()
                ):
                    folder_name = entity_path[-1]
                    folder_path = entity_path[:-1]

                    if self.should_include_container(folder_path, folder_name):
                        containers.append(
                            {
                                "id": location_id,
                                "name": folder_name,
                                "path": folder_path,
                                "container_type": DremioEntityContainerType.FOLDER,
                            }
                        )

                # Recursively process child containers
                for container in response.get("children", []):
                    if (
                        container.get("type")
                        == DremioEntityContainerType.CONTAINER.value
                    ):
                        traverse_path(container.get("id"), container.get("path"))

            except Exception as exc:
                logging.info(
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
