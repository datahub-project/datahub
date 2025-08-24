import concurrent.futures
import json
import logging
import re
import warnings
from collections import defaultdict
from datetime import datetime
from itertools import product
from time import sleep, time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from urllib3.exceptions import InsecureRequestWarning

from datahub.emitter.request_helper import make_curl_command
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_datahub_source_mapping import (
    DremioToDataHubSourceTypeMapping,
)
from datahub.ingestion.source.dremio.dremio_dynamic_chunking import (
    DremioSmartChunker,
)
from datahub.ingestion.source.dremio.dremio_filtering import (
    create_dremio_filter_helper,
)
from datahub.ingestion.source.dremio.dremio_models import (
    DremioColumnInfo,
    DremioDatasetInfo,
    DremioQueryResult,
    DremioViewDefinition,
)
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport
from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.str_enum import StrEnum

if TYPE_CHECKING:
    from datahub.ingestion.source.dremio.dremio_entities import DremioContainer

logger = logging.getLogger(__name__)


class DremioAPIException(Exception):
    pass


class DremioEdition(StrEnum):
    CLOUD = "CLOUD"
    ENTERPRISE = "ENTERPRISE"
    COMMUNITY = "COMMUNITY"


class DremioEntityContainerType(StrEnum):
    SPACE = "SPACE"
    CONTAINER = "CONTAINER"
    FOLDER = "FOLDER"
    SOURCE = "SOURCE"


class DremioAPIOperations:
    _retry_count: int = 5
    _timeout: int = 1800

    def __init__(
        self, connection_args: "DremioSourceConfig", report: "DremioSourceReport"
    ) -> None:
        self.config = connection_args
        self.dremio_to_datahub_source_mapper = DremioToDataHubSourceTypeMapping()
        self.filter_helper = create_dremio_filter_helper(connection_args)
        self._max_workers: int = connection_args.max_workers
        self.is_dremio_cloud = connection_args.is_dremio_cloud
        self.start_time = connection_args.start_time
        self.end_time = connection_args.end_time
        self.report = report
        self.session = requests.Session()

        # Initialize smart chunker for intelligent view processing
        self.smart_chunker = DremioSmartChunker(self)

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
            try:
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

                # Check for HTTP errors
                if response.status_code >= 400:
                    error_msg = f"HTTP {response.status_code} error for {method} {url}"
                    if response.status_code == 500:
                        error_msg += " (Server Error - possibly OOM)"
                    elif response.status_code == 503:
                        error_msg += " (Service Unavailable)"
                    elif response.status_code == 429:
                        error_msg += " (Rate Limited)"

                    logger.error(f"{error_msg}: {response.text}")
                    raise DremioAPIException(f"{error_msg}: {response.text}")

                # Try to parse JSON response
                try:
                    result = response.json()

                    # Additional validation for common error patterns
                    if isinstance(result, dict):
                        if "errorMessage" in result:
                            logger.error(
                                f"API returned error: {result['errorMessage']}"
                            )
                            raise DremioAPIException(
                                f"API error: {result['errorMessage']}"
                            )

                        if "error" in result:
                            logger.error(f"API returned error: {result['error']}")
                            raise DremioAPIException(f"API error: {result['error']}")

                    return result

                except requests.exceptions.JSONDecodeError as e:
                    logger.error(
                        f"On {method} request to {url}, failed to parse JSON from response (status {response.status_code}): {response.text[:500]}..."
                    )
                    logger.debug(
                        f"Request curl equivalent: {make_curl_command(self.session, method, url, data)}"
                    )
                    raise DremioAPIException(
                        f"Failed to parse JSON from response (status {response.status_code}): {response.text[:200]}..."
                    ) from e

            except requests.exceptions.Timeout as e:
                logger.error(f"Timeout on {method} request to {url}")
                raise DremioAPIException(f"Request timeout for {method} {url}") from e
            except requests.exceptions.ConnectionError as e:
                logger.error(f"Connection error on {method} request to {url}: {str(e)}")
                raise DremioAPIException(
                    f"Connection error for {method} {url}: {str(e)}"
                ) from e
            except requests.exceptions.RequestException as e:
                logger.error(
                    f"Request exception on {method} request to {url}: {str(e)}"
                )
                raise DremioAPIException(
                    f"Request failed for {method} {url}: {str(e)}"
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
            try:
                raw_result = self.get_job_result(job_id, offset, limit)

                # Use BaseModel for proper error handling and type safety
                try:
                    query_result = DremioQueryResult.from_api_response(
                        raw_result, job_id
                    )
                except ValueError as e:
                    # Convert ValueError to DremioAPIException with proper reporting
                    error_msg = str(e)
                    if "missing 'rows' key" in error_msg:
                        self.report.failure(
                            message="Query result missing 'rows' key - possible OOM error",
                            context=f"Job {job_id}. Available keys: {list(raw_result.keys()) if isinstance(raw_result, dict) else 'N/A'}",
                        )
                    else:
                        self.report.failure(
                            message="Query execution failed",
                            context=f"Job {job_id}: {error_msg}",
                        )
                    raise DremioAPIException(error_msg) from None

                total_rows = query_result.row_count
                batch_rows = query_result.rows
                if not isinstance(batch_rows, list):
                    self.report.failure(
                        message="Invalid 'rows' format in query result",
                        context=f"Job {job_id}: expected list, got {type(batch_rows)}",
                    )
                    raise DremioAPIException(f"Invalid 'rows' format for job {job_id}")

                rows.extend(batch_rows)
                offset = offset + limit

                # Break if we've fetched all rows or if this batch was smaller than expected
                if offset >= total_rows or len(batch_rows) < limit:
                    break

            except DremioAPIException:
                # Re-raise DremioAPIException without wrapping
                raise
            except Exception as e:
                self.report.failure(
                    message="Unexpected error fetching query results",
                    context=f"Job {job_id}",
                    exc=e,
                )
                raise DremioAPIException(
                    f"Unexpected error fetching results for job {job_id}: {str(e)}"
                ) from e

        return rows

    def cancel_query(self, job_id: str) -> None:
        """Cancel a running query"""
        try:
            self.post(url=f"/job/{job_id}/cancel", data=json.dumps({}))
        except Exception as e:
            self.report.warning(
                message="Failed to cancel query",
                context=f"Job {job_id}",
                exc=e,
            )

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
            self.report.warning(
                message="Dataset ID not found",
                context=f"Schema: {schema}, Dataset: {dataset}",
            )

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

            column_dictionary[table_full_path].append(
                {
                    "name": record["COLUMN_NAME"],
                    "ordinal_position": record.get(
                        "ORDINAL_POSITION", ordinal_position
                    ),
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
                        "VIEW_DEFINITION": self.reassemble_view_definition_chunks(
                            table
                        ),
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

    # Legacy filtering methods - TODO: refactor to use DremioFilterHelper consistently
    def get_pattern_condition(
        self, patterns: List[str], field: str, allow: bool = True
    ) -> str:
        """Legacy method for backward compatibility - should be refactored to use DremioFilterHelper."""
        if not patterns:
            return "1=1" if allow else "1=0"

        conditions = []
        for pattern in patterns:
            if allow:
                conditions.append(f"{field} REGEXP '{pattern}'")
            else:
                conditions.append(f"NOT ({field} REGEXP '{pattern}')")

        return " OR ".join(conditions) if allow else " AND ".join(conditions)

    @property
    def schema_pattern(self):
        """Legacy property for backward compatibility."""
        return (
            self.filter_helper.schema_pattern
            if hasattr(self, "filter_helper")
            else None
        )

    def get_all_tables_and_columns(
        self, containers: List["DremioContainer"]
    ) -> List[Dict]:
        """
        Retrieve all tables and columns from Dremio containers.

        This method now uses intelligent chunking to handle large view definitions
        that would otherwise cause memory issues.
        """
        # Use the smart chunking approach for dataset retrieval
        all_tables_and_columns = self.get_datasets_with_smart_chunking(containers)
        tables = []

        if self.edition == DremioEdition.COMMUNITY:
            tables = self.community_get_formatted_tables(all_tables_and_columns)

        else:
            column_dictionary: Dict[str, List[Dict]] = defaultdict(list)

            for record in all_tables_and_columns:
                if not record.get("COLUMN_NAME"):
                    continue

                table_full_path = record.get("FULL_TABLE_PATH")
                if not table_full_path:
                    continue

                column_dictionary[table_full_path].append(
                    {
                        "name": record["COLUMN_NAME"],
                        "ordinal_position": record["ORDINAL_POSITION"],
                        "is_nullable": record["IS_NULLABLE"],
                        "data_type": record["DATA_TYPE"],
                        "column_size": record["COLUMN_SIZE"],
                    }
                )

            distinct_tables_list = list(
                {
                    tuple(
                        dictionary[key]
                        for key in (
                            "TABLE_SCHEMA",
                            "TABLE_NAME",
                            "FULL_TABLE_PATH",
                            "VIEW_DEFINITION",
                            "LOCATION_ID",
                            "OWNER",
                            "OWNER_TYPE",
                            "CREATED",
                            "FORMAT_TYPE",
                        )
                        if key in dictionary
                    ): dictionary
                    for dictionary in all_tables_and_columns
                }.values()
            )

            for table in distinct_tables_list:
                tables.append(
                    {
                        "TABLE_NAME": table.get("TABLE_NAME"),
                        "TABLE_SCHEMA": table.get("TABLE_SCHEMA"),
                        "COLUMNS": column_dictionary[table["FULL_TABLE_PATH"]],
                        "VIEW_DEFINITION": self.reassemble_view_definition_chunks(
                            table
                        ),
                        "RESOURCE_ID": table.get("RESOURCE_ID"),
                        "LOCATION_ID": table.get("LOCATION_ID"),
                        "OWNER": table.get("OWNER"),
                        "OWNER_TYPE": table.get("OWNER_TYPE"),
                        "CREATED": table.get("CREATED"),
                        "FORMAT_TYPE": table.get("FORMAT_TYPE"),
                    }
                )

        return tables

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

    def extract_all_queries(self) -> List[Dict[str, Any]]:
        """
        Extract all queries using chunked retrieval to prevent OOM errors.

        This method implements intelligent chunking by:
        1. Using pagination to limit memory usage
        2. Processing queries in time-based batches
        3. Leveraging stateful ingestion checkpoints

        Returns:
            List of query dictionaries
        """
        # Use chunked extraction if batch size is configured
        if (
            hasattr(self.config, "query_batch_size")
            and self.config.query_batch_size > 0
        ):
            return self._extract_queries_chunked()
        else:
            # Fall back to original method for backward compatibility
            return self._extract_queries_legacy()

    def _extract_queries_legacy(self) -> List[Dict[str, Any]]:
        """Legacy method for extracting all queries at once."""
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

        return self.execute_query(query=jobs_query)

    def _extract_queries_chunked(self) -> List[Dict[str, Any]]:
        """
        Extract queries using chunked retrieval to prevent OOM errors.

        This method processes queries in batches to avoid memory issues when
        there are large numbers of queries in the specified time window.
        It also optimizes stateful ingestion by using checkpoints when available.
        """
        from datetime import datetime, timedelta

        all_queries = []
        batch_size = self.config.query_batch_size
        max_duration_hours = getattr(self.config, "max_query_duration_hours", 24)

        # Determine time window with stateful ingestion optimization
        end_time = self.end_time or datetime.now()

        # Try to get last checkpoint for stateful ingestion
        last_checkpoint = None
        if hasattr(self, "report") and hasattr(self.report, "window_start_time"):
            last_checkpoint = self.report.window_start_time

        # Use optimized start time calculation
        if self.start_time:
            start_time = self.start_time
        else:
            # Use optimized stateful ingestion logic
            start_timestamp_str = DremioSQLQueries.get_optimized_start_timestamp(
                last_checkpoint=last_checkpoint,
                default_lookback_hours=max_duration_hours,
            )
            start_time = datetime.strptime(start_timestamp_str, "%Y-%m-%d %H:%M:%S.%f")

        logger.info(
            f"Starting chunked query extraction: {start_time} to {end_time}, "
            f"batch_size={batch_size}, max_duration_hours={max_duration_hours}"
        )

        # Process in time-based chunks to prevent overwhelming Dremio
        current_start = start_time
        batch_count = 0

        while current_start < end_time:
            # Calculate end of current batch (limited by max duration)
            max_batch_end = current_start + timedelta(hours=max_duration_hours)
            current_end = min(max_batch_end, end_time)

            batch_count += 1
            logger.info(
                f"Processing query batch {batch_count}: {current_start} to {current_end}"
            )

            try:
                batch_queries = self._extract_query_batch(
                    start_time=current_start,
                    end_time=current_end,
                    batch_size=batch_size,
                )

                all_queries.extend(batch_queries)
                logger.info(
                    f"Batch {batch_count} retrieved {len(batch_queries)} queries"
                )

            except Exception as e:
                logger.error(f"Error processing query batch {batch_count}: {e}")
                if hasattr(self, "report"):
                    self.report.report_failure(
                        "query-batch-error",
                        f"Failed to process query batch {batch_count}",
                        exc=e,
                    )
                # Continue with next batch rather than failing completely

            # Move to next time window
            current_start = current_end

        logger.info(
            f"Chunked query extraction completed: {len(all_queries)} total queries from {batch_count} batches"
        )
        return all_queries

    def _extract_query_batch(
        self, start_time: datetime, end_time: datetime, batch_size: int
    ) -> List[Dict[str, Any]]:
        """
        Extract a single batch of queries with pagination.

        Args:
            start_time: Start of time window
            end_time: End of time window
            batch_size: Maximum number of queries per API call

        Returns:
            List of query dictionaries for this batch
        """
        batch_queries = []
        offset = 0

        # Convert to string format for SQL
        start_timestamp_str = start_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        end_timestamp_str = end_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        while True:
            try:
                # Get paginated query
                if self.edition == DremioEdition.CLOUD:
                    jobs_query = DremioSQLQueries.get_query_all_jobs_cloud(
                        start_timestamp_millis=start_timestamp_str,
                        end_timestamp_millis=end_timestamp_str,
                        limit=batch_size,
                        offset=offset,
                    )
                else:
                    jobs_query = DremioSQLQueries.get_query_all_jobs(
                        start_timestamp_millis=start_timestamp_str,
                        end_timestamp_millis=end_timestamp_str,
                        limit=batch_size,
                        offset=offset,
                    )

                page_results = self.execute_query(query=jobs_query)

                if not page_results:
                    # No more results
                    break

                batch_queries.extend(page_results)

                # If we got fewer results than batch_size, we've reached the end
                if len(page_results) < batch_size:
                    break

                offset += batch_size

                # Safety check to prevent infinite loops
                if offset > 100000:  # Reasonable upper limit
                    logger.warning(
                        f"Query batch exceeded safety limit (offset={offset}), stopping"
                    )
                    break

            except Exception as e:
                logger.error(
                    f"Error in paginated query extraction at offset {offset}: {e}"
                )
                # If we have some results, return them; otherwise re-raise
                if batch_queries:
                    logger.warning(
                        f"Returning partial results ({len(batch_queries)} queries) due to error"
                    )
                    break
                else:
                    raise

        return batch_queries

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

        This uses hierarchical matching logic to ensure we discover intermediate containers
        that might lead to datasets matching our patterns.
        """
        path_components = path + [name] if path else [name]
        full_path = ".".join(path_components)

        # Note: Unlike flat connectors (e.g., Snowflake), Dremio has hierarchical containers
        # (Source -> Space/Folder -> Sub-folder -> Dataset). We must include intermediate
        # containers that could lead to matching datasets, not just exact matches.

        # Default allow everything case
        if self.schema_pattern.allow == [".*"] and not self.schema_pattern.deny:
            self.report.report_container_scanned(full_path)
            return True

        # Check deny patterns first - if explicitly denied, exclude
        for pattern in self.schema_pattern.deny:
            if self._check_pattern_match(
                pattern=pattern,
                paths=[full_path],
                allow_prefix=False,
            ):
                self.report.report_container_filtered(full_path)
                return False

        # Check allow patterns - include if current path could potentially match
        for pattern in self.schema_pattern.allow:
            # Use hierarchical matching to include intermediate containers
            if self._could_match_pattern(pattern, path_components):
                self.report.report_container_scanned(full_path)
                return True

        self.report.report_container_filtered(full_path)
        return False

    def get_all_containers(self):
        """
        Query the Dremio sources API and yield filtered source information.
        Generator to prevent memory pressure from loading all containers at once.
        """
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
                        "container_type": DremioEntityContainerType.SOURCE.value,
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
                        "container_type": DremioEntityContainerType.SPACE.value,
                    }
            return None

        # Process sources sequentially to enable streaming (prevents memory pressure)
        for source in response.get("data", []):
            try:
                container = process_source(source)
                if container:
                    # Yield the main container first
                    yield container

                    # Then yield sub-containers
                    sub_containers = self.get_containers_for_location(
                        resource_id=container.get("id"),
                        path=[container.get("name")],
                    )
                    for sub_container in sub_containers:
                        yield sub_container

            except Exception as exc:
                logger.error(f"Error processing source: {exc}")
                self.report.warning(
                    message="Failed to process source",
                    context=f"{source}",
                    exc=exc,
                )

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
                                "container_type": DremioEntityContainerType.FOLDER.value,
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

    def get_datasets_with_smart_chunking(
        self, containers: List["DremioContainer"]
    ) -> List[Dict]:
        """
        Retrieve datasets using intelligent chunking for large view definitions.

        This method uses the DremioSmartChunker to handle large view definitions
        that would otherwise cause memory issues.

        Args:
            containers: List of Dremio containers to process

        Returns:
            List of dataset dictionaries with properly assembled view definitions
        """
        try:
            # Use smart chunker to create an optimal processing plan
            processing_plan = self.smart_chunker.create_processing_plan(containers)

            logger.info(
                f"Smart chunker created plan: {processing_plan.total_small_views} bulk views, "
                f"{processing_plan.total_large_views} large views requiring individual processing"
            )

            all_datasets = []

            # Process bulk views (small views that can be retrieved together)
            if processing_plan.total_small_views > 0:
                bulk_results = self._execute_bulk_view_query(containers)
                all_datasets.extend(bulk_results)

            # Process large views individually with chunking
            for large_view_id in processing_plan.large_view_ids:
                try:
                    # Parse schema and table name from view_id (format: schema.table)
                    schema_table = large_view_id.split(".", 1)
                    if len(schema_table) == 2:
                        table_schema, table_name = schema_table
                        chunked_result = (
                            self.smart_chunker.process_large_view_individually(
                                large_view_id, table_schema, table_name
                            )
                        )
                        if chunked_result:
                            all_datasets.append(chunked_result)
                    else:
                        logger.warning(f"Invalid view ID format: {large_view_id}")
                except Exception as e:
                    logger.warning(f"Failed to process large view {large_view_id}: {e}")
                    self.report.report_warning(
                        "large-view-processing",
                        f"Failed to process large view {large_view_id}",
                        exc=e,
                    )

            # Log processing statistics
            stats = self.smart_chunker.get_processing_stats()
            logger.info(f"Smart chunker processing stats: {stats}")

            return all_datasets

        except Exception as e:
            logger.error(f"Error in smart chunking workflow: {e}")
            if hasattr(self, "report"):
                self.report.report_failure(
                    "smart-chunking-error",
                    "Smart chunking workflow failed, falling back to original method",
                    exc=e,
                )
            # Fall back to original method
            logger.info("Falling back to original dataset retrieval method")
            return self._get_datasets_fallback(containers)

    def _execute_bulk_view_query(
        self, containers: List["DremioContainer"]
    ) -> List[Dict]:
        """Execute the bulk query for small views that don't need chunking."""
        if self.edition == DremioEdition.ENTERPRISE:
            query_template = DremioSQLQueries.QUERY_DATASETS_EE
        elif self.edition == DremioEdition.CLOUD:
            query_template = DremioSQLQueries.QUERY_DATASETS_CLOUD
        else:
            query_template = DremioSQLQueries.QUERY_DATASETS_CE

        all_results = []

        for schema in containers:
            try:
                # Use filter helper for consistent filtering
                if hasattr(self, "filter_helper") and self.filter_helper:
                    sql_filters = self.filter_helper.generate_sql_filters(
                        container_name=schema.container_name.lower()
                    )
                    formatted_query = query_template.format(
                        schema_pattern=sql_filters["schema_pattern"],
                        deny_schema_pattern=sql_filters["deny_schema_pattern"],
                        system_table_filter=sql_filters["system_table_filter"],
                        container_name=schema.container_name.lower(),
                    )
                else:
                    # Fallback to allow all if no filter helper
                    formatted_query = query_template.format(
                        schema_pattern="",
                        deny_schema_pattern="",
                        system_table_filter="",
                        container_name=schema.container_name.lower(),
                    )

                results = self.execute_query(query=formatted_query)
                all_results.extend(results)

            except DremioAPIException as e:
                logger.warning(
                    f"Error retrieving datasets from {schema.container_name}: {e}"
                )
                self.report.report_warning(
                    "bulk-query-error",
                    f"Error retrieving datasets from {schema.container_name}",
                    exc=e,
                )

        return all_results

    def _get_datasets_fallback(self, containers: List["DremioContainer"]) -> List[Dict]:
        """Fallback method using the original approach."""
        # This is the original get_all_tables_and_columns logic
        return self._execute_bulk_view_query(containers)

    def reassemble_view_definition_chunks(self, row: Dict[str, Any]) -> str:
        """
        Reassemble view definition chunks from Dremio query results using BaseModel.

        Args:
            row: Dictionary containing VIEW_DEFINITION_CHUNK_* columns

        Returns:
            Complete view definition string
        """
        view_def = DremioViewDefinition.from_chunks(row)

        if view_def.chunk_count > 1:
            logger.info(
                f"Reassembled view definition from {view_def.chunk_count} chunks "
                f"(total length: {view_def.total_length} chars)"
            )

        return view_def.definition

    def convert_raw_data_to_datasets(
        self, raw_data: List[Dict[str, Any]]
    ) -> List[DremioDatasetInfo]:
        """
        Convert raw Dremio API data to strongly-typed BaseModel objects.

        Args:
            raw_data: List of raw dictionaries from Dremio API

        Returns:
            List of DremioDatasetInfo objects with proper type safety
        """
        datasets = []

        # Group rows by table to collect columns
        table_groups = defaultdict(list)
        for row in raw_data:
            full_path = row.get("FULL_TABLE_PATH", "")
            if full_path:
                table_groups[full_path].append(row)

        for full_path, rows in table_groups.items():
            if not rows:
                continue

            # Use first row for table metadata
            table_row = rows[0]

            # Create column objects from all rows
            columns = []
            for row in rows:
                if row.get("COLUMN_NAME"):
                    try:
                        column = DremioColumnInfo(**row)
                        columns.append(column)
                    except Exception as e:
                        logger.warning(
                            f"Failed to create column info for {row.get('COLUMN_NAME')}: {e}"
                        )

            # Create dataset object
            try:
                dataset = DremioDatasetInfo.from_raw_data(table_row, columns)
                datasets.append(dataset)
            except Exception as e:
                logger.warning(f"Failed to create dataset info for {full_path}: {e}")

        return datasets

    def cleanup_smart_chunker(self) -> None:
        """Clean up smart chunker resources to free memory."""
        if hasattr(self, "smart_chunker"):
            # The smart chunker doesn't need explicit cleanup as it doesn't store state
            logger.debug("Smart chunker cleanup completed")
