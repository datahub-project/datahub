import logging
import re
import time
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin

from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from datahub.ingestion.source.flink.client import (
    _is_retryable,
    create_authenticated_session,
)
from datahub.ingestion.source.flink.config import FlinkConnectionConfig

logger = logging.getLogger(__name__)

_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_.\-]+$")


def _validate_identifier(name: str, kind: str) -> str:
    """Validate that a SQL identifier is safe for use in backtick-quoted queries."""
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(
            f"Unsafe {kind} name: '{name}'. "
            "Only alphanumeric characters, underscores, dots, and hyphens are allowed."
        )
    return name


class FlinkSQLGatewayClient:
    """Client for the Flink SQL Gateway REST API.

    Manages SQL sessions, executes statements (SHOW/DESCRIBE),
    and parses result rows for catalog metadata extraction.
    """

    def __init__(self, config: FlinkConnectionConfig) -> None:
        if not config.sql_gateway_url:
            raise ValueError("sql_gateway_url must be configured")

        self.base_url = config.sql_gateway_url
        self.timeout = config.timeout_seconds
        self.session = create_authenticated_session(config)
        self._session_handle: Optional[str] = None

        self._request = retry(  # type: ignore[method-assign]  # tenacity wraps method with compatible callable
            retry=retry_if_exception(_is_retryable),
            stop=stop_after_attempt(max(1, config.max_retries)),
            wait=wait_exponential(multiplier=1, min=1, max=10),
            reraise=True,
        )(self._request)

    def _request(
        self,
        method: str,
        endpoint: str,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = urljoin(f"{self.base_url}/", endpoint.lstrip("/"))
        response = self.session.request(
            method=method,
            url=url,
            json=json_body,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def open_session(self) -> str:
        if self._session_handle:
            return self._session_handle
        response = self._request("POST", "/v1/sessions", json_body={"properties": {}})
        self._session_handle = response["sessionHandle"]
        logger.info("Opened SQL Gateway session: %s", self._session_handle)
        return self._session_handle

    def close_session(self) -> None:
        if not self._session_handle:
            return
        try:
            self._request("DELETE", f"/v1/sessions/{self._session_handle}")
            logger.info("Closed SQL Gateway session: %s", self._session_handle)
        except Exception:
            logger.warning("Failed to close SQL Gateway session", exc_info=True)
        self._session_handle = None

    def execute_statement(self, sql: str) -> Iterator[List[Dict[str, Any]]]:
        """Execute a SQL statement and yield batches of result rows."""
        session_handle = self._session_handle or self.open_session()

        response = self._request(
            "POST",
            f"/v1/sessions/{session_handle}/statements",
            json_body={"statement": sql},
        )
        operation_handle = response["operationHandle"]

        result_token = 0
        while True:
            result_response = self._request(
                "GET",
                f"/v1/sessions/{session_handle}/operations/{operation_handle}/result/{result_token}",
            )

            if result_response.get("resultType") == "NOT_READY":
                self._wait_for_operation(session_handle, operation_handle)
                result_response = self._request(
                    "GET",
                    f"/v1/sessions/{session_handle}/operations/{operation_handle}/result/{result_token}",
                )

            data = result_response.get("results", {}).get("data", [])
            if data:
                yield data

            if not result_response.get("nextResultUri"):
                break
            result_token += 1

    def _wait_for_operation(self, session_handle: str, operation_handle: str) -> None:
        max_wait = 30.0
        waited = 0.0
        while waited < max_wait:
            time.sleep(0.5)
            waited += 0.5
            status_response = self._request(
                "GET",
                f"/v1/sessions/{session_handle}/operations/{operation_handle}/status",
            )
            status = status_response.get("status")
            if status == "FINISHED":
                return
            if status == "ERROR":
                raise RuntimeError(
                    f"SQL Gateway operation {operation_handle} failed: "
                    f"{status_response.get('error', 'unknown error')}"
                )
            if status == "CANCELED":
                raise RuntimeError(
                    f"SQL Gateway operation {operation_handle} was canceled"
                )
        raise TimeoutError(
            f"SQL Gateway operation {operation_handle} did not complete within {max_wait}s"
        )

    def _extract_field_values(
        self, batches: Iterator[List[Dict[str, Any]]]
    ) -> List[str]:
        """Extract first field from each row across all batches."""
        values = []
        for batch in batches:
            for row in batch:
                if isinstance(row, dict) and "fields" in row and row["fields"]:
                    values.append(str(row["fields"][0]))
        return values

    def get_catalogs(self) -> List[str]:
        return self._extract_field_values(self.execute_statement("SHOW CATALOGS"))

    def get_catalog_info(self, catalog: str) -> Dict[str, str]:
        """Execute DESCRIBE CATALOG EXTENDED to get catalog type and properties.

        Returns a dict with at minimum 'type' key (e.g., 'jdbc', 'hive', 'iceberg',
        'paimon', 'generic_in_memory'). Extended properties are returned as
        'option:<key>' entries (e.g., 'option:base-url' for JDBC catalogs).
        """
        _validate_identifier(catalog, "catalog")
        info: Dict[str, str] = {}
        for batch in self.execute_statement(f"DESC CATALOG EXTENDED `{catalog}`"):
            for row in batch:
                if (
                    isinstance(row, dict)
                    and "fields" in row
                    and len(row["fields"]) >= 2
                ):
                    key = str(row["fields"][0]).strip()
                    value = str(row["fields"][1]).strip()
                    if key and value:
                        info[key] = value
        return info

    def get_table_connector(
        self, catalog: str, database: str, table: str
    ) -> Dict[str, str]:
        """Execute SHOW CREATE TABLE to extract connector properties from the WITH clause.

        Returns a dict of connector properties (e.g., {'connector': 'kafka', 'topic': 'orders',
        'properties.bootstrap.servers': 'broker:9092'}). Returns empty dict if DDL cannot be
        parsed (e.g., for JdbcCatalog auto-discovered tables that have no Flink DDL).
        """
        _validate_identifier(catalog, "catalog")
        _validate_identifier(database, "database")
        _validate_identifier(table, "table")
        full_name = f"`{catalog}`.`{database}`.`{table}`"
        try:
            ddl_lines: List[str] = []
            for batch in self.execute_statement(f"SHOW CREATE TABLE {full_name}"):
                for row in batch:
                    if isinstance(row, dict) and "fields" in row and row["fields"]:
                        ddl_lines.append(str(row["fields"][0]))
            ddl = "\n".join(ddl_lines)
            return self._parse_with_properties(ddl)
        except Exception:
            logger.debug(
                "Could not get CREATE TABLE DDL for %s (may be auto-discovered table)",
                full_name,
                exc_info=True,
            )
            return {}

    @staticmethod
    def _parse_with_properties(ddl: str) -> Dict[str, str]:
        """Parse the WITH (...) clause from a CREATE TABLE DDL statement."""
        with_match = re.search(r"\bWITH\s*\((.*)\)\s*$", ddl, re.DOTALL | re.IGNORECASE)
        if not with_match:
            return {}
        with_content = with_match.group(1)
        props: Dict[str, str] = {}
        for pair_match in re.finditer(r"'([^']+)'\s*=\s*'([^']*)'", with_content):
            props[pair_match.group(1)] = pair_match.group(2)
        return props

    def test_connection(self) -> Optional[Exception]:
        """Returns None on success, or the exception on failure."""
        try:
            self.open_session()
            list(self.execute_statement("SHOW CATALOGS"))
            return None
        except Exception as e:
            logger.warning("SQL Gateway connection test failed", exc_info=True)
            return e
        finally:
            self.close_session()

    def close(self) -> None:
        self.close_session()
        self.session.close()
