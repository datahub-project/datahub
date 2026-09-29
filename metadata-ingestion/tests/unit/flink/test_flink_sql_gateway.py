from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.flink.config import FlinkConnectionConfig
from datahub.ingestion.source.flink.sql_gateway_client import (
    FlinkSQLGatewayClient,
    _validate_identifier,
)


def _connection() -> FlinkConnectionConfig:
    return FlinkConnectionConfig(
        rest_api_url="http://localhost:8081",
        sql_gateway_url="http://localhost:8083",
    )


class TestValidateIdentifier:
    def test_accepts_simple_name(self) -> None:
        assert _validate_identifier("my_table", "table") == "my_table"

    def test_accepts_dots_and_hyphens(self) -> None:
        assert _validate_identifier("my-catalog.db_1", "catalog") == "my-catalog.db_1"

    def test_rejects_backtick_injection(self) -> None:
        with pytest.raises(ValueError, match="Unsafe"):
            _validate_identifier("test`; DROP TABLE x;--", "table")

    def test_rejects_spaces(self) -> None:
        with pytest.raises(ValueError, match="Unsafe"):
            _validate_identifier("my table", "table")

    def test_rejects_empty_string(self) -> None:
        with pytest.raises(ValueError, match="Unsafe"):
            _validate_identifier("", "table")


class TestExtractFieldValues:
    def test_parses_row_format(self) -> None:
        client = FlinkSQLGatewayClient(_connection())
        batches = iter(
            [
                [
                    {"kind": "INSERT", "fields": ["default_catalog"]},
                    {"kind": "INSERT", "fields": ["hive_catalog"]},
                ]
            ]
        )
        result = client._extract_field_values(batches)
        assert result == ["default_catalog", "hive_catalog"]


class TestWaitForOperation:
    def test_raises_timeout_when_operation_never_finishes(self) -> None:
        """_wait_for_operation raises TimeoutError after max_wait."""
        client = FlinkSQLGatewayClient(_connection())
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "RUNNING"}
        mock_response.raise_for_status.return_value = None
        client.session = MagicMock()
        client.session.request.return_value = mock_response

        with (
            patch("datahub.ingestion.source.flink.sql_gateway_client.time.sleep"),
            pytest.raises(TimeoutError, match="did not complete"),
        ):
            client._wait_for_operation("session-1", "op-1")

    def test_raises_on_error_status(self) -> None:
        """_wait_for_operation raises RuntimeError when operation fails."""
        client = FlinkSQLGatewayClient(_connection())
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "ERROR",
            "error": "SQL syntax error",
        }
        mock_response.raise_for_status.return_value = None
        client.session = MagicMock()
        client.session.request.return_value = mock_response

        with (
            patch("datahub.ingestion.source.flink.sql_gateway_client.time.sleep"),
            pytest.raises(RuntimeError, match="failed"),
        ):
            client._wait_for_operation("session-1", "op-1")

    def test_raises_on_canceled_status(self) -> None:
        """_wait_for_operation raises RuntimeError when operation is canceled."""
        client = FlinkSQLGatewayClient(_connection())
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "CANCELED"}
        mock_response.raise_for_status.return_value = None
        client.session = MagicMock()
        client.session.request.return_value = mock_response

        with (
            patch("datahub.ingestion.source.flink.sql_gateway_client.time.sleep"),
            pytest.raises(RuntimeError, match="canceled"),
        ):
            client._wait_for_operation("session-1", "op-1")


class TestExecuteStatementNotReady:
    def test_waits_then_refetches_on_not_ready(self) -> None:
        """NOT_READY result triggers wait, then re-fetches the same token."""
        client = FlinkSQLGatewayClient(_connection())
        client._session_handle = "session-1"

        submit_response = {"operationHandle": "op-1"}
        not_ready_response = {"resultType": "NOT_READY", "results": {"data": []}}
        data_response = {
            "resultType": "PAYLOAD",
            "results": {
                "data": [{"kind": "INSERT", "fields": ["value1"]}],
            },
        }

        call_count = {"n": 0}

        def mock_request(method: str, url: str, **kwargs: object) -> MagicMock:
            resp = MagicMock()
            resp.raise_for_status.return_value = None
            if method == "POST" and "statements" in url:
                resp.json.return_value = submit_response
            elif method == "GET" and "result/0" in url:
                call_count["n"] += 1
                if call_count["n"] == 1:
                    resp.json.return_value = not_ready_response
                else:
                    resp.json.return_value = data_response
            elif method == "GET" and "status" in url:
                resp.json.return_value = {"status": "FINISHED"}
            else:
                resp.json.return_value = {}
            return resp

        client.session = MagicMock()
        client.session.request.side_effect = mock_request

        with patch("datahub.ingestion.source.flink.sql_gateway_client.time.sleep"):
            batches = list(client.execute_statement("SHOW CATALOGS"))

        assert len(batches) == 1
        assert batches[0][0]["fields"] == ["value1"]


class TestGetCatalogs:
    def test_returns_catalog_names(self) -> None:
        client = FlinkSQLGatewayClient(_connection())
        client._session_handle = "session-1"
        client.session = MagicMock()

        def mock_request(method: str, url: str, **kwargs: object) -> MagicMock:
            resp = MagicMock()
            resp.raise_for_status.return_value = None
            if method == "POST":
                resp.json.return_value = {"operationHandle": "op-1"}
            else:
                resp.json.return_value = {
                    "resultType": "PAYLOAD",
                    "results": {
                        "data": [
                            {"fields": ["default_catalog"]},
                            {"fields": ["hive_catalog"]},
                        ]
                    },
                }
            return resp

        client.session.request.side_effect = mock_request
        result = client.get_catalogs()
        assert result == ["default_catalog", "hive_catalog"]

    def test_returns_empty_list_when_no_catalogs(self) -> None:
        client = FlinkSQLGatewayClient(_connection())
        client._session_handle = "session-1"
        client.session = MagicMock()

        def mock_request(method: str, url: str, **kwargs: object) -> MagicMock:
            resp = MagicMock()
            resp.raise_for_status.return_value = None
            if method == "POST":
                resp.json.return_value = {"operationHandle": "op-1"}
            else:
                resp.json.return_value = {
                    "resultType": "PAYLOAD",
                    "results": {"data": []},
                }
            return resp

        client.session.request.side_effect = mock_request
        result = client.get_catalogs()
        assert result == []


class TestGetCatalogInfo:
    def _make_client_with_rows(self, rows: list) -> FlinkSQLGatewayClient:
        client = FlinkSQLGatewayClient(_connection())
        client._session_handle = "session-1"
        client.session = MagicMock()

        def mock_request(method: str, url: str, **kwargs: object) -> MagicMock:
            resp = MagicMock()
            resp.raise_for_status.return_value = None
            if method == "POST":
                resp.json.return_value = {"operationHandle": "op-1"}
            else:
                resp.json.return_value = {
                    "resultType": "PAYLOAD",
                    "results": {"data": rows},
                }
            return resp

        client.session.request.side_effect = mock_request
        return client

    def test_parses_key_value_rows(self) -> None:
        client = self._make_client_with_rows(
            [
                {"fields": ["type", "hive"]},
                {"fields": ["option:hive.metastore.uris", "thrift://localhost:9083"]},
                {"fields": ["option:hive.metastore.warehouse.dir", "/warehouse"]},
            ]
        )
        result = client.get_catalog_info("hive_catalog")
        assert result["type"] == "hive"
        assert result["option:hive.metastore.uris"] == "thrift://localhost:9083"

    def test_filters_out_empty_key_or_value(self) -> None:
        client = self._make_client_with_rows(
            [
                {"fields": ["type", "jdbc"]},
                {"fields": ["", "should_be_ignored"]},
                {"fields": ["option:base-url", ""]},
                {"fields": ["option:base-url", "jdbc:postgresql://localhost"]},
            ]
        )
        result = client.get_catalog_info("pg_catalog")
        assert "type" in result
        assert "" not in result
        # empty-value row is skipped; non-empty row with same key wins
        assert result.get("option:base-url") == "jdbc:postgresql://localhost"

    def test_returns_empty_dict_when_no_rows(self) -> None:
        client = self._make_client_with_rows([])
        result = client.get_catalog_info("empty_catalog")
        assert result == {}


class TestGetTableConnector:
    def _make_client(self, ddl: str) -> FlinkSQLGatewayClient:
        client = FlinkSQLGatewayClient(_connection())
        client._session_handle = "session-1"
        client.session = MagicMock()

        def mock_request(method: str, url: str, **kwargs: object) -> MagicMock:
            resp = MagicMock()
            resp.raise_for_status.return_value = None
            if method == "POST":
                resp.json.return_value = {"operationHandle": "op-1"}
            else:
                resp.json.return_value = {
                    "resultType": "PAYLOAD",
                    "results": {"data": [{"fields": [ddl]}]},
                }
            return resp

        client.session.request.side_effect = mock_request
        return client

    def test_extracts_connector_from_ddl(self) -> None:
        ddl = (
            "CREATE TABLE `cat`.`db`.`orders` (id INT) WITH (\n"
            "  'connector' = 'kafka',\n"
            "  'topic' = 'orders'\n"
            ")"
        )
        client = self._make_client(ddl)
        result = client.get_table_connector("cat", "db", "orders")
        assert result["connector"] == "kafka"
        assert result["topic"] == "orders"

    def test_returns_empty_dict_on_exception(self) -> None:
        """Auto-discovered JDBC tables have no DDL — exception returns {} silently."""
        client = FlinkSQLGatewayClient(_connection())
        client._session_handle = "session-1"
        client.session = MagicMock()
        client.session.request.side_effect = RuntimeError(
            "no DDL for auto-discovered table"
        )
        result = client.get_table_connector("cat", "db", "orders")
        assert result == {}

    def test_returns_empty_dict_when_no_with_clause(self) -> None:
        client = self._make_client("CREATE TABLE orders (id INT)")
        result = client.get_table_connector("cat", "db", "orders")
        assert result == {}


class TestParseWithProperties:
    def test_normal_with_clause(self) -> None:
        ddl = (
            "CREATE TABLE orders (\n"
            "  order_id INT,\n"
            "  amount DOUBLE\n"
            ") WITH (\n"
            "  'connector' = 'kafka',\n"
            "  'topic' = 'orders',\n"
            "  'properties.bootstrap.servers' = 'broker:9092'\n"
            ")"
        )
        result = FlinkSQLGatewayClient._parse_with_properties(ddl)
        assert result == {
            "connector": "kafka",
            "topic": "orders",
            "properties.bootstrap.servers": "broker:9092",
        }

    def test_no_with_clause(self) -> None:
        ddl = "CREATE TABLE orders (order_id INT, amount DOUBLE)"
        result = FlinkSQLGatewayClient._parse_with_properties(ddl)
        assert result == {}

    def test_empty_with_clause(self) -> None:
        ddl = "CREATE TABLE orders (order_id INT) WITH ()"
        result = FlinkSQLGatewayClient._parse_with_properties(ddl)
        assert result == {}

    def test_multiline_ddl(self) -> None:
        """Realistic DDL as returned by SHOW CREATE TABLE."""
        ddl = (
            "CREATE TABLE `default_catalog`.`default_database`.`orders` (\n"
            "  `order_id` INT NOT NULL,\n"
            "  `customer_id` BIGINT,\n"
            "  `amount` DECIMAL(10, 2),\n"
            "  `order_time` TIMESTAMP(3),\n"
            "  WATERMARK FOR `order_time` AS `order_time` - INTERVAL '5' SECOND\n"
            ") WITH (\n"
            "  'connector' = 'kafka',\n"
            "  'topic' = 'orders',\n"
            "  'properties.bootstrap.servers' = 'broker:9092',\n"
            "  'format' = 'json',\n"
            "  'scan.startup.mode' = 'earliest-offset'\n"
            ")"
        )
        result = FlinkSQLGatewayClient._parse_with_properties(ddl)
        assert result["connector"] == "kafka"
        assert result["topic"] == "orders"
        assert result["format"] == "json"
        assert result["scan.startup.mode"] == "earliest-offset"
        assert result["properties.bootstrap.servers"] == "broker:9092"
