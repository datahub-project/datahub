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

    def test_rejects_semicolon(self) -> None:
        with pytest.raises(ValueError, match="Unsafe"):
            _validate_identifier("table; SELECT 1", "table")

    def test_rejects_spaces(self) -> None:
        with pytest.raises(ValueError, match="Unsafe"):
            _validate_identifier("my table", "table")

    def test_rejects_empty_string(self) -> None:
        with pytest.raises(ValueError, match="Unsafe"):
            _validate_identifier("", "table")


class TestOpenSession:
    def test_reuses_existing_session(self) -> None:
        client = FlinkSQLGatewayClient(_connection())
        client._session_handle = "existing-handle"
        result = client.open_session()
        assert result == "existing-handle"


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


class TestGetTableSchema:
    def test_parses_describe_output(self) -> None:
        client = FlinkSQLGatewayClient(_connection())
        mock_describe_response = [
            [
                {"kind": "INSERT", "fields": ["order_id", "INT", "true", None]},
                {
                    "kind": "INSERT",
                    "fields": ["amount", "DECIMAL(10,2)", "true", "Order amount"],
                },
                {
                    "kind": "INSERT",
                    "fields": ["created_at", "TIMESTAMP(3)", "false", None],
                },
            ]
        ]
        with patch.object(
            client, "execute_statement", return_value=iter(mock_describe_response)
        ):
            columns = client.get_table_schema("cat", "db", "orders")

        assert len(columns) == 3
        assert columns[0].name == "order_id"
        assert columns[0].type == "INT"
        assert columns[0].nullable is True
        assert columns[1].comment == "Order amount"
        assert columns[2].nullable is False


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
