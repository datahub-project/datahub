from unittest.mock import Mock, patch

import pytest
import requests
from requests.auth import HTTPBasicAuth

from datahub.ingestion.source.flink.client import (
    FlinkClusterConfig,
    FlinkJobDetail,
    FlinkJobSummary,
    FlinkRestClient,
    _is_retryable,
)
from datahub.ingestion.source.flink.config import FlinkConnectionConfig


class TestIsRetryable:
    def test_retries_on_500(self) -> None:
        response = Mock(status_code=500)
        exc = requests.exceptions.HTTPError(response=response)
        assert _is_retryable(exc) is True

    def test_retries_on_503(self) -> None:
        response = Mock(status_code=503)
        exc = requests.exceptions.HTTPError(response=response)
        assert _is_retryable(exc) is True

    def test_no_retry_on_404(self) -> None:
        response = Mock(status_code=404)
        exc = requests.exceptions.HTTPError(response=response)
        assert _is_retryable(exc) is False

    def test_no_retry_on_401(self) -> None:
        response = Mock(status_code=401)
        exc = requests.exceptions.HTTPError(response=response)
        assert _is_retryable(exc) is False

    def test_no_retry_on_none_response(self) -> None:
        exc = requests.exceptions.HTTPError(response=None)
        assert _is_retryable(exc) is False

    def test_retries_on_connection_error(self) -> None:
        assert _is_retryable(requests.exceptions.ConnectionError()) is True

    def test_retries_on_timeout(self) -> None:
        assert _is_retryable(requests.exceptions.Timeout()) is True

    def test_no_retry_on_unknown_exception(self) -> None:
        assert _is_retryable(ValueError("something")) is False


@pytest.fixture
def connection() -> FlinkConnectionConfig:
    return FlinkConnectionConfig(rest_api_url="http://localhost:8081")


@pytest.fixture
def client(connection: FlinkConnectionConfig) -> FlinkRestClient:
    return FlinkRestClient(connection)


class TestAuthSetup:
    def test_bearer_token_sets_header(self):
        conn = FlinkConnectionConfig(
            rest_api_url="http://localhost:8081", token="my-token"
        )
        c = FlinkRestClient(conn)
        assert c.session.headers.get("Authorization") == "Bearer my-token"

    def test_basic_auth_sets_session_auth(self):
        conn = FlinkConnectionConfig(
            rest_api_url="http://localhost:8081",
            username="admin",
            password="secret",
        )
        c = FlinkRestClient(conn)
        assert isinstance(c.session.auth, HTTPBasicAuth)
        assert c.session.auth.username == "admin"
        assert c.session.auth.password == "secret"


class TestGetClusterConfig:
    def test_maps_api_response_to_dataclass(self, client: FlinkRestClient) -> None:
        mock_api_response = {
            "flink-version": "1.19.0",
            "flink-revision": "abc123",
            "timezone-name": "Coordinated Universal Time",
            "timezone-offset": "+00:00",
        }
        with patch.object(client, "_get", return_value=mock_api_response):
            result = client.get_cluster_config()
            assert isinstance(result, FlinkClusterConfig)
            assert result.flink_version == "1.19.0"
            assert result.timezone == "Coordinated Universal Time"

    def test_missing_version_defaults_to_unknown(self, client: FlinkRestClient) -> None:
        with patch.object(client, "_get", return_value={}):
            result = client.get_cluster_config()
            assert result.flink_version == "unknown"


class TestGetJobsOverview:
    def test_maps_response_with_last_modification_fallback(
        self, client: FlinkRestClient
    ) -> None:
        mock_api_response = {
            "jobs": [
                {
                    "jid": "abc123",
                    "name": "fraud_detection",
                    "state": "RUNNING",
                    "start-time": 1707676800000,
                    "end-time": -1,
                    "duration": 3600000,
                    "last-modification": 1707676800000,
                },
                {
                    "jid": "def456",
                    "name": "etl_pipeline",
                    "state": "FINISHED",
                    "start-time": 1707590400000,
                    "end-time": 1707594000000,
                    "duration": 3600000,
                    # no last-modification — tests fallback to start-time
                },
            ]
        }
        with patch.object(client, "_get", return_value=mock_api_response):
            result = client.get_jobs_overview()
            assert len(result) == 2
            assert isinstance(result[0], FlinkJobSummary)
            assert result[0].start_time == 1707676800000
            assert result[0].end_time == -1
            # Second job missing last-modification — falls back to start-time
            assert result[1].last_modification == 1707590400000


class TestGetJobDetails:
    def test_maps_response_with_nested_plan_nodes(
        self, client: FlinkRestClient
    ) -> None:
        mock_api_response = {
            "jid": "abc123",
            "name": "fraud_detection",
            "state": "RUNNING",
            "start-time": 1707676800000,
            "end-time": -1,
            "duration": 3600000,
            "job-type": "STREAMING",
            "maxParallelism": 128,
            "plan": {
                "nodes": [
                    {
                        "id": "1",
                        "description": "Source: KafkaSource-transactions -> Filter",
                        "operator": "KafkaSource",
                        "parallelism": 4,
                    },
                    {
                        "id": "2",
                        "description": "Sink: KafkaSink-alerts",
                        "operator": "KafkaSink",
                        "parallelism": 2,
                    },
                ]
            },
        }
        with patch.object(client, "_get", return_value=mock_api_response):
            result = client.get_job_details("abc123")
            assert isinstance(result, FlinkJobDetail)
            assert result.job_type == "STREAMING"
            assert result.max_parallelism == 128
            assert result.start_time == 1707676800000
            assert len(result.plan_nodes) == 2
            assert "KafkaSource-transactions" in result.plan_nodes[0].description


class TestGetCheckpointConfig:
    def test_returns_none_on_404(self, client: FlinkRestClient) -> None:
        response = Mock(status_code=404)
        with patch.object(
            client,
            "_get",
            side_effect=requests.exceptions.HTTPError(response=response),
        ):
            assert client.get_checkpoint_config("abc123") is None

    def test_raises_on_non_404_error(self, client: FlinkRestClient) -> None:
        response = Mock(status_code=500)
        with (
            patch.object(
                client,
                "_get",
                side_effect=requests.exceptions.HTTPError(response=response),
            ),
            pytest.raises(requests.exceptions.HTTPError),
        ):
            client.get_checkpoint_config("abc123")
