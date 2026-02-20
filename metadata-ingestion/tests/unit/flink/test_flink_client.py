from unittest.mock import MagicMock, patch

import pytest
import requests

from datahub.ingestion.source.flink.flink_client import (
    FlinkCheckpointConfig,
    FlinkClusterConfig,
    FlinkJobDetail,
    FlinkJobSummary,
    FlinkRestClient,
    _is_retryable,
)
from datahub.ingestion.source.flink.flink_config import FlinkSourceConfig


@pytest.fixture
def config() -> FlinkSourceConfig:
    return FlinkSourceConfig.model_validate({"rest_endpoint": "http://localhost:8081"})


@pytest.fixture
def client(config: FlinkSourceConfig) -> FlinkRestClient:
    return FlinkRestClient(config)


class TestRetryLogic:
    def test_500_is_retryable(self):
        response = MagicMock()
        response.status_code = 503
        exc = requests.exceptions.HTTPError(response=response)
        assert _is_retryable(exc) is True

    def test_401_is_not_retryable(self):
        response = MagicMock()
        response.status_code = 401
        exc = requests.exceptions.HTTPError(response=response)
        assert _is_retryable(exc) is False

    def test_connection_error_is_retryable(self):
        exc = requests.exceptions.ConnectionError()
        assert _is_retryable(exc) is True

    def test_timeout_is_retryable(self):
        exc = requests.exceptions.Timeout()
        assert _is_retryable(exc) is True

    def test_value_error_is_not_retryable(self):
        assert _is_retryable(ValueError("bad")) is False


class TestGetClusterConfig:
    def test_parses_response(self, client: FlinkRestClient):
        with patch.object(
            client,
            "_get",
            return_value={
                "flink-version": "1.20.0",
                "timezone": "UTC",
            },
        ):
            result = client.get_cluster_config()
            assert isinstance(result, FlinkClusterConfig)
            assert result.flink_version == "1.20.0"
            assert result.timezone == "UTC"

    def test_defaults_version(self, client: FlinkRestClient):
        with patch.object(client, "_get", return_value={}):
            result = client.get_cluster_config()
            assert result.flink_version == "unknown"


class TestGetJobsOverview:
    def test_parses_multiple_jobs(self, client: FlinkRestClient):
        with patch.object(
            client,
            "_get",
            return_value={
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
                    },
                ]
            },
        ):
            result = client.get_jobs_overview()
            assert len(result) == 2
            assert isinstance(result[0], FlinkJobSummary)
            assert result[0].jid == "abc123"
            assert result[0].name == "fraud_detection"
            assert result[1].state == "FINISHED"

    def test_empty_jobs(self, client: FlinkRestClient):
        with patch.object(client, "_get", return_value={"jobs": []}):
            result = client.get_jobs_overview()
            assert result == []


class TestGetJobDetails:
    def test_parses_plan_nodes(self, client: FlinkRestClient):
        with patch.object(
            client,
            "_get",
            return_value={
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
                            "description": "Source: KafkaSource-transactions -> Map",
                            "operator": "KafkaSource",
                            "parallelism": 4,
                            "inputs": [],
                        },
                        {
                            "id": "2",
                            "description": "Sink: KafkaSink-alerts",
                            "operator": "KafkaSink",
                            "parallelism": 2,
                        },
                    ]
                },
            },
        ):
            result = client.get_job_details("abc123")
            assert isinstance(result, FlinkJobDetail)
            assert result.jid == "abc123"
            assert result.job_type == "STREAMING"
            assert result.max_parallelism == 128
            assert len(result.plan_nodes) == 2
            assert (
                result.plan_nodes[0].description
                == "Source: KafkaSource-transactions -> Map"
            )


class TestGetCheckpointConfig:
    def test_parses_config(self, client: FlinkRestClient):
        with patch.object(
            client,
            "_get",
            return_value={
                "mode": "exactly_once",
                "interval": 60000,
                "timeout": 120000,
                "state_backend": "rocksdb",
                "checkpoint_storage": "filesystem",
            },
        ):
            result = client.get_checkpoint_config("abc123")
            assert isinstance(result, FlinkCheckpointConfig)
            assert result.mode == "exactly_once"
            assert result.interval == 60000
            assert result.state_backend == "rocksdb"

    def test_returns_none_on_404(self, client: FlinkRestClient):
        response = MagicMock()
        response.status_code = 404
        with patch.object(
            client,
            "_get",
            side_effect=requests.exceptions.HTTPError(response=response),
        ):
            result = client.get_checkpoint_config("abc123")
            assert result is None

    def test_raises_on_500(self, client: FlinkRestClient):
        response = MagicMock()
        response.status_code = 500
        with (
            patch.object(
                client,
                "_get",
                side_effect=requests.exceptions.HTTPError(response=response),
            ),
            pytest.raises(requests.exceptions.HTTPError),
        ):
            client.get_checkpoint_config("abc123")


class TestAuthSetup:
    def test_bearer_token_header(self):
        config = FlinkSourceConfig.model_validate(
            {"rest_endpoint": "http://localhost:8081", "token": "my-token"}
        )
        client = FlinkRestClient(config)
        assert client.session.headers.get("Authorization") == "Bearer my-token"

    def test_basic_auth(self):
        config = FlinkSourceConfig.model_validate(
            {
                "rest_endpoint": "http://localhost:8081",
                "username": "admin",
                "password": "secret",
            }
        )
        client = FlinkRestClient(config)
        assert client.session.auth is not None

    def test_no_auth(self):
        config = FlinkSourceConfig.model_validate(
            {"rest_endpoint": "http://localhost:8081"}
        )
        client = FlinkRestClient(config)
        assert "Authorization" not in client.session.headers
        assert client.session.auth is None
