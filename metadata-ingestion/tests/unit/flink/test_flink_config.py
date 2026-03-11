import pytest

from datahub.ingestion.source.flink.config import (
    FlinkConnectionConfig,
    FlinkSourceConfig,
)


class TestFlinkConnectionConfig:
    def test_trailing_slash_stripped_from_urls(self) -> None:
        conn = FlinkConnectionConfig(
            rest_api_url="http://localhost:8081/",
            sql_gateway_url="http://localhost:8083/",
        )
        assert conn.rest_api_url == "http://localhost:8081"
        assert conn.sql_gateway_url == "http://localhost:8083"

    def test_token_and_basic_auth_rejected(self):
        with pytest.raises(ValueError, match="Cannot specify both"):
            FlinkConnectionConfig(
                rest_api_url="http://localhost:8081",
                token="my-token",
                username="admin",
                password="secret",
            )

    def test_username_only_rejected(self):
        with pytest.raises(ValueError, match="Both.*must be provided"):
            FlinkConnectionConfig(
                rest_api_url="http://localhost:8081",
                username="admin",
            )

    def test_password_only_rejected(self):
        with pytest.raises(ValueError, match="Both.*must be provided"):
            FlinkConnectionConfig(
                rest_api_url="http://localhost:8081",
                password="secret",
            )


class TestFlinkSourceConfig:
    def test_invalid_job_state_rejected(self):
        with pytest.raises(ValueError, match="Invalid Flink job state"):
            FlinkSourceConfig.model_validate(
                {
                    "connection": {"rest_api_url": "http://localhost:8081"},
                    "include_job_states": ["RUNNING", "BANANA"],
                }
            )

    def test_catalog_requires_sql_gateway(self):
        with pytest.raises(ValueError, match="sql_gateway_url must be configured"):
            FlinkSourceConfig.model_validate(
                {
                    "connection": {"rest_api_url": "http://localhost:8081"},
                    "include_catalog_metadata": True,
                }
            )

    def test_empty_job_states_rejected(self):
        with pytest.raises(ValueError, match="must not be empty"):
            FlinkSourceConfig.model_validate(
                {
                    "connection": {"rest_api_url": "http://localhost:8081"},
                    "include_job_states": [],
                }
            )

    def test_job_states_normalized_to_uppercase(self) -> None:
        config = FlinkSourceConfig.model_validate(
            {
                "connection": {"rest_api_url": "http://localhost:8081"},
                "include_job_states": ["running", "Failed"],
            }
        )
        assert config.include_job_states == ["RUNNING", "FAILED"]
