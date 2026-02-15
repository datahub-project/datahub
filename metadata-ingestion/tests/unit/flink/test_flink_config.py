import pytest

from datahub.ingestion.source.flink.flink_config import FlinkSourceConfig


def test_minimal_config():
    config = FlinkSourceConfig.model_validate(
        {"rest_endpoint": "http://localhost:8081"}
    )
    assert config.rest_endpoint == "http://localhost:8081"
    assert config.env == "PROD"
    assert config.platform_instance is None
    assert config.include_lineage is True
    assert config.include_run_history is True
    assert config.max_workers == 10


def test_trailing_slash_stripped():
    config = FlinkSourceConfig.model_validate(
        {"rest_endpoint": "http://localhost:8081/"}
    )
    assert config.rest_endpoint == "http://localhost:8081"


def test_bearer_token_auth():
    config = FlinkSourceConfig.model_validate(
        {"rest_endpoint": "http://localhost:8081", "token": "my-secret-token"}
    )
    assert config.token is not None
    assert config.token.get_secret_value() == "my-secret-token"


def test_basic_auth():
    config = FlinkSourceConfig.model_validate(
        {
            "rest_endpoint": "http://localhost:8081",
            "username": "admin",
            "password": "secret",
        }
    )
    assert config.username == "admin"
    assert config.password is not None


def test_mixed_auth_rejected():
    with pytest.raises(ValueError, match="Cannot specify both"):
        FlinkSourceConfig.model_validate(
            {
                "rest_endpoint": "http://localhost:8081",
                "token": "my-token",
                "username": "admin",
                "password": "secret",
            }
        )


def test_incomplete_basic_auth_rejected():
    with pytest.raises(ValueError, match="Both 'username' and 'password'"):
        FlinkSourceConfig.model_validate(
            {
                "rest_endpoint": "http://localhost:8081",
                "username": "admin",
            }
        )


def test_invalid_job_state_rejected():
    with pytest.raises(ValueError, match="Invalid Flink job state"):
        FlinkSourceConfig.model_validate(
            {
                "rest_endpoint": "http://localhost:8081",
                "include_job_states": ["RUNNING", "INVALID_STATE"],
            }
        )


def test_job_states_normalized_to_uppercase():
    config = FlinkSourceConfig.model_validate(
        {
            "rest_endpoint": "http://localhost:8081",
            "include_job_states": ["running", "finished"],
        }
    )
    assert config.include_job_states == ["RUNNING", "FINISHED"]


def test_cluster_property_with_platform_instance():
    config = FlinkSourceConfig.model_validate(
        {
            "rest_endpoint": "http://localhost:8081",
            "platform_instance": "my-cluster",
        }
    )
    assert config.cluster == "my-cluster"


def test_cluster_property_fallback_to_env():
    config = FlinkSourceConfig.model_validate(
        {"rest_endpoint": "http://localhost:8081"}
    )
    assert config.cluster == "PROD"


def test_kafka_dataset_env_uses_kafka_env():
    config = FlinkSourceConfig.model_validate(
        {
            "rest_endpoint": "http://localhost:8081",
            "env": "PROD",
            "kafka_env": "DEV",
        }
    )
    assert config.kafka_dataset_env == "DEV"


def test_kafka_dataset_env_fallback_to_env():
    config = FlinkSourceConfig.model_validate(
        {"rest_endpoint": "http://localhost:8081", "env": "PROD"}
    )
    assert config.kafka_dataset_env == "PROD"


def test_job_name_pattern():
    config = FlinkSourceConfig.model_validate(
        {
            "rest_endpoint": "http://localhost:8081",
            "job_name_pattern": {"allow": ["^fraud.*"], "deny": [".*test.*"]},
        }
    )
    assert config.job_name_pattern.allowed("fraud_detection")
    assert not config.job_name_pattern.allowed("test_job")
