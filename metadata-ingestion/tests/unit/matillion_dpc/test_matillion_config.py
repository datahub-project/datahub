import pytest
from pydantic import SecretStr, ValidationError

from datahub.ingestion.source.matillion_dpc.config import (
    MatillionAPIConfig,
    MatillionRegion,
    MatillionSourceConfig,
)


def test_api_config_defaults() -> None:
    config = MatillionAPIConfig(api_token=SecretStr("test_token"))
    assert config.region == MatillionRegion.EU1
    assert config.get_base_url() == "https://eu1.api.matillion.com/dpc"
    assert config.request_timeout_sec == 30
    assert config.custom_base_url is None


@pytest.mark.parametrize(
    "region,expected_url",
    [
        pytest.param(
            MatillionRegion.EU1,
            "https://eu1.api.matillion.com/dpc",
            id="eu1_region",
        ),
        pytest.param(
            MatillionRegion.US1,
            "https://us1.api.matillion.com/dpc",
            id="us1_region",
        ),
    ],
)
def test_api_config_regions(region: MatillionRegion, expected_url: str) -> None:
    config = MatillionAPIConfig(
        api_token=SecretStr("test_token"),
        region=region,
    )
    assert config.region == region
    assert config.get_base_url() == expected_url


@pytest.mark.parametrize(
    "custom_url,region,expected_result",
    [
        pytest.param(
            "https://custom.matillion.com/api",
            MatillionRegion.EU1,
            "https://custom.matillion.com/api",
            id="custom_url_overrides_eu1",
        ),
        pytest.param(
            "https://custom.matillion.com/api",
            MatillionRegion.US1,
            "https://custom.matillion.com/api",
            id="custom_url_overrides_us1",
        ),
        pytest.param(
            "http://localhost:8080/api",
            MatillionRegion.EU1,
            "http://localhost:8080/api",
            id="custom_url_localhost",
        ),
        pytest.param(
            "https://custom.com/",
            MatillionRegion.EU1,
            "https://custom.com",
            id="custom_url_strips_trailing_slash",
        ),
    ],
)
def test_api_config_custom_base_url(
    custom_url: str,
    region: MatillionRegion,
    expected_result: str,
) -> None:
    config = MatillionAPIConfig(
        api_token=SecretStr("test_token"),
        region=region,
        custom_base_url=custom_url,
    )
    assert config.get_base_url() == expected_result


@pytest.mark.parametrize(
    "invalid_url,error_match",
    [
        pytest.param(
            "invalid-url",
            "custom_base_url must start with http",
            id="no_protocol",
        ),
        pytest.param(
            "ftp://custom.com",
            "custom_base_url must start with http",
            id="wrong_protocol",
        ),
        pytest.param(
            "",
            "custom_base_url cannot be empty",
            id="empty_string",
        ),
    ],
)
def test_api_config_custom_base_url_validation_errors(
    invalid_url: str,
    error_match: str,
) -> None:
    with pytest.raises(ValidationError, match=error_match):
        MatillionAPIConfig(api_token=SecretStr("test"), custom_base_url=invalid_url)


@pytest.mark.parametrize(
    "timeout,should_raise",
    [
        pytest.param(30, False, id="valid_timeout_30"),
        pytest.param(60, False, id="valid_timeout_60"),
        pytest.param(1, False, id="valid_timeout_1"),
        pytest.param(0, True, id="invalid_timeout_zero"),
        pytest.param(-1, True, id="invalid_timeout_negative"),
    ],
)
def test_api_config_timeout_validation(timeout: int, should_raise: bool) -> None:
    if should_raise:
        with pytest.raises(
            ValidationError, match="request_timeout_sec must be positive"
        ):
            MatillionAPIConfig(
                api_token=SecretStr("test"),
                request_timeout_sec=timeout,
            )
    else:
        config = MatillionAPIConfig(
            api_token=SecretStr("test"),
            request_timeout_sec=timeout,
        )
        assert config.request_timeout_sec == timeout


def test_source_config_defaults() -> None:
    config = MatillionSourceConfig(
        api_config=MatillionAPIConfig(api_token=SecretStr("test"))
    )
    assert config.env == "PROD"
    assert config.platform_instance is None
    assert config.include_pipeline_executions is True
    assert config.max_executions_per_pipeline == 10
    assert config.extract_projects_to_containers is True
    assert config.include_lineage is True
    assert config.include_column_lineage is True
    assert config.include_streaming_pipelines is True


@pytest.mark.parametrize(
    "max_executions,should_raise",
    [
        pytest.param(0, False, id="zero_valid"),
        pytest.param(10, False, id="ten_valid"),
        pytest.param(100, False, id="hundred_valid"),
        pytest.param(-1, True, id="negative_invalid"),
        pytest.param(-10, True, id="negative_ten_invalid"),
    ],
)
def test_source_config_max_executions_validation(
    max_executions: int,
    should_raise: bool,
) -> None:
    if should_raise:
        with pytest.raises(
            ValidationError, match="max_executions_per_pipeline must be non-negative"
        ):
            MatillionSourceConfig(
                api_config=MatillionAPIConfig(api_token=SecretStr("test")),
                max_executions_per_pipeline=max_executions,
            )
    else:
        config = MatillionSourceConfig(
            api_config=MatillionAPIConfig(api_token=SecretStr("test")),
            max_executions_per_pipeline=max_executions,
        )
        assert config.max_executions_per_pipeline == max_executions


@pytest.mark.parametrize(
    "pattern_type,allow_patterns,deny_patterns,test_name,should_allow",
    [
        pytest.param(
            "pipeline",
            ["prod-.*"],
            ["test-.*"],
            "prod-pipeline",
            True,
            id="pipeline_allow_prod",
        ),
        pytest.param(
            "pipeline",
            ["prod-.*"],
            ["test-.*"],
            "test-pipeline",
            False,
            id="pipeline_deny_test",
        ),
        pytest.param(
            "project",
            ["analytics.*"],
            [],
            "analytics-prod",
            True,
            id="project_allow_analytics",
        ),
        pytest.param(
            "project",
            ["analytics.*"],
            [],
            "marketing-prod",
            False,
            id="project_deny_marketing",
        ),
        pytest.param(
            "streaming_pipeline",
            ["cdc-.*"],
            ["dev-.*"],
            "cdc-prod-stream",
            True,
            id="streaming_allow_cdc",
        ),
        pytest.param(
            "streaming_pipeline",
            ["cdc-.*"],
            ["dev-.*"],
            "dev-test-stream",
            False,
            id="streaming_deny_dev",
        ),
    ],
)
def test_source_config_patterns(
    pattern_type: str,
    allow_patterns: list,
    deny_patterns: list,
    test_name: str,
    should_allow: bool,
) -> None:
    kwargs = {
        f"{pattern_type}_patterns": {
            "allow": allow_patterns,
            "deny": deny_patterns,
        }
    }

    config = MatillionSourceConfig(
        api_config=MatillionAPIConfig(api_token=SecretStr("test")),
        **kwargs,
    )

    pattern = getattr(config, f"{pattern_type}_patterns")
    assert pattern.allowed(test_name) == should_allow
