import pytest
from pydantic import SecretStr

from datahub.ingestion.source.matillion.config import (
    MatillionAPIConfig,
    MatillionSourceConfig,
)
from datahub.ingestion.source.matillion.models import (
    MatillionConnection,
    MatillionEnvironment,
    MatillionPipeline,
    MatillionProject,
)
from datahub.ingestion.source.matillion.urn_builder import MatillionUrnBuilder


@pytest.fixture
def config() -> MatillionSourceConfig:
    return MatillionSourceConfig(
        api_config=MatillionAPIConfig(
            api_token=SecretStr("test_token"),
            custom_base_url="http://test.com",
        ),
        platform_instance="test-instance",
    )


@pytest.fixture
def config_no_platform_instance() -> MatillionSourceConfig:
    return MatillionSourceConfig(
        api_config=MatillionAPIConfig(
            api_token=SecretStr("test_token"),
            custom_base_url="http://test.com",
        ),
    )


@pytest.fixture
def urn_builder(config: MatillionSourceConfig) -> MatillionUrnBuilder:
    return MatillionUrnBuilder(config)


@pytest.fixture
def urn_builder_no_platform(
    config_no_platform_instance: MatillionSourceConfig,
) -> MatillionUrnBuilder:
    return MatillionUrnBuilder(config_no_platform_instance)


def test_make_project_container_urn(urn_builder: MatillionUrnBuilder) -> None:
    project = MatillionProject(
        id="proj-123",
        name="Test Project",
    )

    urn = urn_builder.make_project_container_urn(project)

    assert "urn:li:container:" in urn
    assert "proj-123" in urn


def test_make_environment_container_urn(urn_builder: MatillionUrnBuilder) -> None:
    project = MatillionProject(
        id="proj-123",
        name="Test Project",
    )

    environment = MatillionEnvironment(
        id="env-456",
        name="Production",
        project_id="proj-123",
    )

    urn = urn_builder.make_environment_container_urn(environment, project)

    assert "urn:li:container:" in urn
    assert "env-456" in urn


@pytest.mark.parametrize(
    "has_platform_instance",
    [
        pytest.param(True, id="with_platform_instance"),
        pytest.param(False, id="without_platform_instance"),
    ],
)
def test_make_pipeline_urn(
    has_platform_instance: bool,
    urn_builder: MatillionUrnBuilder,
    urn_builder_no_platform: MatillionUrnBuilder,
) -> None:
    project = MatillionProject(
        id="proj-123",
        name="Test Project",
    )

    pipeline = MatillionPipeline(
        id="pipe-789",
        name="Test Pipeline",
        project_id="proj-123",
        pipeline_type="orchestration",
    )

    builder = urn_builder if has_platform_instance else urn_builder_no_platform
    urn = builder.make_pipeline_urn(pipeline, project)

    assert "urn:li:dataFlow:" in urn
    assert "matillion" in urn
    assert "pipe-789" in urn


@pytest.mark.parametrize(
    "connection_type,expected_platform,connection_name",
    [
        pytest.param(
            "snowflake",
            "snowflake",
            "snowflake_prod",
            id="snowflake",
        ),
        pytest.param(
            "bigquery",
            "bigquery",
            "bigquery_analytics",
            id="bigquery",
        ),
        pytest.param(
            "redshift",
            "redshift",
            "redshift_warehouse",
            id="redshift",
        ),
        pytest.param(
            "postgres",
            "postgres",
            "postgres_source",
            id="postgres",
        ),
        pytest.param(
            "mysql",
            "mysql",
            "mysql_db",
            id="mysql",
        ),
        pytest.param(
            "sqlserver",
            "mssql",
            "sqlserver_db",
            id="sqlserver",
        ),
        pytest.param(
            "oracle",
            "oracle",
            "oracle_db",
            id="oracle",
        ),
        pytest.param(
            "s3",
            "s3",
            "s3_bucket",
            id="s3",
        ),
        pytest.param(
            "azure",
            "abs",
            "azure_storage",
            id="azure",
        ),
        pytest.param(
            "gcs",
            "gcs",
            "gcs_bucket",
            id="gcs",
        ),
        pytest.param(
            "SNOWFLAKE",
            "snowflake",
            "snowflake_upper",
            id="snowflake_uppercase",
        ),
        pytest.param(
            "unknown_db",
            "external",
            "unknown_connection",
            id="unknown_platform",
        ),
    ],
)
def test_make_connection_dataset_urn_platforms(
    urn_builder: MatillionUrnBuilder,
    connection_type: str,
    expected_platform: str,
    connection_name: str,
) -> None:
    connection = MatillionConnection(
        id=f"conn-{connection_type}",
        name=connection_name,
        connection_type=connection_type,
        project_id="proj-123",
    )

    urn = urn_builder.make_connection_dataset_urn(connection)

    assert urn is not None
    assert "urn:li:dataset:" in str(urn)
    assert expected_platform in str(urn)
    assert connection_name in str(urn)


@pytest.mark.parametrize(
    "connection_name,should_be_none",
    [
        pytest.param("", True, id="empty_name"),
        pytest.param("valid_name", False, id="valid_name"),
    ],
)
def test_make_connection_dataset_urn_name_validation(
    urn_builder: MatillionUrnBuilder,
    connection_name: str,
    should_be_none: bool,
) -> None:
    connection = MatillionConnection(
        id="conn-test",
        name=connection_name,
        connection_type="snowflake",
        project_id="proj-123",
    )

    urn = urn_builder.make_connection_dataset_urn(connection)

    if should_be_none:
        assert urn is None
    else:
        assert urn is not None
        assert connection_name in str(urn)


@pytest.mark.parametrize(
    "has_platform_instance,expected_result",
    [
        pytest.param(True, "should_exist", id="with_platform_instance"),
        pytest.param(False, None, id="without_platform_instance"),
    ],
)
def test_make_platform_instance_urn(
    has_platform_instance: bool,
    expected_result: str,
    urn_builder: MatillionUrnBuilder,
    urn_builder_no_platform: MatillionUrnBuilder,
) -> None:
    builder = urn_builder if has_platform_instance else urn_builder_no_platform
    urn = builder.make_platform_instance_urn()

    if expected_result is None:
        assert urn is None
    else:
        assert urn is not None
        assert "urn:li:dataPlatformInstance:" in urn
        assert "matillion" in urn
        if has_platform_instance:
            assert "test-instance" in urn


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
