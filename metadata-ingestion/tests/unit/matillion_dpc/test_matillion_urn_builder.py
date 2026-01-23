import pytest
from pydantic import SecretStr

from datahub.ingestion.source.matillion_dpc.config import (
    MatillionAPIConfig,
    MatillionSourceConfig,
)
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionEnvironment,
    MatillionPipeline,
    MatillionProject,
)
from datahub.ingestion.source.matillion_dpc.urn_builder import MatillionUrnBuilder


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
        name="Production",
        default_agent_id="agent-1",
    )

    urn = urn_builder.make_environment_container_urn(environment, project)

    assert "urn:li:container:" in urn
    assert "Production" in urn


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
        name="Test Pipeline",
    )

    builder = urn_builder if has_platform_instance else urn_builder_no_platform
    urn = builder.make_pipeline_urn(pipeline, project)

    assert "urn:li:dataFlow:" in urn
    assert "matillion" in urn
    # URN uses GUID hash for safety with special characters, not the pipeline name directly


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
