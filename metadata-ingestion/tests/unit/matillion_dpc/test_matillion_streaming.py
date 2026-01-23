from datetime import datetime

import pytest
from pydantic import SecretStr

from datahub.ingestion.source.matillion_dpc.config import (
    MatillionAPIConfig,
    MatillionSourceConfig,
    MatillionSourceReport,
)
from datahub.ingestion.source.matillion_dpc.matillion_container import (
    MatillionContainerHandler,
)
from datahub.ingestion.source.matillion_dpc.matillion_streaming import (
    MatillionStreamingHandler,
)
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionProject,
    MatillionStreamingPipeline,
)
from datahub.ingestion.source.matillion_dpc.urn_builder import MatillionUrnBuilder


@pytest.fixture
def config() -> MatillionSourceConfig:
    return MatillionSourceConfig(
        api_config=MatillionAPIConfig(
            client_id=SecretStr("test_client_id"),
            client_secret=SecretStr("test_client_secret"),
            custom_base_url="http://test.com",
        ),
        include_streaming_pipelines=True,
        extract_projects_to_containers=True,
    )


@pytest.fixture
def report() -> MatillionSourceReport:
    return MatillionSourceReport()


@pytest.fixture
def urn_builder(config: MatillionSourceConfig) -> MatillionUrnBuilder:
    return MatillionUrnBuilder(config)


@pytest.fixture
def container_handler(
    config: MatillionSourceConfig,
    report: MatillionSourceReport,
    urn_builder: MatillionUrnBuilder,
) -> MatillionContainerHandler:
    return MatillionContainerHandler(config, report, urn_builder)


@pytest.fixture
def streaming_handler(
    config: MatillionSourceConfig,
    report: MatillionSourceReport,
    urn_builder: MatillionUrnBuilder,
    container_handler: MatillionContainerHandler,
) -> MatillionStreamingHandler:
    return MatillionStreamingHandler(config, report, urn_builder, container_handler)


@pytest.fixture
def project() -> MatillionProject:
    return MatillionProject(id="proj-1", name="Test Project")


def test_make_streaming_pipeline_urn(
    streaming_handler: MatillionStreamingHandler,
) -> None:
    streaming_pipeline = MatillionStreamingPipeline(
        streaming_pipeline_id="sp-123",
        name="Test Streaming Pipeline",
        project_id="proj-1",
    )

    project = MatillionProject(
        id="proj-1",
        name="Test Project",
    )

    urn = streaming_handler._make_streaming_pipeline_urn(streaming_pipeline, project)

    assert "urn:li:dataFlow:" in urn
    assert "matillion" in urn


@pytest.mark.parametrize(
    "pipeline_data",
    [
        pytest.param(
            {
                "id": "sp-123",
                "name": "Test Streaming Pipeline",
                "project_id": "proj-1",
                "environment_id": "env-1",
                "source_type": "postgres",
                "target_type": "snowflake",
                "description": "A test streaming pipeline",
            },
            id="basic",
        ),
        pytest.param(
            {
                "id": "sp-123",
                "name": "CDC Pipeline",
                "project_id": "proj-1",
                "environment_id": "env-1",
                "source_type": "mysql",
                "target_type": "s3",
                "source_connection_id": "conn-source-1",
                "target_connection_id": "conn-target-1",
                "status": "running",
                "description": "MySQL to S3 CDC pipeline",
            },
            id="with_connections",
        ),
        pytest.param(
            {
                "id": "sp-123",
                "name": "Test Pipeline",
                "project_id": "proj-1",
                "environment_id": "env-1",
                "source_type": "postgres",
                "target_type": "snowflake",
                "created_at": datetime(2024, 1, 1, 12, 0, 0),
            },
            id="with_timestamp",
        ),
    ],
)
def test_emit_streaming_pipeline(
    streaming_handler: MatillionStreamingHandler,
    project: MatillionProject,
    pipeline_data: dict,
) -> None:
    streaming_pipeline = MatillionStreamingPipeline(**pipeline_data)

    workunits = list(
        streaming_handler.emit_streaming_pipeline(streaming_pipeline, project)
    )

    assert len(workunits) > 0


def test_emit_streaming_pipeline_without_containers(
    config: MatillionSourceConfig,
    report: MatillionSourceReport,
    urn_builder: MatillionUrnBuilder,
    container_handler: MatillionContainerHandler,
    project: MatillionProject,
) -> None:
    config.extract_projects_to_containers = False

    streaming_handler = MatillionStreamingHandler(
        config, report, urn_builder, container_handler
    )

    streaming_pipeline = MatillionStreamingPipeline(
        id="sp-123",
        name="Test Pipeline",
        project_id="proj-1",
        environment_id="env-1",
        source_type="postgres",
        target_type="snowflake",
    )

    workunits = list(
        streaming_handler.emit_streaming_pipeline(streaming_pipeline, project)
    )

    assert len(workunits) > 0
