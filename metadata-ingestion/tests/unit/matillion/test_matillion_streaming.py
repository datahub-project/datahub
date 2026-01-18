from datetime import datetime

import pytest
from pydantic import SecretStr

from datahub.ingestion.source.matillion.config import (
    MatillionAPIConfig,
    MatillionSourceConfig,
    MatillionSourceReport,
)
from datahub.ingestion.source.matillion.matillion_container import (
    MatillionContainerHandler,
)
from datahub.ingestion.source.matillion.matillion_streaming import (
    MatillionStreamingHandler,
)
from datahub.ingestion.source.matillion.models import (
    MatillionProject,
    MatillionStreamingPipeline,
)
from datahub.ingestion.source.matillion.urn_builder import MatillionUrnBuilder


@pytest.fixture
def config() -> MatillionSourceConfig:
    return MatillionSourceConfig(
        api_config=MatillionAPIConfig(
            api_token=SecretStr("test_token"),
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


def test_make_streaming_pipeline_urn(
    streaming_handler: MatillionStreamingHandler,
) -> None:
    streaming_pipeline = MatillionStreamingPipeline(
        id="sp-123",
        name="Test Streaming Pipeline",
        project_id="proj-1",
        environment_id="env-1",
        source_type="postgres",
        target_type="snowflake",
    )

    project = MatillionProject(
        id="proj-1",
        name="Test Project",
    )

    urn = streaming_handler._make_streaming_pipeline_urn(streaming_pipeline, project)

    assert "streaming.sp-123" in urn
    assert "matillion" in urn


def test_emit_streaming_pipeline_basic(
    streaming_handler: MatillionStreamingHandler,
) -> None:
    streaming_pipeline = MatillionStreamingPipeline(
        id="sp-123",
        name="Test Streaming Pipeline",
        project_id="proj-1",
        environment_id="env-1",
        source_type="postgres",
        target_type="snowflake",
        description="A test streaming pipeline",
    )

    project = MatillionProject(
        id="proj-1",
        name="Test Project",
    )

    workunits = list(
        streaming_handler.emit_streaming_pipeline(streaming_pipeline, project)
    )

    assert len(workunits) > 0


def test_emit_streaming_pipeline_with_connections(
    streaming_handler: MatillionStreamingHandler,
) -> None:
    streaming_pipeline = MatillionStreamingPipeline(
        id="sp-123",
        name="CDC Pipeline",
        project_id="proj-1",
        environment_id="env-1",
        source_type="mysql",
        target_type="s3",
        source_connection_id="conn-source-1",
        target_connection_id="conn-target-1",
        status="running",
        description="MySQL to S3 CDC pipeline",
    )

    project = MatillionProject(
        id="proj-1",
        name="CDC Project",
    )

    workunits = list(
        streaming_handler.emit_streaming_pipeline(streaming_pipeline, project)
    )

    assert len(workunits) > 0


def test_emit_streaming_pipeline_with_timestamp(
    streaming_handler: MatillionStreamingHandler,
) -> None:
    streaming_pipeline = MatillionStreamingPipeline(
        id="sp-123",
        name="Test Pipeline",
        project_id="proj-1",
        environment_id="env-1",
        source_type="postgres",
        target_type="snowflake",
        created_at=datetime(2024, 1, 1, 12, 0, 0),
    )

    project = MatillionProject(
        id="proj-1",
        name="Test Project",
    )

    workunits = list(
        streaming_handler.emit_streaming_pipeline(streaming_pipeline, project)
    )

    assert len(workunits) > 0


def test_emit_streaming_pipeline_without_containers(
    config: MatillionSourceConfig,
    report: MatillionSourceReport,
    urn_builder: MatillionUrnBuilder,
    container_handler: MatillionContainerHandler,
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

    project = MatillionProject(
        id="proj-1",
        name="Test Project",
    )

    workunits = list(
        streaming_handler.emit_streaming_pipeline(streaming_pipeline, project)
    )

    assert len(workunits) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
