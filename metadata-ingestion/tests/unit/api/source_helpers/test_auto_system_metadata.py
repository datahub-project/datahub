from datetime import datetime
from typing import Optional

from freezegun import freeze_time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source_helpers import AutoSystemMetadata
from datahub.ingestion.run.pipeline_config import (
    FlagsConfig,
    PipelineConfig,
    SourceConfig,
)
from datahub.metadata.schema_classes import SystemMetadataClass

FROZEN_TIME = datetime.fromisoformat("2023-01-01 00:00:00+00:00")
FROZEN_TIME_TS = int(FROZEN_TIME.timestamp() * 1000)
DUMMY_URN = "urn:li:dataset:(snowflake,test.dummy,PROD)"


def get_ctx(flags: FlagsConfig) -> PipelineContext:
    return PipelineContext(
        run_id="auto-source-test",
        pipeline_name="dummy-pipeline",
        pipeline_config=PipelineConfig(source=SourceConfig(type="file"), flags=flags),
    )


@freeze_time(FROZEN_TIME)
def test_stamping_without_existing_metadata():
    ctx = get_ctx(FlagsConfig())
    mcpw = MetadataChangeProposalWrapper(entityUrn=DUMMY_URN)
    proc = AutoSystemMetadata(ctx)
    out = [*proc.stamp([mcpw.as_workunit()])]

    assert len(out) == 1
    system_metadata: Optional[SystemMetadataClass] = out[0].metadata.systemMetadata
    assert system_metadata
    assert system_metadata.runId == "auto-source-test"
    assert system_metadata.pipelineName == "dummy-pipeline"
    assert system_metadata.lastObserved == FROZEN_TIME_TS


@freeze_time(FROZEN_TIME)
def test_stamping_with_existing_metadata():
    ctx = get_ctx(FlagsConfig())
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=DUMMY_URN,
        systemMetadata=SystemMetadataClass(
            runId="other-run-id", lastObserved=FROZEN_TIME_TS + 1
        ),
    )
    proc = AutoSystemMetadata(ctx)
    out = [*proc.stamp([mcpw.as_workunit()])]

    assert len(out) == 1
    system_metadata: Optional[SystemMetadataClass] = out[0].metadata.systemMetadata
    assert system_metadata
    assert system_metadata.runId == "auto-source-test"  # runId is overwritten
    assert system_metadata.pipelineName == "dummy-pipeline"
    assert (
        system_metadata.lastObserved == FROZEN_TIME_TS + 1
    )  # lastObserved is not changed


@freeze_time(FROZEN_TIME)
def test_not_stamping_with_existing_metadata():
    ctx = get_ctx(FlagsConfig(set_system_metadata=False))
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=DUMMY_URN,
        systemMetadata=SystemMetadataClass(
            runId="other-run-id", lastObserved=FROZEN_TIME_TS + 1
        ),
    )
    proc = AutoSystemMetadata(ctx)
    out = [*proc.stamp([mcpw.as_workunit()])]

    assert len(out) == 1
    system_metadata: Optional[SystemMetadataClass] = out[0].metadata.systemMetadata
    assert system_metadata
    assert system_metadata.runId == "other-run-id"  # runId is not changed
    assert system_metadata.pipelineName is None  # pipeline name is not set
    assert (
        system_metadata.lastObserved == FROZEN_TIME_TS + 1
    )  # lastObserved is not changed
