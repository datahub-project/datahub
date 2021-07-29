from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws.athena_managed_by_glue import (
    AthenaManagedByGlueSource,
)
from datahub.ingestion.source.aws.glue import GlueSourceConfig


def athena_managed_by_glue_source() -> AthenaManagedByGlueSource:
    return AthenaManagedByGlueSource(
        ctx=PipelineContext(run_id="athena-managed-by-glue-source-source-test"),
        config=GlueSourceConfig(aws_region="us-west-2", extract_transforms=True),
    )


def test_transforms_are_turned_off():
    source = athena_managed_by_glue_source()
    assert source.extract_transforms is False
