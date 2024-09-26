import functools
from typing import Iterable

from datahub.emitter.mce_builder import get_sys_time
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.transform import Transformer
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.transformer.auto_helper_transformer import AutoHelperTransformer
from datahub.metadata.schema_classes import SystemMetadataClass


def auto_system_metadata(
    ctx: PipelineContext,
    stream: Iterable[MetadataWorkUnit],
) -> Iterable[MetadataWorkUnit]:
    if not ctx.pipeline_config:
        raise ValueError("Pipeline config is required for system metadata")
    set_system_metadata = ctx.pipeline_config.flags.set_system_metadata
    set_pipeline_name = ctx.pipeline_config.flags.set_system_metadata_pipeline_name

    for workunit in stream:
        if set_system_metadata:
            workunit.metadata.systemMetadata = SystemMetadataClass(
                lastObserved=get_sys_time(), runId=ctx.run_id
            )
            if set_pipeline_name:
                workunit.metadata.systemMetadata.pipelineName = ctx.pipeline_name

        yield workunit


class SystemMetadataTransformer(Transformer):
    def __init__(self, ctx: PipelineContext):
        self._inner_transfomer = AutoHelperTransformer(
            functools.partial(auto_system_metadata, ctx)
        )

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        yield from self._inner_transfomer.transform(record_envelopes)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Transformer:
        raise NotImplementedError(f"{cls.__name__} cannot be created from config")
