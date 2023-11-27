import logging
from typing import Iterable, Union

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import get_sys_time
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.api.source import Extractor, WorkUnit
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
    SystemMetadata,
)

logger = logging.getLogger(__name__)


def _try_reformat_with_black(code: str) -> str:
    try:
        import black

        return black.format_str(code, mode=black.FileMode())
    except ImportError:
        return code


class WorkUnitRecordExtractorConfig(ConfigModel):
    set_system_metadata: bool = True
    set_system_metadata_pipeline_name: bool = (
        False  # false for now until the models are available in OSS
    )
    unpack_mces_into_mcps: bool = False


class WorkUnitRecordExtractor(
    Extractor[MetadataWorkUnit, WorkUnitRecordExtractorConfig]
):
    """An extractor that simply returns the data inside workunits back as records."""

    def get_records(
        self, workunit: WorkUnit
    ) -> Iterable[
        RecordEnvelope[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ]
    ]:
        if isinstance(workunit, MetadataWorkUnit):
            if self.config.unpack_mces_into_mcps and isinstance(
                workunit.metadata, MetadataChangeEvent
            ):
                for inner_workunit in workunit.decompose_mce_into_mcps():
                    yield from self.get_records(inner_workunit)
                return

            if isinstance(
                workunit.metadata,
                (
                    MetadataChangeEvent,
                    MetadataChangeProposal,
                    MetadataChangeProposalWrapper,
                ),
            ):
                if self.config.set_system_metadata:
                    workunit.metadata.systemMetadata = SystemMetadata(
                        lastObserved=get_sys_time(), runId=self.ctx.run_id
                    )
                    if self.config.set_system_metadata_pipeline_name:
                        workunit.metadata.systemMetadata.pipelineName = (
                            self.ctx.pipeline_name
                        )
                if (
                    isinstance(workunit.metadata, MetadataChangeEvent)
                    and len(workunit.metadata.proposedSnapshot.aspects) == 0
                ):
                    raise AttributeError("every mce must have at least one aspect")
            if not workunit.metadata.validate():
                invalid_mce = str(workunit.metadata)
                invalid_mce = _try_reformat_with_black(invalid_mce)

                raise ValueError(
                    f"source produced an invalid metadata work unit: {invalid_mce}"
                )

            yield RecordEnvelope(
                workunit.metadata,
                {
                    "workunit_id": workunit.id,
                },
            )
        elif isinstance(workunit, UsageStatsWorkUnit):
            logger.error(
                "Dropping deprecated `UsageStatsWorkUnit`. "
                "Emit a `MetadataWorkUnit` with the `datasetUsageStatistics` aspect instead."
            )
        else:
            raise ValueError(f"unknown WorkUnit type {type(workunit)}")

    def close(self):
        pass
