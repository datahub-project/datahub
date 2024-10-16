import logging
from typing import Iterable, Union

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.api.source import Extractor, WorkUnit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)

logger = logging.getLogger(__name__)


def _try_reformat_with_black(code: str) -> str:
    try:
        import black

        return black.format_str(code, mode=black.FileMode())
    except ImportError:
        return code


class WorkUnitRecordExtractorConfig(ConfigModel):
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
        else:
            raise ValueError(f"unknown WorkUnit type {type(workunit)}")

    def close(self):
        pass
