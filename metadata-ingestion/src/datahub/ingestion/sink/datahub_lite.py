import logging
import os
from typing import Union

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
from datahub.lite.lite_util import LiteLocalConfig, get_datahub_lite
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)

logger = logging.getLogger(__name__)


class DataHubLiteSinkConfig(LiteLocalConfig):
    type: str = "duckdb"
    config: dict = {"file": os.path.expanduser("~/.datahub/lite/datahub.duckdb")}


class DataHubLiteSink(Sink[DataHubLiteSinkConfig, SinkReport]):
    def __post_init__(self) -> None:
        self.datahub_lite = get_datahub_lite(self.config.dict())

    def write_record_async(
        self,
        record_envelope: RecordEnvelope[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ],
        write_callback: WriteCallback,
    ) -> None:
        record = record_envelope.record
        if not isinstance(record, (MetadataChangeEvent, MetadataChangeProposalWrapper)):
            self.report.report_warning(f"datahub-local does not support {type(record)}")
            return

        try:
            self.datahub_lite.write(record)
            self.report.report_record_written(record_envelope)
        except Exception as e:
            self.report.report_failure(f"{record_envelope.metadata}: {type(e)}: {e}")
            if write_callback:
                write_callback.on_failure(record_envelope, e, {})
        else:
            if write_callback:
                write_callback.on_success(record_envelope, success_metadata={})

    def close(self):
        if self.datahub_lite:
            self.datahub_lite.close()
