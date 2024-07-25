from typing import Callable, Iterable, Optional, Union

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import ControlRecord, PipelineContext, RecordEnvelope
from datahub.ingestion.api.transform import Transformer
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)


class AutoHelperTransformer(Transformer):
    """Converts an auto_* source helper into a transformer.

    Important usage note: this assumes that the auto helper is stateless. The converter
    will be called multiple times, once for each batch of records. If the helper
    attempts to maintain state or perform some cleanup at the end of the stream, it
    will not behave correctly.
    """

    def __init__(
        self,
        converter: Callable[[Iterable[MetadataWorkUnit]], Iterable[MetadataWorkUnit]],
    ):
        self.converter = converter

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        records = list(record_envelopes)

        normal_records = [r for r in records if not isinstance(r.record, ControlRecord)]
        control_records = [r for r in records if isinstance(r.record, ControlRecord)]

        yield from self._from_workunits(
            self.converter(
                self._into_workunits(normal_records),
            )
        )

        # Pass through control records as-is. Note that this isn't fully correct, since it technically
        # reorders the control records relative to the normal records. This is ok since the only control
        # record we have marks the end of the stream.
        yield from control_records

    @classmethod
    def _into_workunits(
        cls,
        stream: Iterable[
            RecordEnvelope[
                Union[
                    MetadataChangeEvent,
                    MetadataChangeProposal,
                    MetadataChangeProposalWrapper,
                ]
            ]
        ],
    ) -> Iterable[MetadataWorkUnit]:
        for record in stream:
            workunit_id: Optional[str] = record.metadata.get("workunit_id")
            metadata = record.record
            yield MetadataWorkUnit.from_metadata(metadata, id=workunit_id)

    @classmethod
    def _from_workunits(
        cls, stream: Iterable[MetadataWorkUnit]
    ) -> Iterable[RecordEnvelope]:
        for workunit in stream:
            yield RecordEnvelope(
                workunit.metadata,
                {
                    "workunit_id": workunit.id,
                },
            )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Transformer:
        raise NotImplementedError(f"{cls.__name__} cannot be created from config")
