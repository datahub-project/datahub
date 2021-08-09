from abc import abstractmethod
from typing import Iterable

from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.api.transform import Transformer
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
)


class DatasetTransformer(Transformer):
    """Transformer that does transforms sequentially on each dataset."""

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        for envelope in record_envelopes:
            if isinstance(envelope.record, MetadataChangeEventClass):
                envelope.record = self.transform_one(envelope.record)
            yield envelope

    def is_proposed_dataset_snapshot(self, mce: MetadataChangeEventClass) -> bool:
        return isinstance(mce.proposedSnapshot, DatasetSnapshotClass)

    @abstractmethod
    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        pass
