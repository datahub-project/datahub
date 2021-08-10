from abc import abstractmethod
from typing import Iterable

from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.api.transform import Transformer
from datahub.metadata.schema_classes import MetadataChangeEventClass


class DatasetTransformer(Transformer):
    """Transformer that does transforms sequentially on each dataset."""

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        for envelope in record_envelopes:
            if isinstance(envelope.record, MetadataChangeEventClass):
                envelope.record = self.transform_one(envelope.record)
            yield envelope

    @abstractmethod
    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        pass
