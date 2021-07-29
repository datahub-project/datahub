from typing import Iterable

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.transform import Transformer
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    OwnershipClass,
)


class ClearDatasetOwnershipConfig(ConfigModel):
    pass


class SimpleClearDatasetOwnership(Transformer):
    """Transformer that adds a specified set of owners to each dataset."""

    def __init__(self, config: ClearDatasetOwnershipConfig, ctx: PipelineContext):
        pass

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SimpleClearDatasetOwnership":
        config = ClearDatasetOwnershipConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        for envelope in record_envelopes:
            if isinstance(envelope.record, MetadataChangeEventClass):
                envelope.record = self.transform_one(envelope.record)
            yield envelope

    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        if not isinstance(mce.proposedSnapshot, DatasetSnapshotClass):
            return mce

        ownership = builder.get_or_add_aspect(
            mce,
            OwnershipClass(
                owners=[],
            ),
        )
        ownership.owners = []
        return mce
