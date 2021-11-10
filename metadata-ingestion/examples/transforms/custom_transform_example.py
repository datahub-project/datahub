# see https://datahubproject.io/docs/metadata-ingestion/transformers for original tutorial
import json
from typing import Iterable

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.transform import Transformer
from datahub.ingestion.transformer.add_dataset_ownership import Semantics
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)


class AddCustomOwnershipConfig(ConfigModel):
    owners_json: str
    semantics: Semantics = Semantics.OVERWRITE


class AddCustomOwnership(Transformer):
    """Transformer that adds owners to datasets according to a callback function."""

    # context param to generate run metadata such as a run ID
    ctx: PipelineContext
    # as defined in the previous block
    config: AddCustomOwnershipConfig

    def __init__(self, config: AddCustomOwnershipConfig, ctx: PipelineContext):
        self.ctx = ctx
        self.config = config

        with open(self.config.owners_json, "r") as f:
            raw_owner_urns = json.load(f)

        self.owners = [
            OwnerClass(owner=owner, type=OwnershipTypeClass.DATAOWNER)
            for owner in raw_owner_urns
        ]

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddCustomOwnership":
        config = AddCustomOwnershipConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:

        # loop over envelopes
        for envelope in record_envelopes:

            # if envelope is an MCE, add the ownership classes
            if isinstance(envelope.record, MetadataChangeEventClass):
                envelope.record = self.transform_one(envelope.record)
            yield envelope

    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        if not isinstance(mce.proposedSnapshot, DatasetSnapshotClass):
            return mce

        owners_to_add = self.owners

        if owners_to_add:
            ownership = builder.get_or_add_aspect(
                mce,
                OwnershipClass(
                    owners=[],
                ),
            )
            ownership.owners.extend(owners_to_add)

        return mce
