from typing import Iterable

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.transform import Transformer
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    SchemaMetadataClass,
    MetadataChangeEventClass,
)


class PrependCountryCodeConfig(ConfigModel):
    country_code: str


class PrependCountryCode(Transformer):
    """Transformer that adds tags to datasets according to a callback function."""

    ctx: PipelineContext
    config: PrependCountryCodeConfig

    def __init__(self, config: PrependCountryCodeConfig, ctx: PipelineContext):
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "PrependCountryCode":
        config = PrependCountryCodeConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform(self, record_envelopes: Iterable[RecordEnvelope]) -> Iterable[RecordEnvelope]:
        for envelope in record_envelopes:
            if isinstance(envelope.record, MetadataChangeEventClass):
                envelope.record = self.transform_schema_metadata(envelope.record)
                envelope.record = self.transform_dataset_snapshot(envelope.record)
            yield envelope

    def transform_schema_metadata(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        if not isinstance(mce.proposedSnapshot, DatasetSnapshotClass):
            return mce

        schema_metadata_aspect: SchemaMetadataClass = builder.get_aspect_if_available(mce, SchemaMetadataClass)

        country_code = self.config.country_code
        if schema_metadata_aspect is not None:
            schema_metadata_aspect.schemaName = country_code + "." + schema_metadata_aspect.schemaName

        return mce

    def transform_dataset_snapshot(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        if not isinstance(mce.proposedSnapshot, DatasetSnapshotClass):
            return mce

        snapshot = mce.proposedSnapshot
        country_code = self.config.country_code

        if isinstance(snapshot, DatasetSnapshotClass):
            urn = snapshot.urn
            split_list = urn.split(",")
            split_list[1] = country_code + "." + split_list[1]
            new_urn = ",".join(split_list)

            snapshot.urn = new_urn

        return mce