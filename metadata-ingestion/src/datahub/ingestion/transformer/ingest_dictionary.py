from typing import Callable, Iterable, List, Union
import pandas as pd
import os

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.transform import Transformer
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    GlobalTagsClass,
    MetadataChangeEventClass,
    TagAssociationClass,
    SchemaMetadataClass
)
import logging
logger = logging.getLogger(__name__)

class IngestDictionaryConfig(ConfigModel):
    # Workaround for https://github.com/python/mypy/issues/708.
    # Suggested by https://stackoverflow.com/a/64528725/5004662.
    dictionary_path: str


class InsertIngestionDictionary(Transformer):
    """Transformer that adds tags to datasets according to a callback function."""

    ctx: PipelineContext
    config: IngestDictionaryConfig

    def __init__(self, config: IngestDictionaryConfig, ctx: PipelineContext):
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "InsertIngestionDictionary":
        config = IngestDictionaryConfig.parse_obj(config_dict)
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
        #load data
        if not os.path.exist(self.config.dictionary_path):
            logger.error("dictionary file does not exist")
            return mce
        # df = pd.read_csv(self.config.dictionary_path)
        existing_schema = builder.get_or_add_aspect(
                mce,
                SchemaMetadataClass()
            )
        logger.error(existing_schema)            

        return mce


