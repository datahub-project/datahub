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
    SchemaFieldClass,
    TagAssociationClass,
    SchemaMetadataClass
)
import logging
logger = logging.getLogger(__name__)

class IngestDictionaryConfig(ConfigModel):    
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
        if not os.path.exists(self.config.dictionary_path):
            logger.error("dictionary file does not exist")
            return mce
        df = pd.read_csv(self.config.dictionary_path)
        if not all(elem in df.columns.tolist() for elem in ["column_name", "tag", "description"]):        
            logger.error("Dictionary does not have the correct format - missing column_name, tag or description")
        df.set_index("column_name", inplace=True)
        records = df.to_dict(orient="index")
        
        
        
        existing_schema = builder.get_aspect_if_available(
                mce,
                SchemaMetadataClass
            )
        fields = existing_schema.fields
        
        for item in fields:
            col_name = item.fieldPath            
            if col_name in records.keys():
                item.description = records[col_name].get("description")
                if self._check_tags_empty(item):
                    tags = item.globalTags
                

        return mce
    def _check_tags_empty(self, item:SchemaFieldClass) -> bool:
        if (item.globalTags!="") and (item.globalTags) and (item.globalTags.strip()!=""):
            return True
        else:
            return False


