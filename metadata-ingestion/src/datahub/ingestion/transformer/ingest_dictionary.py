import logging
import os
from typing import Iterable

import pandas as pd

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.transform import Transformer
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    GlobalTagsClass,
    MetadataChangeEventClass,
    SchemaMetadataClass,
    TagAssociationClass,
)

logger = logging.getLogger(__name__)


class IngestDictionaryConfig(ConfigModel):
    dictionary_path: str


# there is an issue with this approach; new tag entities are not created, instead i only created a reference to such a tag.
# it can be solved by going to UI and adding the tag.
# As long as the name is correct, i can "find" the datasets with such a column tag.


class InsertIngestionDictionary(Transformer):
    """Transformer that adds tags to datasets according to a callback function."""

    ctx: PipelineContext
    config: IngestDictionaryConfig

    def __init__(self, config: IngestDictionaryConfig, ctx: PipelineContext):
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "InsertIngestionDictionary":
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
        if not os.path.exists(self.config.dictionary_path):
            logger.error("dictionary file does not exist!")
            return mce
        df = pd.read_csv(self.config.dictionary_path)
        if not all(
            elem in df.columns.tolist()
            for elem in ["column_name", "tag", "description"]
        ):
            logger.error(
                "Dictionary does not have the correct format - missing column_name AND tag AND description"
            )
            return mce
        df.set_index("column_name", inplace=True)
        df = df.where(pd.notnull(df), None)
        records = df.to_dict(orient="index")

        existing_schema = builder.get_aspect_if_available(mce, SchemaMetadataClass)
        if existing_schema:
            fields = existing_schema.fields
            # returns the list of SchemaField in the SchemaMetaData
            for item in fields:
                col_name = item.fieldPath
                if col_name in records.keys():
                    item.description = records[col_name].get("description")
                    proposed_tags_list = records[col_name].get("tag")
                    if not proposed_tags_list:
                        continue
                    proposed_tags_list = records[col_name].get("tag").split(",")
                    proposed_tags_list = [
                        item.strip() for item in proposed_tags_list if item.strip()
                    ]
                    tags = []
                    for proposed_tag in proposed_tags_list:
                        tag_id = builder.make_tag_urn(proposed_tag)
                        tag = TagAssociationClass(tag_id)
                        tags.append(tag)
                    field_tag = GlobalTagsClass(tags=tags)
                    item.globalTags = field_tag

        return mce
