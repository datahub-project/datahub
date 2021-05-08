import logging

from datahub.ingestion.api.transform import Transformer
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    SchemaMetadataClass,
    TagAssociationClass,
)

logger = logging.getLogger(__name__)

TABLE_TAGS = {
    "urn:li:dataset:(urn:li:dataPlatform:postgresql,myapp.information_schema.sql_parts,PROD)": {
        "comments": {
            "tags": ["a_tag", "another_tag"],
            "description": "Description for comments",
        }
    }
}


class AddDatasetOwnership(Transformer):
    def __init__(self, config):
        self.config = config

    def transform(self, record_envelopes):
        for envelope in record_envelopes:
            if isinstance(envelope.record, MetadataChangeEventClass):
                envelope.record = self.transform_one(envelope.record)
            yield envelope
            # print(envelope.record, type(envelope.record))
            # print(
            #     envelope.record.proposedSnapshot, type(envelope.record.proposedSnapshot)
            # )
            # print(
            #     envelope.record.proposedSnapshot.urn,
            #     type(envelope.record.proposedSnapshot.urn),
            # )
            # yield envelope

    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        if not isinstance(mce.proposedSnapshot, DatasetSnapshotClass):
            return mce

        urn = mce.proposedSnapshot.urn
        if urn in TABLE_TAGS:
            for aspect in mce.proposedSnapshot.aspects:
                if isinstance(aspect, SchemaMetadataClass):
                    for field in aspect.fields:
                        if isinstance(field, SchemaFieldClass):
                            if field.fieldPath in TABLE_TAGS[urn]:
                                if "description" in TABLE_TAGS[urn][field.fieldPath]:
                                    desc = TABLE_TAGS[urn][field.fieldPath][
                                        "description"
                                    ]
                                    logger.info(
                                        "Setting table %s field %s description: %s",
                                        urn,
                                        field.fieldPath,
                                        desc,
                                    )
                                    field.description = TABLE_TAGS[urn][
                                        field.fieldPath
                                    ]["description"]
                                if "tags" in TABLE_TAGS[urn][field.fieldPath]:
                                    tags = TABLE_TAGS[urn][field.fieldPath]["tags"]
                                    logger.info(
                                        "Setting table %s field %s tags: %s",
                                        urn,
                                        field.fieldPath,
                                        tags,
                                    )
                                    field.globalTags = GlobalTagsClass(
                                        tags=[TagAssociationClass(tag=t) for t in tags]
                                    )

    @classmethod
    def create(cls, config_dict, ctx):
        return cls(config_dict)
