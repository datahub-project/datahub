import json

import pandas as pd
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.workunit import UsageStatsWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    DatasetPropertiesClass,
    KafkaSchemaClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
)

from datahub.ingestion.source.ib.ib_common import *


class IBKafkaSourceConfig(IBRedashSourceConfig):
    pass


@platform_name("IBKafka")
@config_class(IBKafkaSourceConfig)
class IBKafkaSource(IBRedashSource):
    platform = "kafka"

    def __init__(self, config: IBKafkaSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config: IBKafkaSourceConfig = config

    def fetch_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        json_data = pd.read_json(json.dumps(self.query_get(self.config.query_id)))
        json_data_grouped = json_data.groupby(["dc", "cluster", "topic"], dropna=False)
        return json_data_grouped.apply(
            lambda fields_by_topic: self.fetch_workunit(fields_by_topic)
        )

    def fetch_workunit(self, fields_by_topic: pd.DataFrame):
        first = fields_by_topic.iloc[0]
        topic_name = first.topic

        dataset_path = [first.cluster, topic_name]

        properties = DatasetPropertiesClass(
            name=topic_name,
            description=first.description,
            qualifiedName=build_dataset_qualified_name(first.dc, *dataset_path),
        )

        browse_paths = BrowsePathsClass([build_dataset_browse_path(first.dc, *dataset_path)])

        schema = SchemaMetadataClass(
            schemaName=self.platform,
            version=1,
            hash="",
            platform=builder.make_data_platform_urn(self.platform),
            platformSchema=KafkaSchemaClass.construct_with_defaults(),
            fields=fields_by_topic.dropna(subset="fieldName").apply(lambda field: self.map_column(field),
                                                                    axis=1).values.tolist(),
        )
        owners = [
            builder.make_group_urn(owner.strip()) for owner in first.owners.split(",")
        ]
        ownership = builder.make_ownership_aspect_from_urn_list(
            owners, OwnershipSourceTypeClass.SERVICE, OwnershipTypeClass.TECHNICAL_OWNER
        )
        aspects = [properties, browse_paths, schema, ownership]
        snapshot = DatasetSnapshot(
            urn=build_dataset_urn(self.platform, first.dc, *dataset_path),
            aspects=aspects,
        )
        mce = MetadataChangeEvent(proposedSnapshot=snapshot)
        return MetadataWorkUnit(properties.qualifiedName, mce=mce)

    @staticmethod
    def map_column(field) -> SchemaFieldClass:
        data_type = field.fieldType
        return SchemaFieldClass(
            fieldPath=field.fieldName,
            description=field.valueSet,
            type=SchemaFieldDataTypeClass(type=get_type_class(data_type)),
            nativeDataType=data_type,
            nullable=bool(field.nullable),
        )

    def get_default_ingestion_job_id(self) -> JobId:
        return JobId("ingest_kafka_from_redash_source")
