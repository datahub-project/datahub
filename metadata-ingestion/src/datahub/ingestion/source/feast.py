import json
from dataclasses import dataclass, field
from typing import Dict, Iterable, List

from feast import Client
from feast.data_source import BigQuerySource, FileSource, KafkaSource, KinesisSource

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import MLFeatureDataType
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    MLEntitySnapshot,
    MLFeatureSetSnapshot,
    MLFeatureSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    MLEntityPropertiesClass,
    MLFeaturePropertiesClass,
    MLFeatureSetPropertiesClass,
)

# map Feast types to DataHub classes
_field_type_mapping: Dict[str, str] = {
    "BYTES": MLFeatureDataType.BYTE,
    "STRING": MLFeatureDataType.TEXT,
    "INT32": MLFeatureDataType.ORDINAL,
    "INT64": MLFeatureDataType.ORDINAL,
    "DOUBLE": MLFeatureDataType.CONTINUOUS,
    "FLOAT": MLFeatureDataType.CONTINUOUS,
    "BOOL": MLFeatureDataType.BINARY,
    "UNIX_TIMESTAMP": MLFeatureDataType.TIME,
    "BYTES_LIST": MLFeatureDataType.SEQUENCE,
    "STRING_LIST": MLFeatureDataType.SEQUENCE,
    "INT32_LIST": MLFeatureDataType.SEQUENCE,
    "INT64_LIST": MLFeatureDataType.SEQUENCE,
    "DOUBLE_LIST": MLFeatureDataType.SEQUENCE,
    "FLOAT_LIST": MLFeatureDataType.SEQUENCE,
    "BOOL_LIST": MLFeatureDataType.SEQUENCE,
    "UNIX_TIMESTAMP_LIST": MLFeatureDataType.SEQUENCE,
}


class FeastConfig(ConfigModel):
    # See the MongoDB authentication docs for details and examples.
    # https://pymongo.readthedocs.io/en/stable/examples/authentication.html
    core_url: str = "localhost:6565"
    options: dict = {}


@dataclass
class FeastSourceReport(SourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


@dataclass
class FeastSource(Source):
    config: FeastConfig
    report: FeastSourceReport

    def __init__(self, ctx: PipelineContext, config: FeastConfig):
        super().__init__(ctx)
        self.config = config
        self.report = FeastSourceReport()

        self.feast_client = Client(core_url=self.config.core_url, **self.config.options)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "FeastSource":
        config = FeastConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_field_type(self, field_type: str, table_name: str) -> str:
        """
        Maps types encountered in Feast to corresponding schema types.

        Parameters
        ----------
            field_type:
                type of a Python object
            table_name:
                name of table (for logging)
        """
        TypeClass = _field_type_mapping.get(field_type)

        if TypeClass is None:
            self.report.report_warning(
                table_name, f"unable to map type {field_type} to metadata schema"
            )
            TypeClass = MLFeatureDataType.USELESS

        return TypeClass

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        platform = "feast"

        tables = self.feast_client.list_feature_tables()

        # sort tables by name for consistent outputs
        tables = sorted(tables, key=lambda x: x.name)

        for table in tables:

            # sort features by name for consistent outputs
            features = sorted(table.features, key=lambda x: x.name)

            # sort entities by name for consistent outputs
            entities = [
                self.feast_client.get_entity(entity_name)
                for entity_name in table.entities
            ]
            entities = sorted(entities, key=lambda x: x.name)

            # initialize the schema for the collection
            allFeatures: List[MLFeatureSnapshot] = []
            allEntities: List[MLEntitySnapshot] = []

            for feature in features:

                # create snapshot instance for the feature
                feature_snapshot = MLFeatureSnapshot(
                    urn=f"urn:li:mlFeature:(urn:li:dataPlatform:{platform},{table.name},{feature.name})",
                    aspects=[],
                )

                # append feature name and type
                feature_snapshot.aspects.append(
                    MLFeaturePropertiesClass(
                        name=feature.name,
                        dataType=self.get_field_type(feature.dtype.name, table.name),
                    )
                )

                allFeatures.append(feature_snapshot)

                # make the MCE and workunit
                mce = MetadataChangeEvent(proposedSnapshot=feature_snapshot)
                wu = MetadataWorkUnit(id=table.name, mce=mce)
                self.report.report_workunit(wu)
                yield wu

            for entity in entities:

                # create snapshot instance for the entity
                entity_snapshot = MLEntitySnapshot(
                    urn=f"urn:li:mlEntity:(urn:li:dataPlatform:{platform},{table.name},{entity.name})",
                    aspects=[],
                )

                # append entity name and type
                entity_snapshot.aspects.append(
                    MLEntityPropertiesClass(
                        name=entity.name,
                        description=entity.description,
                        dataType=self.get_field_type(
                            entity.value_type.name, table.name
                        ),
                    )
                )

                allEntities.append(entity_snapshot)

                # make the MCE and workunit
                mce = MetadataChangeEvent(proposedSnapshot=entity_snapshot)
                wu = MetadataWorkUnit(id=table.name, mce=mce)
                self.report.report_workunit(wu)
                yield wu

            # create snapshot instance for the featureset
            featureset_snapshot = MLFeatureSetSnapshot(
                urn=f"urn:li:mlFeatureSet:(urn:li:dataPlatform:{platform},{table.name})",
                aspects=[],
            )

            featureset_batch_source = None
            featureset_stream_source = None

            if isinstance(table.batch_source, BigQuerySource):

                featureset_batch_source = "BigQuerySource"

            if isinstance(table.batch_source, FileSource):

                featureset_batch_source = "FileSource"

            if isinstance(table.stream_source, KafkaSource):

                featureset_stream_source = "KafkaSource"

            if isinstance(table.stream_source, KinesisSource):

                featureset_stream_source = "KinesisSource"

            featureset_stream_config = table.to_dict()["spec"].get("streamSource")

            if featureset_stream_config is not None:
                featureset_stream_config = json.dumps(featureset_stream_config)

            featureset_snapshot.aspects.append(
                MLFeatureSetPropertiesClass(
                    mlFeatures=[x.urn for x in allFeatures],
                    mlEntities=[x.urn for x in allEntities],
                    batchSource=featureset_batch_source,
                    streamSource=featureset_stream_source,
                    batchSourceConfig=json.dumps(
                        table.to_dict()["spec"]["batchSource"]
                    ),
                    streamSourceConfig=featureset_stream_config,
                )
            )

            # make the MCE and workunit
            mce = MetadataChangeEvent(proposedSnapshot=featureset_snapshot)
            wu = MetadataWorkUnit(id=table.name, mce=mce)
            self.report.report_workunit(wu)
            yield wu

    def get_report(self) -> FeastSourceReport:
        return self.report

    def close(self):
        return
