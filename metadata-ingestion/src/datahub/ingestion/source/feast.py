import json
import os
import tempfile
from dataclasses import dataclass, field
from shlex import quote
from typing import Dict, Iterable, List

import docker

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import MLFeatureDataType
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    MLFeatureSnapshot,
    MLFeatureTableSnapshot,
    MLPrimaryKeySnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    MLFeaturePropertiesClass,
    MLFeatureTablePropertiesClass,
    MLPrimaryKeyPropertiesClass,
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

# image to use for initial feast extraction
HOSTED_FEAST_IMAGE = "acryldata/datahub-ingestion-feast-wrapper"


class FeastConfig(ConfigModel):
    core_url: str = "localhost:6565"
    env: str = DEFAULT_ENV
    use_local_build: bool = False


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

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "FeastSource":
        config = FeastConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_field_type(self, field_type: str, parent_name: str) -> str:
        """
        Maps types encountered in Feast to corresponding schema types.

        Parameters
        ----------
            field_type:
                type of a Feast object
            parent_name:
                name of table (for logging)
        """
        enum_type = _field_type_mapping.get(field_type)

        if enum_type is None:
            self.report.report_warning(
                parent_name, f"unable to map type {field_type} to metadata schema"
            )
            enum_type = MLFeatureDataType.UNKNOWN

        return enum_type

    def get_entity_wu(self, ingest_table, ingest_entity):
        """
        Generate an MLPrimaryKey workunit for a Feast entity.

        Parameters
        ----------
            ingest_table:
                ingested Feast table
            ingest_entity:
                ingested Feast entity
        """

        # create snapshot instance for the entity
        entity_snapshot = MLPrimaryKeySnapshot(
            urn=builder.make_ml_primary_key_urn(
                ingest_table["name"], ingest_entity["name"]
            ),
            aspects=[],
        )

        entity_sources = []

        if ingest_entity["batch_source"] is not None:
            entity_sources.append(
                builder.make_dataset_urn(
                    ingest_entity["batch_source_platform"],
                    ingest_entity["batch_source_name"],
                    self.config.env,
                )
            )

        if ingest_entity["stream_source"] is not None:
            entity_sources.append(
                builder.make_dataset_urn(
                    ingest_entity["stream_source_platform"],
                    ingest_entity["stream_source_name"],
                    self.config.env,
                )
            )

        # append entity name and type
        entity_snapshot.aspects.append(
            MLPrimaryKeyPropertiesClass(
                description=ingest_entity["description"],
                dataType=self.get_field_type(
                    ingest_entity["type"], ingest_entity["name"]
                ),
                sources=entity_sources,
            )
        )

        # make the MCE and workunit
        mce = MetadataChangeEvent(proposedSnapshot=entity_snapshot)
        return MetadataWorkUnit(id=ingest_entity["name"], mce=mce)

    def get_feature_wu(self, ingest_table, ingest_feature):
        """
        Generate an MLFeature workunit for a Feast feature.

        Parameters
        ----------
            ingest_table:
                ingested Feast table
            ingest_feature:
                ingested Feast feature
        """

        # create snapshot instance for the feature
        feature_snapshot = MLFeatureSnapshot(
            urn=builder.make_ml_feature_urn(
                ingest_table["name"], ingest_feature["name"]
            ),
            aspects=[],
        )

        feature_sources = []

        if ingest_feature["batch_source"] is not None:
            feature_sources.append(
                builder.make_dataset_urn(
                    ingest_feature["batch_source_platform"],
                    ingest_feature["batch_source_name"],
                    self.config.env,
                )
            )

        if ingest_feature["stream_source"] is not None:
            feature_sources.append(
                builder.make_dataset_urn(
                    ingest_feature["stream_source_platform"],
                    ingest_feature["stream_source_name"],
                    self.config.env,
                )
            )

        # append feature name and type
        feature_snapshot.aspects.append(
            MLFeaturePropertiesClass(
                dataType=self.get_field_type(
                    ingest_feature["type"], ingest_feature["name"]
                ),
                sources=feature_sources,
            )
        )

        # make the MCE and workunit
        mce = MetadataChangeEvent(proposedSnapshot=feature_snapshot)
        return MetadataWorkUnit(id=ingest_feature["name"], mce=mce)

    def get_feature_table_wu(self, ingest_table):
        """
        Generate an MLFeatureTable workunit for a Feast feature table.

        Parameters
        ----------
            ingest_table:
                ingested Feast table
        """

        featuretable_snapshot = MLFeatureTableSnapshot(
            urn=builder.make_ml_feature_table_urn("feast", ingest_table["name"]),
            aspects=[
                BrowsePathsClass(paths=[f"feast/{ingest_table['name']}"]),
            ],
        )

        featuretable_snapshot.aspects.append(
            MLFeatureTablePropertiesClass(
                mlFeatures=[
                    builder.make_ml_feature_urn(
                        ingest_table["name"],
                        feature["name"],
                    )
                    for feature in ingest_table["features"]
                ],
                # a feature table can have multiple primary keys, which then act as a composite key
                mlPrimaryKeys=[
                    builder.make_ml_primary_key_urn(
                        ingest_table["name"], entity["name"]
                    )
                    for entity in ingest_table["entities"]
                ],
            )
        )

        # make the MCE and workunit
        mce = MetadataChangeEvent(proposedSnapshot=featuretable_snapshot)
        return MetadataWorkUnit(id=ingest_table["name"], mce=mce)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        with tempfile.NamedTemporaryFile(suffix=".json") as tf:

            docker_client = docker.from_env()

            feast_image = HOSTED_FEAST_IMAGE

            # build the image locally if specified
            if self.config.use_local_build:
                dirname = os.path.dirname(__file__)
                image_directory = os.path.join(dirname, "feast_image/")

                image, _ = docker_client.images.build(path=image_directory)

                feast_image = image.id

            docker_client.containers.run(
                feast_image,
                f"python3 ingest.py --core_url={quote(self.config.core_url)} --output_path=/out.json",
                # allow the image to access the core URL if on host
                network_mode="host",
                # mount the tempfile so the Docker image has access
                volumes={
                    tf.name: {"bind": "/out.json", "mode": "rw"},
                },
            )

            ingest = json.load(tf)

            # ingest tables
            for ingest_table in ingest:

                # ingest entities in table
                for ingest_entity in ingest_table["entities"]:

                    wu = self.get_entity_wu(ingest_table, ingest_entity)
                    self.report.report_workunit(wu)
                    yield wu

                # ingest features in table
                for ingest_feature in ingest_table["features"]:

                    wu = self.get_feature_wu(ingest_table, ingest_feature)
                    self.report.report_workunit(wu)
                    yield wu

                wu = self.get_feature_table_wu(ingest_table)
                self.report.report_workunit(wu)
                yield wu

    def get_report(self) -> FeastSourceReport:
        return self.report

    def close(self):
        return
