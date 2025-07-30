from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple, Union

import feast.types
from feast import (
    BigQuerySource,
    Entity,
    FeatureStore,
    FeatureView,
    Field as FeastField,
    FileSource,
    KafkaSource,
    KinesisSource,
    OnDemandFeatureView,
    RequestSource,
    SnowflakeSource,
    ValueType,
)
from feast.data_source import DataSource
from pydantic import Field

import datahub.emitter.mce_builder as builder
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import MLFeatureDataType
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    MLFeatureSnapshot,
    MLFeatureTableSnapshot,
    MLPrimaryKeySnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    GlobalTagsClass,
    MLFeaturePropertiesClass,
    MLFeatureTablePropertiesClass,
    MLPrimaryKeyPropertiesClass,
    OwnerClass,
    OwnershipClass,
    StatusClass,
    TagAssociationClass,
)

# FIXME: ValueType module cannot be used as a type
_field_type_mapping: Dict[Union[ValueType, feast.types.FeastType], str] = {
    ValueType.UNKNOWN: MLFeatureDataType.UNKNOWN,
    ValueType.BYTES: MLFeatureDataType.BYTE,
    ValueType.STRING: MLFeatureDataType.TEXT,
    ValueType.INT32: MLFeatureDataType.ORDINAL,
    ValueType.INT64: MLFeatureDataType.ORDINAL,
    ValueType.DOUBLE: MLFeatureDataType.CONTINUOUS,
    ValueType.FLOAT: MLFeatureDataType.CONTINUOUS,
    ValueType.BOOL: MLFeatureDataType.BINARY,
    ValueType.UNIX_TIMESTAMP: MLFeatureDataType.TIME,
    ValueType.BYTES_LIST: MLFeatureDataType.SEQUENCE,
    ValueType.STRING_LIST: MLFeatureDataType.SEQUENCE,
    ValueType.INT32_LIST: MLFeatureDataType.SEQUENCE,
    ValueType.INT64_LIST: MLFeatureDataType.SEQUENCE,
    ValueType.DOUBLE_LIST: MLFeatureDataType.SEQUENCE,
    ValueType.FLOAT_LIST: MLFeatureDataType.SEQUENCE,
    ValueType.BOOL_LIST: MLFeatureDataType.SEQUENCE,
    ValueType.UNIX_TIMESTAMP_LIST: MLFeatureDataType.SEQUENCE,
    ValueType.NULL: MLFeatureDataType.UNKNOWN,
    feast.types.Invalid: MLFeatureDataType.UNKNOWN,
    feast.types.Bytes: MLFeatureDataType.BYTE,
    feast.types.String: MLFeatureDataType.TEXT,
    feast.types.Int32: MLFeatureDataType.ORDINAL,
    feast.types.Int64: MLFeatureDataType.ORDINAL,
    feast.types.Float64: MLFeatureDataType.CONTINUOUS,
    feast.types.Float32: MLFeatureDataType.CONTINUOUS,
    feast.types.Bool: MLFeatureDataType.BINARY,
    feast.types.UnixTimestamp: MLFeatureDataType.TIME,
    feast.types.Array: MLFeatureDataType.SEQUENCE,  # type: ignore
    feast.types.Invalid: MLFeatureDataType.UNKNOWN,
}


class FeastRepositorySourceConfig(
    StatefulIngestionConfigBase,
):
    path: str = Field(description="Path to Feast repository")
    fs_yaml_file: Optional[str] = Field(
        default=None,
        description="Path to the `feature_store.yaml` file used to configure the feature store",
    )
    environment: str = Field(
        default=DEFAULT_ENV, description="Environment to use when constructing URNs"
    )
    # owner_mappings example:
    # This must be added to the recipe in order to extract owners, otherwise NO owners will be extracted
    # owner_mappings:
    #   - feast_owner_name: "<owner>"
    #     datahub_owner_urn: "urn:li:corpGroup:<owner>"
    #     datahub_ownership_type: "BUSINESS_OWNER"
    owner_mappings: Optional[List[Dict[str, str]]] = Field(
        default=None, description="Mapping of owner names to owner types"
    )
    enable_owner_extraction: bool = Field(
        default=False,
        description="If this is disabled, then we NEVER try to map owners. "
        "If this is enabled, then owner_mappings is REQUIRED to extract ownership.",
    )
    enable_tag_extraction: bool = Field(
        default=False,
        description="If this is disabled, then we NEVER try to extract tags.",
    )


@platform_name("Feast")
@config_class(FeastRepositorySourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@dataclass
class FeastRepositorySource(StatefulIngestionSourceBase):
    """
    This plugin extracts:

    - Entities as [`MLPrimaryKey`](https://docs.datahub.com/docs/graphql/objects#mlprimarykey)
    - Fields as [`MLFeature`](https://docs.datahub.com/docs/graphql/objects#mlfeature)
    - Feature views and on-demand feature views as [`MLFeatureTable`](https://docs.datahub.com/docs/graphql/objects#mlfeaturetable)
    - Batch and stream source details as [`Dataset`](https://docs.datahub.com/docs/graphql/objects#dataset)
    - Column types associated with each entity and feature
    """

    platform = "feast"
    source_config: FeastRepositorySourceConfig
    report: StaleEntityRemovalSourceReport
    feature_store: FeatureStore

    def __init__(self, config: FeastRepositorySourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.ctx = ctx
        self.report = StaleEntityRemovalSourceReport()
        self.feature_store = FeatureStore(
            repo_path=self.source_config.path,
            fs_yaml_file=self.source_config.fs_yaml_file,
        )

    def _get_field_type(
        self, field_type: Union[ValueType, feast.types.FeastType], parent_name: str
    ) -> str:
        """
        Maps types encountered in Feast to corresponding schema types.
        """

        ml_feature_data_type = _field_type_mapping.get(field_type)

        if ml_feature_data_type is None:
            self.report.report_warning(
                "unable to map type",
                f"unable to map type {field_type} to metadata schema to parent: {parent_name}",
            )

            ml_feature_data_type = MLFeatureDataType.UNKNOWN

        return ml_feature_data_type

    def _get_data_source_details(self, source: DataSource) -> Tuple[str, str]:
        """
        Get Feast batch/stream source platform and name.
        """

        platform = "unknown"
        name = "unknown"

        if isinstance(source, FileSource):
            platform = "file"
            name = source.path.replace("://", ".").replace("/", ".")
        elif isinstance(source, BigQuerySource):
            platform = "bigquery"
            name = source.table
        elif isinstance(source, KafkaSource):
            platform = "kafka"
            name = source.kafka_options.topic
        elif isinstance(source, KinesisSource):
            platform = "kinesis"
            name = (
                f"{source.kinesis_options.region}:{source.kinesis_options.stream_name}"
            )
        elif isinstance(source, RequestSource):
            platform = "request"
            name = source.name
        elif isinstance(source, SnowflakeSource):
            platform = "snowflake"
            name = source.table

        return platform, name

    def _get_data_sources(self, feature_view: FeatureView) -> List[str]:
        """
        Get data source URN list.
        """

        sources = []

        if feature_view.batch_source is not None:
            batch_source_platform, batch_source_name = self._get_data_source_details(
                feature_view.batch_source
            )
            sources.append(
                builder.make_dataset_urn(
                    batch_source_platform,
                    batch_source_name,
                    self.source_config.environment,
                )
            )

        if feature_view.stream_source is not None:
            stream_source_platform, stream_source_name = self._get_data_source_details(
                feature_view.stream_source
            )
            sources.append(
                builder.make_dataset_urn(
                    stream_source_platform,
                    stream_source_name,
                    self.source_config.environment,
                )
            )

        return sources

    def _get_entity_workunit(
        self, feature_view: FeatureView, entity: Entity
    ) -> MetadataWorkUnit:
        """
        Generate an MLPrimaryKey work unit for a Feast entity.
        """

        feature_view_name = f"{self.feature_store.project}.{feature_view.name}"
        aspects = (
            [StatusClass(removed=False)]
            + self._get_tags(entity)
            + self._get_owners(entity)
        )

        entity_snapshot = MLPrimaryKeySnapshot(
            urn=builder.make_ml_primary_key_urn(feature_view_name, entity.name),
            aspects=aspects,
        )

        entity_snapshot.aspects.append(
            MLPrimaryKeyPropertiesClass(
                description=entity.description,
                dataType=self._get_field_type(entity.value_type, entity.name),
                sources=self._get_data_sources(feature_view),
            )
        )

        mce = MetadataChangeEvent(proposedSnapshot=entity_snapshot)

        return MetadataWorkUnit(id=entity.name, mce=mce)

    def _get_feature_workunit(
        self,
        # FIXME: FeatureView and OnDemandFeatureView cannot be used as a type
        feature_view: Union[FeatureView, OnDemandFeatureView],
        field: FeastField,
    ) -> MetadataWorkUnit:
        """
        Generate an MLFeature work unit for a Feast feature.
        """
        feature_view_name = f"{self.feature_store.project}.{feature_view.name}"
        aspects = [StatusClass(removed=False)] + self._get_tags(field)

        feature_snapshot = MLFeatureSnapshot(
            urn=builder.make_ml_feature_urn(feature_view_name, field.name),
            aspects=aspects,
        )

        feature_sources = []
        if isinstance(feature_view, FeatureView):
            feature_sources = self._get_data_sources(feature_view)
        elif isinstance(feature_view, OnDemandFeatureView):
            if feature_view.source_request_sources is not None:
                for request_source in feature_view.source_request_sources.values():
                    source_platform, source_name = self._get_data_source_details(
                        request_source
                    )

                    feature_sources.append(
                        builder.make_dataset_urn(
                            source_platform,
                            source_name,
                            self.source_config.environment,
                        )
                    )

            if feature_view.source_feature_view_projections is not None:
                for (
                    feature_view_projection
                ) in feature_view.source_feature_view_projections.values():
                    feature_view_source = self.feature_store.get_feature_view(
                        feature_view_projection.name
                    )

                    feature_sources.extend(self._get_data_sources(feature_view_source))

        feature_snapshot.aspects.append(
            MLFeaturePropertiesClass(
                description=field.tags.get("description"),
                dataType=self._get_field_type(field.dtype, field.name),
                sources=feature_sources,
            )
        )

        mce = MetadataChangeEvent(proposedSnapshot=feature_snapshot)

        return MetadataWorkUnit(id=field.name, mce=mce)

    def _get_feature_view_workunit(self, feature_view: FeatureView) -> MetadataWorkUnit:
        """
        Generate an MLFeatureTable work unit for a Feast feature view.
        """

        feature_view_name = f"{self.feature_store.project}.{feature_view.name}"
        aspects = (
            [
                BrowsePathsClass(paths=[f"/feast/{self.feature_store.project}"]),
                StatusClass(removed=False),
            ]
            + self._get_tags(feature_view)
            + self._get_owners(feature_view)
        )

        feature_view_snapshot = MLFeatureTableSnapshot(
            urn=builder.make_ml_feature_table_urn("feast", feature_view_name),
            aspects=aspects,
        )

        feature_view_snapshot.aspects.append(
            MLFeatureTablePropertiesClass(
                mlFeatures=[
                    builder.make_ml_feature_urn(
                        feature_view_name,
                        feature.name,
                    )
                    for feature in feature_view.features
                ],
                mlPrimaryKeys=[
                    builder.make_ml_primary_key_urn(feature_view_name, entity_name)
                    for entity_name in feature_view.entities
                ],
            )
        )

        mce = MetadataChangeEvent(proposedSnapshot=feature_view_snapshot)

        return MetadataWorkUnit(id=feature_view_name, mce=mce)

    def _get_on_demand_feature_view_workunit(
        self, on_demand_feature_view: OnDemandFeatureView
    ) -> MetadataWorkUnit:
        """
        Generate an MLFeatureTable work unit for a Feast on-demand feature view.
        """

        on_demand_feature_view_name = (
            f"{self.feature_store.project}.{on_demand_feature_view.name}"
        )

        on_demand_feature_view_snapshot = MLFeatureTableSnapshot(
            urn=builder.make_ml_feature_table_urn("feast", on_demand_feature_view_name),
            aspects=[
                BrowsePathsClass(paths=[f"/feast/{self.feature_store.project}"]),
                StatusClass(removed=False),
            ],
        )

        on_demand_feature_view_snapshot.aspects.append(
            MLFeatureTablePropertiesClass(
                mlFeatures=[
                    builder.make_ml_feature_urn(
                        on_demand_feature_view_name,
                        feature.name,
                    )
                    for feature in on_demand_feature_view.features
                ],
                mlPrimaryKeys=[],
            )
        )

        mce = MetadataChangeEvent(proposedSnapshot=on_demand_feature_view_snapshot)

        return MetadataWorkUnit(id=on_demand_feature_view_name, mce=mce)

    # If a tag is specified in a Feast object, then the tag will be ingested into Datahub if enable_tag_extraction is
    # True, otherwise NO tags will be ingested
    def _get_tags(self, obj: Union[Entity, FeatureView, FeastField]) -> list:
        """
        Extracts tags from the given object and returns a list of aspects.
        """
        aspects: List[Union[GlobalTagsClass]] = []

        # Extract tags
        if self.source_config.enable_tag_extraction:
            if obj.tags.get("name"):
                tag_name: str = obj.tags["name"]
                tag_association = TagAssociationClass(
                    tag=builder.make_tag_urn(tag_name)
                )
                global_tags_aspect = GlobalTagsClass(tags=[tag_association])
                aspects.append(global_tags_aspect)

        return aspects

    # If an owner is specified in a Feast object, it will only be ingested into Datahub if owner_mappings is specified
    # and enable_owner_extraction is True in FeastRepositorySourceConfig, otherwise NO owners will be ingested
    def _get_owners(self, obj: Union[Entity, FeatureView, FeastField]) -> list:
        """
        Extracts owners from the given object and returns a list of aspects.
        """
        aspects: List[Union[OwnershipClass]] = []

        # Extract owner
        if self.source_config.enable_owner_extraction:
            owner = getattr(obj, "owner", None)
            if owner:
                # Create owner association, skipping if None
                owner_association = self._create_owner_association(owner)
                if owner_association:  # Only add valid owner associations
                    owners_aspect = OwnershipClass(owners=[owner_association])
                    aspects.append(owners_aspect)

        return aspects

    def _create_owner_association(self, owner: str) -> Optional[OwnerClass]:
        """
        Create an OwnerClass instance for the given owner using the owner mappings.
        """
        if self.source_config.owner_mappings is not None:
            for mapping in self.source_config.owner_mappings:
                if mapping["feast_owner_name"] == owner:
                    ownership_type_class: str = mapping.get(
                        "datahub_ownership_type", "TECHNICAL_OWNER"
                    )
                    datahub_owner_urn = mapping.get("datahub_owner_urn")
                    if datahub_owner_urn:
                        return OwnerClass(
                            owner=datahub_owner_urn,
                            type=ownership_type_class,
                        )
        return None

    @classmethod
    def create(cls, config_dict, ctx):
        config = FeastRepositorySourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for feature_view in self.feature_store.list_feature_views():
            for entity_name in feature_view.entities:
                entity = self.feature_store.get_entity(entity_name)
                yield self._get_entity_workunit(feature_view, entity)

            for field in feature_view.features:
                yield self._get_feature_workunit(feature_view, field)

            yield self._get_feature_view_workunit(feature_view)

        for on_demand_feature_view in self.feature_store.list_on_demand_feature_views():
            for feature in on_demand_feature_view.features:
                yield self._get_feature_workunit(on_demand_feature_view, feature)

            yield self._get_on_demand_feature_view_workunit(on_demand_feature_view)

    def get_report(self) -> SourceReport:
        return self.report
