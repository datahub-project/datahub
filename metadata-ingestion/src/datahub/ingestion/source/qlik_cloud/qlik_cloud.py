import logging
from datetime import datetime
from typing import Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_dataset_to_container, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.qlik_cloud.config import (
    Constant,
    QlikSourceConfig,
    QlikSourceReport,
)
from datahub.ingestion.source.qlik_cloud.data_classes import (
    FIELD_TYPE_MAPPING,
    App,
    AppKey,
    QlikDataset,
    SchemaField as QlikDatasetSchemaField,
    Space,
    SpaceKey,
    SpaceType,
)
from datahub.ingestion.source.qlik_cloud.qlik_api import QlikAPI
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    Status,
    SubTypes,
    TimeStamp,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    MySqlDDL,
    NullType,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldDataTypeClass,
)

# Logger instance
logger = logging.getLogger(__name__)


@platform_name("Qlik Cloud")
@config_class(QlikSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class QlikCloudSource(StatefulIngestionSourceBase, TestableSource):
    """
    This plugin extracts Qlik Cloud Spaces, Apps, and Datasets
    """

    config: QlikSourceConfig
    reporter: QlikSourceReport
    platform: str = "qlik-cloud"

    def __init__(self, config: QlikSourceConfig, ctx: PipelineContext):
        super(QlikCloudSource, self).__init__(config, ctx)
        self.config = config
        self.reporter = QlikSourceReport()
        try:
            self.qlik_api = QlikAPI(self.config)
        except Exception as e:
            logger.warning(e)
            exit(
                1
            )  # Exit pipeline as we are not able to connect to Qlik Client Service.

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            QlikAPI(QlikSourceConfig.parse_obj_allow_extras(config_dict))
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    @classmethod
    def create(cls, config_dict, ctx):
        config = QlikSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _gen_space_key(self, space_id: str) -> SpaceKey:
        return SpaceKey(
            space=space_id,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def _gen_app_key(self, app_id: str) -> AppKey:
        return AppKey(
            app=app_id,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def _gen_personal_space(self) -> Space:
        return Space(
            id=Constant.PERSONAL_SPACE_ID,
            name=Constant.PERSONAL_SPACE_NAME,
            description="",
            type=SpaceType.PERSONAL,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

    def _get_allowed_spaces(self) -> List[Space]:
        all_spaces = self.qlik_api.get_spaces()
        allowed_spaces = [
            space
            for space in all_spaces
            if self.config.space_pattern.allowed(space.name)
        ]
        # Add personal space entity if flag is enable
        if self.config.extract_personal_entity:
            allowed_spaces.append(self._gen_personal_space())
        logger.info(f"Number of spaces = {len(allowed_spaces)}")
        self.reporter.report_number_of_spaces(len(all_spaces))
        logger.info(f"Number of allowed spaces = {len(allowed_spaces)}")
        return allowed_spaces

    def _gen_space_workunit(self, space: Space) -> Iterable[MetadataWorkUnit]:
        yield from gen_containers(
            container_key=self._gen_space_key(space.id),
            name=space.name,
            description=space.description,
            sub_types=[BIContainerSubTypes.QLIK_SPACE],
            extra_properties={Constant.TYPE: str(space.type)},
            owner_urn=make_user_urn(space.owner_id)
            if self.config.ingest_owner and space.owner_id
            else None,
            created=int(space.created_at.timestamp() * 1000),
            last_modified=int(space.updated_at.timestamp() * 1000),
        )

    def _gen_app_workunit(self, app: App) -> Iterable[MetadataWorkUnit]:
        yield from gen_containers(
            container_key=self._gen_app_key(app.id),
            name=app.name,
            description=app.description,
            sub_types=[BIContainerSubTypes.QLIK_APP],
            parent_container_key=self._gen_space_key(app.space_id),
            extra_properties={Constant.QRI: app.qri, Constant.USAGE: app.usage},
            owner_urn=make_user_urn(app.owner_id) if self.config.ingest_owner else None,
            created=int(app.created_at.timestamp() * 1000),
            last_modified=int(app.updated_at.timestamp() * 1000),
        )

    def _gen_dataset_urn(self, dataset_identifier: str) -> str:
        return make_dataset_urn_with_platform_instance(
            name=dataset_identifier,
            env=self.config.env,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
        )

    def gen_dataplatform_instance_aspect(
        self, dataset_urn: str
    ) -> Optional[MetadataWorkUnit]:
        if self.config.platform_instance:
            aspect = DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=make_dataplatform_instance_urn(
                    self.platform, self.config.platform_instance
                ),
            )
            return MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=aspect
            ).as_workunit()
        else:
            return None

    def _gen_dataset_owner_aspect(
        self, dataset_urn: str, owner_id: str
    ) -> MetadataWorkUnit:
        aspect = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=make_user_urn(owner_id), type=OwnershipTypeClass.DATAOWNER
                )
            ]
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=aspect,
        ).as_workunit()

    def _gen_schema_fields(
        self, schema: List[QlikDatasetSchemaField]
    ) -> List[SchemaField]:
        schema_fields: List[SchemaField] = []
        for field in schema:
            schema_field = SchemaField(
                fieldPath=field.name,
                type=SchemaFieldDataTypeClass(
                    type=FIELD_TYPE_MAPPING.get(field.data_type, NullType)()
                ),
                # NOTE: nativeDataType will not be in sync with older connector
                nativeDataType=field.data_type,
                nullable=field.nullable,
                isPartOfKey=field.primary_key,
            )
            schema_fields.append(schema_field)
        return schema_fields

    def _gen_schema_metadata(self, dataset: QlikDataset) -> MetadataWorkUnit:
        dataset_urn = self._gen_dataset_urn(dataset.id)

        schema_metadata = SchemaMetadata(
            schemaName=dataset.id,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
            fields=self._gen_schema_fields(dataset.schema),
        )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=schema_metadata
        ).as_workunit()

    def _gen_dataset_properties(self, dataset: QlikDataset) -> MetadataWorkUnit:
        dataset_urn = self._gen_dataset_urn(dataset.id)

        dataset_properties = DatasetProperties(
            name=dataset.name,
            description=dataset.description,
            qualifiedName=dataset.name,
            created=TimeStamp(time=int(dataset.created_at.timestamp() * 1000)),
            lastModified=TimeStamp(time=int(dataset.updated_at.timestamp() * 1000)),
        )
        dataset_properties.customProperties.update(
            {
                Constant.QRI: dataset.qri,
                Constant.SPACEID: dataset.space_id,
                Constant.TYPE: dataset.type,
                Constant.SIZE: str(dataset.size),
                Constant.ROWCOUNT: str(dataset.row_count),
            }
        )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

    def _gen_dataset_workunit(self, dataset: QlikDataset) -> Iterable[MetadataWorkUnit]:
        dataset_urn = self._gen_dataset_urn(dataset.id)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=Status(removed=False)
        ).as_workunit()

        yield self._gen_schema_metadata(dataset)

        yield self._gen_dataset_properties(dataset)

        yield from add_dataset_to_container(
            container_key=self._gen_space_key(dataset.space_id),
            dataset_urn=dataset_urn,
        )

        dpi_aspect = self.gen_dataplatform_instance_aspect(dataset_urn)
        if dpi_aspect:
            yield dpi_aspect

        if self.config.ingest_owner:
            yield self._gen_dataset_owner_aspect(dataset_urn, dataset.owner_id)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypes(typeNames=[DatasetSubTypes.QLIK_DATASET]),
        ).as_workunit()

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invoke this method
        """
        logger.info("Qlik Cloud plugin execution is started")
        for space in self._get_allowed_spaces():
            yield from self._gen_space_workunit(space)
        for item in self.qlik_api.get_items():
            # If item is personal item and flag is Disable, skip ingesting personal item
            if not item.space_id:
                item.space_id = Constant.PERSONAL_SPACE_ID
                if not self.config.extract_personal_entity:
                    continue
            if isinstance(item, App):
                yield from self._gen_app_workunit(item)
            elif isinstance(item, QlikDataset):
                yield from self._gen_dataset_workunit(item)

    def get_report(self) -> SourceReport:
        return self.reporter
