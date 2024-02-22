import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_entity_to_container, gen_containers
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
from datahub.ingestion.source.qlik_sense.config import (
    Constant,
    PlatformDetail,
    QlikSourceConfig,
    QlikSourceReport,
)
from datahub.ingestion.source.qlik_sense.data_classes import (
    FIELD_TYPE_MAPPING,
    KNOWN_DATA_PLATFORM_MAPPING,
    App,
    AppKey,
    BoxType,
    Chart,
    QlikDataset,
    QlikTable,
    SchemaField as QlikDatasetSchemaField,
    Sheet,
    Space,
    SpaceKey,
)
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
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
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    DatasetProperties,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    MySqlDDL,
    NullType,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeAuditStampsClass,
    ChartInfoClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldDataTypeClass,
)
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

# Logger instance
logger = logging.getLogger(__name__)


@platform_name("Qlik Sense")
@config_class(QlikSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.OWNERSHIP,
    "Enabled by default, configured using `ingest_owner`",
)
class QlikSenseSource(StatefulIngestionSourceBase, TestableSource):
    """
    This plugin extracts the following:
    - Qlik Sense Spaces and Apps as Container.
    - Qlik Datasets
    - Sheets as dashboard and its charts
    """

    config: QlikSourceConfig
    reporter: QlikSourceReport
    platform: str = "qlik-sense"

    def __init__(self, config: QlikSourceConfig, ctx: PipelineContext):
        super(QlikSenseSource, self).__init__(config, ctx)
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

    def _get_audit_stamp(self, date: datetime, username: str) -> AuditStampClass:
        return AuditStampClass(
            time=int(date.timestamp() * 1000),
            actor=builder.make_user_urn(username),
        )

    def _get_allowed_spaces(self) -> List[Space]:
        all_spaces = self.qlik_api.get_spaces()
        allowed_spaces = [
            space
            for space in all_spaces
            if self.config.space_pattern.allowed(space.name)
        ]
        logger.info(f"Number of spaces = {len(all_spaces)}")
        self.reporter.report_number_of_spaces(len(all_spaces))
        logger.info(f"Number of allowed spaces = {len(allowed_spaces)}")
        return allowed_spaces

    def _gen_space_workunit(self, space: Space) -> Iterable[MetadataWorkUnit]:
        """
        Map Qlik space to Datahub container
        """
        owner_username: Optional[str] = None
        if space.ownerId:
            owner_username = self.qlik_api.get_user_name(space.ownerId)
        yield from gen_containers(
            container_key=self._gen_space_key(space.id),
            name=space.name,
            description=space.description,
            sub_types=[BIContainerSubTypes.QLIK_SPACE],
            extra_properties={Constant.TYPE: str(space.type)},
            owner_urn=builder.make_user_urn(owner_username)
            if self.config.ingest_owner and owner_username
            else None,
            external_url=f"https://{self.config.tenant_hostname}/catalog?space_filter={space.id}",
            created=int(space.createdAt.timestamp() * 1000),
            last_modified=int(space.updatedAt.timestamp() * 1000),
        )

    def _gen_entity_status_aspect(self, entity_urn: str) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn, aspect=Status(removed=False)
        ).as_workunit()

    def _gen_entity_owner_aspect(
        self, entity_urn: str, user_name: str
    ) -> MetadataWorkUnit:
        aspect = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=builder.make_user_urn(user_name),
                    type=OwnershipTypeClass.DATAOWNER,
                )
            ]
        )
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=aspect,
        ).as_workunit()

    def _gen_dashboard_urn(self, dashboard_identifier: str) -> str:
        return builder.make_dashboard_urn(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            name=dashboard_identifier,
        )

    def _gen_dashboard_info_workunit(
        self, sheet: Sheet, app_id: str
    ) -> MetadataWorkUnit:
        dashboard_urn = self._gen_dashboard_urn(sheet.id)
        custom_properties: Dict[str, str] = {"chartCount": str(len(sheet.charts))}
        dashboard_info_cls = DashboardInfoClass(
            title=sheet.title,
            description=sheet.description,
            charts=[
                builder.make_chart_urn(
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    name=chart.qId,
                )
                for chart in sheet.charts
            ],
            lastModified=ChangeAuditStampsClass(
                created=self._get_audit_stamp(sheet.createdAt, sheet.ownerId),
                lastModified=self._get_audit_stamp(sheet.updatedAt, sheet.ownerId),
            ),
            customProperties=custom_properties,
            dashboardUrl=f"https://{self.config.tenant_hostname}/sense/app/{app_id}/sheet/{sheet.id}/state/analysis",
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn, aspect=dashboard_info_cls
        ).as_workunit()

    def _gen_charts_workunit(
        self, charts: List[Chart], input_tables: List[QlikTable], app_id: str
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Qlik Chart to Datahub Chart
        """
        input_tables_urns: List[str] = []
        for table in input_tables:
            table_identifier = self._get_app_table_identifier(table)
            if table_identifier:
                input_tables_urns.append(self._gen_qlik_dataset_urn(table_identifier))

        for chart in charts:
            chart_urn = builder.make_chart_urn(
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                name=chart.qId,
            )

            yield self._gen_entity_status_aspect(chart_urn)

            custom_properties = {
                "Dimension": str(chart.qDimension),
                "Measure": str(chart.qMeasure),
            }

            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=ChartInfoClass(
                    title=chart.title,
                    description=chart.visualization,
                    lastModified=ChangeAuditStampsClass(),
                    customProperties=custom_properties,
                    inputs=input_tables_urns,
                ),
            ).as_workunit()

            yield from add_entity_to_container(
                container_key=self._gen_app_key(app_id),
                entity_type="chart",
                entity_urn=chart_urn,
            )

    def _gen_sheets_workunit(self, app: App) -> Iterable[MetadataWorkUnit]:
        """
        Map Qlik Sheet to Datahub dashboard
        """
        for sheet in app.sheets:
            dashboard_urn = self._gen_dashboard_urn(sheet.id)

            yield self._gen_entity_status_aspect(dashboard_urn)

            yield self._gen_dashboard_info_workunit(sheet, app.id)

            yield from add_entity_to_container(
                container_key=self._gen_app_key(app.id),
                entity_type="dashboard",
                entity_urn=dashboard_urn,
            )

            dpi_aspect = self._gen_dataplatform_instance_aspect(dashboard_urn)
            if dpi_aspect:
                yield dpi_aspect

            owner_username = self.qlik_api.get_user_name(sheet.ownerId)
            if self.config.ingest_owner and owner_username:
                yield self._gen_entity_owner_aspect(dashboard_urn, owner_username)

            yield from self._gen_charts_workunit(sheet.charts, app.tables, app.id)

    def _gen_app_table_upstream_lineage(
        self, dataset_urn: str, table: QlikTable
    ) -> Optional[MetadataWorkUnit]:
        upstream_dataset_urn: Optional[str] = None
        if table.type == BoxType.BLACKBOX:
            upstream_dataset_platform_detail = (
                self.config.data_connection_to_platform_instance.get(
                    table.dataconnectorName,
                    PlatformDetail(
                        env=self.config.env,
                        platform_instance=self.config.platform_instance,
                    ),
                )
            )
            if not table.selectStatement:
                return None
            upstream_dataset_urn = create_lineage_sql_parsed_result(
                query=table.selectStatement.strip(),
                default_db=None,
                platform=KNOWN_DATA_PLATFORM_MAPPING.get(
                    table.dataconnectorPlatform, table.dataconnectorPlatform
                ),
                env=upstream_dataset_platform_detail.env,
                platform_instance=upstream_dataset_platform_detail.platform_instance,
            ).in_tables[0]
        elif table.type == BoxType.LOADFILE:
            upstream_dataset_urn = self._gen_qlik_dataset_urn(
                f"{table.spaceId}.{table.databaseName}".lower()
            )

        if upstream_dataset_urn:
            # Generate finegrained lineage
            fine_grained_lineages = [
                FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=[
                        builder.make_schema_field_urn(upstream_dataset_urn, field.name)
                    ],
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=[
                        builder.make_schema_field_urn(dataset_urn, field.name)
                    ],
                )
                for field in table.datasetSchema
            ]
            return MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineage(
                    upstreams=[
                        Upstream(
                            dataset=upstream_dataset_urn, type=DatasetLineageType.COPY
                        )
                    ],
                    fineGrainedLineages=fine_grained_lineages,
                ),
            ).as_workunit()
        else:
            return None

    def _gen_app_table_properties(
        self, dataset_urn: str, table: QlikTable
    ) -> MetadataWorkUnit:
        dataset_properties = DatasetProperties(
            name=table.tableName,
            qualifiedName=table.tableAlias,
        )
        dataset_properties.customProperties.update(
            {
                Constant.TYPE: "Qlik Table",
                Constant.DATACONNECTORID: table.dataconnectorid,
                Constant.DATACONNECTORNAME: table.dataconnectorName,
            }
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

    def _get_app_table_identifier(self, table: QlikTable) -> Optional[str]:
        if table.tableQri:
            return table.tableQri.lower()
        return None

    def _gen_app_tables_workunit(
        self, tables: List[QlikTable], app_id: str
    ) -> Iterable[MetadataWorkUnit]:
        for table in tables:
            table_identifier = self._get_app_table_identifier(table)
            if not table_identifier:
                continue
            dataset_urn = self._gen_qlik_dataset_urn(table_identifier)

            yield self._gen_entity_status_aspect(dataset_urn)

            yield self._gen_schema_metadata(table_identifier, table.datasetSchema)

            yield self._gen_app_table_properties(dataset_urn, table)

            yield from add_entity_to_container(
                container_key=self._gen_app_key(app_id),
                entity_type="dataset",
                entity_urn=dataset_urn,
            )

            dpi_aspect = self._gen_dataplatform_instance_aspect(dataset_urn)
            if dpi_aspect:
                yield dpi_aspect

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypes(typeNames=[DatasetSubTypes.QLIK_DATASET]),
            ).as_workunit()

            upstream_lineage_workunit = self._gen_app_table_upstream_lineage(
                dataset_urn, table
            )
            if upstream_lineage_workunit:
                yield upstream_lineage_workunit

    def _gen_app_workunit(self, app: App) -> Iterable[MetadataWorkUnit]:
        """
        Map Qlik App to Datahub container
        """
        owner_username = self.qlik_api.get_user_name(app.ownerId)
        yield from gen_containers(
            container_key=self._gen_app_key(app.id),
            name=app.qTitle,
            description=app.description,
            sub_types=[BIContainerSubTypes.QLIK_APP],
            parent_container_key=self._gen_space_key(app.spaceId),
            extra_properties={Constant.QRI: app.qri, Constant.USAGE: app.qUsage},
            owner_urn=builder.make_user_urn(owner_username)
            if self.config.ingest_owner and owner_username
            else None,
            external_url=f"https://{self.config.tenant_hostname}/sense/app/{app.id}/overview",
            created=int(app.createdAt.timestamp() * 1000),
            last_modified=int(app.updatedAt.timestamp() * 1000),
        )
        yield from self._gen_sheets_workunit(app)
        yield from self._gen_app_tables_workunit(app.tables, app.id)

    def _gen_qlik_dataset_urn(self, dataset_identifier: str) -> str:
        return builder.make_dataset_urn_with_platform_instance(
            name=dataset_identifier,
            env=self.config.env,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
        )

    def _gen_dataplatform_instance_aspect(
        self, entity_urn: str
    ) -> Optional[MetadataWorkUnit]:
        if self.config.platform_instance:
            aspect = DataPlatformInstanceClass(
                platform=builder.make_data_platform_urn(self.platform),
                instance=builder.make_dataplatform_instance_urn(
                    self.platform, self.config.platform_instance
                ),
            )
            return MetadataChangeProposalWrapper(
                entityUrn=entity_urn, aspect=aspect
            ).as_workunit()
        else:
            return None

    def _gen_schema_fields(
        self, schema: List[QlikDatasetSchemaField]
    ) -> List[SchemaField]:
        schema_fields: List[SchemaField] = []
        for field in schema:
            schema_field = SchemaField(
                fieldPath=field.name,
                type=SchemaFieldDataTypeClass(
                    type=FIELD_TYPE_MAPPING.get(field.dataType, NullType)()
                    if field.dataType
                    else NullType()
                ),
                nativeDataType=field.dataType if field.dataType else "",
                nullable=field.nullable,
                isPartOfKey=field.primaryKey,
            )
            schema_fields.append(schema_field)
        return schema_fields

    def _gen_schema_metadata(
        self, dataset_identifier: str, dataset_schema: List[QlikDatasetSchemaField]
    ) -> MetadataWorkUnit:
        dataset_urn = self._gen_qlik_dataset_urn(dataset_identifier)

        schema_metadata = SchemaMetadata(
            schemaName=dataset_identifier,
            platform=builder.make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
            fields=self._gen_schema_fields(dataset_schema),
        )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=schema_metadata
        ).as_workunit()

    def _gen_dataset_properties(
        self, dataset_urn: str, dataset: QlikDataset
    ) -> MetadataWorkUnit:
        dataset_properties = DatasetProperties(
            name=dataset.name,
            description=dataset.description,
            qualifiedName=dataset.name,
            externalUrl=f"https://{self.config.tenant_hostname}/dataset/{dataset.itemId}",
            created=TimeStamp(time=int(dataset.createdAt.timestamp() * 1000)),
            lastModified=TimeStamp(time=int(dataset.updatedAt.timestamp() * 1000)),
        )
        dataset_properties.customProperties.update(
            {
                Constant.QRI: dataset.secureQri,
                Constant.SPACEID: dataset.spaceId,
                Constant.TYPE: "Qlik Dataset",
                Constant.DATASETTYPE: dataset.type,
                Constant.SIZE: str(dataset.size),
                Constant.ROWCOUNT: str(dataset.rowCount),
            }
        )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

    def _get_qlik_dataset_identifier(self, dataset: QlikDataset) -> str:
        return f"{dataset.spaceId}.{dataset.name}".lower()

    def _gen_dataset_workunit(self, dataset: QlikDataset) -> Iterable[MetadataWorkUnit]:
        dataset_identifier = self._get_qlik_dataset_identifier(dataset)
        dataset_urn = self._gen_qlik_dataset_urn(dataset_identifier)

        yield self._gen_entity_status_aspect(dataset_urn)

        yield self._gen_schema_metadata(dataset_identifier, dataset.datasetSchema)

        yield self._gen_dataset_properties(dataset_urn, dataset)

        yield from add_entity_to_container(
            container_key=self._gen_space_key(dataset.spaceId),
            entity_type="dataset",
            entity_urn=dataset_urn,
        )

        dpi_aspect = self._gen_dataplatform_instance_aspect(dataset_urn)
        if dpi_aspect:
            yield dpi_aspect

        owner_username = self.qlik_api.get_user_name(dataset.ownerId)
        if self.config.ingest_owner and owner_username:
            yield self._gen_entity_owner_aspect(dataset_urn, owner_username)

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
        logger.info("Qlik Sense plugin execution is started")
        for space in self._get_allowed_spaces():
            yield from self._gen_space_workunit(space)
        for item in self.qlik_api.get_items():
            if isinstance(item, App):
                yield from self._gen_app_workunit(item)
            elif isinstance(item, QlikDataset):
                yield from self._gen_dataset_workunit(item)

    def get_report(self) -> SourceReport:
        return self.reporter
