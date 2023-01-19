import logging
import re
from typing import Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    CatalogKey,
    MetastoreKey,
    PlatformKey,
    UnitySchemaKey,
    add_dataset_to_container,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.unity import proxy
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.proxy import Catalog, Metastore, Schema
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageUpstreamType,
    ViewProperties,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    DomainsClass,
    MySqlDDLClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.hive_schema_to_avro import get_schema_fields_for_hive_column
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.source_helpers import (
    auto_stale_entity_removal,
    auto_status_aspect,
    auto_workunit_reporter,
)

logger: logging.Logger = logging.getLogger(__name__)


@platform_name("Databricks")
@config_class(UnityCatalogSourceConfig)
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(SourceCapability.LINEAGE_FINE, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Optionally enabled via `stateful_ingestion.remove_stale_metadata`",
    supported=True,
)
@support_status(SupportStatus.INCUBATING)
class UnityCatalogSource(StatefulIngestionSourceBase, TestableSource):
    """
    This plugin extracts the following metadata from Databricks Unity Catalog:
    - metastores
    - schemas
    - tables and column lineage
    """

    config: UnityCatalogSourceConfig
    unity_catalog_api_proxy: proxy.UnityCatalogApiProxy
    platform: str = "databricks"
    platform_instance_name: str

    def get_report(self) -> UnityCatalogReport:
        return self.report

    def __init__(self, ctx: PipelineContext, config: UnityCatalogSourceConfig):
        super(UnityCatalogSource, self).__init__(config, ctx)

        self.config = config
        self.report: UnityCatalogReport = UnityCatalogReport()
        self.unity_catalog_api_proxy = proxy.UnityCatalogApiProxy(
            config.workspace_url, config.token, report=self.report
        )

        # Determine the platform_instance_name
        self.platform_instance_name = (
            config.workspace_name
            if config.workspace_name is not None
            else config.workspace_url.split("//")[1].split(".")[0]
        )

        # Create and register the stateful ingestion use-case handler.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            source=self,
            config=self.config,
            state_type_class=GenericCheckpointState,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )

        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.config.domain], graph=self.ctx.graph
            )

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            config = UnityCatalogSourceConfig.parse_obj_allow_extras(config_dict)
            report = UnityCatalogReport()
            unity_proxy = proxy.UnityCatalogApiProxy(
                config.workspace_url, config.token, report=report
            )
            if unity_proxy.check_connectivity():
                test_report.basic_connectivity = CapabilityReport(capable=True)
            else:
                test_report.basic_connectivity = CapabilityReport(capable=False)

        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"{e}"
            )
        return test_report

    @classmethod
    def create(cls, config_dict, ctx):
        config = UnityCatalogSourceConfig.parse_obj(config_dict)
        return cls(ctx=ctx, config=config)

    def get_platform_instance_id(self) -> str:
        return self.config.platform_instance or self.platform

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return auto_stale_entity_removal(
            self.stale_entity_removal_handler,
            auto_workunit_reporter(
                self.report,
                auto_status_aspect(self.get_workunits_internal()),
            ),
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self.process_metastores()

    def process_metastores(self) -> Iterable[MetadataWorkUnit]:
        for metastore in self.unity_catalog_api_proxy.metastores():
            if not self.config.metastore_id_pattern.allowed(metastore.metastore_id):
                self.report.metastores.dropped(metastore.metastore_id)
                continue

            logger.info(f"Started to process metastore: {metastore.metastore_id}")
            yield from self.gen_metastore_containers(metastore)
            yield from self.process_catalogs(metastore)

            self.report.metastores.processed(metastore.metastore_id)

    def process_catalogs(
        self, metastore: proxy.Metastore
    ) -> Iterable[MetadataWorkUnit]:
        for catalog in self.unity_catalog_api_proxy.catalogs(metastore=metastore):
            if not self.config.catalog_pattern.allowed(catalog.name):
                self.report.catalogs.dropped(catalog.id)
                continue

            yield from self.gen_catalog_containers(catalog)
            yield from self.process_schemas(catalog)

            self.report.catalogs.processed(catalog.id)

    def process_schemas(self, catalog: proxy.Catalog) -> Iterable[MetadataWorkUnit]:
        for schema in self.unity_catalog_api_proxy.schemas(catalog=catalog):
            if not self.config.schema_pattern.allowed(schema.name):
                self.report.schemas.dropped(schema.id)
                continue

            yield from self.gen_schema_containers(schema)
            yield from self.process_tables(schema)

            self.report.schemas.processed(schema.id)

    def process_tables(self, schema: proxy.Schema) -> Iterable[MetadataWorkUnit]:
        for table in self.unity_catalog_api_proxy.tables(schema=schema):
            filter_table_name = (
                f"{table.schema.catalog.name}.{table.schema.name}.{table.name}"
            )

            if not self.config.table_pattern.allowed(filter_table_name):
                self.report.tables.dropped(table.id, type=table.type)
                continue

            yield from self.process_table(table, schema)

            self.report.tables.processed(table.id, type=table.type)

    def process_table(
        self, table: proxy.Table, schema: proxy.Schema
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn: str = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            platform_instance=self.platform_instance_name,
            name=table.id,
        )
        yield from self.add_table_to_dataset_container(dataset_urn, schema)

        table_props = self._create_table_property_aspect(table)

        view_props = None
        if table.view_definition:
            view_props = self._create_view_property_aspect(table)

        sub_type = self._create_table_sub_type_aspect(table)
        schema_metadata = self._create_schema_metadata_aspect(table)

        domain = self._get_domain_aspect(
            dataset_name=str(
                f"{table.schema.catalog.name}.{table.schema.name}.{table.name}"
            )
        )

        if self.config.include_column_lineage:
            self.unity_catalog_api_proxy.get_column_lineage(table)
            lineage = self._generate_column_lineage_aspect(dataset_urn, table)
        else:
            self.unity_catalog_api_proxy.table_lineage(table)
            lineage = self._generate_lineage_aspect(dataset_urn, table)

        yield from [
            mcp.as_workunit()
            for mcp in MetadataChangeProposalWrapper.construct_many(
                entityUrn=dataset_urn,
                aspects=[
                    table_props,
                    view_props,
                    sub_type,
                    schema_metadata,
                    domain,
                    lineage,
                ],
            )
        ]

    def _generate_column_lineage_aspect(
        self, dataset_urn: str, table: proxy.Table
    ) -> Optional[UpstreamLineageClass]:
        upstreams: List[UpstreamClass] = []
        finegrained_lineages: List[FineGrainedLineage] = []
        for upstream in sorted(table.upstreams.keys()):
            upstream_urn = make_dataset_urn_with_platform_instance(
                self.platform,
                f"{table.schema.catalog.metastore.id}.{upstream}",
                self.platform_instance_name,
            )

            for col in sorted(table.upstreams[upstream].keys()):
                fl = FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=[
                        make_schema_field_urn(upstream_urn, upstream_col)
                        for upstream_col in sorted(table.upstreams[upstream][col])
                    ],
                    downstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    downstreams=[make_schema_field_urn(dataset_urn, col)],
                )
                finegrained_lineages.append(fl)

            upstream_table = UpstreamClass(
                upstream_urn,
                DatasetLineageTypeClass.TRANSFORMED,
            )
            upstreams.append(upstream_table)

        if upstreams:
            return UpstreamLineageClass(
                upstreams=upstreams, fineGrainedLineages=finegrained_lineages
            )
        else:
            return None

    def _generate_lineage_aspect(
        self, dataset_urn: str, table: proxy.Table
    ) -> Optional[UpstreamLineageClass]:
        upstreams: List[UpstreamClass] = []
        for upstream in sorted(table.upstreams.keys()):
            upstream_urn = make_dataset_urn_with_platform_instance(
                self.platform,
                f"{table.schema.catalog.metastore.id}.{upstream}",
                self.platform_instance_name,
            )

            upstream_table = UpstreamClass(
                upstream_urn,
                DatasetLineageTypeClass.TRANSFORMED,
            )
            upstreams.append(upstream_table)

        if upstreams:
            return UpstreamLineageClass(upstreams=upstreams)
        else:
            return None

    def _get_domain_aspect(self, dataset_name: str) -> Optional[DomainsClass]:
        domain_urn = self._gen_domain_urn(dataset_name)
        if not domain_urn:
            return None
        return DomainsClass(domains=[domain_urn])

    def gen_schema_containers(self, schema: Schema) -> Iterable[MetadataWorkUnit]:
        domain_urn = self._gen_domain_urn(f"{schema.catalog.name}.{schema.name}")

        schema_container_key = self.gen_schema_key(schema)
        yield from gen_containers(
            container_key=schema_container_key,
            name=schema.name,
            sub_types=["Schema"],
            parent_container_key=self.gen_catalog_key(catalog=schema.catalog),
            domain_urn=domain_urn,
            description=schema.comment,
        )

    def gen_metastore_containers(
        self, metastore: Metastore
    ) -> Iterable[MetadataWorkUnit]:
        domain_urn = self._gen_domain_urn(metastore.name)

        metastore_container_key = self.gen_metastore_key(metastore)

        yield from gen_containers(
            container_key=metastore_container_key,
            name=metastore.name,
            sub_types=["Metastore"],
            domain_urn=domain_urn,
            description=metastore.comment,
        )

    def gen_catalog_containers(self, catalog: Catalog) -> Iterable[MetadataWorkUnit]:
        domain_urn = self._gen_domain_urn(catalog.name)

        metastore_container_key = self.gen_metastore_key(catalog.metastore)

        catalog_container_key = self.gen_catalog_key(catalog)

        yield from gen_containers(
            container_key=catalog_container_key,
            name=catalog.name,
            sub_types=["Catalog"],
            domain_urn=domain_urn,
            parent_container_key=metastore_container_key,
            description=catalog.comment,
        )

    def gen_schema_key(self, schema: Schema) -> PlatformKey:
        return UnitySchemaKey(
            unity_schema=schema.name,
            platform=self.platform,
            instance=self.config.platform_instance,
            catalog=schema.catalog.name,
            metastore=schema.catalog.metastore.name,
        )

    def gen_metastore_key(self, metastore: Metastore) -> MetastoreKey:
        return MetastoreKey(
            metastore=metastore.name,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def gen_catalog_key(self, catalog: Catalog) -> CatalogKey:
        return CatalogKey(
            catalog=catalog.name,
            metastore=catalog.metastore.name,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def _gen_domain_urn(self, dataset_name: str) -> Optional[str]:
        domain_urn: Optional[str] = None

        for domain, pattern in self.config.domain.items():
            if pattern.allowed(dataset_name):
                domain_urn = make_domain_urn(
                    self.domain_registry.get_domain_urn(domain)
                )

        return domain_urn

    def add_table_to_dataset_container(
        self, dataset_urn: str, schema: Schema
    ) -> Iterable[MetadataWorkUnit]:
        schema_container_key = self.gen_schema_key(schema)
        yield from add_dataset_to_container(
            container_key=schema_container_key,
            dataset_urn=dataset_urn,
        )

    def _create_table_property_aspect(
        self, table: proxy.Table
    ) -> DatasetPropertiesClass:
        custom_properties: dict = {}
        if table.storage_location is not None:
            custom_properties["storage_location"] = table.storage_location
        if table.data_source_format is not None:
            custom_properties["data_source_format"] = table.data_source_format

        custom_properties["generation"] = str(table.generation)
        custom_properties["table_type"] = table.table_type

        custom_properties["created_by"] = table.created_by
        custom_properties["created_at"] = str(table.created_at)
        if table.properties:
            custom_properties["properties"] = str(table.properties)
        custom_properties["table_id"] = table.table_id
        custom_properties["owner"] = table.owner
        custom_properties["updated_by"] = table.updated_by
        custom_properties["updated_at"] = str(table.updated_at)

        return DatasetPropertiesClass(
            name=table.name,
            description=table.comment,
            customProperties=custom_properties,
        )

    def _create_table_sub_type_aspect(self, table: proxy.Table) -> SubTypesClass:
        return SubTypesClass(
            typeNames=["View" if table.table_type.lower() == "view" else "Table"]
        )

    def _create_view_property_aspect(self, table: proxy.Table) -> ViewProperties:
        assert table.view_definition
        return ViewProperties(
            materialized=False, viewLanguage="SQL", viewLogic=table.view_definition
        )

    def _create_schema_metadata_aspect(self, table: proxy.Table) -> SchemaMetadataClass:
        schema_fields: List[SchemaFieldClass] = []

        for column in table.columns:
            schema_fields.extend(self._create_schema_field(column))

        return SchemaMetadataClass(
            schemaName=table.id,
            platform=make_data_platform_urn(self.platform),
            fields=schema_fields,
            hash="",
            version=0,
            platformSchema=MySqlDDLClass(tableSchema=""),
        )

    @staticmethod
    def _create_schema_field(column: proxy.Column) -> List[SchemaFieldClass]:
        _COMPLEX_TYPE = re.compile("^(struct|array)")

        if _COMPLEX_TYPE.match(column.type_text.lower()):
            return get_schema_fields_for_hive_column(
                column.name, column.type_text.lower(), description=column.comment
            )
        else:
            return [
                SchemaFieldClass(
                    fieldPath=column.name,
                    type=column.type_name,
                    nativeDataType=column.type_text,
                    nullable=column.nullable,
                    description=column.comment,
                )
            ]
