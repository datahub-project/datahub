import logging
import re
from typing import Dict, Iterable, List, Optional

import pydantic
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    CatalogKey,
    MetastoreKey,
    PlatformKey,
    UnitySchemaKey,
    add_dataset_to_container,
    add_domain_to_entity_wu,
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
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.unity import proxy
from datahub.ingestion.source.unity.proxy import Catalog, Metastore, Schema
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.ingestion.source.unity.unity_state import UnityCatalogCheckpointState
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.dataset import ViewProperties
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    MySqlDDLClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    SubTypesClass,
)
from datahub.utilities.hive_schema_to_avro import get_schema_fields_for_hive_column
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.source_helpers import (
    auto_stale_entity_removal,
    auto_status_aspect,
)

logger: logging.Logger = logging.getLogger(__name__)


class UnityCatalogStatefulIngestionConfig(StatefulStaleMetadataRemovalConfig):
    """
    Specialization of StatefulStaleMetadataRemovalConfig to adding custom config.
    This will be used to override the stateful_ingestion config param of StatefulIngestionConfigBase
    in the TableauConfig.
    """

    _entity_types: List[str] = Field(default=["dataset", "container"])


class UnityCatalogSourceConfig(StatefulIngestionConfigBase):
    token: str = pydantic.Field(description="Databricks personal access token")
    workspace_url: str = pydantic.Field(description="Databricks workspace url")
    workspace_name: str = pydantic.Field(
        default=None,
        description="Name of the workspace. Default to deployment name present in workspace_url",
    )

    metastore_id_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for metastore_id to filter in ingestion.",
    )

    catalog_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for catalogs to filter in ingestion. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )

    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schemas to filter in ingestion. Specify regex to only match the schema name. e.g. to match all tables in schema analytics, use the regex 'analytics'",
    )
    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in database.schema.table format. e.g. to match all tables starting with customer in Customer catalog and public schema, use the regex 'Customer.public.customer.*'",
    )
    domain: Dict[str, AllowDenyPattern] = Field(
        default=dict(),
        description='Attach domains to catalogs, schemas or tables during ingestion using regex patterns. Domain key can be a guid like *urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba* or a string like "Marketing".) If you provide strings, then datahub will attempt to resolve this name to a guid, and will error out if this fails. There can be multiple domain keys specified.',
    )

    stateful_ingestion: Optional[UnityCatalogStatefulIngestionConfig] = pydantic.Field(
        default=None, description="Unity Catalog Stateful Ingestion Config."
    )


@platform_name("Unity Catalog")
@config_class(UnityCatalogSourceConfig)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
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

    def get_platform_instance_id(self) -> str:
        return self.config.platform_instance or self.platform

    config: UnityCatalogSourceConfig
    unity_catalog_api_proxy: proxy.UnityCatalogApiProxy
    platform: str = "unity-catalog"
    platform_instance_name: str

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # emit metadata work unit to DataHub GMS
        yield from self.process_metastores()

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return auto_stale_entity_removal(
            self.stale_entity_removal_handler,
            auto_status_aspect(self.get_workunits_internal()),
        )

    def get_report(self) -> SourceReport:
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
            state_type_class=UnityCatalogCheckpointState,
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
            UnityCatalogSourceConfig.Config.extra = (
                pydantic.Extra.allow
            )  # we are okay with extra fields during this stage
            config = UnityCatalogSourceConfig.parse_obj(config_dict)
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

    def process_metastores(self) -> Iterable[MetadataWorkUnit]:
        for metastore in self.unity_catalog_api_proxy.metastores():
            if not self.config.metastore_id_pattern.allowed(metastore.metastore_id):
                self.report.filtered.append(f"{metastore.metastore_id}.*.*.*")
                continue
            logger.info(
                f"Started to process metastore: {metastore.metastore_id} ({metastore.name})"
            )
            yield from self.gen_metastore_containers(metastore)
            yield from self.process_catalogs(metastore)
            self.report.increment_scanned_metastore(1)
            logger.info(
                f"Finished to process metastore: {metastore.metastore_id} ({metastore.name})"
            )

    def process_catalogs(
        self, metastore: proxy.Metastore
    ) -> Iterable[MetadataWorkUnit]:

        for catalog in self.unity_catalog_api_proxy.catalogs(metastore=metastore):
            if not self.config.catalog_pattern.allowed(catalog.name):
                self.report.filtered.append(f"{catalog.name}.*.*")
                continue
            yield from self.gen_catalog_containers(catalog)
            self.report.increment_scanned_catalog(1)
            yield from self.process_schemas(catalog)

    def process_schemas(self, catalog: proxy.Catalog) -> Iterable[MetadataWorkUnit]:
        for schema in self.unity_catalog_api_proxy.schemas(catalog=catalog):
            if not self.config.schema_pattern.allowed(schema.name):
                self.report.filtered.append(f"{catalog.name}.{schema.name}.*")
                continue

            yield from self.gen_schema_containers(schema)
            self.report.increment_scanned_schema(1)

            yield from self.process_tables(schema)

    def process_tables(self, schema: proxy.Schema) -> Iterable[MetadataWorkUnit]:
        for table in self.unity_catalog_api_proxy.tables(schema=schema):
            if not self.config.table_pattern.allowed(
                f"{table.schema.catalog.name}.{table.schema.name}.{table.name}"
            ):
                self.report.filtered.append(
                    f"{schema.catalog.name}.{schema.name}.{table.name}"
                )
                continue

            dataset_urn: str = make_dataset_urn_with_platform_instance(
                platform=self.platform,
                platform_instance=self.platform_instance_name,
                name=table.id,
            )
            yield from self.add_table_to_dataset_container(dataset_urn, schema)
            yield self._create_table_property_aspect_mcp(table)
            if table.view_definition:
                yield self._create_view_property_aspect(table)
            yield self._create_table_sub_type_aspect_mcp(table)
            yield self._create_schema_metadata_aspect_mcp(table)

            status = Status(removed=False)
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspect=status,
            )

            wu = MetadataWorkUnit(id=f"status-{dataset_urn}", mcp=mcp)
            self.report.report_workunit(wu)
            yield wu

            yield from self._get_domain_wu(
                dataset_name=str(
                    f"{table.schema.catalog.name}.{table.schema.name}.{table.name}"
                ),
                entity_urn=dataset_urn,
                entity_type="dataset",
            )

            self.report.report_entity_scanned(
                f"{table.schema.catalog.name}.{table.schema.name}.{table.name}",
                table.type,
            )

            self.report.increment_scanned_table(1)

    def _get_domain_wu(
        self,
        dataset_name: str,
        entity_urn: str,
        entity_type: str,
    ) -> Iterable[MetadataWorkUnit]:

        domain_urn = self._gen_domain_urn(dataset_name)
        if domain_urn:
            wus = add_domain_to_entity_wu(
                entity_type=entity_type,
                entity_urn=entity_urn,
                domain_urn=domain_urn,
            )
            for wu in wus:
                self.report.report_workunit(wu)
                yield wu

    def gen_schema_containers(self, schema: Schema) -> Iterable[MetadataWorkUnit]:
        domain_urn = self._gen_domain_urn(f"{schema.catalog.name}.{schema.name}")

        schema_container_key = self.gen_schema_key(schema)
        container_workunits = gen_containers(
            container_key=schema_container_key,
            name=schema.name,
            sub_types=["Schema"],
            parent_container_key=self.gen_catalog_key(catalog=schema.catalog),
            domain_urn=domain_urn,
            description=schema.comment,
        )

        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def gen_metastore_containers(
        self, metastore: Metastore
    ) -> Iterable[MetadataWorkUnit]:
        domain_urn = self._gen_domain_urn(metastore.name)

        metastore_container_key = self.gen_metastore_key(metastore)

        container_workunits = gen_containers(
            container_key=metastore_container_key,
            name=metastore.name,
            sub_types=["Metastore"],
            domain_urn=domain_urn,
            description=metastore.comment,
        )

        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def gen_catalog_containers(self, catalog: Catalog) -> Iterable[MetadataWorkUnit]:
        domain_urn = self._gen_domain_urn(catalog.name)

        metastore_container_key = self.gen_metastore_key(catalog.metastore)

        catalog_container_key = self.gen_catalog_key(catalog)

        container_workunits = gen_containers(
            container_key=catalog_container_key,
            name=catalog.name,
            sub_types=["Catalog"],
            domain_urn=domain_urn,
            parent_container_key=metastore_container_key,
            description=catalog.comment,
        )

        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def gen_schema_key(self, schema: Schema) -> PlatformKey:
        return UnitySchemaKey(
            unity_schema=schema.name,
            platform=self.platform,
            instance=self.config.platform_instance,
            catalog=schema.catalog.name,
            metastore=schema.catalog.metastore.name,
        )

    def gen_metastore_key(self, metastore: Metastore) -> PlatformKey:
        return MetastoreKey(
            metastore=metastore.name,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def gen_catalog_key(self, catalog: Catalog) -> PlatformKey:
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
        container_workunits = add_dataset_to_container(
            container_key=schema_container_key,
            dataset_urn=dataset_urn,
        )
        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def _create_table_property_aspect_mcp(self, table: proxy.Table) -> MetadataWorkUnit:
        dataset_urn: str = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            platform_instance=self.platform_instance_name,
            name=table.id,
        )
        custom_properties: dict = {}
        if table.storage_location is not None:
            custom_properties["storage_location"] = table.storage_location
        if table.data_source_format is not None:
            custom_properties["data_source_format"] = table.data_source_format

        custom_properties["generation"] = str(table.generation)

        custom_properties["created_by"] = table.created_by
        custom_properties["created_at"] = str(table.created_at)
        if table.properties:
            custom_properties["properties"] = str(table.properties)
        custom_properties["table_id"] = table.table_id
        custom_properties["owner"] = table.owner
        custom_properties["updated_by"] = table.updated_by
        custom_properties["updated_at"] = str(table.updated_at)

        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name=table.name,
                description=table.comment,
                customProperties=custom_properties,
            ),
        )

        wu = MetadataWorkUnit(id=f"datasetProperties-{dataset_urn}", mcp=mcp)
        self.report.report_workunit(wu)

        return wu

    def _create_table_sub_type_aspect_mcp(self, table: proxy.Table) -> MetadataWorkUnit:
        dataset_urn: str = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            platform_instance=self.platform_instance_name,
            name=table.id,
        )
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=[table.table_type]),
        )

        wu = MetadataWorkUnit(id=f"subType-{dataset_urn}", mcp=mcp)
        self.report.report_workunit(wu)

        return wu

    def _create_view_property_aspect(self, table: proxy.Table) -> MetadataWorkUnit:
        dataset_urn: str = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            platform_instance=self.platform_instance_name,
            name=table.id,
        )
        assert table.view_definition
        view_properties_aspect = ViewProperties(
            materialized=False, viewLanguage="SQL", viewLogic=table.view_definition
        )
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=dataset_urn,
            aspect=view_properties_aspect,
        )
        wu = MetadataWorkUnit(id=f"view_properties-{dataset_urn}", mcp=mcp)
        self.report.report_workunit(wu)

        return wu

    def _create_schema_metadata_aspect_mcp(
        self, table: proxy.Table
    ) -> MetadataWorkUnit:
        schema_fields: List[SchemaFieldClass] = []

        for column in table.columns:
            schema_fields.extend(self._create_schema_field(column))

        dataset_urn: str = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            platform_instance=self.platform_instance_name,
            name=table.id,
        )
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=dataset_urn,
            aspect=SchemaMetadataClass(
                schemaName=table.id,
                platform=make_data_platform_urn(self.platform),
                fields=schema_fields,
                hash="",
                version=0,
                platformSchema=MySqlDDLClass(tableSchema=""),
            ),
        )
        wu = MetadataWorkUnit(id=f"schema_metaclass-{dataset_urn}", mcp=mcp)
        self.report.report_workunit(wu)

        return wu

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

    def close(self) -> None:
        self.prepare_for_commit()
