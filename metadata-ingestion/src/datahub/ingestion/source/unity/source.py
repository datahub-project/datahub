import logging
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Iterable, List, Optional, Set, Union
from urllib.parse import urljoin

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
    make_schema_field_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    CatalogKey,
    CatalogKeyWithMetastore,
    ContainerKey,
    MetastoreKey,
    NotebookKey,
    UnitySchemaKey,
    UnitySchemaKeyWithMetastore,
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
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.s3_util import make_s3_urn_for_lineage
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.unity.analyze_profiler import UnityCatalogAnalyzeProfiler
from datahub.ingestion.source.unity.config import (
    UnityCatalogAnalyzeProfilerConfig,
    UnityCatalogGEProfilerConfig,
    UnityCatalogSourceConfig,
)
from datahub.ingestion.source.unity.connection_test import UnityCatalogConnectionTest
from datahub.ingestion.source.unity.ge_profiler import UnityCatalogGEProfiler
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import (
    DATA_TYPE_REGISTRY,
    Catalog,
    Column,
    Metastore,
    Notebook,
    NotebookId,
    Schema,
    ServicePrincipal,
    Table,
    TableReference,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.ingestion.source.unity.usage import UnityCatalogUsageExtractor
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageUpstreamType,
    ViewProperties,
)
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    DomainsClass,
    MySqlDDLClass,
    NullTypeClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    SubTypesClass,
    TimeStampClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.hive_schema_to_avro import get_schema_fields_for_hive_column
from datahub.utilities.registries.domain_registry import DomainRegistry

logger: logging.Logger = logging.getLogger(__name__)


@platform_name("Databricks")
@config_class(UnityCatalogSourceConfig)
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(SourceCapability.LINEAGE_FINE, "Enabled by default")
@capability(SourceCapability.USAGE_STATS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.OWNERSHIP, "Supported via the `include_ownership` config")
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
    unity_catalog_api_proxy: UnityCatalogApiProxy
    platform: str = "databricks"
    platform_instance_name: Optional[str]

    def get_report(self) -> UnityCatalogReport:
        return self.report

    def __init__(self, ctx: PipelineContext, config: UnityCatalogSourceConfig):
        super(UnityCatalogSource, self).__init__(config, ctx)

        self.config = config
        self.report: UnityCatalogReport = UnityCatalogReport()
        self.unity_catalog_api_proxy = UnityCatalogApiProxy(
            config.workspace_url,
            config.token,
            config.profiling.warehouse_id,
            report=self.report,
        )
        self.external_url_base = urljoin(self.config.workspace_url, "/explore/data")

        # Determine the platform_instance_name
        self.platform_instance_name = self.config.platform_instance
        if self.config.include_metastore:
            self.platform_instance_name = (
                config.workspace_name
                if config.workspace_name is not None
                else config.workspace_url.split("//")[1].split(".")[0]
            )

        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.config.domain], graph=self.ctx.graph
            )

        # Global map of service principal application id -> ServicePrincipal
        self.service_principals: Dict[str, ServicePrincipal] = {}
        # Global set of table refs
        self.table_refs: Set[TableReference] = set()
        self.view_refs: Set[TableReference] = set()
        self.notebooks: FileBackedDict[Notebook] = FileBackedDict()

        # Global map of tables, for profiling
        self.tables: FileBackedDict[Table] = FileBackedDict()

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        return UnityCatalogConnectionTest(config_dict).get_connection_test()

    @classmethod
    def create(cls, config_dict, ctx):
        config = UnityCatalogSourceConfig.parse_obj(config_dict)
        return cls(ctx=ctx, config=config)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.report.report_ingestion_stage_start("Ingestion Setup")
        wait_on_warehouse = None
        if self.config.is_profiling_enabled():
            self.report.report_ingestion_stage_start("Start warehouse")
            # Can take several minutes, so start now and wait later
            wait_on_warehouse = self.unity_catalog_api_proxy.start_warehouse()
            if wait_on_warehouse is None:
                self.report.report_failure(
                    "initialization",
                    f"SQL warehouse {self.config.profiling.warehouse_id} not found",
                )
                return

        if self.config.include_ownership:
            self.report.report_ingestion_stage_start("Ingest service principals")
            self.build_service_principal_map()
        if self.config.include_notebooks:
            self.report.report_ingestion_stage_start("Ingest notebooks")
            yield from self.process_notebooks()

        yield from self.process_metastores()

        if self.config.include_notebooks:
            self.report.report_ingestion_stage_start("Notebook lineage")
            for notebook in self.notebooks.values():
                wu = self._gen_notebook_lineage(notebook)
                if wu:
                    yield wu

        if self.config.include_usage_statistics:
            self.report.report_ingestion_stage_start("Ingest usage")
            usage_extractor = UnityCatalogUsageExtractor(
                config=self.config,
                report=self.report,
                proxy=self.unity_catalog_api_proxy,
                table_urn_builder=self.gen_dataset_urn,
                user_urn_builder=self.gen_user_urn,
            )
            yield from usage_extractor.get_usage_workunits(
                self.table_refs | self.view_refs
            )

        if self.config.is_profiling_enabled():
            self.report.report_ingestion_stage_start("Wait on warehouse")
            assert wait_on_warehouse
            wait_on_warehouse.result()

            self.report.report_ingestion_stage_start("Profiling")
            if isinstance(self.config.profiling, UnityCatalogAnalyzeProfilerConfig):
                yield from UnityCatalogAnalyzeProfiler(
                    self.config.profiling,
                    self.report,
                    self.unity_catalog_api_proxy,
                    self.gen_dataset_urn,
                ).get_workunits(self.table_refs)
            elif isinstance(self.config.profiling, UnityCatalogGEProfilerConfig):
                yield from UnityCatalogGEProfiler(
                    sql_common_config=self.config,
                    profiling_config=self.config.profiling,
                    report=self.report,
                ).get_workunits(list(self.tables.values()))
            else:
                raise ValueError("Unknown profiling config method")

    def build_service_principal_map(self) -> None:
        try:
            for sp in self.unity_catalog_api_proxy.service_principals():
                self.service_principals[sp.application_id] = sp
        except Exception as e:
            self.report.report_warning(
                "service-principals", f"Unable to fetch service principals: {e}"
            )

    def process_notebooks(self) -> Iterable[MetadataWorkUnit]:
        for notebook in self.unity_catalog_api_proxy.workspace_notebooks():
            if not self.config.notebook_pattern.allowed(notebook.path):
                self.report.notebooks.dropped(notebook.path)
                continue

            self.notebooks[str(notebook.id)] = notebook
            yield from self._gen_notebook_workunits(notebook)

    def _gen_notebook_workunits(self, notebook: Notebook) -> Iterable[MetadataWorkUnit]:
        mcps = MetadataChangeProposalWrapper.construct_many(
            entityUrn=self.gen_notebook_urn(notebook),
            aspects=[
                DatasetPropertiesClass(
                    name=notebook.path.rsplit("/", 1)[-1],
                    customProperties={
                        "path": notebook.path,
                        "language": notebook.language.value,
                    },
                    externalUrl=urljoin(
                        self.config.workspace_url, f"#notebook/{notebook.id}"
                    ),
                    created=TimeStampClass(int(notebook.created_at.timestamp() * 1000)),
                    lastModified=TimeStampClass(
                        int(notebook.modified_at.timestamp() * 1000)
                    ),
                ),
                SubTypesClass(typeNames=[DatasetSubTypes.NOTEBOOK]),
                BrowsePathsClass(paths=notebook.path.split("/")),
                self._create_data_platform_instance_aspect(),
            ],
        )
        for mcp in mcps:
            yield mcp.as_workunit()

        self.report.notebooks.processed(notebook.path)

    def _gen_notebook_lineage(self, notebook: Notebook) -> Optional[MetadataWorkUnit]:
        if not notebook.upstreams:
            return None

        return MetadataChangeProposalWrapper(
            entityUrn=self.gen_notebook_urn(notebook),
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=self.gen_dataset_urn(upstream_ref),
                        type=DatasetLineageTypeClass.COPY,
                    )
                    for upstream_ref in notebook.upstreams
                ]
            ),
        ).as_workunit()

    def process_metastores(self) -> Iterable[MetadataWorkUnit]:
        metastore: Optional[Metastore] = None
        if self.config.include_metastore:
            metastore = self.unity_catalog_api_proxy.assigned_metastore()
            yield from self.gen_metastore_containers(metastore)
        yield from self.process_catalogs(metastore)
        if metastore and self.config.include_metastore:
            self.report.metastores.processed(metastore.id)

    def process_catalogs(
        self, metastore: Optional[Metastore]
    ) -> Iterable[MetadataWorkUnit]:
        for catalog in self._get_catalogs(metastore):
            if not self.config.catalog_pattern.allowed(catalog.id):
                self.report.catalogs.dropped(catalog.id)
                continue

            yield from self.gen_catalog_containers(catalog)
            yield from self.process_schemas(catalog)

            self.report.catalogs.processed(catalog.id)

    def _get_catalogs(self, metastore: Optional[Metastore]) -> Iterable[Catalog]:
        if self.config.catalogs:
            for catalog_name in self.config.catalogs:
                catalog = self.unity_catalog_api_proxy.catalog(
                    catalog_name, metastore=metastore
                )
                if catalog:
                    yield catalog
        else:
            yield from self.unity_catalog_api_proxy.catalogs(metastore=metastore)

    def process_schemas(self, catalog: Catalog) -> Iterable[MetadataWorkUnit]:
        for schema in self.unity_catalog_api_proxy.schemas(catalog=catalog):
            if not self.config.schema_pattern.allowed(schema.id):
                self.report.schemas.dropped(schema.id)
                continue

            self.report.report_ingestion_stage_start(f"Ingest schema {schema.id}")
            yield from self.gen_schema_containers(schema)
            yield from self.process_tables(schema)

            self.report.schemas.processed(schema.id)

    def process_tables(self, schema: Schema) -> Iterable[MetadataWorkUnit]:
        for table in self.unity_catalog_api_proxy.tables(schema=schema):
            if not self.config.table_pattern.allowed(table.ref.qualified_table_name):
                self.report.tables.dropped(table.id, f"table ({table.table_type})")
                continue

            if (
                self.config.is_profiling_enabled()
                and self.config.is_ge_profiling()
                and self.config.profiling.pattern.allowed(
                    table.ref.qualified_table_name
                )
                and not table.is_view
            ):
                self.tables[table.ref.qualified_table_name] = table

            if table.is_view:
                self.view_refs.add(table.ref)
            else:
                self.table_refs.add(table.ref)
            yield from self.process_table(table, schema)
            self.report.tables.processed(table.id, f"table ({table.table_type})")

    def process_table(self, table: Table, schema: Schema) -> Iterable[MetadataWorkUnit]:
        dataset_urn = self.gen_dataset_urn(table.ref)
        yield from self.add_table_to_dataset_container(dataset_urn, schema)

        table_props = self._create_table_property_aspect(table)

        view_props = None
        if table.view_definition:
            view_props = self._create_view_property_aspect(table)

        sub_type = self._create_table_sub_type_aspect(table)
        schema_metadata = self._create_schema_metadata_aspect(table)
        domain = self._get_domain_aspect(dataset_name=table.ref.qualified_table_name)
        ownership = self._create_table_ownership_aspect(table)
        data_platform_instance = self._create_data_platform_instance_aspect()

        lineage = self.ingest_lineage(table)

        if self.config.include_notebooks:
            for notebook_id in table.downstream_notebooks:
                self.notebooks[str(notebook_id)] = Notebook.add_upstream(
                    table.ref, self.notebooks[str(notebook_id)]
                )

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
                    ownership,
                    data_platform_instance,
                    lineage,
                ],
            )
        ]

    def ingest_lineage(self, table: Table) -> Optional[UpstreamLineageClass]:
        if self.config.include_table_lineage:
            self.unity_catalog_api_proxy.table_lineage(
                table, include_entity_lineage=self.config.include_notebooks
            )

        if self.config.include_column_lineage and table.upstreams:
            if len(table.columns) > self.config.column_lineage_column_limit:
                self.report.num_column_lineage_skipped_column_count += 1

            with ThreadPoolExecutor(
                max_workers=self.config.lineage_max_workers
            ) as executor:
                for column in table.columns[: self.config.column_lineage_column_limit]:
                    executor.submit(
                        self.unity_catalog_api_proxy.get_column_lineage,
                        table,
                        column.name,
                    )

        return self._generate_lineage_aspect(self.gen_dataset_urn(table.ref), table)

    def _generate_lineage_aspect(
        self, dataset_urn: str, table: Table
    ) -> Optional[UpstreamLineageClass]:
        upstreams: List[UpstreamClass] = []
        finegrained_lineages: List[FineGrainedLineage] = []
        for upstream_ref, downstream_to_upstream_cols in sorted(
            table.upstreams.items()
        ):
            upstream_urn = self.gen_dataset_urn(upstream_ref)

            # Should be empty if config.include_column_lineage is False
            finegrained_lineages.extend(
                FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=[
                        make_schema_field_urn(upstream_urn, upstream_col)
                        for upstream_col in sorted(u_cols)
                    ],
                    downstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    downstreams=[make_schema_field_urn(dataset_urn, d_col)],
                )
                for d_col, u_cols in sorted(downstream_to_upstream_cols.items())
            )

            upstreams.append(
                UpstreamClass(
                    dataset=upstream_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            )

        for notebook in table.upstream_notebooks:
            upstreams.append(
                UpstreamClass(
                    dataset=self.gen_notebook_urn(notebook),
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            )

        if self.config.include_external_lineage:
            for external_ref in table.external_upstreams:
                if not external_ref.has_permission or not external_ref.path:
                    self.report.num_external_upstreams_lacking_permissions += 1
                    logger.warning(
                        f"Lacking permissions for external file upstream on {table.ref}"
                    )
                elif external_ref.path.startswith("s3://"):
                    upstreams.append(
                        UpstreamClass(
                            dataset=make_s3_urn_for_lineage(
                                external_ref.path, self.config.env
                            ),
                            type=DatasetLineageTypeClass.COPY,
                        )
                    )
                else:
                    self.report.num_external_upstreams_unsupported += 1
                    logger.warning(
                        f"Unsupported external file upstream on {table.ref}: {external_ref.path}"
                    )

        if upstreams:
            return UpstreamLineageClass(
                upstreams=upstreams,
                fineGrainedLineages=finegrained_lineages
                if self.config.include_column_lineage
                else None,
            )
        else:
            return None

    def _get_domain_aspect(self, dataset_name: str) -> Optional[DomainsClass]:
        domain_urn = self._gen_domain_urn(dataset_name)
        if not domain_urn:
            return None
        return DomainsClass(domains=[domain_urn])

    def get_owner_urn(self, user: Optional[str]) -> Optional[str]:
        if self.config.include_ownership and user is not None:
            return self.gen_user_urn(user)
        return None

    def gen_user_urn(self, user: str) -> str:
        if user in self.service_principals:
            user = self.service_principals[user].display_name
        return make_user_urn(user)

    def gen_dataset_urn(self, table_ref: TableReference) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            platform_instance=self.platform_instance_name,
            name=str(table_ref),
            env=self.config.env,
        )

    def gen_notebook_urn(self, notebook: Union[Notebook, NotebookId]) -> str:
        notebook_id = notebook.id if isinstance(notebook, Notebook) else notebook
        return NotebookKey(
            notebook_id=notebook_id,
            platform=self.platform,
            instance=self.config.platform_instance,
        ).as_urn()

    def gen_schema_containers(self, schema: Schema) -> Iterable[MetadataWorkUnit]:
        domain_urn = self._gen_domain_urn(f"{schema.catalog.name}.{schema.name}")

        schema_container_key = self.gen_schema_key(schema)
        yield from gen_containers(
            container_key=schema_container_key,
            name=schema.name,
            sub_types=[DatasetContainerSubTypes.SCHEMA],
            parent_container_key=self.gen_catalog_key(catalog=schema.catalog),
            domain_urn=domain_urn,
            description=schema.comment,
            owner_urn=self.get_owner_urn(schema.owner),
            external_url=f"{self.external_url_base}/{schema.catalog.name}/{schema.name}",
        )

    def gen_metastore_containers(
        self, metastore: Metastore
    ) -> Iterable[MetadataWorkUnit]:
        domain_urn = self._gen_domain_urn(metastore.name)

        metastore_container_key = self.gen_metastore_key(metastore)
        yield from gen_containers(
            container_key=metastore_container_key,
            name=metastore.name,
            sub_types=[DatasetContainerSubTypes.DATABRICKS_METASTORE],
            domain_urn=domain_urn,
            description=metastore.comment,
            owner_urn=self.get_owner_urn(metastore.owner),
            external_url=self.external_url_base,
        )

    def gen_catalog_containers(self, catalog: Catalog) -> Iterable[MetadataWorkUnit]:
        domain_urn = self._gen_domain_urn(catalog.name)

        catalog_container_key = self.gen_catalog_key(catalog)
        yield from gen_containers(
            container_key=catalog_container_key,
            name=catalog.name,
            sub_types=[DatasetContainerSubTypes.CATALOG],
            domain_urn=domain_urn,
            parent_container_key=self.gen_metastore_key(catalog.metastore)
            if self.config.include_metastore and catalog.metastore
            else None,
            description=catalog.comment,
            owner_urn=self.get_owner_urn(catalog.owner),
            external_url=f"{self.external_url_base}/{catalog.name}",
        )

    def gen_schema_key(self, schema: Schema) -> ContainerKey:
        if self.config.include_metastore:
            assert schema.catalog.metastore
            return UnitySchemaKeyWithMetastore(
                unity_schema=schema.name,
                platform=self.platform,
                instance=self.config.platform_instance,
                catalog=schema.catalog.name,
                metastore=schema.catalog.metastore.name,
                env=self.config.env,
            )
        else:
            return UnitySchemaKey(
                unity_schema=schema.name,
                platform=self.platform,
                instance=self.config.platform_instance,
                catalog=schema.catalog.name,
                env=self.config.env,
            )

    def gen_metastore_key(self, metastore: Metastore) -> MetastoreKey:
        return MetastoreKey(
            metastore=metastore.name,
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

    def gen_catalog_key(self, catalog: Catalog) -> ContainerKey:
        if self.config.include_metastore:
            assert catalog.metastore
            return CatalogKeyWithMetastore(
                catalog=catalog.name,
                metastore=catalog.metastore.name,
                platform=self.platform,
                instance=self.config.platform_instance,
                env=self.config.env,
            )
        else:
            return CatalogKey(
                catalog=catalog.name,
                platform=self.platform,
                instance=self.config.platform_instance,
                env=self.config.env,
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

    def _create_table_property_aspect(self, table: Table) -> DatasetPropertiesClass:
        custom_properties: dict = {}
        if table.storage_location is not None:
            custom_properties["storage_location"] = table.storage_location
        if table.data_source_format is not None:
            custom_properties["data_source_format"] = table.data_source_format.value
        if table.generation is not None:
            custom_properties["generation"] = str(table.generation)

        custom_properties["table_type"] = table.table_type.value

        custom_properties["created_by"] = table.created_by
        custom_properties["created_at"] = str(table.created_at)
        if table.properties:
            custom_properties.update({k: str(v) for k, v in table.properties.items()})
        custom_properties["table_id"] = table.table_id
        custom_properties["owner"] = table.owner
        custom_properties["updated_by"] = table.updated_by
        custom_properties["updated_at"] = str(table.updated_at)

        created = TimeStampClass(
            int(table.created_at.timestamp() * 1000), make_user_urn(table.created_by)
        )
        last_modified = created
        if table.updated_at:
            last_modified = TimeStampClass(
                int(table.updated_at.timestamp() * 1000),
                table.updated_by and make_user_urn(table.updated_by),
            )

        return DatasetPropertiesClass(
            name=table.name,
            qualifiedName=table.ref.qualified_table_name,
            description=table.comment,
            customProperties=custom_properties,
            created=created,
            lastModified=last_modified,
            externalUrl=f"{self.external_url_base}/{table.ref.external_path}",
        )

    def _create_table_ownership_aspect(self, table: Table) -> Optional[OwnershipClass]:
        owner_urn = self.get_owner_urn(table.owner)
        if owner_urn is not None:
            return OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=owner_urn,
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ]
            )
        return None

    def _create_data_platform_instance_aspect(
        self,
    ) -> Optional[DataPlatformInstanceClass]:
        if self.config.ingest_data_platform_instance_aspect:
            return DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=make_dataplatform_instance_urn(
                    self.platform, self.platform_instance_name
                )
                if self.platform_instance_name
                else None,
            )
        return None

    def _create_table_sub_type_aspect(self, table: Table) -> SubTypesClass:
        return SubTypesClass(
            typeNames=[DatasetSubTypes.VIEW if table.is_view else DatasetSubTypes.TABLE]
        )

    def _create_view_property_aspect(self, table: Table) -> ViewProperties:
        assert table.view_definition
        return ViewProperties(
            materialized=False, viewLanguage="SQL", viewLogic=table.view_definition
        )

    def _create_schema_metadata_aspect(self, table: Table) -> SchemaMetadataClass:
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
    def _create_schema_field(column: Column) -> List[SchemaFieldClass]:
        _COMPLEX_TYPE = re.compile("^(struct|array)")

        if _COMPLEX_TYPE.match(column.type_text.lower()):
            return get_schema_fields_for_hive_column(
                column.name, column.type_text.lower(), description=column.comment
            )
        else:
            return [
                SchemaFieldClass(
                    fieldPath=column.name,
                    type=SchemaFieldDataTypeClass(
                        type=DATA_TYPE_REGISTRY.get(column.type_name, NullTypeClass)()
                    ),
                    nativeDataType=column.type_text,
                    nullable=column.nullable,
                    description=column.comment,
                )
            ]
