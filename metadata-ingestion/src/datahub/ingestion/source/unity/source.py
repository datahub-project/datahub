import logging
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union
from urllib.parse import urljoin

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
    make_group_urn,
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
from datahub.emitter.sql_parsing_builder import SqlParsingBuilder
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
from datahub.ingestion.api.source_helpers import (
    create_dataset_owners_patch_builder,
    create_dataset_props_patch_builder,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws import s3_util
from datahub.ingestion.source.aws.s3_util import (
    make_s3_urn_for_lineage,
    strip_s3_prefix,
)
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
from datahub.ingestion.source.unity.hive_metastore_proxy import (
    HIVE_METASTORE,
    HiveMetastoreProxy,
)
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import (
    DATA_TYPE_REGISTRY,
    Catalog,
    Column,
    CustomCatalogType,
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
from datahub.metadata.com.linkedin.pegasus2avro.common import Siblings
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
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
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    sqlglot_lineage,
    view_definition_lineage_helper,
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
    SourceCapability.DATA_PROFILING, "Supported via the `profiling.enabled` config"
)
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
    sql_parser_schema_resolver: Optional[SchemaResolver] = None

    def get_report(self) -> UnityCatalogReport:
        return self.report

    def __init__(self, ctx: PipelineContext, config: UnityCatalogSourceConfig):
        super().__init__(config, ctx)

        self.config = config
        self.report: UnityCatalogReport = UnityCatalogReport()

        self.init_hive_metastore_proxy()

        self.unity_catalog_api_proxy = UnityCatalogApiProxy(
            config.workspace_url,
            config.token,
            config.warehouse_id,
            report=self.report,
            hive_metastore_proxy=self.hive_metastore_proxy,
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
        self.groups: List[str] = []
        # Global set of table refs
        self.table_refs: Set[TableReference] = set()
        self.view_refs: Set[TableReference] = set()
        self.notebooks: FileBackedDict[Notebook] = FileBackedDict()
        self.view_definitions: FileBackedDict[Tuple[TableReference, str]] = (
            FileBackedDict()
        )

        # Global map of tables, for profiling
        self.tables: FileBackedDict[Table] = FileBackedDict()

    def init_hive_metastore_proxy(self):
        self.hive_metastore_proxy: Optional[HiveMetastoreProxy] = None
        if self.config.include_hive_metastore:
            try:
                self.hive_metastore_proxy = HiveMetastoreProxy(
                    self.config.get_sql_alchemy_url(HIVE_METASTORE),
                    self.config.options,
                    self.report,
                )
                self.report.hive_metastore_catalog_found = True

                if self.config.include_table_lineage:
                    self.sql_parser_schema_resolver = SchemaResolver(
                        platform=self.platform,
                        platform_instance=self.config.platform_instance,
                        env=self.config.env,
                    )
            except Exception as e:
                logger.debug("Exception", exc_info=True)
                self.warn(
                    logger,
                    HIVE_METASTORE,
                    f"Failed to connect to hive_metastore due to {e}",
                )
                self.report.hive_metastore_catalog_found = False

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        try:
            config = UnityCatalogSourceConfig.parse_obj_allow_extras(config_dict)
        except Exception as e:
            return TestConnectionReport(
                internal_failure=True,
                internal_failure_reason=f"Failed to parse config due to {e}",
            )
        return UnityCatalogConnectionTest(config).get_connection_test()

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
        with self.report.new_stage("Ingestion Setup"):
            wait_on_warehouse = None
            if self.config.include_hive_metastore:
                with self.report.new_stage("Start warehouse"):
                    # Can take several minutes, so start now and wait later
                    wait_on_warehouse = self.unity_catalog_api_proxy.start_warehouse()
                    if wait_on_warehouse is None:
                        self.report.report_failure(
                            "initialization",
                            f"SQL warehouse {self.config.profiling.warehouse_id} not found",
                        )
                        return
                    else:
                        # wait until warehouse is started
                        wait_on_warehouse.result()

        if self.config.include_ownership:
            with self.report.new_stage("Ingest service principals"):
                self.build_service_principal_map()
                self.build_groups_map()
        if self.config.include_notebooks:
            with self.report.new_stage("Ingest notebooks"):
                yield from self.process_notebooks()

        yield from self.process_metastores()

        yield from self.get_view_lineage()

        if self.config.include_notebooks:
            with self.report.new_stage("Notebook lineage"):
                for notebook in self.notebooks.values():
                    wu = self._gen_notebook_lineage(notebook)
                    if wu:
                        yield wu

        if self.config.include_usage_statistics:
            with self.report.new_stage("Ingest usage"):
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
            with self.report.new_stage("Start warehouse"):
                # Need to start the warehouse again for profiling,
                # as it may have been stopped after ingestion might take
                # longer time to complete
                wait_on_warehouse = self.unity_catalog_api_proxy.start_warehouse()
                if wait_on_warehouse is None:
                    self.report.report_failure(
                        "initialization",
                        f"SQL warehouse {self.config.profiling.warehouse_id} not found",
                    )
                    return
                else:
                    # wait until warehouse is started
                    wait_on_warehouse.result()

            with self.report.new_stage("Profiling"):
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

    def build_groups_map(self) -> None:
        try:
            self.groups += self.unity_catalog_api_proxy.groups()
        except Exception as e:
            self.report.report_warning("groups", f"Unable to fetch groups: {e}")

    def process_notebooks(self) -> Iterable[MetadataWorkUnit]:
        for notebook in self.unity_catalog_api_proxy.workspace_notebooks():
            if not self.config.notebook_pattern.allowed(notebook.path):
                self.report.notebooks.dropped(notebook.path)
                continue

            self.notebooks[str(notebook.id)] = notebook
            yield from self._gen_notebook_workunits(notebook)

    def _gen_notebook_workunits(self, notebook: Notebook) -> Iterable[MetadataWorkUnit]:
        properties = {"path": notebook.path}
        if notebook.language:
            properties["language"] = notebook.language.value

        mcps = MetadataChangeProposalWrapper.construct_many(
            entityUrn=self.gen_notebook_urn(notebook),
            aspects=[
                DatasetPropertiesClass(
                    name=notebook.path.rsplit("/", 1)[-1],
                    customProperties=properties,
                    externalUrl=urljoin(
                        self.config.workspace_url, f"#notebook/{notebook.id}"
                    ),
                    created=(
                        TimeStampClass(int(notebook.created_at.timestamp() * 1000))
                        if notebook.created_at
                        else None
                    ),
                    lastModified=(
                        TimeStampClass(int(notebook.modified_at.timestamp() * 1000))
                        if notebook.modified_at
                        else None
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
            if not metastore:
                self.report.report_failure("Metastore", "Not found")
                return
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

            with self.report.new_stage(f"Ingest schema {schema.id}"):
                yield from self.gen_schema_containers(schema)
                try:
                    yield from self.process_tables(schema)
                except Exception as e:
                    logger.exception(f"Error parsing schema {schema}")
                    self.report.report_warning(
                        message="Missed schema because of parsing issues",
                        context=str(schema),
                        title="Error parsing schema",
                        exc=e,
                    )
                    continue

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
                if str(notebook_id) in self.notebooks:
                    self.notebooks[str(notebook_id)] = Notebook.add_upstream(
                        table.ref, self.notebooks[str(notebook_id)]
                    )

        # Sql parsing is required only for hive metastore view lineage
        if (
            self.sql_parser_schema_resolver
            and table.schema.catalog.type == CustomCatalogType.HIVE_METASTORE_CATALOG
        ):
            self.sql_parser_schema_resolver.add_schema_metadata(
                dataset_urn, schema_metadata
            )
            if table.view_definition:
                self.view_definitions[dataset_urn] = (table.ref, table.view_definition)

        if (
            table_props.customProperties.get("table_type")
            in {"EXTERNAL", "HIVE_EXTERNAL_TABLE"}
            and table_props.customProperties.get("data_source_format") == "DELTA"
            and self.config.emit_siblings
        ):
            storage_location = str(table_props.customProperties.get("storage_location"))
            if any(
                storage_location.startswith(prefix) for prefix in s3_util.S3_PREFIXES
            ):
                browse_path = strip_s3_prefix(storage_location)
                source_dataset_urn = make_dataset_urn_with_platform_instance(
                    "delta-lake",
                    browse_path,
                    self.config.delta_lake_options.platform_instance_name,
                    self.config.delta_lake_options.env,
                )

                yield from self.gen_siblings_workunit(dataset_urn, source_dataset_urn)
                yield from self.gen_lineage_workunit(dataset_urn, source_dataset_urn)

        if ownership:
            patch_builder = create_dataset_owners_patch_builder(dataset_urn, ownership)
            for patch_mcp in patch_builder.build():
                yield MetadataWorkUnit(
                    id=f"{dataset_urn}-{patch_mcp.aspectName}", mcp_raw=patch_mcp
                )

        if table_props:
            # TODO: use auto_incremental_properties workunit processor instead
            # Consider enabling incremental_properties by default
            patch_builder = create_dataset_props_patch_builder(dataset_urn, table_props)
            for patch_mcp in patch_builder.build():
                yield MetadataWorkUnit(
                    id=f"{dataset_urn}-{patch_mcp.aspectName}", mcp_raw=patch_mcp
                )

        yield from [
            mcp.as_workunit()
            for mcp in MetadataChangeProposalWrapper.construct_many(
                entityUrn=dataset_urn,
                aspects=[
                    view_props,
                    sub_type,
                    schema_metadata,
                    domain,
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
                fineGrainedLineages=(
                    finegrained_lineages if self.config.include_column_lineage else None
                ),
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
            if user in self.groups:
                return make_group_urn(user)
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
            parent_container_key=(
                self.gen_metastore_key(catalog.metastore)
                if self.config.include_metastore and catalog.metastore
                else None
            ),
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

        if table.table_type:
            custom_properties["table_type"] = table.table_type.value

        if table.created_by:
            custom_properties["created_by"] = table.created_by
        if table.properties:
            custom_properties.update({k: str(v) for k, v in table.properties.items()})
        if table.table_id:
            custom_properties["table_id"] = table.table_id
        if table.owner:
            custom_properties["owner"] = table.owner
        if table.updated_by:
            custom_properties["updated_by"] = table.updated_by
        if table.updated_at:
            custom_properties["updated_at"] = str(table.updated_at)

        created: Optional[TimeStampClass] = None
        if table.created_at:
            custom_properties["created_at"] = str(table.created_at)
            created = TimeStampClass(
                int(table.created_at.timestamp() * 1000),
                make_user_urn(table.created_by) if table.created_by else None,
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
                instance=(
                    make_dataplatform_instance_urn(
                        self.platform, self.platform_instance_name
                    )
                    if self.platform_instance_name
                    else None
                ),
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

    def _run_sql_parser(
        self, view_ref: TableReference, query: str, schema_resolver: SchemaResolver
    ) -> Optional[SqlParsingResult]:
        raw_lineage = sqlglot_lineage(
            query,
            schema_resolver=schema_resolver,
            default_db=view_ref.catalog,
            default_schema=view_ref.schema,
        )
        view_urn = self.gen_dataset_urn(view_ref)

        if raw_lineage.debug_info.table_error:
            logger.debug(
                f"Failed to parse lineage for view {view_ref}: "
                f"{raw_lineage.debug_info.table_error}"
            )
            self.report.num_view_definitions_failed_parsing += 1
            self.report.view_definitions_parsing_failures.append(
                f"Table-level sql parsing error for view {view_ref}: {raw_lineage.debug_info.table_error}"
            )
            return None

        elif raw_lineage.debug_info.column_error:
            self.report.num_view_definitions_failed_column_parsing += 1
            self.report.view_definitions_parsing_failures.append(
                f"Column-level sql parsing error for view {view_ref}: {raw_lineage.debug_info.column_error}"
            )
        else:
            self.report.num_view_definitions_parsed += 1
            if raw_lineage.out_tables != [view_urn]:
                self.report.num_view_definitions_view_urn_mismatch += 1
        return view_definition_lineage_helper(raw_lineage, view_urn)

    def get_view_lineage(self) -> Iterable[MetadataWorkUnit]:
        if not (
            self.config.include_hive_metastore
            and self.config.include_table_lineage
            and self.sql_parser_schema_resolver
        ):
            return
        # This is only used for parsing view lineage. Usage, Operations are emitted elsewhere
        builder = SqlParsingBuilder(
            generate_lineage=True,
            generate_usage_statistics=False,
            generate_operations=False,
        )
        for dataset_name in self.view_definitions:
            view_ref, view_definition = self.view_definitions[dataset_name]
            result = self._run_sql_parser(
                view_ref,
                view_definition,
                self.sql_parser_schema_resolver,
            )
            if result and result.out_tables:
                # This does not yield any workunits but we use
                # yield here to execute this method
                yield from builder.process_sql_parsing_result(
                    result=result,
                    query=view_definition,
                    is_view_ddl=True,
                    include_column_lineage=self.config.include_view_column_lineage,
                )
        yield from builder.gen_workunits()

    def close(self):
        if self.hive_metastore_proxy:
            self.hive_metastore_proxy.close()
        if self.view_definitions:
            self.view_definitions.close()
        if self.sql_parser_schema_resolver:
            self.sql_parser_schema_resolver.close()

        super().close()

    def gen_siblings_workunit(
        self,
        dataset_urn: str,
        source_dataset_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate sibling workunit for both unity-catalog dataset and its connector source dataset
        """
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=Siblings(primary=False, siblings=[source_dataset_urn]),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=source_dataset_urn,
            aspect=Siblings(primary=True, siblings=[dataset_urn]),
        ).as_workunit(is_primary_source=False)

    def gen_lineage_workunit(
        self,
        dataset_urn: str,
        source_dataset_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate dataset to source connector lineage workunit
        """
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=UpstreamLineage(
                upstreams=[
                    Upstream(dataset=source_dataset_urn, type=DatasetLineageType.VIEW)
                ]
            ),
        ).as_workunit()
