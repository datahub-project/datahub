import json
import logging
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import dateutil.parser as dp
import tableauserverclient as TSC
from pydantic import validator
from pydantic.fields import Field
from tableauserverclient import (
    PersonalAccessTokenAuth,
    Server,
    ServerResponseError,
    TableauAuth,
)

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.configuration.source_common import DatasetLineageProviderConfigBase
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    PlatformKey,
    add_entity_to_container,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.state.tableau_state import TableauCheckpointState
from datahub.ingestion.source.tableau_common import (
    FIELD_TYPE_MAPPING,
    MetadataQueryException,
    TableauLineageOverrides,
    clean_query,
    custom_sql_graphql_query,
    embedded_datasource_graphql_query,
    get_field_value_in_sheet,
    get_unique_custom_sql,
    make_table_urn,
    published_datasource_graphql_query,
    query_metadata,
    tableau_field_to_schema_field,
    workbook_graphql_query,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    ChangeAuditStamps,
    InputField,
    InputFields,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot,
    DashboardSnapshot,
    DatasetSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    NullTypeClass,
    OtherSchema,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ChangeTypeClass,
    ChartInfoClass,
    ChartUsageStatisticsClass,
    DashboardInfoClass,
    DashboardUsageStatisticsClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SubTypesClass,
    ViewPropertiesClass,
)
from datahub.utilities import config_clean
from datahub.utilities.source_helpers import (
    auto_stale_entity_removal,
    auto_status_aspect,
)

logger: logging.Logger = logging.getLogger(__name__)

# Replace / with |
REPLACE_SLASH_CHAR = "|"


class TableauStatefulIngestionConfig(StatefulStaleMetadataRemovalConfig):
    """
    Specialization of StatefulStaleMetadataRemovalConfig to adding custom config.
    This will be used to override the stateful_ingestion config param of StatefulIngestionConfigBase
    in the TableauConfig.
    """

    _entity_types: List[str] = Field(default=["dataset", "chart", "dashboard"])


class TableauConnectionConfig(ConfigModel):
    connect_uri: str = Field(description="Tableau host URL.")
    username: Optional[str] = Field(
        default=None,
        description="Tableau username, must be set if authenticating using username/password.",
    )
    password: Optional[str] = Field(
        default=None,
        description="Tableau password, must be set if authenticating using username/password.",
    )
    token_name: Optional[str] = Field(
        default=None,
        description="Tableau token name, must be set if authenticating using a personal access token.",
    )
    token_value: Optional[str] = Field(
        default=None,
        description="Tableau token value, must be set if authenticating using a personal access token.",
    )

    site: str = Field(
        default="",
        description="Tableau Site. Always required for Tableau Online. Use emptystring to connect with Default site on Tableau Server.",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Unique relationship between the Tableau Server and site",
    )

    ssl_verify: Union[bool, str] = Field(
        default=True,
        description="Whether to verify SSL certificates. If using self-signed certificates, set to false or provide the path to the .pem certificate bundle.",
    )

    extract_column_level_lineage: bool = Field(
        True,
        description="When enabled, extracts column-level lineage from Tableau Datasources",
    )

    @validator("connect_uri")
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)

    def make_tableau_client(self) -> Server:
        # https://tableau.github.io/server-client-python/docs/api-ref#authentication
        authentication: Union[TableauAuth, PersonalAccessTokenAuth]
        if self.username and self.password:
            authentication = TableauAuth(
                username=self.username,
                password=self.password,
                site_id=self.site,
            )
        elif self.token_name and self.token_value:
            authentication = PersonalAccessTokenAuth(
                self.token_name, self.token_value, self.site
            )
        else:
            raise ConfigurationError(
                "Tableau Source: Either username/password or token_name/token_value must be set"
            )

        try:
            server = Server(self.connect_uri, use_server_version=True)

            # From https://stackoverflow.com/a/50159273/5004662.
            server._session.verify = self.ssl_verify
            server._session.trust_env = False

            server.auth.sign_in(authentication)
            return server
        except ServerResponseError as e:
            raise ValueError(
                f"Unable to login with credentials provided: {str(e)}"
            ) from e
        except Exception as e:
            raise ValueError(f"Unable to login: {str(e)}") from e


class TableauConfig(
    DatasetLineageProviderConfigBase,
    StatefulIngestionConfigBase,
    TableauConnectionConfig,
):
    projects: Optional[List[str]] = Field(
        default=["default"], description="List of projects"
    )
    default_schema_map: dict = Field(
        default={}, description="Default schema to use when schema is not found."
    )
    ingest_tags: Optional[bool] = Field(
        default=False,
        description="Ingest Tags from source. This will override Tags entered from UI",
    )
    ingest_owner: Optional[bool] = Field(
        default=False,
        description="Ingest Owner from source. This will override Owner info entered from UI",
    )
    ingest_tables_external: bool = Field(
        default=False,
        description="Ingest details for tables external to (not embedded in) tableau as entities.",
    )

    page_size: int = Field(
        default=10,
        description="Number of metadata objects (e.g. CustomSQLTable, PublishedDatasource, etc) to query at a time using Tableau api.",
    )

    env: str = Field(
        default=builder.DEFAULT_ENV,
        description="Environment to use in namespace when constructing URNs.",
    )

    lineage_overrides: Optional[TableauLineageOverrides] = Field(
        default=None,
        description="Mappings to change generated dataset urns. Use only if you really know what you are doing.",
    )

    extract_usage_stats: bool = Field(
        default=False,
        description="[experimental] Extract usage statistics for dashboards and charts.",
    )

    stateful_ingestion: Optional[TableauStatefulIngestionConfig] = Field(
        default=None, description=""
    )


class WorkbookKey(PlatformKey):
    workbook_id: str


@dataclass
class UsageStat:
    view_count: int


@platform_name("Tableau")
@config_class(TableauConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Requires transformer", supported=False)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(
    SourceCapability.USAGE_STATS,
    "Dashboard/Chart view counts, enabled using extract_usage_stats config",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default when stateful ingestion is turned on.",
)
@capability(SourceCapability.OWNERSHIP, "Requires recipe configuration")
@capability(SourceCapability.TAGS, "Requires recipe configuration")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
class TableauSource(StatefulIngestionSourceBase):
    config: TableauConfig
    report: StaleEntityRemovalSourceReport
    platform = "tableau"
    server: Optional[Server]
    upstream_tables: Dict[str, Tuple[Any, Optional[str], bool]] = {}
    tableau_stat_registry: Dict[str, UsageStat] = {}

    def __hash__(self):
        return id(self)

    def __init__(
        self,
        config: TableauConfig,
        ctx: PipelineContext,
    ):
        super().__init__(config, ctx)

        self.config = config
        self.report = StaleEntityRemovalSourceReport()
        self.server = None

        # This list keeps track of embedded datasources in workbooks so that we retrieve those
        # when emitting embedded data sources.
        self.embedded_datasource_ids_being_used: List[str] = []
        # This list keeps track of datasource being actively used by workbooks so that we only retrieve those
        # when emitting published data sources.
        self.datasource_ids_being_used: List[str] = []
        # This list keeps track of datasource being actively used by workbooks so that we only retrieve those
        # when emitting custom SQL data sources.
        self.custom_sql_ids_being_used: List[str] = []

        # Create and register the stateful ingestion use-case handlers.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            source=self,
            config=self.config,
            state_type_class=TableauCheckpointState,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )

        self._authenticate()

    def close(self) -> None:
        if self.server is not None:
            self.prepare_for_commit()
            self.server.auth.sign_out()

    def _populate_usage_stat_registry(self):
        if self.server is None:
            return

        for view in TSC.Pager(self.server.views, usage=True):
            self.tableau_stat_registry[view.id] = UsageStat(view_count=view.total_views)
        logger.debug("Tableau stats %s", self.tableau_stat_registry)

    def _authenticate(self):
        try:
            self.server = self.config.make_tableau_client()
        # Note that we're not catching ConfigurationError, since we want that to throw.
        except ValueError as e:
            self.report.report_failure(
                key="tableau-login",
                reason=str(e),
            )

    def get_data_platform_instance(self) -> DataPlatformInstanceClass:
        return DataPlatformInstanceClass(
            platform=builder.make_data_platform_urn(self.platform),
            instance=builder.make_dataplatform_instance_urn(
                self.platform, self.config.platform_instance
            )
            if self.config.platform_instance
            else None,
        )

    def get_connection_object_page(
        self,
        query: str,
        connection_type: str,
        query_filter: str,
        count: int = 0,
        current_count: int = 0,
    ) -> Tuple[dict, int, int]:
        logger.debug(
            f"Query {connection_type} to get {count} objects with offset {current_count}"
        )
        query_data = query_metadata(
            self.server, query, connection_type, count, current_count, query_filter
        )
        if "errors" in query_data:
            errors = query_data["errors"]
            if all(error["extensions"]["severity"] == "WARNING" for error in errors):
                self.report.report_warning(key=connection_type, reason=f"{errors}")
            else:
                raise RuntimeError(f"Query {connection_type} error: {errors}")

        connection_object = (
            query_data.get("data").get(connection_type, {})
            if query_data.get("data")
            else {}
        )

        total_count = connection_object.get("totalCount", 0)
        has_next_page = connection_object.get("pageInfo", {}).get("hasNextPage", False)
        return connection_object, total_count, has_next_page

    def get_connection_objects(
        self,
        query: str,
        connection_type: str,
        query_filter: str,
    ) -> Iterable[dict]:
        # Calls the get_connection_object_page function to get the objects,
        # and automatically handles pagination.

        count_on_query = self.config.page_size

        total_count = count_on_query
        has_next_page = 1
        current_count = 0
        while has_next_page:
            count = (
                count_on_query
                if current_count + count_on_query < total_count
                else total_count - current_count
            )
            (
                connection_objects,
                total_count,
                has_next_page,
            ) = self.get_connection_object_page(
                query,
                connection_type,
                query_filter,
                count,
                current_count,
            )

            current_count += count

            for obj in connection_objects.get("nodes", []):
                yield obj

    def emit_workbooks(self) -> Iterable[MetadataWorkUnit]:
        projects = (
            f"projectNameWithin: {json.dumps(self.config.projects)}"
            if self.config.projects
            else ""
        )

        for workbook in self.get_connection_objects(
            workbook_graphql_query, "workbooksConnection", projects
        ):
            yield from self.emit_workbook_as_container(workbook)
            yield from self.emit_sheets_as_charts(workbook)
            yield from self.emit_dashboards(workbook)
            for ds in workbook.get("embeddedDatasources", []):
                self.embedded_datasource_ids_being_used.append(ds["id"])

    def _track_custom_sql_ids(self, field: dict) -> None:
        # Tableau shows custom sql datasource as a table in ColumnField's upstreamColumns.
        for column in field.get("upstreamColumns", []):
            table_id = (
                column.get("table", {}).get("id")
                if column.get("table")
                and column["table"]["__typename"] == "CustomSQLTable"
                else None
            )

            if table_id is not None and table_id not in self.custom_sql_ids_being_used:
                self.custom_sql_ids_being_used.append(table_id)

    def _create_upstream_table_lineage(
        self,
        datasource: dict,
        project: str,
        is_embedded_ds: bool = False,
    ) -> Tuple:
        upstream_tables: List[Upstream] = []
        fine_grained_lineages: List[FineGrainedLineage] = []
        table_id_to_urn = {}

        upstream_datasources = self.get_upstream_datasources(
            datasource, upstream_tables
        )
        upstream_tables.extend(upstream_datasources)

        # When tableau workbook connects to published datasource, it creates an embedded
        # datasource inside workbook that connects to published datasource. Both embedded
        # and published datasource have same upstreamTables in this case.
        if upstream_tables and is_embedded_ds:
            logger.debug(
                f"Embedded datasource {datasource.get('id')} has upstreamDatasources.\
                Setting only upstreamDatasources lineage. The upstreamTables lineage \
                    will be set via upstream published datasource."
            )
        else:
            # This adds an edge to upstream DatabaseTables using `upstreamTables`
            upstreams, id_to_urn = self.get_upstream_tables(
                datasource.get("upstreamTables", []),
                datasource.get("name"),
                project,
                is_custom_sql=False,
            )
            upstream_tables.extend(upstreams)
            table_id_to_urn.update(id_to_urn)

            # This adds an edge to upstream CustomSQLTables using `fields`.`upstreamColumns`.`table`
            csql_upstreams, csql_id_to_urn = self.get_upstream_csql_tables(
                datasource.get("fields"),
                datasource.get("name"),
                project,
            )
            upstream_tables.extend(csql_upstreams)
            table_id_to_urn.update(csql_id_to_urn)

        logger.debug(
            f"A total of {len(upstream_tables)} upstream table edges found for datasource {datasource['id']}"
        )

        if datasource.get("fields"):
            datasource_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=datasource["id"],
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            if self.config.extract_column_level_lineage:
                # Find fine grained lineage for datasource column to datasource column edge,
                # upstream columns may be from same datasource
                upstream_fields = self.get_upstream_fields_of_field_in_datasource(
                    datasource, datasource_urn
                )
                fine_grained_lineages.extend(upstream_fields)

                # Find fine grained lineage for table column to datasource column edge,
                upstream_columns = self.get_upstream_columns_of_fields_in_datasource(
                    datasource,
                    datasource_urn,
                    table_id_to_urn,
                )
                fine_grained_lineages.extend(upstream_columns)

                logger.debug(
                    f"A total of {len(fine_grained_lineages)} upstream column edges found for datasource {datasource['id']}"
                )

        return upstream_tables, fine_grained_lineages

    def get_upstream_datasources(self, datasource, upstream_tables):
        upstream_tables = []
        for ds in datasource.get("upstreamDatasources", []):
            if ds["id"] not in self.datasource_ids_being_used:
                self.datasource_ids_being_used.append(ds["id"])

            upstream_ds_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=ds["id"],
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
            upstream_table = Upstream(
                dataset=upstream_ds_urn,
                type=DatasetLineageType.TRANSFORMED,
            )
            upstream_tables.append(upstream_table)
        return upstream_tables

    def get_upstream_csql_tables(self, fields, datasource_name, project):
        upstream_csql_urns = set()
        csql_id_to_urn = {}

        for field in fields:
            if not field.get("upstreamColumns"):
                continue
            for upstream_col in field.get("upstreamColumns"):
                if (
                    upstream_col
                    and upstream_col.get("table")
                    and upstream_col.get("table")["__typename"] == "CustomSQLTable"
                ):
                    upstream_table_id = upstream_col.get("table")["id"]

                    csql_urn = builder.make_dataset_urn_with_platform_instance(
                        platform=self.platform,
                        name=upstream_table_id,
                        platform_instance=self.config.platform_instance,
                        env=self.config.env,
                    )
                    csql_id_to_urn[upstream_table_id] = csql_urn

                    upstream_csql_urns.add(csql_urn)

        return [
            Upstream(dataset=csql_urn, type=DatasetLineageType.TRANSFORMED)
            for csql_urn in upstream_csql_urns
        ], csql_id_to_urn

    def get_upstream_tables(self, tables, datasource_name, project, is_custom_sql):
        upstream_tables = []
        # Same table urn can be used when setting fine grained lineage,
        table_id_to_urn: Dict[str, str] = {}
        for table in tables:
            # skip upstream tables when there is no column info when retrieving datasource
            # Lineage and Schema details for these will be taken care in self.emit_custom_sql_datasources()
            if not is_custom_sql and not table.get("columns"):
                logger.debug(
                    f"Skipping upstream table with id {table['id']}, no columns: {table}"
                )
                continue
            elif table["name"] is None:
                logger.warning(
                    f"Skipping upstream table {table['id']} from lineage since its name is none: {table}"
                )
                continue

            schema = table.get("schema", "")
            table_name = table.get("name", "")
            full_name = table.get("fullName", "")
            upstream_db = (
                table.get("database", {}).get("name", "")
                if table.get("database") is not None
                else ""
            )
            logger.debug(
                "Processing Table with Connection Type: {0} and id {1}".format(
                    table.get("connectionType", ""), table.get("id", "")
                )
            )
            schema = self._get_schema(schema, upstream_db, full_name)
            # if the schema is included within the table name we omit it
            if (
                schema
                and table_name
                and full_name
                and table_name == full_name
                and schema in table_name
            ):
                logger.debug(
                    f"Omitting schema for upstream table {table['id']}, schema included in table name"
                )
                schema = ""

            table_urn = make_table_urn(
                self.config.env,
                upstream_db,
                table.get("connectionType", ""),
                schema,
                table_name,
                self.config.platform_instance_map,
                self.config.lineage_overrides,
            )
            table_id_to_urn[table["id"]] = table_urn

            upstream_table = Upstream(
                dataset=table_urn,
                type=DatasetLineageType.TRANSFORMED,
            )
            upstream_tables.append(upstream_table)

            table_path = None
            if project and datasource_name:
                table_path = (
                    f"{project.replace('/', REPLACE_SLASH_CHAR)}/{datasource_name}"
                )

            self.upstream_tables[table_urn] = (
                table.get("columns", []),
                table_path,
                table.get("isEmbedded") or False,
            )
        return upstream_tables, table_id_to_urn

    def get_upstream_columns_of_fields_in_datasource(
        self,
        datasource,
        datasource_urn,
        table_id_to_urn,
    ):
        fine_grained_lineages = []
        for field in datasource.get("fields"):
            field_name = field.get("name")
            # upstreamColumns lineage will be set via upstreamFields.
            # such as for CalculatedField
            if (
                not field_name
                or not field.get("upstreamColumns")
                or field.get("upstreamFields")
            ):
                continue
            input_columns = []
            for upstream_col in field.get("upstreamColumns"):
                if not upstream_col:
                    continue
                name = upstream_col.get("name")
                upstream_table_id = (
                    upstream_col.get("table")["id"]
                    if upstream_col.get("table")
                    else None
                )
                if (
                    name
                    and upstream_table_id
                    and upstream_table_id in table_id_to_urn.keys()
                ):
                    input_columns.append(
                        builder.make_schema_field_urn(
                            parent_urn=table_id_to_urn[upstream_table_id],
                            field_path=name,
                        )
                    )

            if input_columns:
                fine_grained_lineages.append(
                    FineGrainedLineage(
                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                        downstreams=sorted(
                            [builder.make_schema_field_urn(datasource_urn, field_name)]
                        ),
                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                        upstreams=sorted(input_columns),
                    )
                )

        return fine_grained_lineages

    def get_upstream_fields_of_field_in_datasource(self, datasource, datasource_urn):
        fine_grained_lineages = []
        for field in datasource.get("fields"):
            field_name = field.get("name")
            # It is observed that upstreamFields gives one-hop field
            # lineage, and not multi-hop field lineage
            # This behavior is as desired in our case.
            if not field_name or not field.get("upstreamFields"):
                continue
            input_fields = []
            for upstream_field in field.get("upstreamFields"):
                if not upstream_field:
                    continue
                name = upstream_field.get("name")
                upstream_ds_id = (
                    upstream_field.get("datasource")["id"]
                    if upstream_field.get("datasource")
                    else None
                )
                if name and upstream_ds_id:
                    input_fields.append(
                        builder.make_schema_field_urn(
                            parent_urn=builder.make_dataset_urn_with_platform_instance(
                                self.platform,
                                upstream_ds_id,
                                self.config.platform_instance,
                                self.config.env,
                            ),
                            field_path=name,
                        )
                    )
            if input_fields:
                fine_grained_lineages.append(
                    FineGrainedLineage(
                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                        downstreams=[
                            builder.make_schema_field_urn(datasource_urn, field_name)
                        ],
                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                        upstreams=input_fields,
                        transformOperation=self.get_transform_operation(field),
                    )
                )
        return fine_grained_lineages

    def get_transform_operation(self, field):
        field_type = field["__typename"]
        if field_type in ("DatasourceField", "ColumnField"):
            op = "IDENTITY"  # How to specify exact same
        elif field_type == "CalculatedField":
            op = field_type
            if field.get("formula"):
                op += f'formula: {field.get("formula")}'
        else:
            op = field_type  # BinField, CombinedField, etc

        return op

    def emit_custom_sql_datasources(self) -> Iterable[MetadataWorkUnit]:
        custom_sql_filter = f"idWithin: {json.dumps(self.custom_sql_ids_being_used)}"

        custom_sql_connection = list(
            self.get_connection_objects(
                custom_sql_graphql_query,
                "customSQLTablesConnection",
                custom_sql_filter,
            )
        )

        unique_custom_sql = get_unique_custom_sql(custom_sql_connection)

        for csql in unique_custom_sql:
            csql_id: str = csql["id"]
            csql_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=csql_id,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
            dataset_snapshot = DatasetSnapshot(
                urn=csql_urn,
                aspects=[self.get_data_platform_instance()],
            )

            datasource_name = None
            project = None
            if len(csql["datasources"]) > 0:
                # CustomSQLTable id owned by exactly one tableau data source
                logger.debug(
                    f"Number of datasources referencing CustomSQLTable: {len(csql['datasources'])}"
                )

                datasource = csql["datasources"][0]
                datasource_name = datasource.get("name")
                if datasource.get(
                    "__typename"
                ) == "EmbeddedDatasource" and datasource.get("workbook"):
                    datasource_name = (
                        f"{datasource.get('workbook').get('name')}/{datasource_name}"
                        if datasource_name and datasource.get("workbook").get("name")
                        else None
                    )
                    workunits = add_entity_to_container(
                        self.gen_workbook_key(datasource["workbook"]),
                        "dataset",
                        dataset_snapshot.urn,
                    )
                    for wu in workunits:
                        self.report.report_workunit(wu)
                        yield wu
                project = self._get_project(datasource)

                # lineage from custom sql -> datasets/tables #
                tables = csql.get("tables", [])
                yield from self._create_lineage_to_upstream_tables(
                    csql_urn, tables, datasource
                )

            #  Schema Metadata
            columns = csql.get("columns", [])
            schema_metadata = self.get_schema_metadata_for_custom_sql(columns)
            if schema_metadata is not None:
                dataset_snapshot.aspects.append(schema_metadata)

            # Browse path

            if project and datasource_name:
                browse_paths = BrowsePathsClass(
                    paths=[
                        f"/{self.config.env.lower()}/{self.platform}/{project}/{datasource['name']}"
                    ]
                )
                dataset_snapshot.aspects.append(browse_paths)
            else:
                logger.debug(f"Browse path not set for Custom SQL table {csql_id}")

            dataset_properties = DatasetPropertiesClass(
                name=csql.get("name"), description=csql.get("description")
            )

            dataset_snapshot.aspects.append(dataset_properties)

            view_properties = ViewPropertiesClass(
                materialized=False,
                viewLanguage="SQL",
                viewLogic=clean_query(csql.get("query", "")),
            )
            dataset_snapshot.aspects.append(view_properties)

            yield self.get_metadata_change_event(dataset_snapshot)
            yield self.get_metadata_change_proposal(
                dataset_snapshot.urn,
                aspect_name="subTypes",
                aspect=SubTypesClass(typeNames=["view", "Custom SQL"]),
            )

    def get_schema_metadata_for_custom_sql(
        self, columns: List[dict]
    ) -> Optional[SchemaMetadata]:
        fields = []
        schema_metadata = None
        for field in columns:
            # Datasource fields

            if field.get("name") is None:
                logger.warning(
                    f"Skipping field {field['id']} from schema since its name is none"
                )
                continue
            nativeDataType = field.get("remoteType", "UNKNOWN")
            TypeClass = FIELD_TYPE_MAPPING.get(nativeDataType, NullTypeClass)
            schema_field = SchemaField(
                fieldPath=field["name"],
                type=SchemaFieldDataType(type=TypeClass()),
                nativeDataType=nativeDataType,
                description=field.get("description", ""),
            )
            fields.append(schema_field)

        schema_metadata = SchemaMetadata(
            schemaName="test",
            platform=f"urn:li:dataPlatform:{self.platform}",
            version=0,
            fields=fields,
            hash="",
            platformSchema=OtherSchema(rawSchema=""),
        )
        return schema_metadata

    def _get_project(self, node):
        if node.get("__typename") == "EmbeddedDatasource" and node.get("workbook"):
            return node["workbook"].get("projectName")
        elif node.get("__typename") == "PublishedDatasource":
            return node.get("projectName")
        return None

    def _create_lineage_to_upstream_tables(
        self, csql_urn: str, tables: List[dict], datasource: dict
    ) -> Iterable[MetadataWorkUnit]:

        # This adds an edge to upstream DatabaseTables using `upstreamTables`
        upstream_tables, _ = self.get_upstream_tables(
            tables,
            datasource.get("name"),
            self._get_project(datasource),
            is_custom_sql=True,
        )

        if upstream_tables:
            upstream_lineage = UpstreamLineage(upstreams=upstream_tables)
            yield self.get_metadata_change_proposal(
                csql_urn,
                aspect_name="upstreamLineage",
                aspect=upstream_lineage,
            )

    def _get_schema_metadata_for_datasource(
        self, datasource_fields: List[dict]
    ) -> Optional[SchemaMetadata]:
        fields = []
        for field in datasource_fields:
            # check datasource - custom sql relations from a field being referenced
            self._track_custom_sql_ids(field)
            if field.get("name") is None:
                logger.warning(
                    f"Skipping field {field['id']} from schema since its name is none"
                )
                continue

            schema_field = tableau_field_to_schema_field(field, self.config.ingest_tags)
            fields.append(schema_field)

        return (
            SchemaMetadata(
                schemaName="test",
                platform=f"urn:li:dataPlatform:{self.platform}",
                version=0,
                fields=fields,
                hash="",
                platformSchema=OtherSchema(rawSchema=""),
            )
            if fields
            else None
        )

    def get_metadata_change_event(
        self, snap_shot: Union["DatasetSnapshot", "DashboardSnapshot", "ChartSnapshot"]
    ) -> MetadataWorkUnit:
        mce = MetadataChangeEvent(proposedSnapshot=snap_shot)
        work_unit = MetadataWorkUnit(id=snap_shot.urn, mce=mce)
        self.report.report_workunit(work_unit)

        return work_unit

    def get_metadata_change_proposal(
        self,
        urn: str,
        aspect_name: str,
        aspect: Union["UpstreamLineage", "SubTypesClass"],
    ) -> MetadataWorkUnit:
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=urn,
            aspectName=aspect_name,
            aspect=aspect,
        )
        mcp_workunit = MetadataWorkUnit(
            id=f"tableau-{mcp.entityUrn}-{mcp.aspectName}",
            mcp=mcp,
            treat_errors_as_warnings=True,
        )
        self.report.report_workunit(mcp_workunit)
        return mcp_workunit

    def emit_datasource(
        self, datasource: dict, workbook: dict = None, is_embedded_ds: bool = False
    ) -> Iterable[MetadataWorkUnit]:
        datasource_info = workbook
        if not is_embedded_ds:
            datasource_info = datasource

        project = (
            datasource_info.get("projectName", "").replace("/", REPLACE_SLASH_CHAR)
            if datasource_info
            else ""
        )
        datasource_id = datasource["id"]
        datasource_urn = builder.make_dataset_urn_with_platform_instance(
            self.platform, datasource_id, self.config.platform_instance, self.config.env
        )
        if datasource_id not in self.datasource_ids_being_used:
            self.datasource_ids_being_used.append(datasource_id)

        dataset_snapshot = DatasetSnapshot(
            urn=datasource_urn,
            aspects=[self.get_data_platform_instance()],
        )

        datasource_name = datasource.get("name") or datasource_id
        if is_embedded_ds and workbook and workbook.get("name"):
            datasource_name = f"{workbook['name']}/{datasource_name}"
        # Browse path
        browse_paths = BrowsePathsClass(
            paths=[f"/{self.config.env.lower()}/{self.platform}/{project}"]
        )
        dataset_snapshot.aspects.append(browse_paths)

        # Ownership
        owner = (
            self._get_ownership(datasource_info.get("owner", {}).get("username", ""))
            if datasource_info
            else None
        )
        if owner is not None:
            dataset_snapshot.aspects.append(owner)

        # Dataset properties
        dataset_props = DatasetPropertiesClass(
            name=datasource.get("name"),
            description=datasource.get("description"),
            customProperties={
                "hasExtracts": str(datasource.get("hasExtracts", "")),
                "extractLastRefreshTime": datasource.get("extractLastRefreshTime", "")
                or "",
                "extractLastIncrementalUpdateTime": datasource.get(
                    "extractLastIncrementalUpdateTime", ""
                )
                or "",
                "extractLastUpdateTime": datasource.get("extractLastUpdateTime", "")
                or "",
                "type": datasource.get("__typename", ""),
            },
        )
        dataset_snapshot.aspects.append(dataset_props)

        # Upstream Tables
        if datasource.get("upstreamTables") or datasource.get("upstreamDatasources"):
            # datasource -> db table relations
            (
                upstream_tables,
                fine_grained_lineages,
            ) = self._create_upstream_table_lineage(
                datasource, project, is_embedded_ds=is_embedded_ds
            )

            if upstream_tables:
                upstream_lineage = UpstreamLineage(
                    upstreams=upstream_tables,
                    fineGrainedLineages=sorted(
                        fine_grained_lineages,
                        key=lambda x: (x.downstreams, x.upstreams),
                    )
                    or None,
                )
                yield self.get_metadata_change_proposal(
                    datasource_urn,
                    aspect_name="upstreamLineage",
                    aspect=upstream_lineage,
                )

        # Datasource Fields
        schema_metadata = self._get_schema_metadata_for_datasource(
            datasource.get("fields", [])
        )
        if schema_metadata is not None:
            dataset_snapshot.aspects.append(schema_metadata)

        yield self.get_metadata_change_event(dataset_snapshot)
        yield self.get_metadata_change_proposal(
            dataset_snapshot.urn,
            aspect_name="subTypes",
            aspect=SubTypesClass(
                typeNames=(
                    ["Embedded Data Source"]
                    if is_embedded_ds
                    else ["Published Data Source"]
                )
            ),
        )

        if is_embedded_ds:
            workunits = add_entity_to_container(
                self.gen_workbook_key(workbook), "dataset", dataset_snapshot.urn
            )
            for wu in workunits:
                self.report.report_workunit(wu)
                yield wu

    def emit_published_datasources(self) -> Iterable[MetadataWorkUnit]:
        datasource_filter = f"idWithin: {json.dumps(self.datasource_ids_being_used)}"

        for datasource in self.get_connection_objects(
            published_datasource_graphql_query,
            "publishedDatasourcesConnection",
            datasource_filter,
        ):
            yield from self.emit_datasource(datasource)

    def emit_upstream_tables(self) -> Iterable[MetadataWorkUnit]:
        for (table_urn, (columns, path, is_embedded)) in self.upstream_tables.items():
            if not is_embedded and not self.config.ingest_tables_external:
                logger.debug(
                    f"Skipping external table {table_urn} as ingest_tables_external is set to False"
                )
                continue

            dataset_snapshot = DatasetSnapshot(
                urn=table_urn,
                aspects=[],
            )
            if path:
                # Browse path
                browse_paths = BrowsePathsClass(
                    paths=[f"/{self.config.env.lower()}/{self.platform}/{path}"]
                )
                dataset_snapshot.aspects.append(browse_paths)
            else:
                logger.debug(f"Browse path not set for table {table_urn}")
            schema_metadata = None
            if columns:
                fields = []
                for field in columns:
                    if field.get("name") is None:
                        logger.warning(
                            f"Skipping field {field['id']} from schema since its name is none"
                        )
                        continue
                    nativeDataType = field.get("remoteType", "UNKNOWN")
                    TypeClass = FIELD_TYPE_MAPPING.get(nativeDataType, NullTypeClass)

                    schema_field = SchemaField(
                        fieldPath=field["name"],
                        type=SchemaFieldDataType(type=TypeClass()),
                        description="",
                        nativeDataType=nativeDataType,
                    )

                    fields.append(schema_field)

                schema_metadata = SchemaMetadata(
                    schemaName="test",
                    platform=f"urn:li:dataPlatform:{self.platform}",
                    version=0,
                    fields=fields,
                    hash="",
                    platformSchema=OtherSchema(rawSchema=""),
                )
            if schema_metadata is not None:
                dataset_snapshot.aspects.append(schema_metadata)

            yield self.get_metadata_change_event(dataset_snapshot)

    def get_sheetwise_upstream_datasources(self, sheet: dict) -> set:
        sheet_upstream_datasources = set()

        for field in sheet.get("datasourceFields", ""):
            if field and field.get("datasource"):
                sheet_upstream_datasources.add(field["datasource"]["id"])

        return sheet_upstream_datasources

    @staticmethod
    def _create_datahub_chart_usage_stat(
        usage_stat: UsageStat,
    ) -> ChartUsageStatisticsClass:
        return ChartUsageStatisticsClass(
            timestampMillis=round(datetime.now().timestamp() * 1000),
            viewsCount=usage_stat.view_count,
        )

    def _get_chart_stat_wu(
        self, sheet: dict, sheet_urn: str
    ) -> Optional[MetadataWorkUnit]:
        luid: Optional[str] = sheet.get("luid")
        if luid is None:
            logger.debug(
                "stat:luid is none for sheet %s(id:%s)",
                sheet.get("name"),
                sheet.get("id"),
            )
            return None
        usage_stat: Optional[UsageStat] = self.tableau_stat_registry.get(luid)
        if usage_stat is None:
            logger.debug(
                "stat:UsageStat is not available in tableau_stat_registry for sheet %s(id:%s)",
                sheet.get("name"),
                sheet.get("id"),
            )
            return None

        aspect: ChartUsageStatisticsClass = self._create_datahub_chart_usage_stat(
            usage_stat
        )
        logger.debug(
            "stat: Chart usage stat work unit is created for %s(id:%s)",
            sheet.get("name"),
            sheet.get("id"),
        )
        return MetadataChangeProposalWrapper(
            aspect=aspect,
            entityUrn=sheet_urn,
        ).as_workunit()

    def emit_sheets_as_charts(self, workbook: Dict) -> Iterable[MetadataWorkUnit]:
        for sheet in workbook.get("sheets", []):
            sheet_urn: str = builder.make_chart_urn(
                self.platform, sheet.get("id"), self.config.platform_instance
            )
            chart_snapshot = ChartSnapshot(
                urn=sheet_urn,
                aspects=[self.get_data_platform_instance()],
            )

            creator: Optional[str] = workbook["owner"].get("username")
            created_at = sheet.get("createdAt", datetime.now())
            updated_at = sheet.get("updatedAt", datetime.now())
            last_modified = self.get_last_modified(creator, created_at, updated_at)

            if sheet.get("path"):
                site_part = f"/site/{self.config.site}" if self.config.site else ""
                sheet_external_url = (
                    f"{self.config.connect_uri}/#{site_part}/views/{sheet.get('path')}"
                )
            elif sheet.get("containedInDashboards"):
                # sheet contained in dashboard
                site_part = f"/t/{self.config.site}" if self.config.site else ""
                dashboard_path = sheet.get("containedInDashboards")[0].get("path", "")
                sheet_external_url = f"{self.config.connect_uri}{site_part}/authoring/{dashboard_path}/{sheet.get('name', '')}"
            else:
                # hidden or viz-in-tooltip sheet
                sheet_external_url = None
            input_fields: List[InputField] = []
            if sheet.get("datasourceFields"):
                self.populate_sheet_upstream_fields(sheet, input_fields)

            # datasource urn
            datasource_urn = []
            data_sources = self.get_sheetwise_upstream_datasources(sheet)

            for ds_id in data_sources:
                ds_urn = builder.make_dataset_urn_with_platform_instance(
                    self.platform, ds_id, self.config.platform_instance, self.config.env
                )
                datasource_urn.append(ds_urn)
                if ds_id not in self.datasource_ids_being_used:
                    self.datasource_ids_being_used.append(ds_id)

            # Chart Info
            chart_info = ChartInfoClass(
                description="",
                title=sheet.get("name", ""),
                lastModified=last_modified,
                externalUrl=sheet_external_url,
                inputs=sorted(datasource_urn),
                customProperties={"luid": sheet.get("luid") or ""},
            )
            chart_snapshot.aspects.append(chart_info)
            # chart_snapshot doesn't support the stat aspect as list element and hence need to emit MCP

            if self.config.extract_usage_stats:
                wu = self._get_chart_stat_wu(sheet, sheet_urn)
                if wu is not None:
                    self.report.report_workunit(wu)
                    yield wu

            if workbook.get("projectName") and workbook.get("name"):
                # Browse path
                browse_path = BrowsePathsClass(
                    paths=[
                        f"/{self.platform}/{workbook['projectName'].replace('/', REPLACE_SLASH_CHAR)}"
                        f"/{workbook['name']}"
                    ]
                )
                chart_snapshot.aspects.append(browse_path)
            else:
                logger.debug(f"Browse path not set for sheet {sheet['id']}")
            # Ownership
            owner = self._get_ownership(creator)
            if owner is not None:
                chart_snapshot.aspects.append(owner)

            #  Tags
            tag_list = sheet.get("tags", [])
            if tag_list and self.config.ingest_tags:
                tag_list_str = [t.get("name", "") for t in tag_list if t is not None]
                chart_snapshot.aspects.append(
                    builder.make_global_tag_aspect_with_tag_list(tag_list_str)
                )
            yield self.get_metadata_change_event(chart_snapshot)

            workunits = add_entity_to_container(
                self.gen_workbook_key(workbook), "chart", chart_snapshot.urn
            )

            for wu in workunits:
                self.report.report_workunit(wu)
                yield wu

            if input_fields:
                wu = MetadataChangeProposalWrapper(
                    entityUrn=sheet_urn,
                    aspect=InputFields(
                        fields=sorted(input_fields, key=lambda x: x.schemaFieldUrn)
                    ),
                ).as_workunit()
                self.report.report_workunit(wu)
                yield wu

    def populate_sheet_upstream_fields(
        self, sheet: dict, input_fields: List[InputField]
    ) -> None:
        for field in sheet.get("datasourceFields"):  # type: ignore
            if not field:
                continue
            name = field.get("name")
            upstream_ds_id = (
                field.get("datasource")["id"] if field.get("datasource") else None
            )
            if name and upstream_ds_id:
                name = get_field_value_in_sheet(field, "name")

                input_fields.append(
                    InputField(
                        schemaFieldUrn=builder.make_schema_field_urn(
                            parent_urn=builder.make_dataset_urn_with_platform_instance(
                                self.platform,
                                upstream_ds_id,
                                self.config.platform_instance,
                                self.config.env,
                            ),
                            field_path=name,
                        ),
                        schemaField=tableau_field_to_schema_field(
                            field, self.config.ingest_tags
                        ),
                    )
                )

    def emit_workbook_as_container(self, workbook: Dict) -> Iterable[MetadataWorkUnit]:

        workbook_container_key = self.gen_workbook_key(workbook)
        creator = workbook.get("owner", {}).get("username")

        owner_urn = (
            builder.make_user_urn(creator)
            if (creator and self.config.ingest_owner)
            else None
        )

        site_part = f"/site/{self.config.site}" if self.config.site else ""
        workbook_uri = workbook.get("uri", "")
        workbook_part = (
            workbook_uri[workbook_uri.index("/workbooks/") :]
            if workbook.get("uri")
            else None
        )
        workbook_external_url = (
            f"{self.config.connect_uri}/#{site_part}{workbook_part}"
            if workbook_part
            else None
        )

        tag_list = workbook.get("tags", [])
        tag_list_str = (
            [t.get("name", "") for t in tag_list if t is not None]
            if (tag_list and self.config.ingest_tags)
            else None
        )

        container_workunits = gen_containers(
            container_key=workbook_container_key,
            name=workbook.get("name", ""),
            sub_types=["Workbook"],
            description=workbook.get("description"),
            owner_urn=owner_urn,
            external_url=workbook_external_url,
            tags=tag_list_str,
        )

        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def gen_workbook_key(self, workbook):
        return WorkbookKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            workbook_id=workbook["id"],
        )

    @staticmethod
    def _create_datahub_dashboard_usage_stat(
        usage_stat: UsageStat,
    ) -> DashboardUsageStatisticsClass:
        return DashboardUsageStatisticsClass(
            timestampMillis=round(datetime.now().timestamp() * 1000),
            # favoritesCount=looker_dashboard.favorite_count,  It is available in REST API response,
            # however not exposed by tableau python library
            viewsCount=usage_stat.view_count,
            # lastViewedAt=looker_dashboard.last_viewed_at, Not available
        )

    def _get_dashboard_stat_wu(
        self, dashboard: dict, dashboard_urn: str
    ) -> Optional[MetadataWorkUnit]:
        luid: Optional[str] = dashboard.get("luid")
        if luid is None:
            logger.debug(
                "stat:luid is none for dashboard %s(id:%s)",
                dashboard.get("name"),
                dashboard.get("id"),
            )
            return None
        usage_stat: Optional[UsageStat] = self.tableau_stat_registry.get(luid)
        if usage_stat is None:
            logger.debug(
                "stat:UsageStat is not available in tableau_stat_registry for dashboard %s(id:%s)",
                dashboard.get("name"),
                dashboard.get("id"),
            )
            return None

        aspect: DashboardUsageStatisticsClass = (
            self._create_datahub_dashboard_usage_stat(usage_stat)
        )
        logger.debug(
            "stat: Dashboard usage stat is created for %s(id:%s)",
            dashboard.get("name"),
            dashboard.get("id"),
        )

        return MetadataChangeProposalWrapper(
            aspect=aspect,
            entityUrn=dashboard_urn,
        ).as_workunit()

    def emit_dashboards(self, workbook: Dict) -> Iterable[MetadataWorkUnit]:
        for dashboard in workbook.get("dashboards", []):
            dashboard_urn: str = builder.make_dashboard_urn(
                self.platform, dashboard["id"], self.config.platform_instance
            )
            dashboard_snapshot = DashboardSnapshot(
                urn=dashboard_urn,
                aspects=[self.get_data_platform_instance()],
            )

            creator = workbook.get("owner", {}).get("username", "")
            created_at = dashboard.get("createdAt", datetime.now())
            updated_at = dashboard.get("updatedAt", datetime.now())
            last_modified = self.get_last_modified(creator, created_at, updated_at)

            site_part = f"/site/{self.config.site}" if self.config.site else ""
            dashboard_external_url = f"{self.config.connect_uri}/#{site_part}/views/{dashboard.get('path', '')}"
            title = (
                dashboard["name"].replace("/", REPLACE_SLASH_CHAR)
                if dashboard.get("name")
                else ""
            )
            chart_urns = [
                builder.make_chart_urn(
                    self.platform, sheet.get("id"), self.config.platform_instance
                )
                for sheet in dashboard.get("sheets", [])
            ]
            dashboard_info_class = DashboardInfoClass(
                description="",
                title=title,
                charts=chart_urns,
                lastModified=last_modified,
                dashboardUrl=dashboard_external_url,
                customProperties={"luid": dashboard.get("luid") or ""},
            )
            dashboard_snapshot.aspects.append(dashboard_info_class)

            tag_list = dashboard.get("tags", [])
            if tag_list and self.config.ingest_tags:
                tag_list_str = [t.get("name", "") for t in tag_list if t is not None]
                dashboard_snapshot.aspects.append(
                    builder.make_global_tag_aspect_with_tag_list(tag_list_str)
                )

            if self.config.extract_usage_stats:
                # dashboard_snapshot doesn't support the stat aspect as list element and hence need to emit MetadataWorkUnit
                wu = self._get_dashboard_stat_wu(dashboard, dashboard_urn)
                if wu is not None:
                    self.report.report_workunit(wu)
                    yield wu

            if workbook.get("projectName") and workbook.get("name"):
                # browse path
                browse_paths = BrowsePathsClass(
                    paths=[
                        f"/{self.platform}/{workbook['projectName'].replace('/', REPLACE_SLASH_CHAR)}"
                        f"/{workbook['name'].replace('/', REPLACE_SLASH_CHAR)}"
                    ]
                )
                dashboard_snapshot.aspects.append(browse_paths)
            else:
                logger.debug(f"Browse path not set for dashboard {dashboard['id']}")

            # Ownership
            owner = self._get_ownership(creator)
            if owner is not None:
                dashboard_snapshot.aspects.append(owner)

            yield self.get_metadata_change_event(dashboard_snapshot)

            workunits = add_entity_to_container(
                self.gen_workbook_key(workbook), "dashboard", dashboard_snapshot.urn
            )
            for wu in workunits:
                self.report.report_workunit(wu)
                yield wu

    def emit_embedded_datasources(self) -> Iterable[MetadataWorkUnit]:
        datasource_filter = (
            f"idWithin: {json.dumps(self.embedded_datasource_ids_being_used)}"
        )

        for datasource in self.get_connection_objects(
            embedded_datasource_graphql_query,
            "embeddedDatasourcesConnection",
            datasource_filter,
        ):
            yield from self.emit_datasource(
                datasource, datasource.get("workbook"), is_embedded_ds=True
            )

    @lru_cache(maxsize=None)
    def _get_schema(self, schema_provided: str, database: str, fullName: str) -> str:

        # For some databases, the schema attribute in tableau api does not return
        # correct schema name for the table. For more information, see
        # https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_model.html#schema_attribute.
        # Hence we extract schema from fullName whenever fullName is available
        schema = self._extract_schema_from_fullName(fullName) if fullName else ""
        if not schema:
            schema = schema_provided
        elif schema != schema_provided:
            logger.debug(
                "Correcting schema, provided {0}, corrected {1}".format(
                    schema_provided, schema
                )
            )

        if not schema and database in self.config.default_schema_map:
            schema = self.config.default_schema_map[database]

        return schema

    @lru_cache(maxsize=None)
    def _extract_schema_from_fullName(self, fullName: str) -> str:
        # fullName is observed to be in format [schemaName].[tableName]
        # OR simply tableName OR [tableName]
        if fullName.startswith("[") and "].[" in fullName:
            return fullName[1 : fullName.index("]")]
        return ""

    @lru_cache(maxsize=None)
    def get_last_modified(
        self, creator: Optional[str], created_at: bytes, updated_at: bytes
    ) -> ChangeAuditStamps:
        last_modified = ChangeAuditStamps()
        if creator:
            modified_actor = builder.make_user_urn(creator)
            created_ts = int(dp.parse(created_at).timestamp() * 1000)
            modified_ts = int(dp.parse(updated_at).timestamp() * 1000)
            last_modified = ChangeAuditStamps(
                created=AuditStamp(time=created_ts, actor=modified_actor),
                lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
            )
        return last_modified

    @lru_cache(maxsize=None)
    def _get_ownership(self, user: str) -> Optional[OwnershipClass]:
        if self.config.ingest_owner and user:
            owner_urn = builder.make_user_urn(user)
            ownership: OwnershipClass = OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=owner_urn,
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ]
            )
            return ownership

        return None

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = TableauConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return auto_stale_entity_removal(
            self.stale_entity_removal_handler,
            auto_status_aspect(self.get_workunits_internal()),
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        if self.server is None or not self.server.is_signed_in():
            return
        try:
            # Initialise the dictionary to later look-up for chart and dashboard stat
            if self.config.extract_usage_stats:
                self._populate_usage_stat_registry()
            yield from self.emit_workbooks()
            if self.embedded_datasource_ids_being_used:
                yield from self.emit_embedded_datasources()
            if self.datasource_ids_being_used:
                yield from self.emit_published_datasources()
            if self.custom_sql_ids_being_used:
                yield from self.emit_custom_sql_datasources()
            yield from self.emit_upstream_tables()
        except MetadataQueryException as md_exception:
            self.report.report_failure(
                key="tableau-metadata",
                reason=f"Unable to retrieve metadata from tableau. Information: {str(md_exception)}",
            )

    def get_report(self) -> StaleEntityRemovalSourceReport:
        return self.report

    def get_platform_instance_id(self) -> str:
        return self.config.platform_instance or self.platform
