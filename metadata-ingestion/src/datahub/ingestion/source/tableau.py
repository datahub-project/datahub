import json
import logging
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple

import dateutil.parser as dp
from pydantic import validator
from tableauserverclient import (
    PersonalAccessTokenAuth,
    Server,
    ServerResponseError,
    TableauAuth,
)

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.tableau_common import (
    FIELD_TYPE_MAPPING,
    MetadataQueryException,
    clean_query,
    custom_sql_graphql_query,
    get_field_value_in_sheet,
    get_tags_from_params,
    make_description_from_params,
    make_table_urn,
    published_datasource_graphql_query,
    query_metadata,
    workbook_graphql_query,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    ChangeAuditStamps,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
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
    DashboardInfoClass,
    DatasetPropertiesClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SubTypesClass,
    ViewPropertiesClass,
)
from datahub.utilities import config_clean

logger: logging.Logger = logging.getLogger(__name__)

# Replace / with |
REPLACE_SLASH_CHAR = "|"


class TableauConfig(ConfigModel):
    connect_uri: str
    username: Optional[str] = None
    password: Optional[str] = None
    token_name: Optional[str] = None
    token_value: Optional[str] = None
    site: str
    projects: Optional[List] = ["default"]
    default_schema_map: dict = {}
    ingest_tags: Optional[bool] = False
    ingest_owner: Optional[bool] = False
    env: str = builder.DEFAULT_ENV

    @validator("connect_uri")
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)


class TableauSource(Source):
    config: TableauConfig
    report: SourceReport
    platform = "tableau"
    server: Server
    upstream_tables: Dict[str, Tuple[Any, str]] = {}

    def __hash__(self):
        return id(self)

    def __init__(self, ctx: PipelineContext, config: TableauConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
        # This list keeps track of datasource being actively used by workbooks, so we only retrieve those when emitting
        # published data sources.
        self.datasource_ids_being_used: List[str] = []
        # This list keeps track of datasource being actively used by workbooks, so we only retrieve those when emitting
        # custom SQL data sources.
        self.custom_sql_ids_being_used: List[str] = []

        self._authenticate()

    def close(self) -> None:
        self.server.auth.sign_out()

    def _authenticate(self):
        # https://tableau.github.io/server-client-python/docs/api-ref#authentication
        authentication = None
        if self.config.username and self.config.password:
            authentication = TableauAuth(
                self.config.username, self.config.password, self.config.site
            )
        elif self.config.token_name and self.config.token_value:
            authentication = PersonalAccessTokenAuth(
                self.config.token_name, self.config.token_value, self.config.site
            )
        else:
            self.report.report_failure(
                key="tableau-login", reason="No valid authentication was found"
            )

        try:
            self.server = Server(self.config.connect_uri, use_server_version=True)
            self.server.auth.sign_in(authentication)
        except ServerResponseError as e:
            self.report.report_failure(
                key="tableau-login",
                reason=f"Unable to Login with credentials provided" f"Reason: {str(e)}",
            )
        except Exception as e:
            self.report.report_failure(
                key="tableau-login", reason=f"Unable to Login" f"Reason: {str(e)}"
            )

    def get_connection_object(
        self,
        query: str,
        connection_type: str,
        query_filter: str,
        count: int = 0,
        current_count: int = 0,
    ) -> Tuple[dict, int, int]:
        query_data = query_metadata(
            self.server, query, connection_type, count, current_count, query_filter
        )
        connection_object = query_data.get("data", {}).get(connection_type, {})
        total_count = connection_object.get("totalCount", 0)
        has_next_page = connection_object.get("pageInfo", {}).get("hasNextPage", False)
        return connection_object, total_count, has_next_page

    def emit_workbooks(self, count_on_query: int) -> Iterable[MetadataWorkUnit]:
        projects = f"projectNameWithin: {json.dumps(self.config.projects)}"
        workbook_connection, total_count, has_next_page = self.get_connection_object(
            workbook_graphql_query, "workbooksConnection", projects
        )

        current_count = 0
        while has_next_page:
            count = (
                count_on_query
                if current_count + count_on_query < total_count
                else total_count - current_count
            )
            (
                workbook_connection,
                total_count,
                has_next_page,
            ) = self.get_connection_object(
                workbook_graphql_query,
                "workbooksConnection",
                projects,
                count,
                current_count,
            )
            current_count += count

            for workbook in workbook_connection.get("nodes", []):
                yield from self.emit_sheets_as_charts(workbook)
                yield from self.emit_dashboards(workbook)
                yield from self.emit_embedded_datasource(workbook)
                yield from self.emit_upstream_tables()

    def _create_upstream_table_lineage(
        self, ds: dict, project: str, is_custom_sql: bool = False
    ) -> List[UpstreamClass]:
        upstream_tables = []
        upstream_dbs = ds.get("upstreamDatabases", [])
        upstream_db = upstream_dbs[0].get("name", "") if upstream_dbs else ""

        for table in ds.get("upstreamTables", []):
            # skip upstream tables when there is no column info when retrieving embedded datasource
            # Schema details for these will be taken care in self.emit_custom_sql_ds()
            if not is_custom_sql and not table.get("columns"):
                continue

            schema = self._get_schema(table.get("schema", ""), upstream_db)
            table_urn = make_table_urn(
                self.config.env,
                upstream_db,
                table.get("connectionType", ""),
                schema,
                table.get("name", ""),
            )

            upstream_table = UpstreamClass(
                dataset=table_urn,
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            upstream_tables.append(upstream_table)
            table_path = f"{project.replace('/', REPLACE_SLASH_CHAR)}/{ds.get('name', '')}/{table.get('name', '')}"
            self.upstream_tables[table_urn] = (
                table.get("columns", []),
                table_path,
            )
        return upstream_tables

    def emit_custom_sql_datasources(self) -> Iterable[MetadataWorkUnit]:
        count_on_query = len(self.custom_sql_ids_being_used)
        custom_sql_filter = "idWithin: {}".format(
            json.dumps(self.custom_sql_ids_being_used)
        )
        custom_sql_connection, total_count, has_next_page = self.get_connection_object(
            custom_sql_graphql_query, "customSQLTablesConnection", custom_sql_filter
        )

        current_count = 0
        while has_next_page:
            count = (
                count_on_query
                if current_count + count_on_query < total_count
                else total_count - current_count
            )
            (
                custom_sql_connection,
                total_count,
                has_next_page,
            ) = self.get_connection_object(
                custom_sql_graphql_query,
                "customSQLTablesConnection",
                custom_sql_filter,
                count,
                current_count,
            )
            current_count += count
            # get relationship between custom sql and embedded data sources only via columns and fields
            unique_custom_sql = []
            for custom_sql in custom_sql_connection.get("nodes", []):
                unique_csql = {
                    "id": custom_sql.get("id"),
                    "name": custom_sql.get("name"),
                    "query": custom_sql.get("query"),
                    "columns": custom_sql.get("columns"),
                    "tables": custom_sql.get("tables"),
                }
                datasource_for_csql = []
                for column in custom_sql.get("columns", []):
                    for field in column.get("referencedByFields", []):
                        datasource = field.get("datasource")
                        if datasource not in datasource_for_csql:
                            datasource_for_csql.append(datasource)

                unique_csql["datasources"] = datasource_for_csql
                unique_custom_sql.append(unique_csql)

            for csql in unique_custom_sql:
                csql_id: str = csql.get("id", "")
                csql_urn = builder.make_dataset_urn(
                    self.platform, csql_id, self.config.env
                )
                dataset_snapshot = DatasetSnapshot(
                    urn=csql_urn,
                    aspects=[],
                )

                # lineage from datasource -> custom sql source
                csql_datasource = csql.get("datasources", [])
                if csql_datasource is not None:
                    for ds in csql_datasource:
                        ds_urn = builder.make_dataset_urn(
                            self.platform, ds.get("id"), self.config.env
                        )
                        upstream_csql = UpstreamClass(
                            dataset=csql_urn,
                            type=DatasetLineageTypeClass.TRANSFORMED,
                        )

                        upstream_lineage = UpstreamLineage(upstreams=[upstream_csql])
                        lineage_mcp = MetadataChangeProposalWrapper(
                            entityType="dataset",
                            changeType=ChangeTypeClass.UPSERT,
                            entityUrn=ds_urn,
                            aspectName="upstreamLineage",
                            aspect=upstream_lineage,
                        )
                        mcp_workunit = MetadataWorkUnit(
                            id=f"tableau-{lineage_mcp.entityUrn}-{lineage_mcp.aspectName}-csql",
                            mcp=lineage_mcp,
                            treat_errors_as_warnings=True,
                        )
                        self.report.report_workunit(mcp_workunit)
                        yield mcp_workunit

                #  lineage from custom sql -> datasets/tables
                used_ds = []
                fields: List[SchemaField] = []
                for field in csql.get("columns", []):
                    data_sources = [
                        reference.get("datasource")
                        for reference in field.get("referencedByFields")
                        if reference.get("datasource") is not None
                    ]
                    for datasource in data_sources:
                        if datasource.get("id", "") in used_ds:
                            continue
                        used_ds.append(datasource.get("id", ""))

                        upstream_tables = self._create_upstream_table_lineage(
                            datasource,
                            datasource.get("workbook", {}).get("projectName", ""),
                            True,
                        )
                        if upstream_tables:
                            upstream_lineage = UpstreamLineage(
                                upstreams=upstream_tables
                            )
                            lineage_mcp = MetadataChangeProposalWrapper(
                                entityType="dataset",
                                changeType=ChangeTypeClass.UPSERT,
                                entityUrn=csql_urn,
                                aspectName="upstreamLineage",
                                aspect=upstream_lineage,
                            )
                            mcp_workunit = MetadataWorkUnit(
                                id=f"tableau-{lineage_mcp.entityUrn}-{dataset_snapshot.urn}-{lineage_mcp.aspectName}-tables",
                                mcp=lineage_mcp,
                                treat_errors_as_warnings=True,
                            )
                            self.report.report_workunit(mcp_workunit)
                            yield mcp_workunit

                    # Datasource fields
                    fields = []
                    TypeClass = FIELD_TYPE_MAPPING.get(
                        field.get("remoteType", "UNKNOWN"), NullTypeClass
                    )
                    schema_field = SchemaField(
                        fieldPath=field.get("name", ""),
                        type=SchemaFieldDataType(type=TypeClass()),
                        nativeDataType=field.get("remoteType", "UNKNOWN"),
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

                if schema_metadata is not None:
                    dataset_snapshot.aspects.append(schema_metadata)

                browse_paths = BrowsePathsClass(
                    paths=[
                        f"/{self.config.env.lower()}/{self.platform}/Custom SQL/{csql.get('name', '')}/{csql_id}"
                    ]
                )
                dataset_snapshot.aspects.append(browse_paths)

                view_properties = ViewPropertiesClass(
                    materialized=False,
                    viewLanguage="SQL",
                    viewLogic=clean_query(csql.get("query", "")),
                )
                dataset_snapshot.aspects.append(view_properties)

                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                workunit = MetadataWorkUnit(id=dataset_snapshot.urn, mce=mce)
                self.report.report_workunit(workunit)
                yield workunit

                mcp = MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=dataset_snapshot.urn,
                    aspectName="subTypes",
                    aspect=SubTypesClass(typeNames=["View", "Custom SQL"]),
                )
                mcp_workunit = MetadataWorkUnit(
                    id=f"tableau-{mcp.entityUrn}-{mcp.aspectName}",
                    mcp=mcp,
                    treat_errors_as_warnings=True,
                )
                self.report.report_workunit(mcp_workunit)
                yield mcp_workunit

    def emit_datasource(
        self, datasource: dict, workbook: dict = None
    ) -> Iterable[MetadataWorkUnit]:
        datasource_info = workbook
        if workbook is None:
            datasource_info = datasource

        project = (
            datasource_info.get("projectName", "").replace("/", REPLACE_SLASH_CHAR)
            if datasource_info
            else ""
        )
        datasource_id = datasource.get("id", "")
        datasource_name = f"{datasource.get('name')}.{datasource_id}"
        datasource_urn = builder.make_dataset_urn(
            self.platform, datasource_id, self.config.env
        )
        dataset_snapshot = DatasetSnapshot(
            urn=datasource_urn,
            aspects=[],
        )
        browse_paths = BrowsePathsClass(
            paths=[
                f"/{self.config.env.lower()}/{self.platform}/{project}/{datasource.get('name', '')}/{datasource_name}"
            ]
        )
        dataset_snapshot.aspects.append(browse_paths)

        if datasource_id not in self.datasource_ids_being_used:
            self.datasource_ids_being_used.append(datasource_id)

        owner = (
            self._get_ownership(datasource_info.get("owner", {}).get("username", ""))
            if datasource_info
            else None
        )
        if owner is not None:
            dataset_snapshot.aspects.append(owner)

        dataset_props = DatasetPropertiesClass(
            description=datasource.get("name", ""),
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
        if datasource.get("upstreamTables") is not None:
            # datasource -> db table relations
            upstream_tables = self._create_upstream_table_lineage(datasource, project)

            if upstream_tables:
                upstream_lineage = UpstreamLineage(upstreams=upstream_tables)
                lineage_mcp = MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=datasource_urn,
                    aspectName="upstreamLineage",
                    aspect=upstream_lineage,
                )
                mcp_workunit = MetadataWorkUnit(
                    id=f"tableau-{lineage_mcp.entityUrn}-{dataset_snapshot.urn}-{lineage_mcp.aspectName}-tables",
                    mcp=lineage_mcp,
                    treat_errors_as_warnings=True,
                )
                self.report.report_workunit(mcp_workunit)
                yield mcp_workunit

        # Datasource Fields
        ds_fields = datasource.get("fields", []) or []
        fields = []
        for field in ds_fields:
            # check datasource - custom sql relations
            if field.get("__typename", "") == "ColumnField":
                for column in field.get("columns", []):
                    table_id = column.get("table", {}).get("id")
                    if (
                        table_id is not None
                        and table_id not in self.custom_sql_ids_being_used
                    ):
                        self.custom_sql_ids_being_used.append(table_id)

            TypeClass = FIELD_TYPE_MAPPING.get(
                field.get("dataType", "UNKNOWN"), NullTypeClass
            )
            schema_field = SchemaField(
                fieldPath=field["name"],
                type=SchemaFieldDataType(type=TypeClass()),
                description=make_description_from_params(
                    field.get("description", ""), field.get("formula")
                ),
                nativeDataType=field.get("dataType", "UNKNOWN"),
                globalTags=get_tags_from_params(
                    [
                        field.get("role"),
                        field.get("__typename", ""),
                        field.get("aggregation"),
                    ]
                )
                if self.config.ingest_tags
                else None,
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

        dataset_mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        workunit = MetadataWorkUnit(id=dataset_snapshot.urn, mce=dataset_mce)
        self.report.report_workunit(workunit)
        yield workunit

        # Dataset Subtype as datasource
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=dataset_snapshot.urn,
            aspectName="subTypes",
            aspect=SubTypesClass(typeNames=["Data Source"]),
        )
        mcp_workunit = MetadataWorkUnit(
            id=f"tableau-{mcp.entityUrn}-{mcp.aspectName}",
            mcp=mcp,
            treat_errors_as_warnings=True,
        )
        self.report.report_workunit(mcp_workunit)
        yield mcp_workunit

    def emit_published_datasources(self) -> Iterable[MetadataWorkUnit]:
        count_on_query = len(self.datasource_ids_being_used)
        datasource_filter = "idWithin: {}".format(
            json.dumps(self.datasource_ids_being_used)
        )
        (
            published_datasource_conn,
            total_count,
            has_next_page,
        ) = self.get_connection_object(
            published_datasource_graphql_query,
            "publishedDatasourcesConnection",
            datasource_filter,
        )

        current_count = 0
        while has_next_page:
            count = (
                count_on_query
                if current_count + count_on_query < total_count
                else total_count - current_count
            )
            (
                published_datasource_conn,
                total_count,
                has_next_page,
            ) = self.get_connection_object(
                published_datasource_graphql_query,
                "publishedDatasourcesConnection",
                datasource_filter,
                count,
                current_count,
            )

            current_count += count
            for datasource in published_datasource_conn.get("nodes", []):
                yield from self.emit_datasource(datasource)

    def emit_upstream_tables(self) -> Iterable[MetadataWorkUnit]:
        for (table_urn, (columns, path)) in self.upstream_tables.items():
            dataset_snapshot = DatasetSnapshot(
                urn=table_urn,
                aspects=[],
            )

            browse_paths = BrowsePathsClass(
                paths=[f"/{self.config.env.lower()}/{self.platform}/{path}"]
            )
            dataset_snapshot.aspects.append(browse_paths)

            fields = []
            for field in columns:
                TypeClass = FIELD_TYPE_MAPPING.get(
                    field.get("remoteType", "UNKNOWN"), NullTypeClass
                )
                schema_field = SchemaField(
                    fieldPath=field["name"],
                    type=SchemaFieldDataType(type=TypeClass()),
                    description="",
                    nativeDataType=field.get("remoteType") or "unknown",
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

            dataset_mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            workunit = MetadataWorkUnit(id=dataset_snapshot.urn, mce=dataset_mce)
            self.report.report_workunit(workunit)
            yield workunit

    def emit_sheets_as_charts(self, workbook: Dict) -> Iterable[MetadataWorkUnit]:
        for sheet in workbook.get("sheets", []):
            chart_snapshot = ChartSnapshot(
                urn=builder.make_chart_urn(self.platform, sheet.get("id")),
                aspects=[],
            )

            creator = workbook.get("owner", {}).get("username", "")
            created_at = sheet.get("createdAt", datetime.now())
            updated_at = sheet.get("updatedAt", datetime.now())
            last_modified = self.get_last_modified(creator, created_at, updated_at)

            if sheet.get("path", ""):
                sheet_external_url = f"{self.config.connect_uri}#/site/{self.config.site}/views/{sheet.get('path', '')}"
            else:
                # sheet contained in dashboard
                dashboard_path = sheet.get("containedInDashboards")[0].get("path", "")
                sheet_external_url = f"{self.config.connect_uri}/t/{self.config.site}/authoring/{dashboard_path}/{sheet.get('name', '')}"

            fields = {}
            for field in sheet.get("datasourceFields", ""):
                description = make_description_from_params(
                    get_field_value_in_sheet(field, "description"),
                    get_field_value_in_sheet(field, "formula"),
                )
                fields[get_field_value_in_sheet(field, "name")] = description

            # datasource urn
            datasource_urn = []
            data_sources = sheet.get("upstreamDatasources", [])
            for datasource in data_sources:
                ds_id = datasource.get("id")
                if ds_id is None or not ds_id:
                    continue
                ds_urn = builder.make_dataset_urn(self.platform, ds_id, self.config.env)
                datasource_urn.append(ds_urn)
                if ds_id not in self.datasource_ids_being_used:
                    self.datasource_ids_being_used.append(ds_id)

            # Chart Info
            chart_info = ChartInfoClass(
                description="",
                title=sheet.get("name", ""),
                lastModified=last_modified,
                externalUrl=sheet_external_url,
                inputs=datasource_urn,
                customProperties=fields,
            )
            chart_snapshot.aspects.append(chart_info)

            browse_path = BrowsePathsClass(
                paths=[
                    f"/{self.platform}/{workbook.get('projectName', '').replace('/', REPLACE_SLASH_CHAR)}"
                    f"/{workbook.get('name', '')}"
                    f"/{sheet.get('name', '').replace('/', REPLACE_SLASH_CHAR)}"
                ]
            )
            chart_snapshot.aspects.append(browse_path)

            # Ownership
            owner = self._get_ownership(creator)
            if owner is not None:
                chart_snapshot.aspects.append(owner)

            #  Tags
            tag_list = sheet.get("tags", [])
            if tag_list and self.config.ingest_tags:
                tag_list_str = [
                    t.get("name", "").upper() for t in tag_list if t is not None
                ]
                chart_snapshot.aspects.append(
                    builder.make_global_tag_aspect_with_tag_list(tag_list_str)
                )

            mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)
            wu = MetadataWorkUnit(id=chart_snapshot.urn, mce=mce)
            self.report.report_workunit(wu)
            yield wu

    def emit_dashboards(self, workbook: Dict) -> Iterable[MetadataWorkUnit]:
        for dashboard in workbook.get("dashboards", []):
            dashboard_snapshot = DashboardSnapshot(
                urn=builder.make_dashboard_urn(self.platform, dashboard.get("id", "")),
                aspects=[],
            )

            creator = workbook.get("owner", {}).get("username", "")
            created_at = dashboard.get("createdAt", datetime.now())
            updated_at = dashboard.get("updatedAt", datetime.now())
            last_modified = self.get_last_modified(creator, created_at, updated_at)

            dashboard_external_url = f"{self.config.connect_uri}/#/site/{self.config.site}/views/{dashboard.get('path', '')}"
            title = dashboard.get("name", "").replace("/", REPLACE_SLASH_CHAR) or ""
            chart_urns = [
                builder.make_chart_urn(self.platform, sheet.get("id"))
                for sheet in dashboard.get("sheets", [])
            ]
            dashboard_info_class = DashboardInfoClass(
                description="",
                title=title,
                charts=chart_urns,
                lastModified=last_modified,
                dashboardUrl=dashboard_external_url,
                customProperties={},
            )
            dashboard_snapshot.aspects.append(dashboard_info_class)

            # browse path
            browse_paths = BrowsePathsClass(
                paths=[
                    f"/{self.platform}/{workbook.get('projectName', '').replace('/', REPLACE_SLASH_CHAR)}"
                    f"/{workbook.get('name', '').replace('/', REPLACE_SLASH_CHAR)}"
                    f"/{title}"
                ]
            )
            dashboard_snapshot.aspects.append(browse_paths)

            # Ownership
            owner = self._get_ownership(creator)
            if owner is not None:
                dashboard_snapshot.aspects.append(owner)

            db_mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
            db_workunit = MetadataWorkUnit(id=dashboard_snapshot.urn, mce=db_mce)
            self.report.report_workunit(db_workunit)
            yield db_workunit

    def emit_embedded_datasource(self, wb: Dict) -> Iterable[MetadataWorkUnit]:
        for datasource in wb.get("embeddedDatasources", []):
            yield from self.emit_datasource(datasource, wb)

    @lru_cache(maxsize=None)
    def _get_schema(self, schema_provided: str, database: str) -> str:
        schema = schema_provided
        if not schema_provided and database in self.config.default_schema_map:
            schema = self.config.default_schema_map[database]

        return schema

    @lru_cache(maxsize=None)
    def get_last_modified(
        self, creator: str, created_at: bytes, updated_at: bytes
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
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        try:
            yield from self.emit_workbooks(10)
            if self.datasource_ids_being_used:
                yield from self.emit_published_datasources()
            if self.custom_sql_ids_being_used:
                yield from self.emit_custom_sql_datasources()
        except MetadataQueryException as md_exception:
            self.report.report_failure(
                key="tableau-metadata",
                reason=f"Unable to retrieve metadata from tableau. Information: {str(md_exception)}",
            )

    def get_report(self) -> SourceReport:
        return self.report
