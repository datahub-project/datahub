import json
import logging
import uuid
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple

import dateutil.parser as dp
from pydantic import validator
from tableauserverclient import Server, ServerResponseError, TableauAuth

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.tableau_common import (
    FIELD_TYPE_MAPPING,
    clean_query,
    custom_sql_graphql_query,
    find_sheet_path,
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


class TableauConfig(ConfigModel):
    connect_uri: str = "localhost:8088"
    username: Optional[str] = None
    password: Optional[str] = None
    site: Optional[str] = None
    projects: Optional[List] = None
    default_schema: str = "public"
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
        self.server = Server(config.connect_uri, use_server_version=True)
        self.used_ds_id: List[str] = []
        self.used_csql_id: List[str] = []
        try:
            auth = TableauAuth(config.username, config.password, self.config.site)
            self.server.auth.sign_in(auth)
        except ServerResponseError as e:
            self.report.report_failure(
                key="tableau-login",
                reason=f"Unable to Login for user "
                f"{self.config.username}, "
                f"Reason: {str(e)}",
            )
        # TODO login through PersonalAccessTokenAuth class
        # https://tableau.github.io/server-client-python/docs/api-ref#authentication

    def close(self) -> None:
        self.server.auth.sign_out()

    def emit_workbook_mces(self, count_on_query: int) -> Iterable[MetadataWorkUnit]:
        projects = (
            f"projectNameWithin: {json.dumps(self.config.projects)}"
            if self.config.projects
            else ""
        )

        wbs = query_metadata(
            self.server, workbook_graphql_query, "workbooksConnection", 0, 0, projects
        )
        workbook_connection = wbs.get("data", {}).get("workbooksConnection", {})
        total_count = workbook_connection.get("totalCount", 0)
        has_next_page = workbook_connection.get("pageInfo", {}).get(
            "hasNextPage", False
        )
        current_count = 0
        while has_next_page:
            count = (
                count_on_query
                if current_count + count_on_query < total_count
                else total_count - current_count
            )
            wbs = query_metadata(
                self.server,
                workbook_graphql_query,
                "workbooksConnection",
                count,
                current_count,
                projects,
            )
            workbook_connection = wbs.get("data", {}).get("workbooksConnection", {})
            total_count = workbook_connection.get("totalCount", 0)
            has_next_page = workbook_connection.get("pageInfo", {}).get(
                "hasNextPage", False
            )
            current_count += count

            for wb in workbook_connection.get("nodes", []):
                yield from self.emit_sheets_as_charts(wb)
                yield from self.emit_dashboards(wb)
                yield from self.emit_embedded_ds(wb)
                yield from self.emit_upstream_tables()

    def _get_chart_urn(self, sheet: Dict, wb: Dict) -> str:
        chart_urn = builder.make_chart_urn(
            self.platform,
            sheet.get(
                "id",
                f"{wb.get('projectName', '')}"
                f".{wb.get('name', '')}"
                f".{sheet.get('name', '')}",
            ),
        )
        return chart_urn

    def _create_upstream_table_lineage(
        self, ds: dict, project: str, isCustomSQL: bool = False
    ) -> List[UpstreamClass]:
        upstream_tables = []
        upstream_dbs = ds.get("upstreamDatabases", [])
        upstream_db = upstream_dbs[0].get("name", "") if upstream_dbs else ""

        for table in ds.get("upstreamTables", []):
            # skip upstream tables when there is no column info when retrieving embedded datasource
            # Schema details for these will be taken care in self.emit_custom_sql_ds()
            if not isCustomSQL:
                if not table.get("columns"):
                    continue

            schema = table.get("schema", "")
            if schema is None or not schema:
                schema = self.config.default_schema

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
            table_path = f"{project.replace('/', '|')}/{ds.get('name', '')}/{table.get('name', '')}"
            self.upstream_tables[table_urn] = (
                table.get("columns", []),
                table_path,
            )
        return upstream_tables

    def emit_custom_sql_ds(self) -> Iterable[MetadataWorkUnit]:
        count_on_query = len(self.used_csql_id)
        customsqls = query_metadata(
            self.server,
            custom_sql_graphql_query,
            "customSQLTablesConnection",
            0,
            0,
            "idWithin: {}".format(json.dumps(self.used_csql_id)),
        )
        c_sql_connection = customsqls.get("data", {}).get(
            "customSQLTablesConnection", {}
        )
        total_count = c_sql_connection.get("totalCount")
        has_next_page = c_sql_connection.get("pageInfo", {}).get("hasNextPage", False)
        current_count = 0
        while has_next_page:
            count = (
                count_on_query
                if current_count + count_on_query < total_count
                else total_count - current_count
            )
            customsqls = query_metadata(
                self.server,
                custom_sql_graphql_query,
                "customSQLTablesConnection",
                count,
                current_count,
                "idWithin: {}".format(json.dumps(self.used_csql_id)),
            )
            c_sql_connection = customsqls.get("data", {}).get(
                "customSQLTablesConnection", {}
            )
            total_count = c_sql_connection.get("totalCount")
            has_next_page = c_sql_connection.get("pageInfo", {}).get(
                "hasNextPage", False
            )
            current_count += count
            # get relationship between custom sql and embedded data sources only via columns and fields
            unique_customsql = []
            for customsql in c_sql_connection.get("nodes", []):
                unique_csql = {
                    "id": customsql.get("id"),
                    "name": customsql.get("name"),
                    "query": customsql.get("query"),
                    "columns": customsql.get("columns"),
                    "tables": customsql.get("tables"),
                }
                datasource_for_csql = []
                for column in customsql.get("columns", []):
                    for field in column.get("referencedByFields", []):
                        ds = field.get("datasource")
                        if ds not in datasource_for_csql:
                            datasource_for_csql.append(ds)

                unique_csql["datasources"] = datasource_for_csql
                unique_customsql.append(unique_csql)

            for csql in unique_customsql:
                csql_name = f"{csql['name']}.{csql.get('id', uuid.uuid4())}"
                csql_urn = builder.make_dataset_urn(
                    self.platform, csql_name, self.config.env
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
                            self.platform,
                            f"{ds.get('name')}.{ds.get('id')}",
                            self.config.env,
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
                    datasources = [
                        ref.get("datasource") for ref in field.get("referencedByFields")
                    ]
                    for ds in datasources:
                        if ds.get("id", "") in used_ds:
                            continue
                        used_ds.append(ds.get("id", ""))

                        upstream_tables = self._create_upstream_table_lineage(
                            ds, ds.get("workbook", {}).get("projectName", ""), True
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

                view_properties = ViewPropertiesClass(
                    materialized=False,
                    viewLanguage="SQL",
                    viewLogic=clean_query(csql["query"]),
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
                    aspect=SubTypesClass(typeNames=["custom_sql", "view"]),
                )
                mcp_workunit = MetadataWorkUnit(
                    id=f"tableau-{mcp.entityUrn}-{mcp.aspectName}",
                    mcp=mcp,
                    treat_errors_as_warnings=True,
                )
                self.report.report_workunit(mcp_workunit)
                yield mcp_workunit

    def emit_datasource(self, ds: dict, wb: dict = None) -> Iterable[MetadataWorkUnit]:
        if wb is None:
            wb = ds

        project = wb.get("projectName", "")
        ds_id = ds.get("id", uuid.uuid4())
        ds_name = f"{ds.get('name')}.{ds_id}"
        ds_urn = builder.make_dataset_urn(self.platform, ds_name, self.config.env)
        dataset_snapshot = DatasetSnapshot(
            urn=ds_urn,
            aspects=[],
        )
        browse_paths = BrowsePathsClass(
            paths=[
                f"/{self.config.env.lower()}/{self.platform}/{project.replace('/', '|')}/{ds.get('name', '')}/{ds_name}"
            ]
        )
        dataset_snapshot.aspects.append(browse_paths)

        # maintain a list of ds being actively used and retrieve metadata for
        # only for those in self.emit_published_ds()
        if ds_id not in self.used_ds_id:
            self.used_ds_id.append(ds_id)

        owner = self._get_ownership(wb.get("owner", {}).get("username", ""))
        if owner is not None:
            dataset_snapshot.aspects.append(owner)

        dataset_props = DatasetPropertiesClass(
            description=ds.get("name", ""),
            customProperties={
                "hasExtracts": str(ds.get("hasExtracts", "")),
                "extractLastRefreshTime": ds.get("extractLastRefreshTime", "") or "",
                "extractLastIncrementalUpdateTime": ds.get(
                    "extractLastIncrementalUpdateTime", ""
                )
                or "",
                "extractLastUpdateTime": ds.get("extractLastUpdateTime", "") or "",
                "type": ds.get("__typename", ""),
            },
        )
        dataset_snapshot.aspects.append(dataset_props)

        # Upstream Tables
        if ds.get("upstreamTables") is not None:
            # datasource -> db table relations
            upstream_tables = self._create_upstream_table_lineage(ds, project)

            if upstream_tables:
                upstream_lineage = UpstreamLineage(upstreams=upstream_tables)
                lineage_mcp = MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=ds_urn,
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
        ds_fields = ds.get("fields", []) or []
        fields = []
        for field in ds_fields:
            # check datasource - custom sql relations
            if field.get("__typename", "") == "ColumnField":
                for column in field.get("columns", []):
                    table_id = column.get("table", {}).get("id")
                    if table_id is not None and table_id not in self.used_csql_id:
                        self.used_csql_id.append(table_id)

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
                ),
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
            aspect=SubTypesClass(typeNames=["datasource"]),
        )
        mcp_workunit = MetadataWorkUnit(
            id=f"tableau-{mcp.entityUrn}-{mcp.aspectName}",
            mcp=mcp,
            treat_errors_as_warnings=True,
        )
        self.report.report_workunit(mcp_workunit)
        yield mcp_workunit

    def emit_published_ds(self) -> Iterable[MetadataWorkUnit]:
        count_on_query = len(self.used_ds_id)
        dss = query_metadata(
            self.server,
            published_datasource_graphql_query,
            "publishedDatasourcesConnection",
            0,
            0,
            "idWithin: {}".format(json.dumps(self.used_ds_id)),
        )
        published_ds_conn = dss.get("data", {}).get(
            "publishedDatasourcesConnection", {}
        )
        total_count = published_ds_conn.get("totalCount", 0)
        has_next_page = published_ds_conn.get("pageInfo", {}).get("hasNextPage", False)
        current_count = 0
        while has_next_page:
            count = (
                count_on_query
                if current_count + count_on_query < total_count
                else total_count - current_count
            )
            dss = query_metadata(
                self.server,
                published_datasource_graphql_query,
                "publishedDatasourcesConnection",
                count,
                current_count,
                "idWithin: {}".format(json.dumps(self.used_ds_id)),
            )
            published_ds_conn = dss.get("data", {}).get(
                "publishedDatasourcesConnection", {}
            )
            total_count = published_ds_conn.get("totalCount", 0)
            has_next_page = published_ds_conn.get("pageInfo", {}).get(
                "hasNextPage", False
            )
            current_count += count
            for ds in published_ds_conn.get("nodes", []):
                yield from self.emit_datasource(ds)

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

    def emit_sheets_as_charts(self, wb: Dict) -> Iterable[MetadataWorkUnit]:
        for sheet in wb.get("sheets", []):
            chart_urn = self._get_chart_urn(sheet, wb)
            chart_snapshot = ChartSnapshot(
                urn=chart_urn,
                aspects=[],
            )

            last_modified = ChangeAuditStamps()
            creator = wb.get("owner", {}).get("username", "")
            if creator is not None:
                modified_actor = builder.make_user_urn(creator)
                created_ts = int(
                    dp.parse(sheet.get("createdAt", datetime.now())).timestamp() * 1000
                )
                modified_ts = int(
                    dp.parse(sheet.get("updatedAt", datetime.now())).timestamp() * 1000
                )
                last_modified = ChangeAuditStamps(
                    created=AuditStamp(time=created_ts, actor=modified_actor),
                    lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
                )

            sheet_path = find_sheet_path(
                sheet.get("name", ""), wb.get("dashboards", [])
            )
            if self.config.site is not None:
                sheet_external_url = f"{self.config.connect_uri}/t/{self.config.site}/authoring/{sheet_path}"
            else:
                sheet_external_url = f"{self.config.connect_uri}#/{wb.get('uri', '').replace('sites/1/', '')}"

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
            for ds in data_sources:
                ds_id = ds.get("id", uuid.uuid4())
                ds_urn = builder.make_dataset_urn(
                    self.platform,
                    f"{ds.get('name')}.{ds_id}",
                    self.config.env,
                )
                datasource_urn.append(ds_urn)
                if ds_id not in self.used_ds_id:
                    self.used_ds_id.append(ds_id)

            # Chart Info
            chart_info = ChartInfoClass(
                description="",
                title=sheet.get("name", ""),
                lastModified=last_modified,
                chartUrl=sheet_external_url,
                inputs=datasource_urn,
                customProperties=fields,
            )
            chart_snapshot.aspects.append(chart_info)

            browse_path = BrowsePathsClass(
                paths=[
                    f"/{self.platform}/{wb.get('projectName', '').replace('/', '|')}"
                    f"/{wb.get('name', '')}"
                    f"/{sheet.get('name', '').replace('/', '|')}"
                ]
            )
            chart_snapshot.aspects.append(browse_path)

            # Ownership
            owner = self._get_ownership(creator)
            if owner is not None:
                chart_snapshot.aspects.append(owner)

            #  Tags
            tag_list = sheet.get("tags", [])
            if tag_list:
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

    def emit_dashboards(self, wb: Dict) -> Iterable[MetadataWorkUnit]:
        for dashboard in wb.get("dashboards", []):
            dashboard_urn = builder.make_dashboard_urn(
                self.platform, dashboard.get("id", "")
            )

            dashboard_snapshot = DashboardSnapshot(
                urn=dashboard_urn,
                aspects=[],
            )

            last_modified = ChangeAuditStamps()
            creator = wb.get("owner", {}).get("username", "")
            if creator is not None:
                modified_actor = builder.make_user_urn(creator)
                modified_ts = int(
                    dp.parse(
                        f"{dashboard.get('updatedAt', datetime.now())}"
                    ).timestamp()
                    * 1000
                )
                created_ts = int(
                    dp.parse(
                        f"{dashboard.get('createdAt', datetime.now())}"
                    ).timestamp()
                    * 1000
                )
                last_modified = ChangeAuditStamps(
                    created=AuditStamp(time=created_ts, actor=modified_actor),
                    lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
                )

            if self.config.site is not None:
                dashboard_external_url = f"{self.config.connect_uri}/#/site/{self.config.site}/views/{dashboard.get('path', '')}"
            else:
                dashboard_external_url = f"{self.config.connect_uri}#/{wb.get('uri', '').replace('sites/1/', '')}"

            title = dashboard.get("name", "") or ""
            chart_urns = [
                self._get_chart_urn(s, wb) for s in dashboard.get("sheets", [])
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
                    f"/{self.platform}/{wb.get('projectName', '').replace('/', '|')}"
                    f"/{wb.get('name', '')}"
                    f"/{dashboard.get('name', '').replace('/', '|')}"
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

    def emit_embedded_ds(self, wb: Dict) -> Iterable[MetadataWorkUnit]:
        for ds in wb.get("embeddedDatasources", []):
            yield from self.emit_datasource(ds, wb)

    @lru_cache(maxsize=None)
    def _get_ownership(self, user: str) -> Optional[OwnershipClass]:
        if user is not None:
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
        yield from self.emit_workbook_mces(10)
        if self.used_ds_id:
            yield from self.emit_published_ds()
        if self.used_csql_id:
            yield from self.emit_custom_sql_ds()

    def get_report(self) -> SourceReport:
        return self.report
