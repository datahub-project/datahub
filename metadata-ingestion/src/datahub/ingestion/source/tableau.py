import logging
import json
from functools import lru_cache

import dateutil.parser as dp

from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.tableau_common import *
from datahub.metadata.schema_classes import ChangeTypeClass, DatasetPropertiesClass, OwnerClass, SubTypesClass, \
    ViewPropertiesClass, BrowsePathsClass, OwnershipClass, ChartInfoClass, GlobalTagsClass, TagAssociationClass
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineage,
)

from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchema,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
)
import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import make_user_urn
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.common import PipelineContext
from tableauserverclient import Server, TableauAuth, ServerResponseError

from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    ChangeAuditStamps,
)
from datetime import datetime

from pydantic import validator
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
    used_ds_id = []
    used_csql_id = []
    upstream_lineage_map = {}

    def __hash__(self):
        return id(self)

    def __init__(self, ctx: PipelineContext, config: TableauConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
        self.server = Server(config.connect_uri, use_server_version=True)
        self.input_urn = []
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

    def emit_workbook_dashboard_sheet_mces(self, count_on_query) -> Iterable[MetadataWorkUnit]:
        """
        Generate entities:
            workbooks(chart), dashboards(dataset), sheets(dataset)
        relations:
            workbook -> dashboard, workbook -> sheet, dashboard -> sheet
        """

        projects = f"projectNameWithin: {json.dumps(self.config.projects)}" if self.config.projects else ""

        wbs = query_metadata(self.server, workbook_graphql_query, 'workbooksConnection', 0, 0, projects)
        workbook_connection = wbs.get('data', {}).get('workbooksConnection', {})
        total_count = workbook_connection.get('totalCount', 0)
        has_next_page = workbook_connection.get('pageInfo', {}).get('hasNextPage', False)
        current_count = 0
        while has_next_page:
            count = count_on_query if current_count + count_on_query < total_count else total_count - current_count
            wbs = query_metadata(self.server, workbook_graphql_query, 'workbooksConnection', count, current_count, projects)
            workbook_connection = wbs.get('data', {}).get('workbooksConnection', {})
            total_count = workbook_connection.get('totalCount', 0)
            has_next_page = workbook_connection.get('pageInfo', {}).get('hasNextPage', False)
            current_count += count

            for wb in workbook_connection.get('nodes', []):
                self.input_urn = []
                # list input entities for workbook
                yield from self.get_sheets_as_dataset_mce(wb)
                yield from self.get_dashboards_as_dataset_mce(wb)
                yield from self.get_embedded_ds_as_dataset_mce(wb)

                for ds in wb.get('upstreamDatasources', []):
                    ds_id = ds.get('id')
                    if ds_id is not None and ds_id not in self.used_ds_id:
                        self.used_ds_id.append(ds_id)

                wb_snapshot = self.construct_wb_as_chart(wb, self.input_urn)
                mce = MetadataChangeEvent(proposedSnapshot=wb_snapshot)
                wu = MetadataWorkUnit(id=wb_snapshot.urn, mce=mce)

                self.report.report_workunit(wu)
                yield wu

    def get_sheets_as_dataset_mce(self, wb):
        for sheet in wb.get('sheets', []):
            # generate sheet wu
            # because we dont have displayed name option for dataset, using composite key for all entities
            sheet_urn = make_dataset_urn(self.platform, self.config.env, 'sh', wb.get('projectName', ''),
                                         wb.get('name', ''), sheet.get('name', ''))
            self.input_urn.append(sheet_urn)

            # don't know what to do with worksheetFields for now
            fields = []
            for field in sheet.get('datasourceFields', ''):
                TypeClass = TARGET_FIELD_TYPE_MAPPING.get(
                    get_field_value_in_sheet(field, 'dataType'),
                    NullTypeClass
                )
                schema_field = SchemaField(
                    fieldPath=get_field_value_in_sheet(field, 'name'),
                    type=SchemaFieldDataType(type=TypeClass()),
                    nativeDataType=get_field_value_in_sheet(field, 'dataType') or '',
                    description=make_description_from_params(
                        get_field_value_in_sheet(field, 'description'),
                        get_field_value_in_sheet(field, 'formula')
                    ),
                    globalTags=get_tags_from_params(
                        [
                            get_field_value_in_sheet(field, 'role'),
                            get_field_value_in_sheet(field, '__typename'),
                            get_field_value_in_sheet(field, 'aggregation')
                        ]
                    )
                )
                fields.append(schema_field)

            schema_metadata = SchemaMetadata(
                schemaName='test',
                platform=builder.make_data_platform_urn(self.platform),
                version=0,
                fields=fields,
                hash="",
                platformSchema=OtherSchema(rawSchema=""),
            )

            browse_paths = BrowsePathsClass(
                paths=[
                    f"/{self.platform}/{wb.get('projectName', '').replace('/', '|')}/"
                    f"{sheet.get('name', '').replace('/', '|')}"])

            sheet_path = find_sheet_path(sheet.get('name', ''), wb.get('dashboards', []))

            if self.config.site is not None:
                sheet_external_url = f"{self.config.connect_uri}/t/{self.config.site}/authoring/{sheet_path}"
            else:
                sheet_external_url = f"{self.config.connect_uri}#/{wb.get('uri', '').replace('sites/1/', '')}"

            dataset_properties = DatasetPropertiesClass(
                description='',
                customProperties={},
                externalUrl=sheet_external_url
            )

            dataset_snapshot = DatasetSnapshot(
                urn=sheet_urn,
                aspects=[browse_paths, dataset_properties, schema_metadata],
            )
            dataset_snapshot.aspects.append(self._get_ownership(wb.get('owner', {}).get('username', '')))

            sheet_mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            sheet_workunit = MetadataWorkUnit(
                id=dataset_snapshot.urn,
                mce=sheet_mce
            )
            self.report.report_workunit(sheet_workunit)
            yield sheet_workunit

            sheet_subtype_mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_snapshot.urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["sheet"]),
            )
            sheet_subtype_workunit = MetadataWorkUnit(
                id=f"{self.platform}-{sheet_subtype_mcp.entityUrn}-{sheet_subtype_mcp.aspectName}",
                mcp=sheet_subtype_mcp,
                treat_errors_as_warnings=True
            )
            self.report.report_workunit(sheet_subtype_workunit)
            yield sheet_subtype_workunit

    def get_dashboards_as_dataset_mce(self, wb):
        for dashboard in wb.get('dashboards', []):
            # generate dashboards wu with dashboard -> sheet relations
            db_urn = make_dataset_urn(self.platform, self.config.env, 'db', wb.get('projectName', ''),
                                      wb.get('name', ''), dashboard.get('name', ''))
            browse_paths = BrowsePathsClass(
                paths=[
                    f"/{self.platform}/{wb.get('projectName', '').replace('/', '|')}/"
                    f"{dashboard.get('name', '').replace('/', '|')}"])

            if self.config.site is not None:
                dashboard_external_url = f"{self.config.connect_uri}/#/site/{self.config.site}/views/{dashboard.get('path','')}"
            else:
                dashboard_external_url = f"{self.config.connect_uri}#/{wb.get('uri', '').replace('sites/1/', '')}"

            dataset_properties_class = DatasetPropertiesClass(
                description='',
                customProperties={},
                externalUrl=dashboard_external_url
            )
            dataset_snapshot = DatasetSnapshot(
                urn=db_urn,
                aspects=[browse_paths, dataset_properties_class]
            )
            dataset_snapshot.aspects.append(self._get_ownership(wb.get('owner', {}).get('username', '')))

            db_mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            db_workunit = MetadataWorkUnit(
                id=dataset_snapshot.urn,
                mce=db_mce
            )
            self.report.report_workunit(db_workunit)
            yield db_workunit

            # subtype as dashboard
            db_subtype_mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_snapshot.urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["dashboard"]),
            )
            db_subtype_workunit = MetadataWorkUnit(
                id=f"{self.platform}-{db_subtype_mcp.entityUrn}-{db_subtype_mcp.aspectName}",
                mcp=db_subtype_mcp,
                treat_errors_as_warnings=True
            )
            self.report.report_workunit(db_subtype_workunit)
            yield db_subtype_workunit

            # upstream tables
            upstream_tables: List[UpstreamClass] = []
            for upstream_sheet in dashboard.get('sheets', []):
                sheet_urn = make_dataset_urn(self.platform, self.config.env, 'sh', wb.get('projectName', ''),
                                             wb.get('name', ''), upstream_sheet.get('name', ''))
                upstream_table = UpstreamClass(
                    dataset=sheet_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
                upstream_tables.append(upstream_table)
            upstream_lineage = UpstreamLineage(upstreams=upstream_tables)

            lineage_mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=db_urn,
                aspectName="upstreamLineage",
                aspect=upstream_lineage,
            )
            mcp_workunit = MetadataWorkUnit(
                id=f"{self.platform}-{lineage_mcp.entityUrn}-{db_urn}-{lineage_mcp.aspectName}",
                mcp=lineage_mcp,
                treat_errors_as_warnings=True
            )
            self.report.report_workunit(mcp_workunit)
            yield mcp_workunit

    def get_embedded_ds_as_dataset_mce(self, wb):
        for ds in wb.get('embeddedDatasources', []):
            # generate datasource wu with sheet -> datasource and datasource -> db table relations
            # skipping nested ds
            if ds.get("upstreamDatasources", ""):
                continue

            project = wb.get('projectName', '')
            ds_urn = make_dataset_urn(self.platform, self.config.env, 'ds', project,
                                      ds.get('workbook', {}).get('name', ''), ds.get('name', ''))

            dataset_snapshot = DatasetSnapshot(
                urn=ds_urn,
                aspects=[],
            )
            browse_paths = BrowsePathsClass(paths=[
                f"{self.platform}/{project.replace('/', '|')}/{ds.get('name', '').replace('/', '|')}"])
            dataset_snapshot.aspects.append(browse_paths)

            dataset_props = DatasetPropertiesClass(
                description=ds.get('description', ''),
                customProperties={
                    'hasExtracts': str(ds.get('hasExtracts', '')),
                    'extractLastRefreshTime': ds.get('extractLastRefreshTime', '') or '',
                    'extractLastIncrementalUpdateTime': ds.get('extractLastIncrementalUpdateTime', '') or '',
                    'extractLastUpdateTime': ds.get('extractLastUpdateTime', '') or '',
                    'type': ds.get('__typename', ''),
                    'owner': ds.get('owner', {}).get('username', ''),  # TODO create owner aspect
                },
            )

            # need to solve problem with published ds and add it to dataset_props.externalUrl
            # if ds.get('uri'):
            #     externalUrl = f"{self.config.connect_uri}#/{ds['uri'].replace('sites/1/','')}"
            #     dataset_props.externalUrl = externalUrl

            dataset_snapshot.aspects.append(dataset_props)

            if ds.get('downstreamSheets', '') is not None:
                # sheet -> datasource relations
                for sheet in ds.get('downstreamSheets', ''):
                    sheet_urn = make_dataset_urn(self.platform, self.config.env, 'sh',
                                                 wb.get('projectName', ''),
                                                 wb.get('name', ''), sheet.get('name', ''))
                    if sheet_urn in self.upstream_lineage_map:
                        upstream_tables = self.upstream_lineage_map.get(sheet_urn)
                        upstream_tables.append(
                            UpstreamClass(
                                dataset=ds_urn,
                                type=DatasetLineageTypeClass.TRANSFORMED,
                            )
                        )
                        self.upstream_lineage_map[sheet_urn] = upstream_tables

                        upstream_lineage = UpstreamLineage(upstreams=upstream_tables)
                        lineage_mcp = MetadataChangeProposalWrapper(
                            entityType="dataset",
                            changeType=ChangeTypeClass.UPSERT,
                            entityUrn=sheet_urn,
                            aspectName="upstreamLineage",
                            aspect=upstream_lineage,
                        )
                        mcp_workunit = MetadataWorkUnit(
                            id=f"tableau-{lineage_mcp.entityUrn}-{dataset_snapshot.urn}-{lineage_mcp.aspectName}-sh",
                            mcp=lineage_mcp,
                            treat_errors_as_warnings=True
                        )
                        self.report.report_workunit(mcp_workunit)
                        yield mcp_workunit

            if ds.get('upstreamTables', '') is not None:
                # datasource -> db table relations
                upstream_tables: List[UpstreamClass] = []
                for table in ds.get('upstreamTables', ''):
                    table_urn = make_table_urn(self.config.env, table.get('connectionType', ''),
                                               table.get('fullName', ''))
                    upstream_table = UpstreamClass(
                        dataset=table_urn,
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                    upstream_tables.append(upstream_table)
                self.upstream_lineage_map[ds_urn] = upstream_tables

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
                    treat_errors_as_warnings=True
                )
                self.report.report_workunit(mcp_workunit)
                yield mcp_workunit

            ds_fields = ds.get('fields', []) or []
            fields = []
            for field in ds_fields:
                # check datasource - custom sql relations
                if field.get('__typename', '') == "ColumnField":
                    for column in field.get('columns', []):
                        table_id = column.get('table', {}).get('id')
                        if table_id is not None and table_id not in self.used_csql_id:
                            self.used_csql_id.append(table_id)

                TypeClass = TARGET_FIELD_TYPE_MAPPING.get(
                    field.get('dataType'),
                    NullTypeClass
                )
                schema_field = SchemaField(
                    fieldPath=field['name'],
                    type=SchemaFieldDataType(type=TypeClass()),
                    description=make_description_from_params(field['description'], field.get('formula')),
                    nativeDataType=field.get('dataType', 'unknown'),
                    globalTags=get_tags_from_params(
                        [field.get('role'), field['__typename'], field.get('aggregation')])
                )
                fields.append(schema_field)

            schema_metadata = SchemaMetadata(
                schemaName='test',
                platform=f"urn:li:dataPlatform:{self.platform}",
                version=0,
                fields=fields,
                hash="",
                platformSchema=OtherSchema(rawSchema=""),
            )
            if schema_metadata is not None:
                dataset_snapshot.aspects.append(schema_metadata)

            sheet_mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            workunit = MetadataWorkUnit(
                id=dataset_snapshot.urn,
                mce=sheet_mce
            )
            self.report.report_workunit(workunit)
            yield workunit

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
                treat_errors_as_warnings=True
            )
            self.report.report_workunit(mcp_workunit)
            yield mcp_workunit

    def construct_wb_as_chart(self, wb_data, input_urn) -> ChartSnapshot:
        dashboards = wb_data.get('dashboards', [])
        for dashboard in dashboards:
            chart_urn = builder.make_chart_urn(self.platform, dashboard.get('id'))
            chart_snapshot = ChartSnapshot(
                urn=chart_urn,
                aspects=[],
            )

            actor_username = wb_data.get('owner', {}).get('username')
            modified_actor = builder.make_user_urn(actor_username)
            created_ts = int(
                dp.parse(f"{dashboard.get('createdAt', datetime.now())}").timestamp() * 1000
            )
            modified_ts = int(
                dp.parse(f"{dashboard.get('updatedAt', datetime.now())}").timestamp() * 1000
            )
            title = dashboard.get("name", "") or ""

            last_modified = ChangeAuditStamps(
                created=AuditStamp(time=created_ts, actor=modified_actor),
                lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
            )
            if self.config.site is not None:
                chart_url = f"{self.config.connect_uri}/#/site/{self.config.site}/views/{dashboard.get('path', '')}"
            else:
                chart_url = f"{self.config.connect_uri}/#/{wb_data.get('uri', '').replace('sites/1/','')}"
            description = wb_data.get('description', '')

            customProperties = {
                "createdAt": wb_data.get('createdAt'),
                "updatedAt": wb_data.get('updatedAt'),
            }

            chart_info = ChartInfoClass(
                description=description,
                title=title,
                lastModified=last_modified,
                chartUrl=chart_url,
                inputs=input_urn,
                customProperties=customProperties,
            )
            chart_snapshot.aspects.append(chart_info)

            browse_paths_info = BrowsePathsClass(paths=[f"/{self.platform}/"
                                                        f"{wb_data.get('projectName','').replace('/','|')}/"
                                                        f"{title}"])
            chart_snapshot.aspects.append(browse_paths_info)
            chart_snapshot.aspects.append(self._get_ownership(actor_username))

            return chart_snapshot

    @lru_cache(maxsize=None)
    def _get_ownership(self, user: str) -> Optional[OwnershipClass]:
        if user is not None:
            owner_urn = builder.make_user_urn(user)
            ownership: OwnershipClass = OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=owner_urn,
                        type=models.OwnershipTypeClass.DATAOWNER,
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
        yield from self.emit_workbook_dashboard_sheet_mces(10) # TODO recipefy this count

    def get_report(self) -> SourceReport:
        return self.report
