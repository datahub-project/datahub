from datetime import datetime
from functools import lru_cache
from typing import Dict, Iterable, Optional

import dateutil.parser as dp
import requests
from pydantic import validator
from pydantic.fields import Field
from requests.models import HTTPError
from sqllineage.runner import LineageRunner

import datahub.emitter.mce_builder as builder
from datahub.configuration.source_common import DatasetLineageProviderConfigBase
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    ChangeAuditStamps,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot,
    DashboardSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    ChartQueryClass,
    ChartQueryTypeClass,
    ChartTypeClass,
    DashboardInfoClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.utilities import config_clean


class MetabaseConfig(DatasetLineageProviderConfigBase):
    # See the Metabase /api/session endpoint for details
    # https://www.metabase.com/docs/latest/api-documentation.html#post-apisession
    connect_uri: str = Field(default="localhost:3000", description="Metabase host URL.")
    username: str = Field(default=None, description="Metabase username.")
    password: str = Field(default=None, description="Metabase password.")
    database_alias_map: Optional[dict] = Field(
        default=None,
        description="Database name map to use when constructing dataset URN.",
    )
    engine_platform_map: Optional[Dict[str, str]] = Field(
        default=None,
        description="Custom mappings between metabase database engines and DataHub platforms",
    )
    default_schema: str = Field(
        default="public",
        description="Default schema name to use when schema is not provided in an SQL query",
    )

    @validator("connect_uri")
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)


@platform_name("Metabase")
@config_class(MetabaseConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class MetabaseSource(Source):
    """
    This plugin extracts Charts, dashboards, and associated metadata. This plugin is in beta and has only been tested
    on PostgreSQL and H2 database.
    ### Dashboard

    [/api/dashboard](https://www.metabase.com/docs/latest/api-documentation.html#dashboard) endpoint is used to
    retrieve the following dashboard information.

    - Title and description
    - Last edited by
    - Owner
    - Link to the dashboard in Metabase
    - Associated charts

    ### Chart

    [/api/card](https://www.metabase.com/docs/latest/api-documentation.html#card) endpoint is used to
    retrieve the following information.

    - Title and description
    - Last edited by
    - Owner
    - Link to the chart in Metabase
    - Datasource and lineage

    The following properties for a chart are ingested in DataHub.

    | Name          | Description                                     |
    | ------------- | ----------------------------------------------- |
    | `Dimensions`  | Column names                                    |
    | `Filters`     | Any filters applied to the chart                |
    | `Metrics`     | All columns that are being used for aggregation |


    """

    config: MetabaseConfig
    report: SourceReport
    platform = "metabase"

    def __hash__(self):
        return id(self)

    def __init__(self, ctx: PipelineContext, config: MetabaseConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

        login_response = requests.post(
            f"{self.config.connect_uri}/api/session",
            None,
            {
                "username": self.config.username,
                "password": self.config.password,
            },
        )

        login_response.raise_for_status()
        self.access_token = login_response.json().get("id", "")

        self.session = requests.session()
        self.session.headers.update(
            {
                "X-Metabase-Session": f"{self.access_token}",
                "Content-Type": "application/json",
                "Accept": "*/*",
            }
        )

        # Test the connection
        try:
            test_response = self.session.get(
                f"{self.config.connect_uri}/api/user/current"
            )
            test_response.raise_for_status()
        except HTTPError as e:
            self.report.report_failure(
                key="metabase-session",
                reason=f"Unable to retrieve user {self.config.username} information. %s"
                % str(e),
            )

    def close(self) -> None:
        response = requests.delete(
            f"{self.config.connect_uri}/api/session",
            headers={"X-Metabase-Session": self.access_token},
        )
        if response.status_code not in (200, 204):
            self.report.report_failure(
                key="metabase-session",
                reason=f"Unable to logout for user {self.config.username}",
            )

    def emit_dashboard_mces(self) -> Iterable[MetadataWorkUnit]:
        try:
            dashboard_response = self.session.get(
                f"{self.config.connect_uri}/api/dashboard"
            )
            dashboard_response.raise_for_status()
            dashboards = dashboard_response.json()

            for dashboard_info in dashboards:
                dashboard_snapshot = self.construct_dashboard_from_api_data(
                    dashboard_info
                )
                if dashboard_snapshot is not None:
                    mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
                    wu = MetadataWorkUnit(id=dashboard_snapshot.urn, mce=mce)
                    self.report.report_workunit(wu)
                    yield wu

        except HTTPError as http_error:
            self.report.report_failure(
                key="metabase-dashboard",
                reason=f"Unable to retrieve dashboards. " f"Reason: {str(http_error)}",
            )

    @staticmethod
    def get_timestamp_millis_from_ts_string(ts_str: str) -> int:
        """
        Converts the given timestamp string to milliseconds. If parsing fails,
        returns the utc-now in milliseconds.
        """
        try:
            return int(dp.parse(ts_str).timestamp() * 1000)
        except (dp.ParserError, OverflowError):
            return int(datetime.utcnow().timestamp() * 1000)

    def construct_dashboard_from_api_data(
        self, dashboard_info: dict
    ) -> Optional[DashboardSnapshot]:

        dashboard_id = dashboard_info.get("id", "")
        dashboard_url = f"{self.config.connect_uri}/api/dashboard/{dashboard_id}"
        try:
            dashboard_response = self.session.get(dashboard_url)
            dashboard_response.raise_for_status()
            dashboard_details = dashboard_response.json()
        except HTTPError as http_error:
            self.report.report_failure(
                key=f"metabase-dashboard-{dashboard_id}",
                reason=f"Unable to retrieve dashboard. " f"Reason: {str(http_error)}",
            )
            return None

        dashboard_urn = builder.make_dashboard_urn(
            self.platform, dashboard_details.get("id", "")
        )
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[],
        )
        last_edit_by = dashboard_details.get("last-edit-info") or {}
        modified_actor = builder.make_user_urn(last_edit_by.get("email", "unknown"))
        modified_ts = self.get_timestamp_millis_from_ts_string(
            f"{last_edit_by.get('timestamp')}"
        )
        title = dashboard_details.get("name", "") or ""
        description = dashboard_details.get("description", "") or ""
        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=modified_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )

        chart_urns = []
        cards_data = dashboard_details.get("ordered_cards", "{}")
        for card_info in cards_data:
            chart_urn = builder.make_chart_urn(self.platform, card_info.get("id", ""))
            chart_urns.append(chart_urn)

        dashboard_info_class = DashboardInfoClass(
            description=description,
            title=title,
            charts=chart_urns,
            lastModified=last_modified,
            dashboardUrl=f"{self.config.connect_uri}/dashboard/{dashboard_id}",
            customProperties={},
        )
        dashboard_snapshot.aspects.append(dashboard_info_class)

        # Ownership
        ownership = self._get_ownership(dashboard_details.get("creator_id", ""))
        if ownership is not None:
            dashboard_snapshot.aspects.append(ownership)

        return dashboard_snapshot

    @lru_cache(maxsize=None)
    def _get_ownership(self, creator_id: int) -> Optional[OwnershipClass]:
        user_info_url = f"{self.config.connect_uri}/api/user/{creator_id}"
        try:
            user_info_response = self.session.get(user_info_url)
            user_info_response.raise_for_status()
            user_details = user_info_response.json()
        except HTTPError as http_error:
            self.report.report_failure(
                key=f"metabase-user-{creator_id}",
                reason=f"Unable to retrieve User info. " f"Reason: {str(http_error)}",
            )
            return None

        owner_urn = builder.make_user_urn(user_details.get("email", ""))
        if owner_urn is not None:
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

    def emit_card_mces(self) -> Iterable[MetadataWorkUnit]:
        try:
            card_response = self.session.get(f"{self.config.connect_uri}/api/card")
            card_response.raise_for_status()
            cards = card_response.json()

            for card_info in cards:
                chart_snapshot = self.construct_card_from_api_data(card_info)
                if chart_snapshot is not None:
                    mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)
                    wu = MetadataWorkUnit(id=chart_snapshot.urn, mce=mce)
                    self.report.report_workunit(wu)
                    yield wu

        except HTTPError as http_error:
            self.report.report_failure(
                key="metabase-cards",
                reason=f"Unable to retrieve cards. " f"Reason: {str(http_error)}",
            )
            return None

    def construct_card_from_api_data(self, card_data: dict) -> Optional[ChartSnapshot]:
        card_id = card_data.get("id", "")
        card_url = f"{self.config.connect_uri}/api/card/{card_id}"
        try:
            card_response = self.session.get(card_url)
            card_response.raise_for_status()
            card_details = card_response.json()
        except HTTPError as http_error:
            self.report.report_failure(
                key=f"metabase-card-{card_id}",
                reason=f"Unable to retrieve Card info. " f"Reason: {str(http_error)}",
            )
            return None

        chart_urn = builder.make_chart_urn(self.platform, card_id)
        chart_snapshot = ChartSnapshot(
            urn=chart_urn,
            aspects=[],
        )

        last_edit_by = card_details.get("last-edit-info") or {}
        modified_actor = builder.make_user_urn(last_edit_by.get("email", "unknown"))
        modified_ts = self.get_timestamp_millis_from_ts_string(
            f"{last_edit_by.get('timestamp')}"
        )
        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=modified_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )

        chart_type = self._get_chart_type(
            card_details.get("id", ""), card_details.get("display")
        )
        description = card_details.get("description") or ""
        title = card_details.get("name") or ""
        datasource_urn = self.get_datasource_urn(card_details)
        custom_properties = self.construct_card_custom_properties(card_details)

        chart_info = ChartInfoClass(
            type=chart_type,
            description=description,
            title=title,
            lastModified=last_modified,
            chartUrl=f"{self.config.connect_uri}/card/{card_id}",
            inputs=datasource_urn,
            customProperties=custom_properties,
        )
        chart_snapshot.aspects.append(chart_info)

        if card_details.get("query_type", "") == "native":
            raw_query = (
                card_details.get("dataset_query", {}).get("native", {}).get("query", "")
            )
            chart_query_native = ChartQueryClass(
                rawQuery=raw_query,
                type=ChartQueryTypeClass.SQL,
            )
            chart_snapshot.aspects.append(chart_query_native)

        # Ownership
        ownership = self._get_ownership(card_details.get("creator_id", ""))
        if ownership is not None:
            chart_snapshot.aspects.append(ownership)

        return chart_snapshot

    def _get_chart_type(self, card_id: int, display_type: str) -> Optional[str]:
        type_mapping = {
            "table": ChartTypeClass.TABLE,
            "bar": ChartTypeClass.BAR,
            "line": ChartTypeClass.LINE,
            "row": ChartTypeClass.BAR,
            "area": ChartTypeClass.AREA,
            "pie": ChartTypeClass.PIE,
            "funnel": ChartTypeClass.BAR,
            "scatter": ChartTypeClass.SCATTER,
            "scalar": ChartTypeClass.TEXT,
            "smartscalar": ChartTypeClass.TEXT,
            "pivot": ChartTypeClass.TABLE,
            "waterfall": ChartTypeClass.BAR,
            "progress": None,
            "combo": None,
            "gauge": None,
            "map": None,
        }
        if not display_type:
            self.report.report_warning(
                key=f"metabase-card-{card_id}",
                reason=f"Card type {display_type} is missing. Setting to None",
            )
            return None
        try:
            chart_type = type_mapping[display_type]
        except KeyError:
            self.report.report_warning(
                key=f"metabase-card-{card_id}",
                reason=f"Chart type {display_type} not supported. Setting to None",
            )
            chart_type = None

        return chart_type

    def construct_card_custom_properties(self, card_details: dict) -> Dict:
        result_metadata = card_details.get("result_metadata") or []
        metrics, dimensions = [], []
        for meta_data in result_metadata:
            display_name = meta_data.get("display_name", "") or ""
            metrics.append(display_name) if "aggregation" in meta_data.get(
                "field_ref", ""
            ) else dimensions.append(display_name)

        filters = (card_details.get("dataset_query", {}).get("query", {})).get(
            "filter", []
        )

        custom_properties = {
            "Metrics": ", ".join(metrics),
            "Filters": f"{filters}" if len(filters) else "",
            "Dimensions": ", ".join(dimensions),
        }

        return custom_properties

    def get_datasource_urn(self, card_details):
        platform, database_name, platform_instance = self.get_datasource_from_id(
            card_details.get("database_id", "")
        )
        query_type = card_details.get("dataset_query", {}).get("type", {})
        source_paths = set()

        if query_type == "query":
            source_table_id = (
                card_details.get("dataset_query", {})
                .get("query", {})
                .get("source-table")
            )
            if source_table_id is not None:
                schema_name, table_name = self.get_source_table_from_id(source_table_id)
                if table_name:
                    source_paths.add(
                        f"{schema_name + '.' if schema_name else ''}{table_name}"
                    )
        else:
            try:
                raw_query = (
                    card_details.get("dataset_query", {})
                    .get("native", {})
                    .get("query", "")
                )
                parser = LineageRunner(raw_query)

                for table in parser.source_tables:
                    sources = str(table).split(".")
                    source_schema, source_table = sources[-2], sources[-1]
                    if source_schema == "<default>":
                        source_schema = str(self.config.default_schema)

                    source_paths.add(f"{source_schema}.{source_table}")
            except Exception as e:
                self.report.report_failure(
                    key="metabase-query",
                    reason=f"Unable to retrieve lineage from query. "
                    f"Query: {raw_query} "
                    f"Reason: {str(e)} ",
                )
                return None

        # Create dataset URNs
        dataset_urn = []
        dbname = f"{database_name + '.' if database_name else ''}"
        source_tables = list(map(lambda tbl: f"{dbname}{tbl}", source_paths))
        dataset_urn = [
            builder.make_dataset_urn_with_platform_instance(
                platform=platform,
                name=name,
                platform_instance=platform_instance,
                env=self.config.env,
            )
            for name in source_tables
        ]

        return dataset_urn

    @lru_cache(maxsize=None)
    def get_source_table_from_id(self, table_id):
        try:
            dataset_response = self.session.get(
                f"{self.config.connect_uri}/api/table/{table_id}"
            )
            dataset_response.raise_for_status()
            dataset_json = dataset_response.json()
            schema = dataset_json.get("schema", "")
            name = dataset_json.get("name", "")
            return schema, name

        except HTTPError as http_error:
            self.report.report_failure(
                key=f"metabase-table-{table_id}",
                reason=f"Unable to retrieve source table. "
                f"Reason: {str(http_error)}",
            )

        return None, None

    @lru_cache(maxsize=None)
    def get_datasource_from_id(self, datasource_id):
        try:
            dataset_response = self.session.get(
                f"{self.config.connect_uri}/api/database/{datasource_id}"
            )
            dataset_response.raise_for_status()
            dataset_json = dataset_response.json()
        except HTTPError as http_error:
            self.report.report_failure(
                key=f"metabase-datasource-{datasource_id}",
                reason=f"Unable to retrieve Datasource. " f"Reason: {str(http_error)}",
            )
            return None, None

        # Map engine names to what datahub expects in
        # https://github.com/datahub-project/datahub/blob/master/metadata-service/war/src/main/resources/boot/data_platforms.json
        engine = dataset_json.get("engine", "")
        platform = engine

        engine_mapping = {
            "sparksql": "spark",
            "mongo": "mongodb",
            "presto-jdbc": "presto",
            "sqlserver": "mssql",
            "bigquery-cloud-sdk": "bigquery",
        }

        if self.config.engine_platform_map is not None:
            engine_mapping.update(self.config.engine_platform_map)

        if engine in engine_mapping:
            platform = engine_mapping[engine]
        else:
            self.report.report_warning(
                key=f"metabase-platform-{datasource_id}",
                reason=f"Platform was not found in DataHub. Using {platform} name as is",
            )
        # Set platform_instance if configuration provides a mapping from platform name to instance
        platform_instance = (
            self.config.platform_instance_map.get(platform)
            if self.config.platform_instance_map
            else None
        )

        field_for_dbname_mapping = {
            "postgres": "dbname",
            "sparksql": "dbname",
            "mongo": "dbname",
            "redshift": "db",
            "snowflake": "db",
            "presto-jdbc": "catalog",
            "presto": "catalog",
            "mysql": "dbname",
            "sqlserver": "db",
        }

        dbname = (
            dataset_json.get("details", {}).get(field_for_dbname_mapping[engine])
            if engine in field_for_dbname_mapping
            else None
        )

        if (
            self.config.database_alias_map is not None
            and platform in self.config.database_alias_map
        ):
            dbname = self.config.database_alias_map[platform]
        else:
            self.report.report_warning(
                key=f"metabase-dbname-{datasource_id}",
                reason=f"Cannot determine database name for platform: {platform}",
            )

        return platform, dbname, platform_instance

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = MetabaseConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_card_mces()
        yield from self.emit_dashboard_mces()

    def get_report(self) -> SourceReport:
        return self.report
