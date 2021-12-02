from functools import lru_cache
from typing import Dict, Iterable, Optional

import dateutil.parser as dp
import requests
from pydantic import validator
from requests.models import HTTPError

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
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


class MetabaseConfig(ConfigModel):
    # See the Metabase /api/session endpoint for details
    # https://www.metabase.com/docs/latest/api-documentation.html#post-apisession
    connect_uri: str = "localhost:3000"
    username: Optional[str] = None
    password: Optional[str] = None
    database_alias_map: Optional[dict] = None
    options: Dict = {}
    env: str = builder.DEFAULT_ENV

    @validator("connect_uri")
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)


class MetabaseSource(Source):
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
        self.access_token = login_response.json()["id"]

        self.session = requests.Session()
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
        if response.status_code != 204:
            self.report.report_failure(
                key="metabase-session",
                reason=f"Unable to logout for user {self.config.username}",
            )

    def emit_dashboard_mces(self) -> Iterable[MetadataWorkUnit]:
        dashboard_response = self.session.get(
            f"{self.config.connect_uri}/api/dashboard"
        )
        payload = dashboard_response.json()
        for dashboard_info in payload:
            dashboard_snapshot = self.construct_dashboard_from_api_data(dashboard_info)

            mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
            wu = MetadataWorkUnit(id=dashboard_snapshot.urn, mce=mce)
            self.report.report_workunit(wu)

            yield wu

    def construct_dashboard_from_api_data(
        self, dashboard_info: dict
    ) -> DashboardSnapshot:
        dashboard_url = (
            f"{self.config.connect_uri}/api/dashboard/{dashboard_info['id']}"
        )
        dashboard_response = self.session.get(dashboard_url)
        dashboard_details = dashboard_response.json()
        dashboard_urn = builder.make_dashboard_urn(
            self.platform, dashboard_details["id"]
        )
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[],
        )
        last_edit_by = dashboard_details.get("last-edit-info") or {}
        modified_actor = builder.make_user_urn(last_edit_by.get("email", "unknown"))
        modified_ts = int(
            dp.parse(f"{last_edit_by.get('timestamp', 'now')}").timestamp() * 1000
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
            chart_urn = builder.make_chart_urn(self.platform, card_info["id"])
            chart_urns.append(chart_urn)

        dashboard_info_class = DashboardInfoClass(
            description=description,
            title=title,
            charts=chart_urns,
            lastModified=last_modified,
            dashboardUrl=f"{self.config.connect_uri}/dashboard/{dashboard_info['id']}",
            customProperties={},
        )
        dashboard_snapshot.aspects.append(dashboard_info_class)

        # Ownership
        ownership = self._get_ownership(dashboard_details["creator_id"])
        if ownership is not None:
            dashboard_snapshot.aspects.append(ownership)

        return dashboard_snapshot

    @lru_cache(maxsize=None)
    def _get_ownership(self, creator_id: int) -> Optional[OwnershipClass]:
        user_info_url = f"{self.config.connect_uri}/api/user/{creator_id}"
        user_info_response = self.session.get(user_info_url)
        user_details = user_info_response.json()
        owner_urn = builder.make_user_urn(user_details["email"])
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
        card_response = self.session.get(f"{self.config.connect_uri}/api/card")
        payload = card_response.json()
        for card_info in payload:
            chart_snapshot = self.construct_card_from_api_data(card_info)

            mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)
            wu = MetadataWorkUnit(id=chart_snapshot.urn, mce=mce)
            self.report.report_workunit(wu)

            yield wu

    def construct_card_from_api_data(self, card_data: dict) -> ChartSnapshot:
        card_url = f"{self.config.connect_uri}/api/card/{card_data['id']}"
        card_response = self.session.get(card_url)
        card_details = card_response.json()

        chart_urn = builder.make_chart_urn(self.platform, card_data["id"])
        chart_snapshot = ChartSnapshot(
            urn=chart_urn,
            aspects=[],
        )

        last_edit_by = card_details.get("last-edit-info") or {}
        modified_actor = builder.make_user_urn(last_edit_by.get("email", "unknown"))
        modified_ts = int(
            dp.parse(f"{last_edit_by.get('timestamp', 'now')}").timestamp() * 1000
        )
        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=modified_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )

        chart_type = self._get_chart_type(
            card_details["id"], card_details.get("display")
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
            chartUrl=f"{self.config.connect_uri}/card/{card_data['id']}",
            inputs=datasource_urn,
            customProperties=custom_properties,
        )
        chart_snapshot.aspects.append(chart_info)

        if card_details["query_type"] == "native":
            chart_query_native = ChartQueryClass(
                rawQuery=card_details["dataset_query"]
                .get("native", {})
                .get("query", ""),
                type=ChartQueryTypeClass.SQL,
            )
            chart_snapshot.aspects.append(chart_query_native)

        # Ownership
        ownership = self._get_ownership(card_details["creator_id"])
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
        result_metadata = card_details.get("result_metadata", [])
        metrics, dimensions = [], []
        for meta_data in result_metadata:
            display_name = meta_data["display_name"] or ""
            metrics.append(display_name) if "aggregation" in meta_data.get(
                "field_ref", ""
            ) else dimensions.append(display_name)

        filters = (card_details["dataset_query"].get("query", {})).get("filter", [])

        custom_properties = {
            "Metrics": ", ".join(metrics),
            "Filters": f"{filters}" if len(filters) else "",
            "Dimensions": ", ".join(dimensions),
        }

        return custom_properties

    def get_datasource_urn(self, card_details):
        platform, database_name = self.get_datasource_from_id(
            card_details["database_id"]
        )
        query_type = card_details.get("dataset_query", {}).get("type", {})
        if query_type == "query":
            source_table_id = (
                card_details.get("dataset_query", {})
                .get("query", {})
                .get("source-table", {})
            )
            schema_name, table_name = self.get_source_table_from_id(source_table_id)
            if table_name:
                name = f"{database_name + '.' if database_name else ''}{schema_name + '.' if schema_name else ''}{table_name}"
                dataset_urn = builder.make_dataset_urn(platform, name, self.config.env)
                return [dataset_urn]
        else:
            self.report.report_warning(
                key=f"metabase-card-{card_details['id']}",
                reason=f"Cannot create datasource urn from query type: {query_type}",
            )

        return None

    @lru_cache(maxsize=None)
    def get_source_table_from_id(self, table_id):
        dataset_response = self.session.get(
            f"{self.config.connect_uri}/api/table/{table_id}"
        ).json()

        schema = dataset_response.get("schema", "")
        name = dataset_response.get("name", "")

        return schema, name

    @lru_cache(maxsize=None)
    def get_datasource_from_id(self, datasource_id):
        dataset_response = self.session.get(
            f"{self.config.connect_uri}/api/database/{datasource_id}"
        ).json()

        # Map engine names to what datahub expects in
        # https://github.com/linkedin/datahub/blob/master/metadata-service/war/src/main/resources/boot/data_platforms.json
        engine = dataset_response["engine"]
        platform = engine

        engine_mapping = {
            "sparksql": "spark",
            "mongo": "mongodb",
            "presto-jdbc": "presto",
            "sqlserver": "mssql",
            "bigquery-cloud-sdk": "bigquery",
        }
        if engine in engine_mapping:
            platform = engine_mapping[engine]
        else:
            self.report.report_warning(
                key=f"metabase-platform-{datasource_id}",
                reason=f"Platform was not found in DataHub. Using {platform} name as is",
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
            dataset_response.get("details", {}).get(field_for_dbname_mapping[engine])
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

        return platform, dbname

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = MetabaseConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_dashboard_mces()
        yield from self.emit_card_mces()

    def get_report(self) -> SourceReport:
        return self.report
