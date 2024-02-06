import logging
from datetime import datetime, timezone
from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Tuple, Union

import dateutil.parser as dp
import pydantic
import requests
from pydantic import Field, root_validator, validator
from requests.models import HTTPError

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
from datahub.utilities.sqlglot_lineage import create_lineage_sql_parsed_result

logger = logging.getLogger(__name__)

DATASOURCE_URN_RECURSION_LIMIT = 5


class MetabaseConfig(DatasetLineageProviderConfigBase):
    # See the Metabase /api/session endpoint for details
    # https://www.metabase.com/docs/latest/api-documentation.html#post-apisession
    connect_uri: str = Field(default="localhost:3000", description="Metabase host URL.")
    display_uri: Optional[str] = Field(
        default=None,
        description="optional URL to use in links (if `connect_uri` is only for ingestion)",
    )
    username: Optional[str] = Field(default=None, description="Metabase username.")
    password: Optional[pydantic.SecretStr] = Field(
        default=None, description="Metabase password."
    )
    # TODO: Check and remove this if no longer needed.
    # Config database_alias is removed from sql sources.
    database_alias_map: Optional[dict] = Field(
        default=None,
        description="Database name map to use when constructing dataset URN.",
    )
    engine_platform_map: Optional[Dict[str, str]] = Field(
        default=None,
        description="Custom mappings between metabase database engines and DataHub platforms",
    )
    database_id_to_instance_map: Optional[Dict[str, str]] = Field(
        default=None,
        description="Custom mappings between metabase database id and DataHub platform instance",
    )
    default_schema: str = Field(
        default="public",
        description="Default schema name to use when schema is not provided in an SQL query",
    )

    @validator("connect_uri", "display_uri")
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)

    @root_validator(skip_on_failure=True)
    def default_display_uri_to_connect_uri(cls, values):
        base = values.get("display_uri")
        if base is None:
            values["display_uri"] = values.get("connect_uri")
        return values


@platform_name("Metabase")
@config_class(MetabaseConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Supported by default")
class MetabaseSource(Source):
    """
    This plugin extracts Charts, dashboards, and associated metadata. This plugin is in beta and has only been tested
    on PostgreSQL and H2 database.

    ### Collection

    [/api/collection](https://www.metabase.com/docs/latest/api/collection) endpoint is used to
    retrieve the available collections.

    [/api/collection/<COLLECTION_ID>/items?models=dashboard](https://www.metabase.com/docs/latest/api/collection#get-apicollectioniditems) endpoint is used to retrieve a given collection and list their dashboards.

     ### Dashboard

    [/api/dashboard/<DASHBOARD_ID>](https://www.metabase.com/docs/latest/api/dashboard) endpoint is used to retrieve a given Dashboard and grab its information.

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
        self.setup_session()

    def setup_session(self) -> None:
        login_response = requests.post(
            f"{self.config.connect_uri}/api/session",
            None,
            {
                "username": self.config.username,
                "password": self.config.password.get_secret_value()
                if self.config.password
                else None,
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
        super().close()

    def emit_dashboard_mces(self) -> Iterable[MetadataWorkUnit]:
        try:
            collections_response = self.session.get(
                f"{self.config.connect_uri}/api/collection/"
            )
            collections_response.raise_for_status()
            collections = collections_response.json()

            for collection in collections:
                collection_dashboards_response = self.session.get(
                    f"{self.config.connect_uri}/api/collection/{collection['id']}/items?models=dashboard"
                )
                collection_dashboards_response.raise_for_status()
                collection_dashboards = collection_dashboards_response.json()

                if not collection_dashboards.get("data"):
                    continue

                for dashboard_info in collection_dashboards.get("data"):
                    dashboard_snapshot = self.construct_dashboard_from_api_data(
                        dashboard_info
                    )
                    if dashboard_snapshot is not None:
                        mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
                        yield MetadataWorkUnit(id=dashboard_snapshot.urn, mce=mce)

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
            return int(datetime.now(timezone.utc).timestamp() * 1000)

    def construct_dashboard_from_api_data(
        self, dashboard_info: dict
    ) -> Optional[DashboardSnapshot]:
        dashboard_id = dashboard_info.get("id", "")
        dashboard_url = f"{self.config.display_uri}/api/dashboard/{dashboard_id}"
        try:
            dashboard_response = self.session.get(dashboard_url)
            dashboard_response.raise_for_status()
            dashboard_details = dashboard_response.json()
        except HTTPError as http_error:
            self.report.report_warning(
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
        cards_data = dashboard_details.get("dashcards", {})
        for card_info in cards_data:
            chart_urn = builder.make_chart_urn(
                self.platform, card_info.get("card").get("id", "")
            )
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
        user_info_url = f"{self.config.display_uri}/api/user/{creator_id}"
        try:
            user_info_response = self.session.get(user_info_url)
            user_info_response.raise_for_status()
            user_details = user_info_response.json()
        except HTTPError as http_error:
            if (
                http_error.response is not None
                and http_error.response.status_code == 404
            ):
                self.report.report_warning(
                    key=f"metabase-user-{creator_id}",
                    reason=f"User {creator_id} is blocked in Metabase or missing",
                )
                return None
            # For cases when the error is not 404 but something else
            self.report.report_warning(
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
                    yield MetadataWorkUnit(id=chart_snapshot.urn, mce=mce)

        except HTTPError as http_error:
            self.report.report_failure(
                key="metabase-cards",
                reason=f"Unable to retrieve cards. " f"Reason: {str(http_error)}",
            )
            return None

    def get_card_details_by_id(self, card_id: Union[int, str]) -> dict:
        """
        Method will attempt to get detailed information on card
        from Metabase API by card ID and return this info as dict.
        If information can't be retrieved, an empty dict is returned
        to unify return value of failed call with successful call of the method.
        :param Union[int, str] card_id: ID of card (question) in Metabase
        :param int datasource_id: Numeric datasource ID received from Metabase API
        :return: dict with info or empty dict
        """
        card_url = f"{self.config.display_uri}/api/card/{card_id}"
        try:
            card_response = self.session.get(card_url)
            card_response.raise_for_status()
            return card_response.json()
        except HTTPError as http_error:
            self.report.report_warning(
                key=f"metabase-card-{card_id}",
                reason=f"Unable to retrieve Card info. " f"Reason: {str(http_error)}",
            )
            return {}

    def construct_card_from_api_data(self, card_data: dict) -> Optional[ChartSnapshot]:
        card_id = card_data.get("id")
        if card_id is None:
            self.report.report_warning(
                key="metabase-card",
                reason=f"Unable to get Card id from card data {str(card_data)}",
            )
            return None

        card_details = self.get_card_details_by_id(card_id)
        if not card_details:
            self.report.report_warning(
                key=f"metabase-card-{card_id}",
                reason="Unable to construct Card due to empty card details",
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

        chart_type = self._get_chart_type(card_id, card_details.get("display") or "")
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

    def get_datasource_urn(
        self, card_details: dict, recursion_depth: int = 0
    ) -> Optional[List]:
        if recursion_depth > DATASOURCE_URN_RECURSION_LIMIT:
            self.report.report_warning(
                key=f"metabase-card-{card_details.get('id')}",
                reason="Unable to retrieve Card info. Reason: source table recursion depth exceeded",
            )
            return None

        datasource_id = card_details.get("database_id") or ""
        (
            platform,
            database_name,
            database_schema,
            platform_instance,
        ) = self.get_datasource_from_id(datasource_id)
        if not platform:
            self.report.report_warning(
                key=f"metabase-datasource-{datasource_id}",
                reason=f"Unable to detect platform for database id {datasource_id}",
            )
            return None

        query_type = card_details.get("dataset_query", {}).get("type", {})

        if query_type == "query":
            source_table_id = (
                card_details.get("dataset_query", {})
                .get("query", {})
                .get("source-table")
                or ""
            )
            if str(source_table_id).startswith("card__"):
                # question is built not directly from table in DB but from results of other question in Metabase
                # trying to get source table from source question. Recursion depth is limited
                return self.get_datasource_urn(
                    card_details=self.get_card_details_by_id(
                        source_table_id.replace("card__", "")
                    ),
                    recursion_depth=recursion_depth + 1,
                )
            elif source_table_id != "":
                # the question is built directly from table in DB
                schema_name, table_name = self.get_source_table_from_id(source_table_id)
                if table_name:
                    name_components = [database_name, schema_name, table_name]
                    return [
                        builder.make_dataset_urn_with_platform_instance(
                            platform=platform,
                            name=".".join([v for v in name_components if v]),
                            platform_instance=platform_instance,
                            env=self.config.env,
                        )
                    ]
        else:
            raw_query = (
                card_details.get("dataset_query", {}).get("native", {}).get("query", "")
            )
            result = create_lineage_sql_parsed_result(
                query=raw_query,
                default_db=database_name,
                default_schema=database_schema or self.config.default_schema,
                platform=platform,
                platform_instance=platform_instance,
                env=self.config.env,
                graph=self.ctx.graph,
            )
            if result.debug_info.table_error:
                logger.info(
                    f"Failed to parse lineage from query {raw_query}: "
                    f"{result.debug_info.table_error}"
                )
                self.report.report_warning(
                    key="metabase-query",
                    reason=f"Unable to retrieve lineage from query: {raw_query}",
                )
            return result.in_tables

        return None

    @lru_cache(maxsize=None)
    def get_source_table_from_id(
        self, table_id: Union[int, str]
    ) -> Tuple[Optional[str], Optional[str]]:
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
            self.report.report_warning(
                key=f"metabase-table-{table_id}",
                reason=f"Unable to retrieve source table. Reason: {str(http_error)}",
            )

        return None, None

    @lru_cache(maxsize=None)
    def get_platform_instance(
        self, platform: Optional[str] = None, datasource_id: Optional[int] = None
    ) -> Optional[str]:
        """
        Method will attempt to detect `platform_instance` by checking
        `database_id_to_instance_map` and `platform_instance_map` mappings.
        If `database_id_to_instance_map` is defined it is first checked for
        `datasource_id` extracted from Metabase. If this mapping is not defined
        or corresponding key is not found, `platform_instance_map` mapping
        is checked for datasource platform. If no mapping found `None`
        is returned.
        :param str platform: DataHub platform name (e.g. `postgres` or `clickhouse`)
        :param int datasource_id: Numeric datasource ID received from Metabase API
        :return: platform instance name or None
        """
        platform_instance = None
        # For cases when metabase has several platform instances (e.g. several individual ClickHouse clusters)
        if datasource_id is not None and self.config.database_id_to_instance_map:
            platform_instance = self.config.database_id_to_instance_map.get(
                str(datasource_id)
            )

        # If Metabase datasource ID is not mapped to platform instace, fall back to platform mapping
        # Set platform_instance if configuration provides a mapping from platform name to instance
        if platform and self.config.platform_instance_map and platform_instance is None:
            platform_instance = self.config.platform_instance_map.get(platform)

        return platform_instance

    @lru_cache(maxsize=None)
    def get_datasource_from_id(
        self, datasource_id: Union[int, str]
    ) -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
        try:
            dataset_response = self.session.get(
                f"{self.config.connect_uri}/api/database/{datasource_id}"
            )
            dataset_response.raise_for_status()
            dataset_json = dataset_response.json()
        except HTTPError as http_error:
            self.report.report_warning(
                key=f"metabase-datasource-{datasource_id}",
                reason=f"Unable to retrieve Datasource. " f"Reason: {str(http_error)}",
            )
            # returning empty string as `platform` because
            # `make_dataset_urn_with_platform_instance()` only accepts `str`
            return "", None, None, None

        # Map engine names to what datahub expects in
        # https://github.com/datahub-project/datahub/blob/master/metadata-service/war/src/main/resources/boot/data_platforms.json
        engine = dataset_json.get("engine", "")

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
            platform = engine

            self.report.report_warning(
                key=f"metabase-platform-{datasource_id}",
                reason=f"Platform was not found in DataHub. Using {platform} name as is",
            )

        platform_instance = self.get_platform_instance(
            platform, dataset_json.get("id", None)
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
            "bigquery-cloud-sdk": "project-id",
        }

        dbname = (
            dataset_json.get("details", {}).get(field_for_dbname_mapping[engine])
            if engine in field_for_dbname_mapping
            else None
        )

        schema = dataset_json.get("details", {}).get("schema")

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

        return platform, dbname, schema, platform_instance

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = MetabaseConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_card_mces()
        yield from self.emit_dashboard_mces()

    def get_report(self) -> SourceReport:
        return self.report
