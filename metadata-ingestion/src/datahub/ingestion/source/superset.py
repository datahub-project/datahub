import json
import logging
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional

import dateutil.parser as dp
import requests
from pydantic import BaseModel
from pydantic.class_validators import root_validator, validator
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.emitter.mce_builder import (
    make_chart_urn,
    make_dashboard_urn,
    make_data_platform_urn,
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
)
from datahub.emitter.mcp_builder import add_domain_to_entity_wu
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_types import resolve_sql_type
from datahub.ingestion.source.sql.sqlalchemy_uri_mapper import (
    get_platform_from_sqlalchemy_uri,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    ChangeAuditStamps,
    Status,
    TimeStamp,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot,
    DashboardSnapshot,
    DatasetSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    MySqlDDL,
    NullType,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    ChartTypeClass,
    DashboardInfoClass,
    DatasetPropertiesClass,
)
from datahub.utilities import config_clean
from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)

PAGE_SIZE = 25


chart_type_from_viz_type = {
    "line": ChartTypeClass.LINE,
    "big_number": ChartTypeClass.LINE,
    "table": ChartTypeClass.TABLE,
    "dist_bar": ChartTypeClass.BAR,
    "area": ChartTypeClass.AREA,
    "bar": ChartTypeClass.BAR,
    "pie": ChartTypeClass.PIE,
    "histogram": ChartTypeClass.HISTOGRAM,
    "big_number_total": ChartTypeClass.LINE,
    "dual_line": ChartTypeClass.LINE,
    "line_multi": ChartTypeClass.LINE,
    "treemap": ChartTypeClass.AREA,
    "box_plot": ChartTypeClass.BAR,
}


platform_without_databases = ["druid"]


class SupersetDataset(BaseModel):
    id: int
    table_name: str
    changed_on_utc: Optional[str] = None
    explore_url: Optional[str] = ""

    @property
    def modified_dt(self) -> Optional[datetime]:
        if self.changed_on_utc:
            return dp.parse(self.changed_on_utc)
        return None

    @property
    def modified_ts(self) -> Optional[int]:
        if self.modified_dt:
            return int(self.modified_dt.timestamp() * 1000)
        return None


class SupersetConfig(
    StatefulIngestionConfigBase, EnvConfigMixin, PlatformInstanceConfigMixin
):
    # See the Superset /security/login endpoint for details
    # https://superset.apache.org/docs/rest-api
    connect_uri: str = Field(
        default="http://localhost:8088", description="Superset host URL."
    )
    display_uri: Optional[str] = Field(
        default=None,
        description="optional URL to use in links (if `connect_uri` is only for ingestion)",
    )
    domain: Dict[str, AllowDenyPattern] = Field(
        default=dict(),
        description="regex patterns for tables to filter to assign domain_key. ",
    )
    username: Optional[str] = Field(default=None, description="Superset username.")
    password: Optional[str] = Field(default=None, description="Superset password.")
    # Configuration for stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="Superset Stateful Ingestion Config."
    )
    ingest_dashboards: bool = Field(
        default=True, description="Enable to ingest dashboards."
    )
    ingest_charts: bool = Field(default=True, description="Enable to ingest charts.")
    ingest_datasets: bool = Field(
        default=False, description="Enable to ingest datasets."
    )

    provider: str = Field(default="db", description="Superset provider.")
    options: Dict = Field(default={}, description="")

    # TODO: Check and remove this if no longer needed.
    # Config database_alias is removed from sql sources.
    database_alias: Dict[str, str] = Field(
        default={},
        description="Can be used to change mapping for database names in superset to what you have in datahub",
    )

    class Config:
        # This is required to allow preset configs to get parsed
        extra = "allow"

    @validator("connect_uri", "display_uri")
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)

    @root_validator(skip_on_failure=True)
    def default_display_uri_to_connect_uri(cls, values):
        base = values.get("display_uri")
        if base is None:
            values["display_uri"] = values.get("connect_uri")
        return values


def get_metric_name(metric):
    if not metric:
        return ""
    if isinstance(metric, str):
        return metric
    label = metric.get("label")
    if not label:
        return ""
    return label


def get_filter_name(filter_obj):
    sql_expression = filter_obj.get("sqlExpression")
    if sql_expression:
        return sql_expression

    clause = filter_obj.get("clause")
    column = filter_obj.get("subject")
    operator = filter_obj.get("operator")
    comparator = filter_obj.get("comparator")
    return f"{clause} {column} {operator} {comparator}"


@platform_name("Superset")
@config_class(SupersetConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(
    SourceCapability.DELETION_DETECTION, "Optionally enabled via stateful_ingestion"
)
@capability(SourceCapability.DOMAINS, "Enabled by `domain` config to assign domain_key")
@capability(SourceCapability.LINEAGE_COARSE, "Supported by default")
class SupersetSource(StatefulIngestionSourceBase):
    """
    This plugin extracts the following:
    - Charts, dashboards, and associated metadata

    See documentation for superset's /security/login at https://superset.apache.org/docs/rest-api for more details on superset's login api.
    """

    config: SupersetConfig
    report: StaleEntityRemovalSourceReport
    platform = "superset"

    def __hash__(self):
        return id(self)

    def __init__(self, ctx: PipelineContext, config: SupersetConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report = StaleEntityRemovalSourceReport()
        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[domain_id for domain_id in self.config.domain],
                graph=self.ctx.graph,
            )
        self.session = self.login()

    def login(self) -> requests.Session:
        login_response = requests.post(
            f"{self.config.connect_uri}/api/v1/security/login",
            json={
                "username": self.config.username,
                "password": self.config.password,
                "refresh": True,
                "provider": self.config.provider,
            },
        )

        self.access_token = login_response.json()["access_token"]
        logger.debug("Got access token from superset")

        requests_session = requests.Session()
        requests_session.headers.update(
            {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
                "Accept": "*/*",
            }
        )

        # Test the connection
        test_response = requests_session.get(
            f"{self.config.connect_uri}/api/v1/dashboard/"
        )
        if test_response.status_code == 200:
            pass
            # TODO(Gabe): how should we message about this error?
        return requests_session

    def paginate_entity_api_results(self, entity_type, page_size=100):
        current_page = 0
        total_items = page_size

        while current_page * page_size < total_items:
            response = self.session.get(
                f"{self.config.connect_uri}/api/v1/{entity_type}/",
                params={"q": f"(page:{current_page},page_size:{page_size})"},
            )

            if response.status_code != 200:
                logger.warning(f"Failed to get {entity_type} data: {response.text}")

            payload = response.json()
            # Update total_items with the actual count from the response
            total_items = payload.get("count", total_items)
            # Yield each item in the result, this gets passed into the construct functions
            for item in payload.get("result", []):
                yield item

            current_page += 1

    @lru_cache(maxsize=None)
    def get_platform_from_database_id(self, database_id):
        database_response = self.session.get(
            f"{self.config.connect_uri}/api/v1/database/{database_id}"
        ).json()
        sqlalchemy_uri = database_response.get("result", {}).get("sqlalchemy_uri")
        if sqlalchemy_uri is None:
            platform_name = database_response.get("result", {}).get(
                "backend", "external"
            )
        else:
            platform_name = get_platform_from_sqlalchemy_uri(sqlalchemy_uri)
        if platform_name == "awsathena":
            return "athena"
        if platform_name == "clickhousedb":
            return "clickhouse"
        if platform_name == "postgresql":
            return "postgres"
        return platform_name

    @lru_cache(maxsize=None)
    def get_dataset_info(self, dataset_id: int) -> dict:
        dataset_response = self.session.get(
            f"{self.config.connect_uri}/api/v1/dataset/{dataset_id}",
        )
        if dataset_response.status_code != 200:
            logger.warning(f"Failed to get dataset info: {dataset_response.text}")
            dataset_response.raise_for_status()
        return dataset_response.json()

    def get_datasource_urn_from_id(
        self, dataset_response: dict, platform_instance: str
    ) -> str:
        schema_name = dataset_response.get("result", {}).get("schema")
        table_name = dataset_response.get("result", {}).get("table_name")
        database_id = dataset_response.get("result", {}).get("database", {}).get("id")
        platform = self.get_platform_from_database_id(database_id)

        database_name = (
            dataset_response.get("result", {}).get("database", {}).get("database_name")
        )
        database_name = self.config.database_alias.get(database_name, database_name)

        # Druid do not have a database concept and has a limited schema concept, but they are nonetheless reported
        # from superset. There is only one database per platform instance, and one schema named druid, so it would be
        # redundant to systemically store them both in the URN.
        if platform in platform_without_databases:
            database_name = None

        if platform == "druid" and schema_name == "druid":
            # Follow DataHub's druid source convention.
            schema_name = None

        if database_id and table_name:
            return make_dataset_urn(
                platform=platform,
                name=".".join(
                    name for name in [database_name, schema_name, table_name] if name
                ),
                env=self.config.env,
            )
        raise ValueError("Could not construct dataset URN")

    def construct_dashboard_from_api_data(
        self, dashboard_data: dict
    ) -> DashboardSnapshot:
        dashboard_urn = make_dashboard_urn(
            platform=self.platform,
            name=dashboard_data["id"],
            platform_instance=self.config.platform_instance,
        )
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[Status(removed=False)],
        )

        modified_actor = f"urn:li:corpuser:{(dashboard_data.get('changed_by') or {}).get('username', 'unknown')}"
        modified_ts = int(
            dp.parse(dashboard_data.get("changed_on_utc", "now")).timestamp() * 1000
        )
        title = dashboard_data.get("dashboard_title", "")
        # note: the API does not currently supply created_by usernames due to a bug
        last_modified = ChangeAuditStamps(
            created=None,
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )
        dashboard_url = f"{self.config.display_uri}{dashboard_data.get('url', '')}"

        chart_urns = []
        raw_position_data = dashboard_data.get("position_json", "{}")
        position_data = (
            json.loads(raw_position_data) if raw_position_data is not None else {}
        )
        for key, value in position_data.items():
            if not key.startswith("CHART-"):
                continue
            chart_urns.append(
                make_chart_urn(
                    platform=self.platform,
                    name=value.get("meta", {}).get("chartId", "unknown"),
                    platform_instance=self.config.platform_instance,
                )
            )

        # Build properties
        custom_properties = {
            "Status": str(dashboard_data.get("status")),
            "IsPublished": str(dashboard_data.get("published", False)).lower(),
            "Owners": ", ".join(
                map(
                    lambda owner: owner.get("username", "unknown"),
                    dashboard_data.get("owners", []),
                )
            ),
            "IsCertified": str(
                True if dashboard_data.get("certified_by") else False
            ).lower(),
        }

        if dashboard_data.get("certified_by"):
            custom_properties["CertifiedBy"] = dashboard_data.get("certified_by", "")
            custom_properties["CertificationDetails"] = str(
                dashboard_data.get("certification_details")
            )

        # Create DashboardInfo object
        dashboard_info = DashboardInfoClass(
            description="",
            title=title,
            charts=chart_urns,
            lastModified=last_modified,
            dashboardUrl=dashboard_url,
            customProperties=custom_properties,
        )
        dashboard_snapshot.aspects.append(dashboard_info)
        return dashboard_snapshot

    def emit_dashboard_mces(self) -> Iterable[MetadataWorkUnit]:
        for dashboard_data in self.paginate_entity_api_results("dashboard", PAGE_SIZE):
            try:
                dashboard_snapshot = self.construct_dashboard_from_api_data(
                    dashboard_data
                )
            except Exception as e:
                self.report.warning(
                    f"Failed to construct dashboard snapshot. Dashboard name: {dashboard_data.get('dashboard_title')}. Error: \n{e}"
                )
                continue
            # Emit the dashboard
            mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
            yield MetadataWorkUnit(id=dashboard_snapshot.urn, mce=mce)
            yield from self._get_domain_wu(
                title=dashboard_data.get("dashboard_title", ""),
                entity_urn=dashboard_snapshot.urn,
            )

    def construct_chart_from_chart_data(self, chart_data: dict) -> ChartSnapshot:
        chart_urn = make_chart_urn(
            platform=self.platform,
            name=chart_data["id"],
            platform_instance=self.config.platform_instance,
        )
        chart_snapshot = ChartSnapshot(
            urn=chart_urn,
            aspects=[Status(removed=False)],
        )

        modified_actor = f"urn:li:corpuser:{(chart_data.get('changed_by') or {}).get('username', 'unknown')}"
        modified_ts = int(
            dp.parse(chart_data.get("changed_on_utc", "now")).timestamp() * 1000
        )
        title = chart_data.get("slice_name", "")

        # note: the API does not currently supply created_by usernames due to a bug
        last_modified = ChangeAuditStamps(
            created=None,
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )
        chart_type = chart_type_from_viz_type.get(chart_data.get("viz_type", ""))
        chart_url = f"{self.config.display_uri}{chart_data.get('url', '')}"

        datasource_id = chart_data.get("datasource_id")
        dataset_response = self.get_dataset_info(datasource_id)
        datasource_urn = self.get_datasource_urn_from_id(
            dataset_response, self.platform
        )

        params = json.loads(chart_data.get("params", "{}"))
        metrics = [
            get_metric_name(metric)
            for metric in (params.get("metrics", []) or [params.get("metric")])
        ]
        filters = [
            get_filter_name(filter_obj)
            for filter_obj in params.get("adhoc_filters", [])
        ]
        group_bys = params.get("groupby", []) or []
        if isinstance(group_bys, str):
            group_bys = [group_bys]
        # handling List[Union[str, dict]] case
        # a dict containing two keys: sqlExpression and label
        elif isinstance(group_bys, list) and len(group_bys) != 0:
            temp_group_bys = []
            for item in group_bys:
                # if the item is a custom label
                if isinstance(item, dict):
                    item_value = item.get("label", "")
                    if item_value != "":
                        temp_group_bys.append(f"{item_value}_custom_label")
                    else:
                        temp_group_bys.append(str(item))

                # if the item is a string
                elif isinstance(item, str):
                    temp_group_bys.append(item)

            group_bys = temp_group_bys

        custom_properties = {
            "Metrics": ", ".join(metrics),
            "Filters": ", ".join(filters),
            "Dimensions": ", ".join(group_bys),
        }

        chart_info = ChartInfoClass(
            type=chart_type,
            description="",
            title=title,
            lastModified=last_modified,
            chartUrl=chart_url,
            inputs=[datasource_urn] if datasource_urn else None,
            customProperties=custom_properties,
        )
        chart_snapshot.aspects.append(chart_info)
        return chart_snapshot

    def emit_chart_mces(self) -> Iterable[MetadataWorkUnit]:
        for chart_data in self.paginate_entity_api_results("chart", PAGE_SIZE):
            try:
                chart_snapshot = self.construct_chart_from_chart_data(chart_data)

                mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)
            except Exception as e:
                self.report.warning(
                    f"Failed to construct chart snapshot. Chart name: {chart_data.get('table_name')}. Error: \n{e}"
                )
                continue
            # Emit the chart
            yield MetadataWorkUnit(id=chart_snapshot.urn, mce=mce)
            yield from self._get_domain_wu(
                title=chart_data.get("slice_name", ""),
                entity_urn=chart_snapshot.urn,
            )

    def gen_schema_fields(self, column_data: List[Dict[str, str]]) -> List[SchemaField]:
        schema_fields: List[SchemaField] = []
        for col in column_data:
            col_type = (col.get("type") or "").lower()
            data_type = resolve_sql_type(col_type)
            if data_type is None:
                data_type = NullType()

            field = SchemaField(
                fieldPath=col.get("column_name", ""),
                type=SchemaFieldDataType(data_type),
                nativeDataType="",
                description=col.get("column_name", ""),
                nullable=True,
            )
            schema_fields.append(field)
        return schema_fields

    def gen_schema_metadata(
        self,
        dataset_response: dict,
    ) -> SchemaMetadata:
        dataset_response = dataset_response.get("result", {})
        column_data = dataset_response.get("columns", [])
        schema_metadata = SchemaMetadata(
            schemaName=dataset_response.get("table_name", ""),
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
            fields=self.gen_schema_fields(column_data),
        )
        return schema_metadata

    def gen_dataset_urn(self, datahub_dataset_name: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=datahub_dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def construct_dataset_from_dataset_data(
        self, dataset_data: dict
    ) -> DatasetSnapshot:
        dataset_response = self.get_dataset_info(dataset_data.get("id"))
        dataset = SupersetDataset(**dataset_response["result"])
        datasource_urn = self.get_datasource_urn_from_id(
            dataset_response, self.platform
        )

        dataset_url = f"{self.config.display_uri}{dataset.explore_url or ''}"

        dataset_info = DatasetPropertiesClass(
            name=dataset.table_name,
            description="",
            lastModified=TimeStamp(time=dataset.modified_ts)
            if dataset.modified_ts
            else None,
            externalUrl=dataset_url,
        )
        aspects_items: List[Any] = []
        aspects_items.extend(
            [
                self.gen_schema_metadata(dataset_response),
                dataset_info,
            ]
        )

        dataset_snapshot = DatasetSnapshot(
            urn=datasource_urn,
            aspects=aspects_items,
        )
        return dataset_snapshot

    def emit_dataset_mces(self) -> Iterable[MetadataWorkUnit]:
        for dataset_data in self.paginate_entity_api_results("dataset", PAGE_SIZE):
            try:
                dataset_snapshot = self.construct_dataset_from_dataset_data(
                    dataset_data
                )
                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            except Exception as e:
                self.report.warning(
                    f"Failed to construct dataset snapshot. Dataset name: {dataset_data.get('table_name')}. Error: \n{e}"
                )
                continue
            # Emit the dataset
            yield MetadataWorkUnit(id=dataset_snapshot.urn, mce=mce)
            yield from self._get_domain_wu(
                title=dataset_data.get("table_name", ""),
                entity_urn=dataset_snapshot.urn,
            )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        if self.config.ingest_dashboards:
            yield from self.emit_dashboard_mces()
        if self.config.ingest_charts:
            yield from self.emit_chart_mces()
        if self.config.ingest_datasets:
            yield from self.emit_dataset_mces()

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_report(self) -> StaleEntityRemovalSourceReport:
        return self.report

    def _get_domain_wu(self, title: str, entity_urn: str) -> Iterable[MetadataWorkUnit]:
        domain_urn = None
        for domain, pattern in self.config.domain.items():
            if pattern.allowed(title):
                domain_urn = make_domain_urn(
                    self.domain_registry.get_domain_urn(domain)
                )
                break

        if domain_urn:
            yield from add_domain_to_entity_wu(
                entity_urn=entity_urn,
                domain_urn=domain_urn,
            )
