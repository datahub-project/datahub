import json
import logging
from functools import lru_cache
from typing import Dict, Iterable, List, Optional

import dateutil.parser as dp
import requests
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
    make_dataset_urn,
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
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, Source
from datahub.ingestion.api.workunit import MetadataWorkUnit
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
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot,
    DashboardSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    ChartTypeClass,
    DashboardInfoClass,
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

    provider: str = Field(default="db", description="Superset provider.")
    options: Dict = Field(default={}, description="")

    # TODO: Check and remove this if no longer needed.
    # Config database_alias is removed from sql sources.
    database_alias: Dict[str, str] = Field(
        default={},
        description="Can be used to change mapping for database names in superset to what you have in datahub",
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
    stale_entity_removal_handler: StaleEntityRemovalHandler

    def __hash__(self):
        return id(self)

    def __init__(self, ctx: PipelineContext, config: SupersetConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report = StaleEntityRemovalSourceReport()

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

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
                "Accept": "*/*",
            }
        )

        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[domain_id for domain_id in self.config.domain],
                graph=self.ctx.graph,
            )

        # Test the connection
        test_response = self.session.get(f"{self.config.connect_uri}/api/v1/dashboard/")
        if test_response.status_code == 200:
            pass
            # TODO(Gabe): how should we message about this error?

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = SupersetConfig.parse_obj(config_dict)
        return cls(ctx, config)

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
        return platform_name

    @lru_cache(maxsize=None)
    def get_datasource_urn_from_id(self, datasource_id):
        dataset_response = self.session.get(
            f"{self.config.connect_uri}/api/v1/dataset/{datasource_id}"
        ).json()
        schema_name = dataset_response.get("result", {}).get("schema")
        table_name = dataset_response.get("result", {}).get("table_name")
        database_id = dataset_response.get("result", {}).get("database", {}).get("id")
        database_name = (
            dataset_response.get("result", {}).get("database", {}).get("database_name")
        )
        database_name = self.config.database_alias.get(database_name, database_name)

        if database_id and table_name:
            return make_dataset_urn(
                platform=self.get_platform_from_database_id(database_id),
                name=".".join(
                    name for name in [database_name, schema_name, table_name] if name
                ),
                env=self.config.env,
            )
        return None

    def construct_dashboard_from_api_data(self, dashboard_data):
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
        # note: the API does not currently supply created_by usernames due to a bug, but we are required to
        # provide a created AuditStamp to comply with ChangeAuditStamp model. For now, I sub in the last
        # modified actor urn
        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=modified_ts, actor=modified_actor),
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
            custom_properties["CertifiedBy"] = dashboard_data.get("certified_by")
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
        current_dashboard_page = 0
        # we will set total dashboards to the actual number after we get the response
        total_dashboards = PAGE_SIZE

        while current_dashboard_page * PAGE_SIZE <= total_dashboards:
            dashboard_response = self.session.get(
                f"{self.config.connect_uri}/api/v1/dashboard/",
                params=f"q=(page:{current_dashboard_page},page_size:{PAGE_SIZE})",
            )
            if dashboard_response.status_code != 200:
                logger.warning(
                    f"Failed to get dashboard data: {dashboard_response.text}"
                )
            dashboard_response.raise_for_status()

            payload = dashboard_response.json()
            total_dashboards = payload.get("count") or 0

            current_dashboard_page += 1

            for dashboard_data in payload["result"]:
                dashboard_snapshot = self.construct_dashboard_from_api_data(
                    dashboard_data
                )
                mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
                yield MetadataWorkUnit(id=dashboard_snapshot.urn, mce=mce)
                yield from self._get_domain_wu(
                    title=dashboard_data.get("dashboard_title", ""),
                    entity_urn=dashboard_snapshot.urn,
                )

    def construct_chart_from_chart_data(self, chart_data):
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

        # note: the API does not currently supply created_by usernames due to a bug, but we are required to
        # provide a created AuditStamp to comply with ChangeAuditStamp model. For now, I sub in the last
        # modified actor urn
        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=modified_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )
        chart_type = chart_type_from_viz_type.get(chart_data.get("viz_type", ""))
        chart_url = f"{self.config.display_uri}{chart_data.get('url', '')}"

        datasource_id = chart_data.get("datasource_id")
        datasource_urn = self.get_datasource_urn_from_id(datasource_id)

        params = json.loads(chart_data.get("params"))
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
        current_chart_page = 0
        # we will set total charts to the actual number after we get the response
        total_charts = PAGE_SIZE

        while current_chart_page * PAGE_SIZE <= total_charts:
            chart_response = self.session.get(
                f"{self.config.connect_uri}/api/v1/chart/",
                params=f"q=(page:{current_chart_page},page_size:{PAGE_SIZE})",
            )
            if chart_response.status_code != 200:
                logger.warning(f"Failed to get chart data: {chart_response.text}")
            chart_response.raise_for_status()

            current_chart_page += 1

            payload = chart_response.json()
            total_charts = payload["count"]
            for chart_data in payload["result"]:
                chart_snapshot = self.construct_chart_from_chart_data(chart_data)

                mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)
                yield MetadataWorkUnit(id=chart_snapshot.urn, mce=mce)
                yield from self._get_domain_wu(
                    title=chart_data.get("slice_name", ""),
                    entity_urn=chart_snapshot.urn,
                )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_dashboard_mces()
        yield from self.emit_chart_mces()

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
