import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import dateutil.parser as dp
import requests
from pydantic import BaseModel
from pydantic.class_validators import root_validator, validator
from pydantic.fields import Field

import datahub.emitter.mce_builder as builder
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
    make_schema_field_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
    ChangeAuditStamps,
    InputField,
    InputFields,
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
    BooleanTypeClass,
    DateTypeClass,
    MySqlDDL,
    NullType,
    NullTypeClass,
    NumberTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChartInfoClass,
    ChartTypeClass,
    DashboardInfoClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)
from datahub.utilities import config_clean
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor

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

FIELD_TYPE_MAPPING = {
    "INT": NumberTypeClass,
    "STRING": StringTypeClass,
    "FLOAT": NumberTypeClass,
    "DATETIME": DateTypeClass,
    "BOOLEAN": BooleanTypeClass,
    "SQL": StringTypeClass,
}


@dataclass
class SupersetSourceReport(StaleEntityRemovalSourceReport):
    filtered: LossyList[str] = field(default_factory=LossyList)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


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
    dataset_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for dataset to filter in ingestion.",
    )
    chart_pattern: AllowDenyPattern = Field(
        AllowDenyPattern.allow_all(),
        description="Patterns for selecting chart names that are to be included",
    )
    dashboard_pattern: AllowDenyPattern = Field(
        AllowDenyPattern.allow_all(),
        description="Patterns for selecting dashboard names that are to be included",
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

    timeout: int = Field(
        default=10, description="Timeout of single API call to superset."
    )

    max_threads: int = Field(
        default_factory=lambda: os.cpu_count() or 40,
        description="Max parallelism for API calls. Defaults to cpuCount or 40",
    )

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
    report: SupersetSourceReport
    platform = "superset"

    def __hash__(self):
        return id(self)

    def __init__(self, ctx: PipelineContext, config: SupersetConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report = SupersetSourceReport()
        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[domain_id for domain_id in self.config.domain],
                graph=self.ctx.graph,
            )
        self.session = self.login()
        self.owner_info = self.parse_owner_info()

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

        test_response = requests_session.get(
            f"{self.config.connect_uri}/api/v1/dashboard/",
            timeout=self.config.timeout,
        )
        if test_response.status_code != 200:
            # throw an error and terminate ingestion,
            # cannot proceed without access token
            logger.error(
                f"Failed to log in to Superset with status: {test_response.status_code}"
            )
        return requests_session

    def paginate_entity_api_results(self, entity_type, page_size=100):
        current_page = 0
        total_items = page_size

        while current_page * page_size < total_items:
            response = self.session.get(
                f"{self.config.connect_uri}/api/v1/{entity_type}",
                params={"q": f"(page:{current_page},page_size:{page_size})"},
                timeout=self.config.timeout,
            )

            if response.status_code != 200:
                logger.warning(f"Failed to get {entity_type} data: {response.text}")
                continue

            payload = response.json()
            # Update total_items with the actual count from the response
            total_items = payload.get("count", total_items)
            # Yield each item in the result, this gets passed into the construct functions
            for item in payload.get("result", []):
                yield item

            current_page += 1

    def parse_owner_info(self) -> Dict[str, Any]:
        entity_types = ["dataset", "dashboard", "chart"]
        owners_info = {}

        for entity in entity_types:
            for owner in self.paginate_entity_api_results(f"{entity}/related/owners"):
                owner_id = owner.get("value")
                if owner_id:
                    owners_info[owner_id] = owner.get("extra", {}).get("email", "")

        return owners_info

    def build_owner_urn(self, data: Dict[str, Any]) -> List[str]:
        return [
            make_user_urn(self.owner_info.get(owner.get("id"), ""))
            for owner in data.get("owners", [])
            if owner.get("id")
        ]

    @lru_cache(maxsize=None)
    def get_dataset_info(self, dataset_id: int) -> dict:
        dataset_response = self.session.get(
            f"{self.config.connect_uri}/api/v1/dataset/{dataset_id}",
            timeout=self.config.timeout,
        )
        if dataset_response.status_code != 200:
            logger.warning(f"Failed to get dataset info: {dataset_response.text}")
            return {}
        return dataset_response.json()

    def get_datasource_urn_from_id(
        self, dataset_response: dict, platform_instance: str
    ) -> str:
        schema_name = dataset_response.get("result", {}).get("schema")
        table_name = dataset_response.get("result", {}).get("table_name")
        database_id = dataset_response.get("result", {}).get("database", {}).get("id")
        database_name = (
            dataset_response.get("result", {}).get("database", {}).get("database_name")
        )
        database_name = self.config.database_alias.get(database_name, database_name)

        # Druid do not have a database concept and has a limited schema concept, but they are nonetheless reported
        # from superset. There is only one database per platform instance, and one schema named druid, so it would be
        # redundant to systemically store them both in the URN.
        if platform_instance in platform_without_databases:
            database_name = None

        if platform_instance == "druid" and schema_name == "druid":
            # Follow DataHub's druid source convention.
            schema_name = None

        # If the information about the datasource is already contained in the dataset response,
        # can just return the urn directly
        if table_name and database_id:
            return make_dataset_urn(
                platform=platform_instance,
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
            name=str(dashboard_data["id"]),
            platform_instance=self.config.platform_instance,
        )
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[Status(removed=False)],
        )

        modified_actor = f"urn:li:corpuser:{self.owner_info.get((dashboard_data.get('changed_by') or {}).get('id', -1), 'unknown')}"
        now = datetime.now().strftime("%I:%M%p on %B %d, %Y")
        modified_ts = int(
            dp.parse(dashboard_data.get("changed_on_utc", now)).timestamp() * 1000
        )
        title = dashboard_data.get("dashboard_title", "")
        # note: the API does not currently supply created_by usernames due to a bug
        last_modified = AuditStampClass(time=modified_ts, actor=modified_actor)

        change_audit_stamps = ChangeAuditStamps(
            created=None, lastModified=last_modified
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
                    name=str(value.get("meta", {}).get("chartId", "unknown")),
                    platform_instance=self.config.platform_instance,
                )
            )

        # Build properties
        custom_properties = {
            "Status": str(dashboard_data.get("status")),
            "IsPublished": str(dashboard_data.get("published", False)).lower(),
            "Owners": ", ".join(
                map(
                    lambda owner: self.owner_info.get(owner.get("id", -1), "unknown"),
                    dashboard_data.get("owners", []),
                )
            ),
            "IsCertified": str(bool(dashboard_data.get("certified_by"))).lower(),
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
            dashboardUrl=dashboard_url,
            customProperties=custom_properties,
            lastModified=change_audit_stamps,
        )
        dashboard_snapshot.aspects.append(dashboard_info)

        dashboard_owners_list = self.build_owner_urn(dashboard_data)
        owners_info = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=urn,
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
                for urn in (dashboard_owners_list or [])
            ],
            lastModified=last_modified,
        )
        dashboard_snapshot.aspects.append(owners_info)

        return dashboard_snapshot

    def _process_dashboard(self, dashboard_data: Any) -> Iterable[MetadataWorkUnit]:
        dashboard_title = ""
        try:
            dashboard_id = str(dashboard_data.get("id"))
            dashboard_title = dashboard_data.get("dashboard_title", "")
            if not self.config.dashboard_pattern.allowed(dashboard_title):
                self.report.report_dropped(
                    f"Dashboard '{dashboard_title}' (id: {dashboard_id}) filtered by dashboard_pattern"
                )
                return
            dashboard_snapshot = self.construct_dashboard_from_api_data(dashboard_data)
        except Exception as e:
            self.report.warning(
                f"Failed to construct dashboard snapshot. Dashboard name: {dashboard_data.get('dashboard_title')}. Error: \n{e}"
            )
            return
        mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
        yield MetadataWorkUnit(id=dashboard_snapshot.urn, mce=mce)
        yield from self._get_domain_wu(
            title=dashboard_title, entity_urn=dashboard_snapshot.urn
        )

    def emit_dashboard_mces(self) -> Iterable[MetadataWorkUnit]:
        dashboard_data_list = [
            (dashboard_data,)
            for dashboard_data in self.paginate_entity_api_results(
                "dashboard/", PAGE_SIZE
            )
        ]

        yield from ThreadedIteratorExecutor.process(
            worker_func=self._process_dashboard,
            args_list=dashboard_data_list,
            max_workers=self.config.max_threads,
        )

    def build_input_fields(
        self,
        chart_columns: List[Tuple[str, str, str]],
        datasource_urn: Union[str, None],
    ) -> List[InputField]:
        input_fields: List[InputField] = []

        for column in chart_columns:
            col_name, col_type, description = column
            if not col_type or not datasource_urn:
                continue

            type_class = FIELD_TYPE_MAPPING.get(
                col_type.upper(), NullTypeClass
            )  # gets the type mapping

            input_fields.append(
                InputField(
                    schemaFieldUrn=builder.make_schema_field_urn(
                        parent_urn=str(datasource_urn),
                        field_path=col_name,
                    ),
                    schemaField=SchemaField(
                        fieldPath=col_name,
                        type=SchemaFieldDataType(type=type_class()),  # type: ignore
                        description=(description if description != "null" else ""),
                        nativeDataType=col_type,
                        globalTags=None,
                        nullable=True,
                    ),
                )
            )

        return input_fields

    def construct_chart_cll(
        self,
        chart_data: dict,
        datasource_urn: Union[str, None],
        datasource_id: Union[Any, int],
    ) -> List[InputField]:
        column_data: List[Union[str, dict]] = chart_data.get("form_data", {}).get(
            "all_columns", []
        )

        # the second field represents whether its a SQL expression,
        # false being just regular column and true being SQL col
        chart_column_data: List[Tuple[str, bool]] = [
            (column, False)
            if isinstance(column, str)
            else (column.get("label", ""), True)
            for column in column_data
        ]

        dataset_columns: List[Tuple[str, str, str]] = []

        # parses the superset dataset's column info, to build type and description info
        if datasource_id:
            dataset_info = self.get_dataset_info(datasource_id).get("result", {})
            dataset_column_info = dataset_info.get("columns", [])

            for column in dataset_column_info:
                col_name = column.get("column_name", "")
                col_type = column.get("type", "")
                col_description = column.get("description", "")

                # if missing column name or column type, cannot construct the column,
                # so we skip this column, missing description is fine
                if col_name == "" or col_type == "":
                    logger.info(f"could not construct column lineage for {column}")
                    continue

                dataset_columns.append((col_name, col_type, col_description))
        else:
            # if no datasource id, cannot build cll, just return
            logger.warning(
                "no datasource id was found, cannot build column level lineage"
            )
            return []

        chart_columns: List[Tuple[str, str, str]] = []
        for chart_col in chart_column_data:
            chart_col_name, is_sql = chart_col
            if is_sql:
                chart_columns.append(
                    (
                        chart_col_name,
                        "SQL",
                        "",
                    )
                )
                continue

            # find matching upstream column
            for dataset_col in dataset_columns:
                dataset_col_name, dataset_col_type, dataset_col_description = (
                    dataset_col
                )
                if dataset_col_name == chart_col_name:
                    chart_columns.append(
                        (chart_col_name, dataset_col_type, dataset_col_description)
                    )  # column name, column type, description
                    break

            # if no matching upstream column was found
            if len(chart_columns) == 0 or chart_columns[-1][0] != chart_col_name:
                chart_columns.append((chart_col_name, "", ""))

        return self.build_input_fields(chart_columns, datasource_urn)

    def construct_chart_from_chart_data(
        self, chart_data: dict
    ) -> Iterable[MetadataWorkUnit]:
        chart_urn = make_chart_urn(
            platform=self.platform,
            name=str(chart_data["id"]),
            platform_instance=self.config.platform_instance,
        )
        chart_snapshot = ChartSnapshot(
            urn=chart_urn,
            aspects=[Status(removed=False)],
        )

        modified_actor = f"urn:li:corpuser:{self.owner_info.get((chart_data.get('changed_by') or {}).get('id', -1), 'unknown')}"
        now = datetime.now().strftime("%I:%M%p on %B %d, %Y")
        modified_ts = int(
            dp.parse(chart_data.get("changed_on_utc", now)).timestamp() * 1000
        )
        title = chart_data.get("slice_name", "")

        # note: the API does not currently supply created_by usernames due to a bug
        last_modified = AuditStampClass(time=modified_ts, actor=modified_actor)

        change_audit_stamps = ChangeAuditStamps(
            created=None, lastModified=last_modified
        )

        chart_type = chart_type_from_viz_type.get(chart_data.get("viz_type", ""))
        chart_url = f"{self.config.display_uri}{chart_data.get('url', '')}"

        datasource_id = chart_data.get("datasource_id")
        if not datasource_id:
            logger.debug(
                f"chart {chart_data['id']} has no datasource_id, skipping fetching dataset info"
            )
            datasource_urn = None
        else:
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
            chartUrl=chart_url,
            inputs=[datasource_urn] if datasource_urn else None,
            customProperties=custom_properties,
            lastModified=change_audit_stamps,
        )
        chart_snapshot.aspects.append(chart_info)

        input_fields = self.construct_chart_cll(
            chart_data, datasource_urn, datasource_id
        )

        if input_fields:
            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=InputFields(
                    fields=sorted(input_fields, key=lambda x: x.schemaFieldUrn)
                ),
            ).as_workunit()

        chart_owners_list = self.build_owner_urn(chart_data)
        owners_info = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=urn,
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
                for urn in (chart_owners_list or [])
            ],
            lastModified=last_modified,
        )
        chart_snapshot.aspects.append(owners_info)
        yield MetadataWorkUnit(
            id=chart_urn, mce=MetadataChangeEvent(proposedSnapshot=chart_snapshot)
        )

        yield from self._get_domain_wu(
            title=chart_data.get("slice_name", ""),
            entity_urn=chart_urn,
        )

    def _process_chart(self, chart_data: Any) -> Iterable[MetadataWorkUnit]:
        chart_name = ""
        try:
            chart_id = str(chart_data.get("id"))
            chart_name = chart_data.get("slice_name", "")
            if not self.config.chart_pattern.allowed(chart_name):
                self.report.report_dropped(
                    f"Chart '{chart_name}' (id: {chart_id}) filtered by chart_pattern"
                )
                return
            if self.config.dataset_pattern != AllowDenyPattern.allow_all():
                datasource_id = chart_data.get("datasource_id")
                if datasource_id:
                    dataset_response = self.get_dataset_info(datasource_id)
                    dataset_name = dataset_response.get("result", {}).get(
                        "table_name", ""
                    )
                    if dataset_name and not self.config.dataset_pattern.allowed(
                        dataset_name
                    ):
                        self.report.warning(
                            f"Chart '{chart_name}' (id: {chart_id}) uses dataset '{dataset_name}' which is filtered by dataset_pattern"
                        )
            yield from self.construct_chart_from_chart_data(chart_data)
        except Exception as e:
            self.report.warning(
                f"Failed to construct chart snapshot. Chart name: {chart_name}. Error: \n{e}"
            )
            return

    def emit_chart_mces(self) -> Iterable[MetadataWorkUnit]:
        chart_data_list = [
            (chart_data,)
            for chart_data in self.paginate_entity_api_results("chart/", PAGE_SIZE)
        ]
        yield from ThreadedIteratorExecutor.process(
            worker_func=self._process_chart,
            args_list=chart_data_list,
            max_workers=self.config.max_threads,
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

    def generate_virtual_dataset_lineage(
        self,
        parsed_query_object: SqlParsingResult,
        datasource_urn: str,
    ) -> UpstreamLineageClass:
        cll = (
            parsed_query_object.column_lineage
            if parsed_query_object.column_lineage is not None
            else []
        )

        fine_grained_lineages: List[FineGrainedLineageClass] = []

        for cll_info in cll:
            downstream = (
                [make_schema_field_urn(datasource_urn, cll_info.downstream.column)]
                if cll_info.downstream and cll_info.downstream.column
                else []
            )
            upstreams = [
                make_schema_field_urn(column_ref.table, column_ref.column)
                for column_ref in cll_info.upstreams
            ]
            fine_grained_lineages.append(
                FineGrainedLineageClass(
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=downstream,
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=upstreams,
                )
            )

        upstream_lineage = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    type=DatasetLineageTypeClass.TRANSFORMED,
                    dataset=input_table_urn,
                )
                for input_table_urn in parsed_query_object.in_tables
            ],
            fineGrainedLineages=fine_grained_lineages,
        )
        return upstream_lineage

    def generate_physical_dataset_lineage(
        self,
        dataset_response: dict,
        upstream_dataset: str,
        datasource_urn: str,
    ) -> UpstreamLineageClass:
        # To generate column level lineage, we can manually decode the metadata
        # to produce the ColumnLineageInfo
        columns = dataset_response.get("result", {}).get("columns", [])
        fine_grained_lineages: List[FineGrainedLineageClass] = []

        for column in columns:
            column_name = column.get("column_name", "")
            if not column_name:
                continue

            downstream = [make_schema_field_urn(datasource_urn, column_name)]
            upstreams = [make_schema_field_urn(upstream_dataset, column_name)]
            fine_grained_lineages.append(
                FineGrainedLineageClass(
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=downstream,
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=upstreams,
                )
            )

        upstream_lineage = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    type=DatasetLineageTypeClass.TRANSFORMED,
                    dataset=upstream_dataset,
                )
            ],
            fineGrainedLineages=fine_grained_lineages,
        )
        return upstream_lineage

    def construct_dataset_from_dataset_data(
        self, dataset_data: dict
    ) -> DatasetSnapshot:
        dataset_response = self.get_dataset_info(dataset_data.get("id"))
        dataset = SupersetDataset(**dataset_response["result"])

        datasource_urn = self.get_datasource_urn_from_id(
            dataset_response, self.platform
        )
        dataset_url = f"{self.config.display_uri}{dataset_response.get('result', {}).get('url', '')}"

        modified_actor = f"urn:li:corpuser:{self.owner_info.get((dataset_data.get('changed_by') or {}).get('id', -1), 'unknown')}"
        now = datetime.now().strftime("%I:%M%p on %B %d, %Y")
        modified_ts = int(
            dp.parse(dataset_data.get("changed_on_utc", now)).timestamp() * 1000
        )
        last_modified = AuditStampClass(time=modified_ts, actor=modified_actor)

        upstream_warehouse_platform = (
            dataset_response.get("result", {}).get("database", {}).get("backend")
        )
        upstream_warehouse_db_name = (
            dataset_response.get("result", {}).get("database", {}).get("database_name")
        )

        # if we have rendered sql, we always use that and defualt back to regular sql
        sql = dataset_response.get("result", {}).get(
            "rendered_sql"
        ) or dataset_response.get("result", {}).get("sql")

        # Preset has a way of naming their platforms differently than
        # how datahub names them, so map the platform name to the correct naming
        warehouse_naming = {
            "awsathena": "athena",
            "clickhousedb": "clickhouse",
            "postgresql": "postgres",
        }

        if upstream_warehouse_platform in warehouse_naming:
            upstream_warehouse_platform = warehouse_naming[upstream_warehouse_platform]

        upstream_dataset = self.get_datasource_urn_from_id(
            dataset_response, upstream_warehouse_platform
        )

        # Sometimes the field will be null instead of not existing
        if sql == "null" or not sql:
            tag_urn = f"urn:li:tag:{self.platform}:physical"
            upstream_lineage = self.generate_physical_dataset_lineage(
                dataset_response, upstream_dataset, datasource_urn
            )
        else:
            tag_urn = f"urn:li:tag:{self.platform}:virtual"
            parsed_query_object = create_lineage_sql_parsed_result(
                query=sql,
                default_db=upstream_warehouse_db_name,
                platform=upstream_warehouse_platform,
                platform_instance=None,
                env=self.config.env,
            )
            upstream_lineage = self.generate_virtual_dataset_lineage(
                parsed_query_object, datasource_urn
            )

        dataset_info = DatasetPropertiesClass(
            name=dataset.table_name,
            description="",
            externalUrl=dataset_url,
            lastModified=TimeStamp(time=modified_ts),
        )
        global_tags = GlobalTagsClass(tags=[TagAssociationClass(tag=tag_urn)])

        aspects_items: List[Any] = []
        aspects_items.extend(
            [
                self.gen_schema_metadata(dataset_response),
                dataset_info,
                upstream_lineage,
                global_tags,
            ]
        )

        dataset_snapshot = DatasetSnapshot(
            urn=datasource_urn,
            aspects=aspects_items,
        )

        dataset_owners_list = self.build_owner_urn(dataset_data)
        owners_info = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=urn,
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
                for urn in (dataset_owners_list or [])
            ],
            lastModified=last_modified,
        )
        aspects_items.append(owners_info)

        return dataset_snapshot

    def _process_dataset(self, dataset_data: Any) -> Iterable[MetadataWorkUnit]:
        dataset_name = ""
        try:
            dataset_name = dataset_data.get("table_name", "")
            if not self.config.dataset_pattern.allowed(dataset_name):
                self.report.report_dropped(
                    f"Dataset '{dataset_name}' filtered by dataset_pattern"
                )
                return
            dataset_snapshot = self.construct_dataset_from_dataset_data(dataset_data)
            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        except Exception as e:
            self.report.warning(
                f"Failed to construct dataset snapshot. Dataset name: {dataset_data.get('table_name')}. Error: \n{e}"
            )
            return
        yield MetadataWorkUnit(id=dataset_snapshot.urn, mce=mce)
        yield from self._get_domain_wu(
            title=dataset_data.get("table_name", ""),
            entity_urn=dataset_snapshot.urn,
        )

    def emit_dataset_mces(self) -> Iterable[MetadataWorkUnit]:
        dataset_data_list = [
            (dataset_data,)
            for dataset_data in self.paginate_entity_api_results("dataset/", PAGE_SIZE)
        ]
        yield from ThreadedIteratorExecutor.process(
            worker_func=self._process_dataset,
            args_list=dataset_data_list,
            max_workers=self.config.max_threads,
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
