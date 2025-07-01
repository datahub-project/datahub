import dataclasses
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from json import JSONDecodeError
from typing import Dict, Iterable, Iterator, List, Optional, Set, Tuple, Union

import dateutil.parser as dp
import psutil
import pydantic
import requests
import sqlglot
import tenacity
import yaml
from liquid import Template, Undefined
from pydantic import Field, validator
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import ConnectionError
from requests.models import HTTPBasicAuth, HTTPError
from tenacity import retry_if_exception_type, stop_after_attempt, wait_exponential

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    DatasetLineageProviderConfigBase,
)
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
    add_dataset_to_container,
    gen_containers,
)
from datahub.emitter.request_helper import make_curl_command
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    BIContainerSubTypes,
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
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot,
    DashboardSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsClass,
    BrowsePathsV2Class,
    ChartInfoClass,
    ChartQueryClass,
    ChartQueryTypeClass,
    ChartTypeClass,
    DashboardInfoClass,
    DashboardUsageStatisticsClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    EmbedClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    InputFieldClass,
    InputFieldsClass,
    OperationClass,
    OperationTypeClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    QueryLanguageClass,
    QueryPropertiesClass,
    QuerySourceClass,
    QueryStatementClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    SubTypesClass,
    TagAssociationClass,
    TagPropertiesClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.metadata.urns import QueryUrn
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    SqlParsingResult,
    create_lineage_sql_parsed_result,
    infer_output_schema,
)
from datahub.utilities import config_clean
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.perf_timer import PerfTimer

logger: logging.Logger = logging.getLogger(__name__)
# Default API limit for items returned per API call
# Used for the default per_page value for paginated API requests
DEFAULT_API_ITEMS_PER_PAGE = 30


class SpaceKey(ContainerKey):
    # Note that Mode has renamed Spaces to Collections.
    space_token: str


class ModeAPIConfig(ConfigModel):
    retry_backoff_multiplier: Union[int, float] = Field(
        default=2,
        description="Multiplier for exponential backoff when waiting to retry",
    )
    max_retry_interval: Union[int, float] = Field(
        default=10, description="Maximum interval to wait when retrying"
    )
    max_attempts: int = Field(
        default=5, description="Maximum number of attempts to retry before failing"
    )
    timeout: int = Field(
        default=40,
        description="Timout setting, how long to wait for the Mode rest api to send data before giving up",
    )


class ModeConfig(
    StatefulIngestionConfigBase,
    DatasetLineageProviderConfigBase,
):
    # See https://mode.com/developer/api-reference/authentication/
    # for authentication
    connect_uri: str = Field(
        default="https://app.mode.com", description="Mode host URL."
    )
    token: str = Field(
        description="When creating workspace API key this is the 'Key ID'."
    )
    password: pydantic.SecretStr = Field(
        description="When creating workspace API key this is the 'Secret'."
    )
    exclude_restricted: bool = Field(
        default=False, description="Exclude restricted collections"
    )

    workspace: str = Field(
        description="The Mode workspace username. If you navigate to Workspace Settings > Details, "
        "the url will be `https://app.mode.com/organizations/<workspace-username>`. "
        # The lowercase comment is derived from a comment in a Mode API example.
        # https://mode.com/developer/api-cookbook/management/get-all-reports/
        # > "Note: workspace_name value should be all lowercase"
        "This is distinct from the workspace's display name, and should be all lowercase."
    )
    _default_schema = pydantic_removed_field("default_schema")

    space_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern(
            deny=["^Personal$"],
        ),
        description="Regex patterns for mode spaces to filter in ingestion (Spaces named as 'Personal' are filtered by default.) Specify regex to only match the space name. e.g. to only ingest space named analytics, use the regex 'analytics'",
    )

    owner_username_instead_of_email: Optional[bool] = Field(
        default=True, description="Use username for owner URN instead of Email"
    )
    api_options: ModeAPIConfig = Field(
        default=ModeAPIConfig(),
        description='Retry/Wait settings for Mode API to avoid "Too many Requests" error. See Mode API Options below',
    )

    ingest_embed_url: bool = Field(
        default=True, description="Whether to Ingest embed URL for Reports"
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    tag_measures_and_dimensions: Optional[bool] = Field(
        default=True, description="Tag measures and dimensions in the schema"
    )

    items_per_page: int = Field(
        default=DEFAULT_API_ITEMS_PER_PAGE,
        description="Number of items per page for paginated API requests.",
        hidden_from_docs=True,
    )

    @validator("connect_uri")
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)

    @validator("items_per_page")
    def validate_items_per_page(cls, v):
        if 1 <= v <= DEFAULT_API_ITEMS_PER_PAGE:
            return v
        else:
            raise ValueError(
                f"items_per_page must be between 1 and {DEFAULT_API_ITEMS_PER_PAGE}"
            )


class HTTPError429(HTTPError):
    pass


class HTTPError504(HTTPError):
    pass


ModeRequestError = (HTTPError, JSONDecodeError)


@dataclass
class ModeSourceReport(StaleEntityRemovalSourceReport):
    filtered_spaces: LossyList[str] = dataclasses.field(default_factory=LossyList)
    num_sql_parsed: int = 0
    num_sql_parser_failures: int = 0
    num_sql_parser_success: int = 0
    num_sql_parser_table_error: int = 0
    num_sql_parser_column_error: int = 0
    num_query_template_render: int = 0
    num_query_template_render_failures: int = 0
    num_query_template_render_success: int = 0
    num_requests_exceeding_rate_limit: int = 0
    num_requests_retried_on_timeout: int = 0
    num_spaces_retrieved: int = 0
    space_get_api_called: int = 0
    report_get_api_called: int = 0
    dataset_get_api_called: int = 0
    query_get_api_called: int = 0
    chart_get_api_called: int = 0
    get_cache_hits: int = 0
    get_cache_misses: int = 0
    get_cache_size: int = 0
    process_memory_used_mb: float = 0
    space_get_timer: PerfTimer = dataclasses.field(default_factory=PerfTimer)
    report_get_timer: PerfTimer = dataclasses.field(default_factory=PerfTimer)
    dataset_get_timer: PerfTimer = dataclasses.field(default_factory=PerfTimer)
    query_get_timer: PerfTimer = dataclasses.field(default_factory=PerfTimer)
    chart_get_timer: PerfTimer = dataclasses.field(default_factory=PerfTimer)

    def report_dropped_space(self, ent_name: str) -> None:
        self.filtered_spaces.append(ent_name)


@platform_name("Mode")
@config_class(ModeConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Supported by default")
@capability(SourceCapability.LINEAGE_FINE, "Supported by default")
@capability(SourceCapability.OWNERSHIP, "Enabled by default")
class ModeSource(StatefulIngestionSourceBase):
    """

    This plugin extracts Charts, Reports, and associated metadata from a given Mode workspace. This plugin is in beta and has only been tested
    on PostgreSQL database.

    ### Report

    [/api/{account}/reports/{report}](https://mode.com/developer/api-reference/analytics/reports/) endpoint is used to
    retrieve the following report information.

    - Title and description
    - Last edited by
    - Owner
    - Link to the Report in Mode for exploration
    - Associated charts within the report

    ### Chart

    [/api/{workspace}/reports/{report}/queries/{query}/charts'](https://mode.com/developer/api-reference/analytics/charts/#getChart) endpoint is used to
    retrieve the following information.

    - Title and description
    - Last edited by
    - Owner
    - Link to the chart in Metabase
    - Datasource and lineage information from Report queries.

    The following properties for a chart are ingested in DataHub.

    #### Chart Information
    | Name      | Description                            |
    |-----------|----------------------------------------|
    | `Filters` | Filters applied to the chart           |
    | `Metrics` | Fields or columns used for aggregation |
    | `X`       | Fields used in X-axis                  |
    | `X2`      | Fields used in second X-axis           |
    | `Y`       | Fields used in Y-axis                  |
    | `Y2`      | Fields used in second Y-axis           |


    #### Table Information
    | Name      | Description                  |
    |-----------|------------------------------|
    | `Columns` | Column names in a table      |
    | `Filters` | Filters applied to the table |



    #### Pivot Table Information
    | Name      | Description                            |
    |-----------|----------------------------------------|
    | `Columns` | Column names in a table                |
    | `Filters` | Filters applied to the table           |
    | `Metrics` | Fields or columns used for aggregation |
    | `Rows`    | Row names in a table                   |

    """

    ctx: PipelineContext
    config: ModeConfig
    report: ModeSourceReport
    platform = "mode"

    DIMENSION_TAG_URN = "urn:li:tag:Dimension"
    MEASURE_TAG_URN = "urn:li:tag:Measure"

    tag_definitions: Dict[str, TagPropertiesClass] = {
        DIMENSION_TAG_URN: TagPropertiesClass(
            name=DIMENSION_TAG_URN.split(":")[-1],
            description="A tag that is applied to all dimension fields.",
        ),
        MEASURE_TAG_URN: TagPropertiesClass(
            name=MEASURE_TAG_URN.split(":")[-1],
            description="A tag that is applied to all measures (metrics). Measures are typically the columns that you aggregate on",
        ),
    }

    def __hash__(self):
        return id(self)

    def __init__(self, ctx: PipelineContext, config: ModeConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report = ModeSourceReport()
        self.ctx = ctx

        self.session = requests.Session()
        # Handling retry and backoff
        retries = 3
        backoff_factor = 10
        retry = Retry(total=retries, backoff_factor=backoff_factor)
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.session.auth = HTTPBasicAuth(
            self.config.token,
            self.config.password.get_secret_value(),
        )
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/hal+json",
            }
        )

        # Test the connection
        try:
            key_info = self._get_request_json(f"{self.config.connect_uri}/api/verify")
            logger.debug(f"Auth info: {key_info}")
        except ModeRequestError as e:
            self.report.report_failure(
                title="Failed to Connect",
                message="Unable to verify connection to mode.",
                context=f"Error: {str(e)}",
            )

        self.workspace_uri = f"{self.config.connect_uri}/api/{self.config.workspace}"
        self.space_tokens = self._get_space_name_and_tokens()

    def _browse_path_space(self) -> List[BrowsePathEntryClass]:
        # TODO: Use containers for the workspace?
        return [
            BrowsePathEntryClass(id=self.config.workspace),
        ]

    def _browse_path_dashboard(self, space_token: str) -> List[BrowsePathEntryClass]:
        space_container_urn = self.gen_space_key(space_token).as_urn()
        return [
            *self._browse_path_space(),
            BrowsePathEntryClass(id=space_container_urn, urn=space_container_urn),
        ]

    def _browse_path_query(
        self, space_token: str, report_info: dict
    ) -> List[BrowsePathEntryClass]:
        dashboard_urn = self._dashboard_urn(report_info)
        return [
            *self._browse_path_dashboard(space_token),
            BrowsePathEntryClass(id=dashboard_urn, urn=dashboard_urn),
        ]

    def _browse_path_chart(
        self, space_token: str, report_info: dict, query_info: dict
    ) -> List[BrowsePathEntryClass]:
        query_urn = self.get_dataset_urn_from_query(query_info)
        return [
            *self._browse_path_query(space_token, report_info),
            BrowsePathEntryClass(id=query_urn, urn=query_urn),
        ]

    def _dashboard_urn(self, report_info: dict) -> str:
        return builder.make_dashboard_urn(self.platform, str(report_info.get("id", "")))

    def _parse_last_run_at(self, report_info: dict) -> Optional[int]:
        # Mode queries are refreshed, and that timestamp is reflected correctly here.
        # However, datasets are synced, and that's captured by the sync timestamps.
        # However, this is probably accurate enough for now.
        last_refreshed_ts = None
        last_refreshed_ts_str = report_info.get("last_run_at")
        if last_refreshed_ts_str:
            last_refreshed_ts = int(dp.parse(last_refreshed_ts_str).timestamp() * 1000)

        return last_refreshed_ts

    def construct_dashboard(
        self, space_token: str, report_info: dict
    ) -> Optional[Tuple[DashboardSnapshot, MetadataChangeProposalWrapper]]:
        report_token = report_info.get("token", "")
        # logger.debug(f"Processing report {report_info.get('name', '')}: {report_info}")

        if not report_token:
            self.report.report_warning(
                title="Missing Report Token",
                message=f"Report token is missing for {report_info.get('id', '')}",
            )
            return None

        if not report_info.get("id"):
            self.report.report_warning(
                title="Missing Report ID",
                message=f"Report id is missing for {report_info.get('token', '')}",
            )
            return None

        dashboard_urn = self._dashboard_urn(report_info)
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[],
        )

        title = report_info.get("name", "")
        description = report_info.get("description", "")
        last_modified = ChangeAuditStamps()

        # Creator + created ts.
        creator = self._get_creator(
            report_info.get("_links", {}).get("creator", {}).get("href", "")
        )
        if creator:
            creator_actor = builder.make_user_urn(creator)
            created_ts = int(
                dp.parse(f"{report_info.get('created_at', 'now')}").timestamp() * 1000
            )
            last_modified.created = AuditStamp(time=created_ts, actor=creator_actor)

        # Last modified ts.
        last_modified_ts_str = report_info.get("last_saved_at")
        if not last_modified_ts_str:
            # Sometimes mode returns null for last_saved_at.
            # In that case, we use the edited_at timestamp instead.
            last_modified_ts_str = report_info.get("edited_at")
        if last_modified_ts_str:
            modified_ts = int(dp.parse(last_modified_ts_str).timestamp() * 1000)
            last_modified.lastModified = AuditStamp(
                time=modified_ts, actor="urn:li:corpuser:unknown"
            )

        # Last refreshed ts.
        last_refreshed_ts = self._parse_last_run_at(report_info)

        # Datasets
        datasets = []
        for imported_dataset_name in report_info.get("imported_datasets", {}):
            try:
                mode_dataset = self._get_request_json(
                    f"{self.workspace_uri}/reports/{imported_dataset_name.get('token')}"
                )
            except HTTPError as http_error:
                status_code = http_error.response.status_code
                if status_code == 404:
                    self.report.report_warning(
                        title="Report Not Found",
                        message="Referenced report for reusable dataset was not found.",
                        context=f"Report: {report_info.get('id')}, "
                        f"Imported Dataset Report: {imported_dataset_name.get('token')}",
                    )
                    continue
                else:
                    raise http_error

            dataset_urn = builder.make_dataset_urn_with_platform_instance(
                self.platform,
                str(mode_dataset.get("id")),
                platform_instance=None,
                env=self.config.env,
            )
            datasets.append(dataset_urn)

        dashboard_info_class = DashboardInfoClass(
            description=description if description else "",
            title=title if title else "",
            charts=self._get_chart_urns(report_token),
            datasets=datasets if datasets else None,
            lastModified=last_modified,
            lastRefreshed=last_refreshed_ts,
            dashboardUrl=f"{self.config.connect_uri}/{self.config.workspace}/reports/{report_token}",
            customProperties={},
        )
        dashboard_snapshot.aspects.append(dashboard_info_class)

        # browse path
        space_name = self.space_tokens[space_token]
        browse_path = BrowsePathsClass(
            paths=[
                f"/mode/{self.config.workspace}/"
                f"{space_name}/"
                f"{title if title else report_info.get('id', '')}"
            ]
        )
        dashboard_snapshot.aspects.append(browse_path)

        browse_path_v2 = BrowsePathsV2Class(
            path=self._browse_path_dashboard(space_token)
        )
        browse_mcp = MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=browse_path_v2,
        )

        # Ownership
        ownership = self._get_ownership(
            self._get_creator(
                report_info.get("_links", {}).get("creator", {}).get("href", "")
            )
        )
        if ownership is not None:
            dashboard_snapshot.aspects.append(ownership)

        return dashboard_snapshot, browse_mcp

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

    @lru_cache(maxsize=None)
    def _get_creator(self, href: str) -> Optional[str]:
        user = None
        try:
            user_json = self._get_request_json(f"{self.config.connect_uri}{href}")
            user = (
                user_json.get("username")
                if self.config.owner_username_instead_of_email
                else user_json.get("email")
            )
        except ModeRequestError as e:
            self.report.report_warning(
                title="Failed to retrieve Mode creator",
                message=f"Unable to retrieve user for {href}",
                context=f"Reason: {str(e)}",
            )
        return user

    def _get_chart_urns(self, report_token: str) -> list:
        chart_urns = []
        queries = self._get_queries(report_token)
        for query in queries:
            charts = self._get_charts(report_token, query.get("token", ""))
            # build chart urns
            for chart in charts:
                logger.debug(f"Chart: {chart.get('token')}")
                chart_urn = builder.make_chart_urn(
                    self.platform, chart.get("token", "")
                )
                chart_urns.append(chart_urn)

        return chart_urns

    def _get_space_name_and_tokens(self) -> dict:
        space_info = {}
        try:
            logger.debug(f"Retrieving spaces for {self.workspace_uri}")
            with self.report.space_get_timer:
                for spaces_page in self._get_paged_request_json(
                    f"{self.workspace_uri}/spaces?filter=all",
                    "spaces",
                    self.config.items_per_page,
                ):
                    self.report.space_get_api_called += 1
                    logger.debug(
                        f"Read {len(spaces_page)} spaces records from workspace {self.workspace_uri}"
                    )
                    self.report.num_spaces_retrieved += len(spaces_page)
                    for s in spaces_page:
                        logger.debug(f"Space: {s.get('name')}")
                        space_name = s.get("name", "")
                        # Using both restricted and default_access_level because
                        # there is a current bug with restricted returning False everytime
                        # which has been reported to Mode team
                        if self.config.exclude_restricted and (
                            s.get("restricted")
                            or s.get("default_access_level") == "restricted"
                        ):
                            logging.debug(
                                f"Skipping space {space_name} due to exclude restricted"
                            )
                            continue
                        if not self.config.space_pattern.allowed(space_name):
                            self.report.report_dropped_space(space_name)
                            logging.debug(
                                f"Skipping space {space_name} due to space pattern"
                            )
                            continue
                        space_info[s.get("token", "")] = s.get("name", "")
        except ModeRequestError as e:
            self.report.report_failure(
                title="Failed to Retrieve Spaces",
                message="Unable to retrieve spaces / collections for workspace.",
                context=f"Workspace: {self.workspace_uri}, Error: {str(e)}",
            )

        return space_info

    def _get_chart_type(self, token: str, display_type: str) -> Optional[str]:
        type_mapping = {
            "table": ChartTypeClass.TABLE,
            "bar": ChartTypeClass.BAR,
            "bigNumber": ChartTypeClass.TEXT,
            "line": ChartTypeClass.LINE,
            "stackedBar100": ChartTypeClass.BAR,
            "stackedBar": ChartTypeClass.BAR,
            "hStackedBar": ChartTypeClass.BAR,
            "hStackedBar100": ChartTypeClass.BAR,
            "hBar": ChartTypeClass.BAR,
            "area": ChartTypeClass.AREA,
            "totalArea": ChartTypeClass.AREA,
            "pie": ChartTypeClass.PIE,
            "donut": ChartTypeClass.PIE,
            "scatter": ChartTypeClass.SCATTER,
            "bigValue": ChartTypeClass.TEXT,
            "pivotTable": ChartTypeClass.TABLE,
            "linePlusBar": None,
            "vegas": None,
            "vegasPivotTable": ChartTypeClass.TABLE,
            "histogram": ChartTypeClass.HISTOGRAM,
        }
        if not display_type:
            self.report.info(
                title="Missing chart type found",
                message="Chart type is missing. Setting to None",
                context=f"Token: {token}",
            )
            return None
        try:
            chart_type = type_mapping[display_type]
        except KeyError:
            self.report.info(
                title="Unrecognized chart type found",
                message=f"Chart type {display_type} not supported. Setting to None",
                context=f"Token: {token}",
            )
            chart_type = None

        return chart_type

    def construct_chart_custom_properties(
        self, chart_detail: dict, chart_type: str
    ) -> Dict:
        custom_properties = {
            "ChartType": chart_type,
        }
        metadata = chart_detail.get("encoding", {})
        if chart_type == "table":
            columns = list(chart_detail.get("fieldFormats", {}).keys())
            str_columns = ",".join([c[1:-1] for c in columns])
            filters = metadata.get("filter", [])
            filters = filters[0].get("formula", "") if len(filters) else ""

            custom_properties.update(
                {
                    "Columns": str_columns,
                    "Filters": filters[1:-1] if len(filters) else "",
                }
            )

        elif chart_type == "pivotTable":
            pivot_table = chart_detail.get("pivotTable", {})
            columns = pivot_table.get("columns", [])
            rows = pivot_table.get("rows", [])
            values = pivot_table.get("values", [])
            filters = pivot_table.get("filters", [])

            custom_properties.update(
                {
                    "Columns": ", ".join(columns) if len(columns) else "",
                    "Rows": ", ".join(rows) if len(rows) else "",
                    "Metrics": ", ".join(values) if len(values) else "",
                    "Filters": ", ".join(filters) if len(filters) else "",
                }
            )
            # list filters in their own row
            for filter in filters:
                custom_properties[f"Filter: {filter}"] = ", ".join(
                    pivot_table.get("filterValues", {}).get(filter, "")
                )
        # Chart
        else:
            x = metadata.get("x", [])
            x2 = metadata.get("x2", [])
            y = metadata.get("y", [])
            y2 = metadata.get("y2", [])
            value = metadata.get("value", [])
            filters = metadata.get("filter", [])

            custom_properties.update(
                {
                    "X": x[0].get("formula", "") if len(x) else "",
                    "Y": y[0].get("formula", "") if len(y) else "",
                    "X2": x2[0].get("formula", "") if len(x2) else "",
                    "Y2": y2[0].get("formula", "") if len(y2) else "",
                    "Metrics": value[0].get("formula", "") if len(value) else "",
                    "Filters": filters[0].get("formula", "") if len(filters) else "",
                }
            )

        return custom_properties

    def _get_datahub_friendly_platform(self, adapter, platform):
        # Map adaptor names to what datahub expects in
        # https://github.com/datahub-project/datahub/blob/master/metadata-service/configuration/src/main/resources/bootstrap_mcps/data-platforms.yaml

        platform_mapping = {
            "jdbc:athena": "athena",
            "jdbc:bigquery": "bigquery",
            "jdbc:druid": "druid",
            "jdbc:hive": "hive",
            "jdbc:mysql": "mysql",
            "jdbc:oracle": "oracle",
            "jdbc:postgresql": "postgres",
            "jdbc:presto": "presto",
            "jdbc:redshift": "redshift",
            "jdbc:snowflake": "snowflake",
            "jdbc:spark": "spark",
            "jdbc:trino": "trino",
            "jdbc:sqlserver": "mssql",
            "jdbc:teradata": "teradata",
        }
        if adapter in platform_mapping:
            return platform_mapping[adapter]
        else:
            self.report.report_warning(
                title="Unrecognized Platform Found",
                message=f"Platform was not found in DataHub. "
                f"Using {platform} name as is",
            )

        return platform

    @lru_cache(maxsize=None)
    def _get_data_sources(self) -> List[dict]:
        data_sources = []
        try:
            ds_json = self._get_request_json(f"{self.workspace_uri}/data_sources")
            data_sources = ds_json.get("_embedded", {}).get("data_sources", [])
        except ModeRequestError as e:
            self.report.report_failure(
                title="Failed to retrieve Data Sources",
                message="Unable to retrieve data sources from Mode.",
                context=f"Error: {str(e)}",
            )

        return data_sources

    @lru_cache(maxsize=None)
    def _get_platform_and_dbname(
        self, data_source_id: int
    ) -> Union[Tuple[str, str], Tuple[None, None]]:
        data_sources = self._get_data_sources()

        if not data_sources:
            self.report.report_failure(
                title="No Data Sources Found",
                message="Could not find data sources matching some ids",
                context=f"Data Soutce ID: {data_source_id}",
            )
            return None, None

        for data_source in data_sources:
            if data_source.get("id", -1) == data_source_id:
                platform = self._get_datahub_friendly_platform(
                    data_source.get("adapter", ""), data_source.get("name", "")
                )
                database = data_source.get("database", "")
                # This is hacky but on bigquery we want to change the database if its default
                # For lineage we need project_id.db.table
                if platform == "bigquery" and database == "default":
                    database = data_source.get("host", "")
                return platform, database
        else:
            self.report.report_warning(
                title="Unable to construct upstream lineage",
                message="We did not find a data source / connection with a matching ID, meaning that we do not know the platform/database to use in lineage.",
                context=f"Data Source ID: {data_source_id}",
            )
        return None, None

    def _replace_definitions(self, raw_query: str) -> str:
        query = raw_query
        definitions = re.findall(r"({{(?:\s+)?@[^}{]+}})", raw_query)
        for definition_variable in definitions:
            definition_name, definition_alias = self._parse_definition_name(
                definition_variable
            )
            definition_query = self._get_definition(definition_name)
            # if unable to retrieve definition, then replace the {{}} so that it doesn't get picked up again in recursive call
            if definition_query is not None:
                query = query.replace(
                    definition_variable, f"({definition_query}) as {definition_alias}"
                )
            else:
                query = query.replace(
                    definition_variable, f"{definition_name} as {definition_alias}"
                )
            query = self._replace_definitions(query)
            query = query.replace("\\n", "\n")
            query = query.replace("\\t", "\t")

        return query

    def _parse_definition_name(self, definition_variable: str) -> Tuple[str, str]:
        name, alias = "", ""
        # i.e '{{ @join_on_definition as alias}}'
        name_match = re.findall("@[a-zA-Z_]+", definition_variable)
        if len(name_match):
            name = name_match[0][1:]
        alias_match = re.findall(
            r"as\s+\S+\w+", definition_variable
        )  # i.e ['as    alias_name']
        if len(alias_match):
            alias_match = alias_match[0].split(" ")
            alias = alias_match[-1]

        return name, alias

    @lru_cache(maxsize=None)
    def _get_definition(self, definition_name):
        try:
            definition_json = self._get_request_json(
                f"{self.workspace_uri}/definitions"
            )
            definitions = definition_json.get("_embedded", {}).get("definitions", [])
            for definition in definitions:
                if definition.get("name", "") == definition_name:
                    return definition.get("source", "")

        except ModeRequestError as e:
            self.report.report_failure(
                title="Failed to Retrieve Definition",
                message="Unable to retrieve definition from Mode.",
                context=f"Definition Name: {definition_name}, Error: {str(e)}",
            )
        return None

    def _get_datasource_urn(
        self,
        platform: str,
        platform_instance: Optional[str],
        database: str,
        source_tables: List[str],
    ) -> List[str]:
        dataset_urn = None
        if platform or database is not None:
            dataset_urn = [
                builder.make_dataset_urn_with_platform_instance(
                    platform,
                    f"{database}.{s_table}",
                    platform_instance=platform_instance,
                    env=self.config.env,
                )
                for s_table in source_tables
            ]

        return dataset_urn

    def get_custom_props_from_dict(self, obj: dict, keys: List[str]) -> Optional[dict]:
        return {key: str(obj[key]) for key in keys if obj.get(key)} or None

    def get_dataset_urn_from_query(self, query_data: dict) -> str:
        return builder.make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=str(query_data.get("id")),
            platform_instance=None,
            env=self.config.env,
        )

    def get_query_instance_urn_from_query(self, query_data: dict) -> str:
        id = query_data.get("id")
        last_run_id = query_data.get("last_run_id")
        data_source_id = query_data.get("data_source_id")
        return QueryUrn(f"{id}.{data_source_id}.{last_run_id}").urn()

    def set_field_tags(self, fields: List[SchemaFieldClass]) -> None:
        for field in fields:
            # It is not clear how to distinguish between measures and dimensions in Mode.
            # We are using a heuristic to tag fields that are not named `id` or `_id` or ends with '_number' and are not of type `NumberType` as dimensions.
            # This is a heuristic and may not be accurate.
            if field.type.type.RECORD_SCHEMA.name in ["NumberType"] and (
                not field.fieldPath.endswith("_number")
                and not re.findall(r"(^(id)[_\d]?)|([_\d?](id)$)", field.fieldPath)
            ):
                tag = TagAssociationClass(tag=self.MEASURE_TAG_URN)
            else:
                tag = TagAssociationClass(tag=self.DIMENSION_TAG_URN)
            field.globalTags = GlobalTagsClass(tags=[tag])

    def normalize_mode_query(self, query: str) -> str:
        regex = r"{% form %}(.*?){% endform %}"
        rendered_query: str = query
        normalized_query: str = query

        self.report.num_query_template_render += 1
        matches = re.findall(regex, query, re.MULTILINE | re.DOTALL | re.IGNORECASE)
        try:
            jinja_params: Dict = {}
            if matches:
                for match in matches:
                    definition = Template(source=match).render()
                    parameters = yaml.safe_load(definition)
                    for key in parameters:
                        jinja_params[key] = parameters[key].get("default", "")

                normalized_query = re.sub(
                    pattern=r"{% form %}(.*){% endform %}",
                    repl="",
                    string=query,
                    count=0,
                    flags=re.MULTILINE | re.DOTALL,
                )

            # Wherever we don't resolve the jinja params, we replace it with NULL
            Undefined.__str__ = lambda self: "NULL"  # type: ignore
            rendered_query = Template(normalized_query).render(jinja_params)
            self.report.num_query_template_render_success += 1
        except Exception as e:
            logger.debug(f"Rendering query {query} failed with {e}")
            self.report.num_query_template_render_failures += 1
            return rendered_query

        return rendered_query

    def construct_query_or_dataset(
        self,
        report_token: str,
        query_data: dict,
        space_token: str,
        report_info: dict,
        is_mode_dataset: bool,
    ) -> Iterable[MetadataWorkUnit]:
        query_urn = (
            self.get_dataset_urn_from_query(query_data)
            if not is_mode_dataset
            else self.get_dataset_urn_from_query(report_info)
        )

        query_token = query_data.get("token")

        externalUrl = (
            f"{self.config.connect_uri}/{self.config.workspace}/datasets/{report_token}"
            if is_mode_dataset
            else f"{self.config.connect_uri}/{self.config.workspace}/reports/{report_token}/details/queries/{query_token}"
        )

        dataset_props = DatasetPropertiesClass(
            name=report_info.get("name") if is_mode_dataset else query_data.get("name"),
            description=None,
            externalUrl=externalUrl,
            customProperties=self.get_custom_props_from_dict(
                query_data,
                [
                    "id",
                    "created_at",
                    "updated_at",
                    "last_run_id",
                    "data_source_id",
                    "explorations_count",
                    "report_imports_count",
                    "dbt_metric_id",
                ],
            ),
        )
        yield (
            MetadataChangeProposalWrapper(
                entityUrn=query_urn,
                aspect=dataset_props,
            ).as_workunit()
        )

        if raw_query := query_data.get("raw_query"):
            yield MetadataChangeProposalWrapper(
                entityUrn=query_urn,
                aspect=ViewPropertiesClass(
                    viewLogic=raw_query,
                    viewLanguage=QueryLanguageClass.SQL,
                    materialized=False,
                ),
            ).as_workunit()

        if is_mode_dataset:
            space_container_key = self.gen_space_key(space_token)
            yield from add_dataset_to_container(
                container_key=space_container_key,
                dataset_urn=query_urn,
            )

        subtypes = SubTypesClass(
            typeNames=(
                [
                    BIAssetSubTypes.MODE_DATASET
                    if is_mode_dataset
                    else BIAssetSubTypes.MODE_QUERY
                ]
            )
        )
        yield (
            MetadataChangeProposalWrapper(
                entityUrn=query_urn,
                aspect=subtypes,
            ).as_workunit()
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=query_urn,
            aspect=BrowsePathsV2Class(
                path=self._browse_path_dashboard(space_token)
                if is_mode_dataset
                else self._browse_path_query(space_token, report_info)
            ),
        ).as_workunit()

        (
            upstream_warehouse_platform,
            upstream_warehouse_db_name,
        ) = self._get_platform_and_dbname(query_data.get("data_source_id"))
        if upstream_warehouse_platform is None:
            # this means we can't infer the platform
            return

        query = query_data["raw_query"]
        query = self._replace_definitions(query)
        normalized_query = self.normalize_mode_query(query)
        query_to_parse = normalized_query
        # If multiple query is present in the query, we get the last one.
        # This won't work for complex cases where temp table is created and used in the same query.
        # But it should be good enough for simple use-cases.
        try:
            for partial_query in sqlglot.parse(normalized_query):
                if not partial_query:
                    continue
                # This is hacky but on snowlake we want to change the default warehouse if use warehouse is present
                if upstream_warehouse_platform == "snowflake":
                    regexp = r"use\s+warehouse\s+(.*)(\s+)?;"
                    matches = re.search(
                        regexp,
                        partial_query.sql(dialect=upstream_warehouse_platform),
                        re.MULTILINE | re.DOTALL | re.IGNORECASE,
                    )
                    if matches and matches.group(1):
                        upstream_warehouse_db_name = matches.group(1)

                query_to_parse = partial_query.sql(dialect=upstream_warehouse_platform)
        except Exception as e:
            logger.debug(f"sqlglot.parse failed on: {normalized_query}, error: {e}")
            query_to_parse = normalized_query

        parsed_query_object = create_lineage_sql_parsed_result(
            query=query_to_parse,
            default_db=upstream_warehouse_db_name,
            platform=upstream_warehouse_platform,
            platform_instance=(
                self.config.platform_instance_map.get(upstream_warehouse_platform)
                if upstream_warehouse_platform and self.config.platform_instance_map
                else None
            ),
            env=self.config.env,
            graph=self.ctx.graph,
        )

        self.report.num_sql_parsed += 1
        if parsed_query_object.debug_info.table_error:
            self.report.num_sql_parser_table_error += 1
            self.report.num_sql_parser_failures += 1
            logger.info(
                f"Failed to parse compiled code for report: {report_token} query: {query_token} {parsed_query_object.debug_info.error} the query was [{query_to_parse}]"
            )
        elif parsed_query_object.debug_info.column_error:
            self.report.num_sql_parser_column_error += 1
            self.report.num_sql_parser_failures += 1
            logger.info(
                f"Failed to generate CLL for report: {report_token} query: {query_token}: {parsed_query_object.debug_info.column_error} the query was [{query_to_parse}]"
            )
        else:
            self.report.num_sql_parser_success += 1

        schema_fields = infer_output_schema(parsed_query_object)
        if schema_fields:
            schema_metadata = SchemaMetadataClass(
                schemaName="mode_dataset" if is_mode_dataset else "mode_query",
                platform=f"urn:li:dataPlatform:{self.platform}",
                version=0,
                fields=schema_fields,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
            )
            if self.config.tag_measures_and_dimensions:
                self.set_field_tags(schema_fields)

            yield (
                MetadataChangeProposalWrapper(
                    entityUrn=query_urn,
                    aspect=schema_metadata,
                ).as_workunit()
            )

        yield from self.get_upstream_lineage_for_parsed_sql(
            query_urn, query_data, parsed_query_object
        )

        operation = OperationClass(
            operationType=OperationTypeClass.UPDATE,
            lastUpdatedTimestamp=int(
                dp.parse(query_data.get("updated_at", "now")).timestamp() * 1000
            ),
            timestampMillis=int(datetime.now(tz=timezone.utc).timestamp() * 1000),
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=query_urn,
            aspect=operation,
        ).as_workunit()

        creator = self._get_creator(
            query_data.get("_links", {}).get("creator", {}).get("href", "")
        )
        modified_actor = builder.make_user_urn(
            creator if creator is not None else "unknown"
        )

        created_ts = int(
            dp.parse(query_data.get("created_at", "now")).timestamp() * 1000
        )
        modified_ts = int(
            dp.parse(query_data.get("updated_at", "now")).timestamp() * 1000
        )

        query_instance_urn = self.get_query_instance_urn_from_query(query_data)
        value = query_data.get("raw_query")
        if value:
            query_properties = QueryPropertiesClass(
                statement=QueryStatementClass(
                    value=value,
                    language=QueryLanguageClass.SQL,
                ),
                source=QuerySourceClass.SYSTEM,
                created=AuditStamp(time=created_ts, actor=modified_actor),
                lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=query_instance_urn,
                aspect=query_properties,
            ).as_workunit()

    def get_upstream_lineage_for_parsed_sql(
        self, query_urn: str, query_data: dict, parsed_query_object: SqlParsingResult
    ) -> List[MetadataWorkUnit]:
        wu = []

        if parsed_query_object is None:
            logger.info(
                f"Failed to extract column level lineage from datasource {query_urn}"
            )
            return []
        if parsed_query_object.debug_info.error:
            logger.info(
                f"Failed to extract column level lineage from datasource {query_urn}: {parsed_query_object.debug_info.error}"
            )
            return []

        cll: List[ColumnLineageInfo] = (
            parsed_query_object.column_lineage
            if parsed_query_object.column_lineage is not None
            else []
        )

        fine_grained_lineages: List[FineGrainedLineageClass] = []

        table_urn = None

        for cll_info in cll:
            if table_urn is None:
                for column_ref in cll_info.upstreams:
                    table_urn = column_ref.table
                    break

            downstream = (
                [builder.make_schema_field_urn(query_urn, cll_info.downstream.column)]
                if cll_info.downstream is not None
                and cll_info.downstream.column is not None
                else []
            )
            upstreams = [
                builder.make_schema_field_urn(column_ref.table, column_ref.column)
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
                    query=self.get_query_instance_urn_from_query(query_data),
                )
                for input_table_urn in parsed_query_object.in_tables
            ],
            fineGrainedLineages=fine_grained_lineages,
        )

        wu.append(
            MetadataChangeProposalWrapper(
                entityUrn=query_urn,
                aspect=upstream_lineage,
            ).as_workunit()
        )

        return wu

    def get_formula_columns(
        self, node: Dict, columns: Optional[Set[str]] = None
    ) -> Set[str]:
        columns = columns if columns is not None else set()
        if isinstance(node, dict):
            for key, item in node.items():
                if isinstance(item, dict):
                    self.get_formula_columns(item, columns)
                elif isinstance(item, list):
                    for i in item:
                        if isinstance(i, dict):
                            self.get_formula_columns(i, columns)
                elif isinstance(item, str):
                    if key == "formula":
                        column_names = re.findall(r"\[(.+?)\]", item)
                        columns.update(column_names)
        return columns

    def get_input_fields(
        self,
        chart_urn: str,
        chart_data: Dict,
        chart_fields: Dict[str, SchemaFieldClass],
        query_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        # TODO: Identify which fields are used as X, Y, filters, etc and tag them accordingly.
        fields = self.get_formula_columns(chart_data)

        input_fields = []

        for field in fields:
            if field.lower() not in chart_fields:
                continue
            input_field = InputFieldClass(
                schemaFieldUrn=builder.make_schema_field_urn(query_urn, field.lower()),
                schemaField=chart_fields[field.lower()],
            )
            input_fields.append(input_field)

        if not input_fields:
            return

        inputFields = InputFieldsClass(fields=input_fields)

        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=inputFields,
        ).as_workunit()

    def construct_chart_from_api_data(
        self,
        index: int,
        chart_data: dict,
        chart_fields: Dict[str, SchemaFieldClass],
        query: dict,
        space_token: str,
        report_info: dict,
        query_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        # logger.debug(f"Processing chart {chart_data.get('token', '')}: {chart_data}")
        chart_urn = builder.make_chart_urn(self.platform, chart_data.get("token", ""))
        chart_snapshot = ChartSnapshot(
            urn=chart_urn,
            aspects=[],
        )

        last_modified = ChangeAuditStamps()
        creator = self._get_creator(
            chart_data.get("_links", {}).get("creator", {}).get("href", "")
        )
        if creator is not None:
            modified_actor = builder.make_user_urn(creator)
            created_ts = int(
                dp.parse(chart_data.get("created_at", "now")).timestamp() * 1000
            )
            modified_ts = int(
                dp.parse(chart_data.get("updated_at", "now")).timestamp() * 1000
            )
            last_modified = ChangeAuditStamps(
                created=AuditStamp(time=created_ts, actor=modified_actor),
                lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
            )

        # Last refreshed ts.
        last_refreshed_ts = self._parse_last_run_at(report_info)

        chart_detail = (
            chart_data.get("view", {})
            if len(chart_data.get("view", {})) != 0
            else chart_data.get("view_vegas", {})
        )

        mode_chart_type = chart_detail.get("chartType", "") or chart_detail.get(
            "selectedChart", ""
        )
        chart_type = self._get_chart_type(chart_data.get("token", ""), mode_chart_type)
        description = (
            chart_detail.get("description")
            or chart_detail.get("chartDescription")
            or ""
        )

        title = (
            chart_detail.get("title")
            or chart_detail.get("chartTitle")
            or f"Chart {index}"
        )

        # create datasource urn
        custom_properties = self.construct_chart_custom_properties(
            chart_detail, mode_chart_type
        )

        query_urn = self.get_dataset_urn_from_query(query)

        # Chart Info
        chart_info = ChartInfoClass(
            type=chart_type,
            description=description,
            title=title,
            lastModified=last_modified,
            lastRefreshed=last_refreshed_ts,
            # The links href starts with a slash already.
            chartUrl=f"{self.config.connect_uri}{chart_data.get('_links', {}).get('report_viz_web', {}).get('href', '')}",
            inputs=[query_urn],
            customProperties=custom_properties,
            inputEdges=[],
        )
        chart_snapshot.aspects.append(chart_info)

        query_urn = self.get_dataset_urn_from_query(query)
        yield from self.get_input_fields(chart_urn, chart_data, chart_fields, query_urn)

        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=SubTypesClass(typeNames=[BIAssetSubTypes.MODE_CHART]),
        ).as_workunit()

        # Browse Path
        space_name = self.space_tokens[space_token]
        report_name = report_info["name"]
        path = f"/mode/{self.config.workspace}/{space_name}/{report_name}/{query_name}/{title}"
        browse_path = BrowsePathsClass(paths=[path])
        chart_snapshot.aspects.append(browse_path)

        # Browse path v2
        browse_path_v2 = BrowsePathsV2Class(
            path=self._browse_path_chart(space_token, report_info, query),
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=browse_path_v2,
        ).as_workunit()

        # Query
        chart_query = ChartQueryClass(
            rawQuery=query.get("raw_query", ""),
            type=ChartQueryTypeClass.SQL,
        )
        chart_snapshot.aspects.append(chart_query)

        # Ownership
        ownership = self._get_ownership(
            self._get_creator(
                chart_data.get("_links", {}).get("creator", {}).get("href", "")
            )
        )
        if ownership is not None:
            chart_snapshot.aspects.append(ownership)

        mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)
        yield MetadataWorkUnit(id=chart_snapshot.urn, mce=mce)

    def _get_reports(self, space_token: str) -> Iterator[List[dict]]:
        try:
            with self.report.report_get_timer:
                for reports_page in self._get_paged_request_json(
                    f"{self.workspace_uri}/spaces/{space_token}/reports?filter=all",
                    "reports",
                    self.config.items_per_page,
                ):
                    self.report.report_get_api_called += 1
                    logger.debug(
                        f"Read {len(reports_page)} reports records from workspace {self.workspace_uri} space {space_token}"
                    )
                    yield reports_page
        except ModeRequestError as e:
            if isinstance(e, HTTPError) and e.response.status_code == 404:
                self.report.report_warning(
                    title="No Reports Found in Space",
                    message="No reports were found in the space. It may have been recently deleted.",
                    context=f"Space Token: {space_token}, Error: {str(e)}",
                )
            else:
                self.report.report_failure(
                    title="Failed to Retrieve Reports for Space",
                    message="Unable to retrieve reports for space token.",
                    context=f"Space Token: {space_token}, Error: {str(e)}",
                )

    def _get_datasets(self, space_token: str) -> Iterator[List[dict]]:
        """
        Retrieves datasets for a given space token.
        """
        try:
            with self.report.dataset_get_timer:
                for dataset_page in self._get_paged_request_json(
                    f"{self.workspace_uri}/spaces/{space_token}/datasets?filter=all",
                    "reports",
                    self.config.items_per_page,
                ):
                    self.report.dataset_get_api_called += 1
                    logger.debug(
                        f"Read {len(dataset_page)} datasets records from workspace {self.workspace_uri} space {space_token}"
                    )
                    yield dataset_page
        except ModeRequestError as e:
            if isinstance(e, HTTPError) and e.response.status_code == 404:
                self.report.report_warning(
                    title="No Datasets Found in Space",
                    message="No datasets were found in the space. It may have been recently deleted.",
                    context=f"Space Token: {space_token}, Error: {str(e)}",
                )
            else:
                self.report.report_failure(
                    title="Failed to Retrieve Datasets for Space",
                    message=f"Unable to retrieve datasets for space token {space_token}.",
                    context=f"Space Token: {space_token}, Error: {str(e)}",
                )

    def _get_queries(self, report_token: str) -> List[dict]:
        try:
            with self.report.query_get_timer:
                # This endpoint does not handle pagination properly
                queries = self._get_request_json(
                    f"{self.workspace_uri}/reports/{report_token}/queries"
                )
                self.report.query_get_api_called += 1
                logger.debug(
                    f"Read {len(queries)} queries records from workspace {self.workspace_uri} report {report_token}"
                )
                return queries.get("_embedded", {}).get("queries", [])
        except ModeRequestError as e:
            if isinstance(e, HTTPError) and e.response.status_code == 404:
                self.report.report_warning(
                    title="No Queries Found",
                    message="No queries found for the report token. Maybe the report is deleted...",
                    context=f"Report Token: {report_token}, Error: {str(e)}",
                )
            else:
                self.report.report_failure(
                    title="Failed to Retrieve Queries",
                    message="Unable to retrieve queries for report token.",
                    context=f"Report Token: {report_token}, Error: {str(e)}",
                )
            return []

    @lru_cache(maxsize=None)
    def _get_last_query_run(self, report_token: str, report_run_id: str) -> list:
        # This function is unused and may be subject to removal in a future revision of this source
        query_runs = []
        try:
            for query_run_page in self._get_paged_request_json(
                f"{self.workspace_uri}/reports/{report_token}/runs/{report_run_id}/query_runs?filter=all",
                "query_runs",
                self.config.items_per_page,
            ):
                query_runs.extend(query_run_page)
        except ModeRequestError as e:
            self.report.report_failure(
                title="Failed to Retrieve Queries for Report",
                message="Unable to retrieve queries for report token.",
                context=f"Report Token:{report_token}, Error: {str(e)}",
            )
        return query_runs

    def _get_charts(self, report_token: str, query_token: str) -> List[dict]:
        try:
            with self.report.chart_get_timer:
                # This endpoint does not handle pagination properly
                charts = self._get_request_json(
                    f"{self.workspace_uri}/reports/{report_token}/queries/{query_token}/charts"
                )
                self.report.chart_get_api_called += 1
                logger.debug(
                    f"Read {len(charts)} charts records from workspace {self.workspace_uri} report {report_token} query {query_token}"
                )
                return charts.get("_embedded", {}).get("charts", [])
        except ModeRequestError as e:
            if isinstance(e, HTTPError) and e.response.status_code == 404:
                self.report.report_warning(
                    title="No Charts Found for Query",
                    message="No charts were found for the query. The query may have been recently deleted.",
                    context=f"Report Token: {report_token}, Query Token: {query_token}, Error: {str(e)}",
                )
            else:
                self.report.report_failure(
                    title="Failed to Retrieve Charts",
                    message="Unable to retrieve charts from Mode.",
                    context=f"Report Token: {report_token}, Query Token: {query_token}, Error: {str(e)}",
                )
            return []

    def _get_paged_request_json(
        self, url: str, key: str, per_page: int
    ) -> Iterator[List[Dict]]:
        page: int = 1
        while True:
            page_url = f"{url}&per_page={per_page}&page={page}"
            response = self._get_request_json(page_url)
            data: List[Dict] = response.get("_embedded", {}).get(key, [])
            if not data:
                break
            yield data
            page += 1

    @lru_cache(maxsize=None)
    def _get_request_json(self, url: str) -> Dict:
        r = tenacity.Retrying(
            wait=wait_exponential(
                multiplier=self.config.api_options.retry_backoff_multiplier,
                max=self.config.api_options.max_retry_interval,
            ),
            retry=retry_if_exception_type(
                (HTTPError429, HTTPError504, ConnectionError)
            ),
            stop=stop_after_attempt(self.config.api_options.max_attempts),
        )

        @r.wraps
        def get_request():
            curl_command = make_curl_command(self.session, "GET", url, "")
            logger.debug(f"Issuing request; curl equivalent: {curl_command}")

            try:
                response = self.session.get(
                    url, timeout=self.config.api_options.timeout
                )
                if response.status_code == 204:  # No content, don't parse json
                    return {}

                response.raise_for_status()
                return response.json()
            except HTTPError as http_error:
                error_response = http_error.response
                if error_response.status_code == 429:
                    self.report.num_requests_exceeding_rate_limit += 1
                    # respect Retry-After
                    sleep_time = error_response.headers.get("retry-after")
                    if sleep_time is not None:
                        time.sleep(float(sleep_time))
                    raise HTTPError429 from None
                elif error_response.status_code == 504:
                    self.report.num_requests_retried_on_timeout += 1
                    time.sleep(0.1)
                    raise HTTPError504 from None

                logger.debug(
                    f"Error response ({error_response.status_code}): {error_response.text}"
                )
                raise http_error

        return get_request()

    @staticmethod
    def _get_process_memory():
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        return {
            "rss": mem_info.rss / (1024 * 1024),
            "vms": mem_info.vms / (1024 * 1024),
            "shared": getattr(mem_info, "shared", 0) / (1024 * 1024),
            "data": getattr(mem_info, "data", 0) / (1024 * 1024),
        }

    @staticmethod
    def create_embed_aspect_mcp(
        entity_urn: str, embed_url: str
    ) -> MetadataChangeProposalWrapper:
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=EmbedClass(renderUrl=embed_url),
        )

    def gen_space_key(self, space_token: str) -> SpaceKey:
        return SpaceKey(platform=self.platform, space_token=space_token)

    def construct_space_container(
        self, space_token: str, space_name: str
    ) -> Iterable[MetadataWorkUnit]:
        key = self.gen_space_key(space_token)
        yield from gen_containers(
            container_key=key,
            name=space_name,
            sub_types=[BIContainerSubTypes.MODE_COLLECTION],
            # TODO: Support extracting the documentation for a space.
        )

        # We have a somewhat atypical browse path here, since we include the workspace name
        # as what's effectively but not officially a platform instance.
        yield MetadataChangeProposalWrapper(
            entityUrn=key.as_urn(),
            aspect=BrowsePathsV2Class(path=self._browse_path_space()),
        ).as_workunit()

    def emit_dashboard_mces(self) -> Iterable[MetadataWorkUnit]:
        for space_token, space_name in self.space_tokens.items():
            yield from self.construct_space_container(space_token, space_name)
            space_container_key = self.gen_space_key(space_token)

            for report_page in self._get_reports(space_token):
                for report in report_page:
                    logger.debug(
                        f"Report: name: {report.get('name')} token: {report.get('token')}"
                    )
                    dashboard_tuple_from_report = self.construct_dashboard(
                        space_token=space_token, report_info=report
                    )

                    if dashboard_tuple_from_report is None:
                        continue
                    (
                        dashboard_snapshot_from_report,
                        browse_mcpw,
                    ) = dashboard_tuple_from_report

                    mce = MetadataChangeEvent(
                        proposedSnapshot=dashboard_snapshot_from_report
                    )

                    mcpw = MetadataChangeProposalWrapper(
                        entityUrn=dashboard_snapshot_from_report.urn,
                        aspect=SubTypesClass(typeNames=[BIAssetSubTypes.MODE_REPORT]),
                    )
                    yield mcpw.as_workunit()
                    yield from add_dataset_to_container(
                        container_key=space_container_key,
                        dataset_urn=dashboard_snapshot_from_report.urn,
                    )
                    yield browse_mcpw.as_workunit()

                    usage_statistics = DashboardUsageStatisticsClass(
                        timestampMillis=round(datetime.now().timestamp() * 1000),
                        viewsCount=report.get("view_count", 0),
                    )

                    yield MetadataChangeProposalWrapper(
                        entityUrn=dashboard_snapshot_from_report.urn,
                        aspect=usage_statistics,
                    ).as_workunit()

                    if self.config.ingest_embed_url is True:
                        yield self.create_embed_aspect_mcp(
                            entity_urn=dashboard_snapshot_from_report.urn,
                            embed_url=f"{self.config.connect_uri}/{self.config.workspace}/reports/{report.get('token')}/embed",
                        ).as_workunit()

                    yield MetadataWorkUnit(
                        id=dashboard_snapshot_from_report.urn, mce=mce
                    )

    def emit_chart_mces(self) -> Iterable[MetadataWorkUnit]:
        # Space/collection -> report -> query -> Chart
        for space_token in self.space_tokens:
            for report_page in self._get_reports(space_token):
                for report in report_page:
                    report_token = report.get("token", "")

                    queries = self._get_queries(report_token)
                    for query in queries:
                        query_mcps = self.construct_query_or_dataset(
                            report_token,
                            query,
                            space_token=space_token,
                            report_info=report,
                            is_mode_dataset=False,
                        )
                        chart_fields: Dict[str, SchemaFieldClass] = {}
                        for wu in query_mcps:
                            if isinstance(
                                wu.metadata, MetadataChangeProposalWrapper
                            ) and isinstance(wu.metadata.aspect, SchemaMetadataClass):
                                schema_metadata = wu.metadata.aspect
                                for field in schema_metadata.fields:
                                    chart_fields.setdefault(field.fieldPath, field)

                            yield wu

                        charts = self._get_charts(report_token, query.get("token", ""))
                        # build charts
                        for i, chart in enumerate(charts):
                            yield from self.construct_chart_from_api_data(
                                i,
                                chart,
                                chart_fields,
                                query,
                                space_token=space_token,
                                report_info=report,
                                query_name=query["name"],
                            )

    def emit_dataset_mces(self):
        """
        Emits MetadataChangeEvents (MCEs) for datasets within each space.
        """
        for space_token, _ in self.space_tokens.items():
            for dataset_page in self._get_datasets(space_token):
                for report in dataset_page:
                    report_token = report.get("token", "")
                    queries = self._get_queries(report_token)
                    for query in queries:
                        query_mcps = self.construct_query_or_dataset(
                            report_token,
                            query,
                            space_token=space_token,
                            report_info=report,
                            is_mode_dataset=True,
                        )
                        for wu in query_mcps:
                            yield wu

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "ModeSource":
        config: ModeConfig = ModeConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_dashboard_mces()
        yield from self.emit_dataset_mces()
        yield from self.emit_chart_mces()
        cache_info = self._get_request_json.cache_info()
        self.report.get_cache_hits = cache_info.hits
        self.report.get_cache_misses = cache_info.misses
        self.report.get_cache_size = cache_info.currsize
        memory_used = self._get_process_memory()
        self.report.process_memory_used_mb = round(memory_used["rss"], 2)

    def get_report(self) -> SourceReport:
        return self.report
