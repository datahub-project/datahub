import json
import logging
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Type, Union

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
    make_data_platform_urn,
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
    make_schema_field_urn,
    make_term_urn,
    make_user_urn,
)
from datahub.emitter.mcp_builder import (
    MetadataChangeProposalWrapper,
    add_domain_to_entity_wu,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter
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
    TimeStamp,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot,
    DashboardSnapshot,
    DatasetSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayType,
    BooleanType,
    BytesType,
    MySqlDDL,
    NullType,
    NumberType,
    RecordType,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringType,
    TimeType,
)

# Imports for metadata model classes
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
    GlossaryNodeInfoClass,
    GlossaryTermAssociationClass,
    GlossaryTermInfoClass,
    GlossaryTermsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaMetadataClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    SqlParsingResult,
    create_lineage_sql_parsed_result,
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

SUPERSET_FIELD_TYPE_MAPPINGS: Dict[
    str,
    Type[
        Union[
            ArrayType,
            BytesType,
            BooleanType,
            NumberType,
            RecordType,
            StringType,
            TimeType,
            NullType,
        ]
    ],
] = {
    "BYTES": BytesType,
    "BOOL": BooleanType,
    "BOOLEAN": BooleanType,
    "DOUBLE": NumberType,
    "DOUBLE PRECISION": NumberType,
    "DECIMAL": NumberType,
    "NUMERIC": NumberType,
    "BIGNUMERIC": NumberType,
    "BIGDECIMAL": NumberType,
    "FLOAT64": NumberType,
    "INT": NumberType,
    "INT64": NumberType,
    "SMALLINT": NumberType,
    "INTEGER": NumberType,
    "BIGINT": NumberType,
    "TINYINT": NumberType,
    "BYTEINT": NumberType,
    "STRING": StringType,
    "TIME": TimeType,
    "TIMESTAMP": TimeType,
    "DATE": TimeType,
    "DATETIME": TimeType,
    "GEOGRAPHY": NullType,
    "JSON": NullType,
    "INTERVAL": NullType,
    "ARRAY": ArrayType,
    "STRUCT": RecordType,
    "CHARACTER VARYING": StringType,
    "CHARACTER": StringType,
    "CHAR": StringType,
    "TIMESTAMP WITHOUT TIME ZONE": TimeType,
    "REAL": NumberType,
    "VARCHAR": StringType,
    "TIMESTAMPTZ": TimeType,
    "GEOMETRY": NullType,
    "HLLSKETCH": NullType,
    "TIMETZ": TimeType,
    "VARBYTE": StringType,
}

platform_without_databases = ["druid"]


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
    api_key: Optional[str] = Field(default=None, description="Preset.io API key.")
    api_secret: Optional[str] = Field(default=None, description="Preset.io API secret.")
    manager_uri: str = Field(
        default="https://api.app.preset.io/", description="Preset.io API URL"
    )
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


def get_metric_name(metric: dict) -> str:
    if not metric:
        return ""
    if isinstance(metric, str):
        return metric
    label = metric.get("label")
    if not label:
        return ""
    return label


def get_filter_name(filter_obj: dict) -> str:
    sql_expression = filter_obj.get("sqlExpression")
    if sql_expression:
        return sql_expression

    clause = filter_obj.get("clause")
    column = filter_obj.get("subject")
    operator = filter_obj.get("operator")
    comparator = filter_obj.get("comparator")
    return f"{clause} {column} {operator} {comparator}"


def format_to_full_name(change_by_data: dict) -> str:
    return f"{change_by_data.get('first_name')} {change_by_data.get('last_name')}"


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
        self.sink_config = None
        if (
            ctx.pipeline_config
            and ctx.pipeline_config.sink
            and ctx.pipeline_config.sink.config
        ):
            self.sink_config = ctx.pipeline_config.sink.config
            self.rest_emitter = DatahubRestEmitter(
                gms_server=self.sink_config.get("server", ""),
                token=self.sink_config.get("token", ""),
            )
        self.session = self.login()
        self.full_owners_dict = self.build_preset_owner_dict()

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

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = SupersetConfig.parse_obj(config_dict)
        return cls(ctx, config)

    @lru_cache(maxsize=None)
    def get_platform_from_database_id(self, database_id: int) -> str:
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
            f"{self.config.connect_uri}/api/v1/dataset/{dataset_id}?include_rendered_sql=true",
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
        database_name = (
            dataset_response.get("result", {}).get("database", {}).get("database_name")
        )
        database_name = self.config.database_alias.get(database_name, database_name)

        if database_id and table_name:
            return make_dataset_urn(
                platform=platform_instance,
                name=".".join(
                    name for name in [database_name, schema_name, table_name] if name
                ),
                env=self.config.env,
            )
        raise ValueError("Could not construct dataset URN")

    def parse_owner_payload(self, payload, owners_dict):
        for owner_data in payload.get("result", []):
            email = owner_data.get("extra", {}).get("email")
            owner_id = owner_data.get("value")

            if owner_id and email:
                owners_dict[owner_id] = email
        return owners_dict

    def build_preset_owner_dict(self) -> dict:
        owners_dict: dict = {}
        dataset_payload = self.get_all_dataset_owners()
        chart_payload = self.get_all_chart_owners()
        dashboard_payload = self.get_all_dashboard_owners()

        owners_dict = self.parse_owner_payload(dataset_payload, owners_dict)
        owners_dict = self.parse_owner_payload(chart_payload, owners_dict)
        owners_dict = self.parse_owner_payload(dashboard_payload, owners_dict)
        return owners_dict

    def build_owners_urn_list(self, data):
        owners_urn_list = []
        for owner in data.get("owners", []):
            owner_id = owner.get("id")
            owner_email = self.full_owners_dict.get(owner_id)
            if owner_email:
                owners_urn = make_user_urn(owner_email)
                owners_urn_list.append(owners_urn)
        return owners_urn_list

    def get_all_dataset_owners(self) -> dict:
        current_dataset_page = 1
        total_dataset_owners = PAGE_SIZE
        all_dataset_owners = []

        while (current_dataset_page - 1) * PAGE_SIZE <= total_dataset_owners:
            full_owners_response = self.session.get(
                f"{self.config.connect_uri}/api/v1/dataset/related/owners",
                params=f"q=(page:{current_dataset_page},page_size:{PAGE_SIZE})",
            )
            if full_owners_response.status_code != 200:
                logger.warning(
                    f"Failed to get dataset data: {full_owners_response.text}"
                )
            full_owners_response.raise_for_status()

            payload = full_owners_response.json()
            total_dataset_owners = payload.get("count", total_dataset_owners)
            all_dataset_owners.extend(payload.get("result", []))
            current_dataset_page += 1

        # return combined payload
        return {"result": all_dataset_owners, "count": total_dataset_owners}

    def get_all_chart_owners(self) -> dict:
        current_chart_page = 1
        total_chart_owners = PAGE_SIZE
        all_chart_owners = []

        while (current_chart_page - 1) * PAGE_SIZE <= total_chart_owners:
            full_owners_response = self.session.get(
                f"{self.config.connect_uri}/api/v1/chart/related/owners",
                params=f"q=(page:{current_chart_page},page_size:{PAGE_SIZE})",
            )
            if full_owners_response.status_code != 200:
                logger.warning(f"Failed to get chart data: {full_owners_response.text}")
            full_owners_response.raise_for_status()

            payload = full_owners_response.json()
            total_chart_owners = payload.get("count", total_chart_owners)
            all_chart_owners.extend(payload.get("result", []))
            current_chart_page += 1

        return {"result": all_chart_owners, "count": total_chart_owners}

    def get_all_dashboard_owners(self) -> dict:
        current_dashboard_page = 1
        total_dashboard_owners = PAGE_SIZE
        all_dashboard_owners = []

        while (current_dashboard_page - 1) * PAGE_SIZE <= total_dashboard_owners:
            full_owners_response = self.session.get(
                f"{self.config.connect_uri}/api/v1/dashboard/related/owners",
                params=f"q=(page:{current_dashboard_page},page_size:{PAGE_SIZE})",
            )
            if full_owners_response.status_code != 200:
                logger.warning(
                    f"Failed to get dashboard data: {full_owners_response.text}"
                )
            full_owners_response.raise_for_status()

            payload = full_owners_response.json()
            total_dashboard_owners = payload.get("count", total_dashboard_owners)
            all_dashboard_owners.extend(payload.get("result", []))
            current_dashboard_page += 1

        return {"result": all_dashboard_owners, "count": total_dashboard_owners}

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
            custom_properties["CertifiedBy"] = dashboard_data.get(
                "certified_by", "None"
            )
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

        dashboard_owners_list = self.build_owners_urn_list(dashboard_data)
        owners_info = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=urn,
                    # default as Technical Owners from Preset
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
                for urn in (dashboard_owners_list or [])
            ],
        )
        dashboard_snapshot.aspects.append(owners_info)

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

        chart_owners_list = self.build_owners_urn_list(chart_data)
        owners_info = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=urn,
                    # default as Technical Owners from Preset
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
                for urn in (chart_owners_list or [])
            ],
        )
        chart_snapshot.aspects.append(owners_info)

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
                try:
                    chart_snapshot = self.construct_chart_from_chart_data(chart_data)

                    mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)
                    yield MetadataWorkUnit(id=chart_snapshot.urn, mce=mce)
                    yield from self._get_domain_wu(
                        title=chart_data.get("slice_name", ""),
                        entity_urn=chart_snapshot.urn,
                    )
                except Exception as e:
                    logger.info(f"Error: {e}")

    def gen_schema_fields(self, column_data: List[Dict[str, str]]) -> List[SchemaField]:
        schema_fields: List[SchemaField] = []
        for col in column_data:
            data_type = SUPERSET_FIELD_TYPE_MAPPINGS.get(col.get("type", ""))
            field = SchemaField(
                fieldPath=col.get("column_name", ""),
                type=SchemaFieldDataType(data_type() if data_type else NullType()),
                nativeDataType="",
                description=col.get("column_name", ""),
                nullable=True,
            )
            schema_fields.append(field)
        return schema_fields

    def gen_schema_metadata(
        self,
        dataset_response: dict,
    ) -> SchemaMetadataClass:
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

    def create_glossary_terms(
        self, dataset_response: dict
    ) -> List[GlossaryTermAssociationClass]:
        metrics = dataset_response.get("result", {}).get("metrics", [])
        glossary_term_urns = []
        for metric in metrics:
            if dataset_response.get("result", {}).get(
                "extra", {}
            ) and "certification" in dataset_response.get("result", {}).get(
                "extra", {}
            ):
                expression = metric.get("expression", "")
                certification_details = metric.get("extra", "")
                metric_name = metric.get("metric_name", "")
                schema_name = dataset_response.get("result", {}).get(
                    "schema", "Default"
                )
                description = metric.get("description", "")

                term_urn = make_term_urn(metric_name)
                # Count is the default metric on all datasets
                if metric_name == "count":
                    continue
                # Testing for adding glossary Nodes (Groups)=====
                node_term_urn = f"urn:li:glossaryNode:{schema_name}"
                term_node_properties_aspect = GlossaryNodeInfoClass(
                    name=schema_name,
                    definition=f"Contains metrics associated with {schema_name}.",
                )

                update_node_event: MetadataChangeProposalWrapper = (
                    MetadataChangeProposalWrapper(
                        entityUrn=node_term_urn,
                        aspect=term_node_properties_aspect,
                    )
                )

                self.rest_emitter.emit(update_node_event)
                logger.info(f"Created Glossary node created {node_term_urn}")
                # Testing for adding glossary Nodes (Groups) ===

                term_properties_aspect = GlossaryTermInfoClass(
                    name=metric_name,
                    definition=f"Description: {description} \n\nSql Expression: {expression} \n\nCertification details: {certification_details}",
                    termSource="",
                    parentNode=node_term_urn,
                )

                update_term_event: MetadataChangeProposalWrapper = (
                    MetadataChangeProposalWrapper(
                        entityUrn=term_urn,
                        aspect=term_properties_aspect,
                    )
                )

                self.rest_emitter.emit(update_term_event)
                logger.info(f"Created Glossary term {term_urn}")
                glossary_term_urns.append(GlossaryTermAssociationClass(urn=term_urn))
        return glossary_term_urns

    def generate_virtual_dataset_lineage(
        self, parsed_query_object: SqlParsingResult, datasource_urn: str
    ) -> UpstreamLineageClass:
        cll: List[ColumnLineageInfo] = (
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

    # Ingestion for Preset Dataset
    def construct_dataset_from_dataset_data(
        self, dataset_data: dict
    ) -> DatasetSnapshot:
        dataset_response = self.get_dataset_info(dataset_data.get("id"))
        datasource_urn = self.get_datasource_urn_from_id(
            dataset_response, self.platform
        )
        # Check API format for dataset
        modified_actor = f"urn:li:corpuser:{(dataset_data.get('changed_by') or {}).get('username', 'unknown')}"
        modified_ts = int(
            dp.parse(dataset_data.get("changed_on_utc", "now")).timestamp() * 1000
        )
        table_name = dataset_data.get("table_name", "")
        database_id = dataset_response.get("result", {}).get("database", {}).get("id")
        upstream_warehouse_db_name = (
            dataset_response.get("result", {}).get("database", {}).get("database_name")
        )
        upstream_warehouse_platform = self.get_platform_from_database_id(database_id)
        # We prefer rendered SQL over SQL if the dataset contains jinja templating
        sql = dataset_response.get("result", {}).get(
            "rendered_sql"
        ) or dataset_response.get("result", {}).get("sql")
        last_modified = AuditStampClass(time=modified_ts, actor=modified_actor)
        dataset_url = f"{self.config.display_uri}{dataset_data.get('explore_url', '')}"
        metrics = [
            metric.get("metric_name")
            for metric in (dataset_response.get("result", {}).get("metrics", []))
        ]

        owners = [
            owner.get("first_name") + "_" + str(owner.get("id"))
            for owner in (dataset_response.get("result", {}).get("owners", []))
        ]
        custom_properties = {
            "Metrics": ", ".join(metrics),
            "Owners": ", ".join(owners),
        }

        dataset_info = DatasetPropertiesClass(
            name=table_name,
            description="",
            lastModified=TimeStamp(time=modified_ts),
            externalUrl=dataset_url,
            customProperties=custom_properties,
        )
        aspects_items: List[Any] = []

        # Create Glossary Terms for certified datasets
        glossary_term_urns = self.create_glossary_terms(dataset_response)
        if glossary_term_urns:
            glossary_terms = GlossaryTermsClass(
                terms=glossary_term_urns, auditStamp=last_modified
            )
            aspects_items.append(glossary_terms)
        # Create Lineage:
        db_platform_instance = self.get_platform_from_database_id(database_id)

        parsed_query_object = create_lineage_sql_parsed_result(
            query=sql,
            default_db=upstream_warehouse_db_name,
            platform=upstream_warehouse_platform,
            platform_instance=None,
            env=self.config.env,
        )
        if sql:
            # To Account for virtual datasets
            tag_urn = f"urn:li:tag:{self.platform}:virtual"
            upstream_lineage = self.generate_virtual_dataset_lineage(
                parsed_query_object, datasource_urn
            )
        else:
            # To Account for physical datasets
            tag_urn = f"urn:li:tag:{self.platform}:physical"
            upstream_dataset = self.get_datasource_urn_from_id(
                dataset_response, db_platform_instance
            )
            upstream_lineage = UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        type=DatasetLineageTypeClass.TRANSFORMED,
                        dataset=upstream_dataset,
                    )
                ]
            )

        global_tags = GlobalTagsClass(tags=[TagAssociationClass(tag=tag_urn)])

        aspects_items.extend(
            [
                self.gen_schema_metadata(dataset_response),
                dataset_info,
                upstream_lineage,
                global_tags,
            ]
        )
        dataset_owners_list = self.build_owners_urn_list(dataset_data)
        owners_info = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=urn,
                    # default as Technical Owners from Preset
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
                for urn in (dataset_owners_list or [])
            ],
        )

        aspects_items.extend(
            [
                self.gen_schema_metadata(dataset_response),
                dataset_info,
                upstream_lineage,
                global_tags,
                owners_info,
            ]
        )

        dataset_snapshot = DatasetSnapshot(
            urn=datasource_urn,
            aspects=aspects_items,
        )
        return dataset_snapshot

    def emit_dataset_mces(self) -> Iterable[MetadataWorkUnit]:
        current_dataset_page = 0
        total_datasets = PAGE_SIZE
        while current_dataset_page * PAGE_SIZE <= total_datasets:
            full_dataset_response = self.session.get(
                f"{self.config.connect_uri}/api/v1/dataset/",
                params=f"q=(page:{current_dataset_page},page_size:{PAGE_SIZE})",
            )
            if full_dataset_response.status_code != 200:
                logger.warning(
                    f"Failed to get dataset data: {full_dataset_response.text}"
                )
            full_dataset_response.raise_for_status()

            current_dataset_page += 1

            payload = full_dataset_response.json()
            total_datasets = payload["count"]

            for dataset_data in payload["result"]:
                dataset_snapshot = self.construct_dataset_from_dataset_data(
                    dataset_data
                )
                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                yield MetadataWorkUnit(id=dataset_snapshot.urn, mce=mce)
                yield from self._get_domain_wu(
                    title=dataset_data.get("table_name", ""),
                    entity_urn=dataset_snapshot.urn,
                )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_dashboard_mces()
        yield from self.emit_chart_mces()
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
