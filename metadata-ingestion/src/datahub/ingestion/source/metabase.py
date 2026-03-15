import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Pattern, Tuple, Type, Union

import dateutil.parser as dp
import requests
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)
from requests.models import HTTPError

import datahub.emitter.mce_builder as builder
from datahub.configuration.source_common import (
    DatasetLineageProviderConfigBase,
    LowerCaseDatasetUrnConfigMixin,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BooleanTypeClass,
    BytesTypeClass,
    ChangeAuditStampsClass,
    ChartInfoClass,
    ChartQueryClass,
    ChartQueryTypeClass,
    ChartTypeClass,
    DashboardInfoClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    EdgeClass,
    EnumTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    MySqlDDLClass,
    NullTypeClass,
    NumberTypeClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result
from datahub.utilities import config_clean

logger = logging.getLogger(__name__)

_CARD_TYPE_MODEL = "model"

_QUERY_TYPE_NATIVE = "native"
_QUERY_TYPE_QUERY = "query"

_MBQL_REF_FIELD = "field"
_MBQL_REF_EXPRESSION = "expression"
_MBQL_REF_AGGREGATION = "aggregation"

# Source-table value prefix when referencing another card
_CARD_REF_PREFIX = "card__"

_SPECIAL_CHARS_PATTERN: Pattern[str] = re.compile(r"[^a-zA-Z0-9_]")
_MULTIPLE_UNDERSCORES_PATTERN: Pattern[str] = re.compile(r"_+")

# JDBC connection-string constants used when sanitizing the `db` detail field
_JDBC_URI_SCHEMES = ("file:", "mem:")
_JDBC_DB_EXTENSION = ".db"


def _extract_field_ids_from_mbql(clause: object) -> List[int]:
    """
    Recursively extract Metabase field IDs from an MBQL clause.
    MBQL field refs look like: ["field", 100, null] or ["field", 20, {"join-alias": "x"}]
    """
    field_ids: List[int] = []
    if not isinstance(clause, list) or not clause:
        return field_ids
    if clause[0] == _MBQL_REF_FIELD and len(clause) >= 2 and isinstance(clause[1], int):
        field_ids.append(clause[1])
    else:
        for item in clause:
            if isinstance(item, list):
                field_ids.extend(_extract_field_ids_from_mbql(item))
    return field_ids


class MetabaseBaseModel(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class MetabaseUser(MetabaseBaseModel):
    id: int
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    common_name: Optional[str] = None


class MetabaseResultMetadata(MetabaseBaseModel):
    name: Optional[str] = None
    display_name: Optional[str] = None
    base_type: Optional[str] = None
    semantic_type: Optional[str] = None
    field_ref: Optional[List] = None  # Heterogeneous list: ["field", "name", {...}]


class MetabaseNativeQuery(MetabaseBaseModel):
    query: Optional[str] = None
    template_tags: Optional[Dict] = Field(
        None, alias="template-tags"
    )  # Metabase template tag definitions


class MetabaseField(MetabaseBaseModel):
    id: int
    name: str
    display_name: Optional[str] = None
    table_id: Optional[int] = None
    base_type: Optional[str] = None


class MetabaseJoin(MetabaseBaseModel):
    source_table: Optional[Union[int, str]] = Field(None, alias="source-table")
    alias: Optional[str] = None
    condition: Optional[List[object]] = None


class MetabaseQuery(MetabaseBaseModel):
    source_table: Optional[Union[int, str]] = Field(None, alias="source-table")
    # MBQL clauses are heterogeneous arrays; List[object] forces isinstance narrowing at call sites.
    filter: Optional[List[object]] = None
    # aggregation may be a list-of-clauses or a bare single clause in older MBQL
    aggregation: Optional[List[object]] = None
    breakout: Optional[List[object]] = None
    fields: Optional[List[object]] = None
    joins: Optional[List[MetabaseJoin]] = None
    expressions: Optional[Dict[str, object]] = None

    @property
    def source_table_refs(self) -> List[Union[int, str]]:
        refs: List[Union[int, str]] = []
        if self.source_table is not None:
            refs.append(self.source_table)
        for join in self.joins or []:
            if join.source_table is not None:
                refs.append(join.source_table)
        return refs

    def collect_field_ids(self) -> List[int]:
        field_ids: List[int] = []
        for clause_list in [
            self.fields or [],
            self.breakout or [],
            self.aggregation or [],
        ]:
            for clause in clause_list:
                if isinstance(clause, list):
                    field_ids.extend(_extract_field_ids_from_mbql(clause))
        if self.expressions:
            for expr_clause in self.expressions.values():
                if isinstance(expr_clause, list):
                    field_ids.extend(_extract_field_ids_from_mbql(expr_clause))
        return field_ids


class MetabaseDatasetQuery(MetabaseBaseModel):
    type: str
    database: Optional[int] = None
    native: Optional[MetabaseNativeQuery] = None
    query: Optional[MetabaseQuery] = None


class MetabaseLastEditInfo(MetabaseBaseModel):
    id: Optional[int] = None
    email: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    timestamp: Optional[str] = None


class MetabaseCard(MetabaseBaseModel):
    id: int
    name: str
    description: Optional[str] = None
    display: Optional[str] = None
    dataset_query: MetabaseDatasetQuery = Field(
        default_factory=lambda: MetabaseDatasetQuery(type=_QUERY_TYPE_NATIVE)
    )
    query_type: Optional[str] = None
    result_metadata: List[MetabaseResultMetadata] = Field(default_factory=list)
    creator_id: Optional[int] = None
    collection_id: Optional[int] = None
    type: Optional[str] = None
    database_id: Optional[int] = None
    table_id: Optional[int] = None
    last_edit_info: Optional[MetabaseLastEditInfo] = Field(None, alias="last-edit-info")
    public_uuid: Optional[str] = None
    created_at: Optional[str] = None

    @property
    def custom_properties(self) -> Dict[str, str]:
        metrics, dimensions = [], []
        for meta in self.result_metadata:
            display_name = meta.display_name or ""
            if _MBQL_REF_AGGREGATION in str(meta.field_ref or ""):
                metrics.append(display_name)
            else:
                dimensions.append(display_name)
        filters: List = (
            self.dataset_query.query.filter or []
            if self.dataset_query and self.dataset_query.query
            else []
        )
        return {
            "Metrics": ", ".join(metrics),
            "Filters": str(filters) if filters else "",
            "Dimensions": ", ".join(dimensions),
        }


class MetabaseCardInfo(MetabaseBaseModel):
    """Basic card info (used in dashcards)."""

    id: Optional[int] = None
    name: Optional[str] = None


class MetabaseDashCard(MetabaseBaseModel):
    id: int
    card: MetabaseCardInfo
    dashboard_id: int


class MetabaseDashboard(MetabaseBaseModel):
    id: int
    name: str
    description: Optional[str] = None
    creator_id: Optional[int] = None
    collection_id: Optional[int] = None
    dashcards: List[MetabaseDashCard] = Field(default_factory=list)
    last_edit_info: Optional[MetabaseLastEditInfo] = Field(None, alias="last-edit-info")


class MetabaseDashboardListItem(MetabaseBaseModel):
    """Dashboard item from collection items API (minimal fields)."""

    id: int
    name: Optional[str] = None
    model: str  # Always "dashboard" for these items


class MetabaseCardListItem(MetabaseBaseModel):
    """Card/question item from cards list API (minimal fields)."""

    id: int
    name: Optional[str] = None
    type: Optional[str] = None  # "question" or "model"

    @property
    def is_model(self) -> bool:
        return self.type == _CARD_TYPE_MODEL


class MetabaseCollectionItemsResponse(MetabaseBaseModel):
    """Response from /api/collection/{id}/items API."""

    data: List[MetabaseDashboardListItem] = Field(default_factory=list)
    total: Optional[int] = None


class MetabaseCollection(MetabaseBaseModel):
    id: int
    name: str
    slug: Optional[str] = None
    description: Optional[str] = None
    archived: Optional[bool] = None

    @property
    def tag_slug(self) -> str:
        """Sanitized collection name for use in a DataHub tag URN."""
        sanitized = self.name.replace(" ", "_")
        sanitized = _SPECIAL_CHARS_PATTERN.sub("", sanitized)
        sanitized = sanitized.lower()
        sanitized = _MULTIPLE_UNDERSCORES_PATTERN.sub("_", sanitized)
        return sanitized.strip("_")


class MetabaseLoginResponse(MetabaseBaseModel):
    """Response from /api/session API."""

    id: str  # Session token


class MetabaseDatabaseDetails(MetabaseBaseModel):
    model_config = ConfigDict(extra="allow")

    host: Optional[str] = None
    port: Optional[int] = None
    dbname: Optional[str] = None
    db: Optional[str] = None
    database: Optional[str] = None
    catalog: Optional[str] = None
    project_id: Optional[str] = Field(None, alias="project-id")
    dataset_id: Optional[str] = Field(None, alias="dataset-id")
    schema_: Optional[str] = Field(None, alias="schema")
    service_name: Optional[str] = Field(None, alias="service-name")

    @property
    def _clean_db(self) -> Optional[str]:
        """
        Sanitize the `db` field for use as a dataset name.

        H2 stores a full JDBC path here (e.g. `file:/plugins/sample-database.db;USER=GUEST;PASSWORD=guest`).
        Other engines using this field (Redshift, Snowflake, SQL Server) store a plain name,
        so stripping is a no-op for them.
        """
        if not self.db:
            return None
        path = self.db.split(";")[0]
        for prefix in _JDBC_URI_SCHEMES:
            if path.startswith(prefix):
                path = path[len(prefix) :]
                break
        name = path.rsplit("/", 1)[-1]
        if name.endswith(_JDBC_DB_EXTENSION):
            name = name[: -len(_JDBC_DB_EXTENSION)]
        return name or None

    def get_database_name(self, engine: str) -> Optional[str]:
        """Return the logical database name for the given engine, or None if unknown."""
        field = _ENGINE_TO_DB_DETAIL_FIELD.get(engine)
        return getattr(self, field) if field else None


class MetabaseDatabase(MetabaseBaseModel):
    id: int
    name: str
    engine: str
    details: MetabaseDatabaseDetails = Field(default_factory=MetabaseDatabaseDetails)


class MetabaseTable(MetabaseBaseModel):
    id: int
    name: str
    display_name: Optional[str] = None
    schema_: Optional[str] = Field(None, alias="schema")
    db_id: int


class DatasourceInfo(MetabaseBaseModel):
    platform: str
    database_name: Optional[str] = None
    schema: Optional[str] = None  # type: ignore[assignment]
    platform_instance: Optional[str] = None

    @property
    def has_schema(self) -> bool:
        return self.platform not in _TWO_TIER_PLATFORMS


class _MBQLContext(MetabaseBaseModel):
    """Shared context threaded through MBQL column-level lineage resolution."""

    query: MetabaseQuery
    datasource: DatasourceInfo
    resolved: Dict[int, MetabaseField] = Field(default_factory=dict)


DATASOURCE_URN_RECURSION_LIMIT = 5  # prevent stack overflow on circular card refs

# Metabase engine names that differ from their DataHub platform identifier.
# Engines not listed here are used as-is (e.g. "postgres" → "postgres").
# Reference: metadata-service/configuration/src/main/resources/bootstrap_mcps/data-platforms.yaml
METABASE_ENGINE_TO_DATAHUB_PLATFORM: Dict[str, str] = {
    "sparksql": "spark",
    "mongo": "mongodb",
    "presto-jdbc": "presto",
    "sqlserver": "mssql",
    "bigquery-cloud-sdk": "bigquery",
}

# Maps each Metabase engine to the MetabaseDatabaseDetails attribute that holds
# the logical database name. Engines mapped to "_clean_db" go through JDBC
# sanitization; all others return the named field directly.
_ENGINE_TO_DB_DETAIL_FIELD: Dict[str, str] = {
    "athena": "catalog",
    "bigquery": "dataset_id",
    "bigquery-cloud-sdk": "project_id",
    "clickhouse": "dbname",
    "databricks": "catalog",
    "druid": "dbname",
    "h2": "_clean_db",
    "mongo": "dbname",
    "mysql": "dbname",
    "oracle": "service_name",
    "postgres": "dbname",
    "presto": "catalog",
    "presto-jdbc": "catalog",
    "redshift": "_clean_db",
    "snowflake": "_clean_db",
    "sparksql": "dbname",
    "sqlserver": "_clean_db",
    "trino": "catalog",
    "vertica": "database",
}

# Platforms that use a two-tier naming scheme (database.table) rather than
# three-tier (database.schema.table).  For these platforms the schema component
# returned by Metabase's /api/table endpoint is omitted from the dataset URN so
# that lineage URNs match those produced by the corresponding DataHub connector.
_TWO_TIER_PLATFORMS: frozenset = frozenset(
    {
        "mysql",  # MySQL has no schema layer; "schema" == database
        "mongodb",  # MongoDB: database + collection only
        "druid",  # Druid: datasource only, no schema concept
        "h2",  # H2: file-based embedded DB, no schema layer
    }
)

METABASE_CHART_DISPLAY_TYPE_MAP: Dict[str, Optional[str]] = {
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

METABASE_TYPE_TO_DATAHUB_TYPE: Dict[
    str,
    Type[
        Union[
            NumberTypeClass,
            StringTypeClass,
            BooleanTypeClass,
            EnumTypeClass,
            BytesTypeClass,
            DateTypeClass,
            TimeTypeClass,
        ]
    ],
] = {
    "Integer": NumberTypeClass,
    "BigInteger": NumberTypeClass,
    "Float": NumberTypeClass,
    "Decimal": NumberTypeClass,
    "Number": NumberTypeClass,
    "Text": StringTypeClass,
    "String": StringTypeClass,
    "UUID": StringTypeClass,
    "Array": StringTypeClass,  # Metabase serializes as text
    "JSON": StringTypeClass,  # Displayed as text unless unfolded
    "Enum": EnumTypeClass,  # PostgresEnum, MySQLEnum
    "Boolean": BooleanTypeClass,
    "JSONB": BytesTypeClass,
    "Blob": BytesTypeClass,
    "Bytes": BytesTypeClass,
    "Date": DateTypeClass,
    "DateTime": DateTypeClass,
    "DateTimeWithTZ": DateTypeClass,
    "Time": TimeTypeClass,
}


class MetabaseConfig(
    DatasetLineageProviderConfigBase,
    StatefulIngestionConfigBase,
    LowerCaseDatasetUrnConfigMixin,
):
    # See the Metabase /api/session endpoint for details
    # https://www.metabase.com/docs/latest/api-documentation.html#post-apisession
    connect_uri: str = Field(default="localhost:3000", description="Metabase host URL.")
    display_uri: Optional[str] = Field(
        default=None,
        description="optional URL to use in links (if `connect_uri` is only for ingestion)",
    )
    username: Optional[str] = Field(
        default=None,
        description="Metabase username, used when an API key is not provided.",
    )
    password: Optional[SecretStr] = Field(
        default=None,
        description="Metabase password, used when an API key is not provided.",
    )

    # https://www.metabase.com/learn/metabase-basics/administration/administration-and-operation/metabase-api#example-get-request
    api_key: Optional[SecretStr] = Field(
        default=None,
        description="Metabase API key. If provided, the username and password will be ignored. Recommended method.",
    )
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
    exclude_other_user_collections: bool = Field(
        default=False,
        description="Flag that if true, exclude other user collections",
    )
    extract_collections_as_tags: bool = Field(
        default=True,
        description="Extract Metabase collections as tags on dashboards and charts",
    )
    extract_models: bool = Field(
        default=True,
        description="Extract Metabase models (saved questions used as data sources) as datasets",
    )
    convert_lineage_urns_to_lowercase: bool = Field(
        default=True,
        description="Whether to convert upstream dataset URN names to lowercase. "
        "Required for platforms that normalise identifiers to lowercase (e.g. Snowflake). "
        "Disable only if all upstream connectors preserve original identifier case.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @field_validator("connect_uri", "display_uri", mode="after")
    @classmethod
    def remove_trailing_slash(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        return config_clean.remove_trailing_slashes(v)

    @model_validator(mode="after")
    def default_display_uri_to_connect_uri(self) -> "MetabaseConfig":
        if self.display_uri is None:
            self.display_uri = self.connect_uri
        return self


@dataclass
class MetabaseReport(StaleEntityRemovalSourceReport):
    pass


@platform_name("Metabase")
@config_class(MetabaseConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE, "Supported by default for charts and dashboards"
)
class MetabaseSource(StatefulIngestionSourceBase):
    """Extracts dashboards, charts, and models from Metabase via REST API."""

    config: MetabaseConfig
    report: MetabaseReport
    platform = "metabase"

    def __hash__(self) -> int:
        return id(self)

    def __init__(self, ctx: PipelineContext, config: MetabaseConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report = MetabaseReport()
        self.setup_session()
        self.source_config: MetabaseConfig = config

    def _normalize(self, value: str) -> str:
        return value.lower() if self.config.convert_lineage_urns_to_lowercase else value

    def setup_session(self) -> None:
        self.session = requests.session()
        if self.config.api_key:
            self.session.headers.update(
                {
                    "x-api-key": self.config.api_key.get_secret_value(),
                    "Content-Type": "application/json",
                    "Accept": "*/*",
                }
            )
        else:
            login_response = requests.post(
                f"{self.config.connect_uri}/api/session",
                None,
                {
                    "username": self.config.username,
                    "password": (
                        self.config.password.get_secret_value()
                        if self.config.password
                        else None
                    ),
                },
            )

            login_response.raise_for_status()
            login_data = MetabaseLoginResponse.model_validate(login_response.json())
            self.access_token = login_data.id

            self.session.headers.update(
                {
                    "X-Metabase-Session": f"{self.access_token}",
                    "Content-Type": "application/json",
                    "Accept": "*/*",
                }
            )

        try:
            test_response = self.session.get(
                f"{self.config.connect_uri}/api/user/current"
            )
            test_response.raise_for_status()
        except HTTPError as e:
            self.report.report_failure(
                title="Unable to Retrieve Current User",
                message=f"Unable to retrieve user {self.config.username} information. %s"
                % str(e),
            )

    def close(self) -> None:
        # Only username/password auth creates sessions that need cleanup
        if not self.config.api_key:
            response = requests.delete(
                f"{self.config.connect_uri}/api/session",
                headers={"X-Metabase-Session": self.access_token},
            )
            if response.status_code not in (200, 204):
                self.report.report_failure(
                    title="Unable to Log User Out",
                    message=f"Unable to logout for user {self.config.username}",
                )
        super().close()

    def emit_dashboard_workunits(self) -> Iterable[MetadataWorkUnit]:
        try:
            collections_response = self.session.get(
                f"{self.config.connect_uri}/api/collection/"
                f"?exclude-other-user-collections={json.dumps(self.config.exclude_other_user_collections)}"
            )
            collections_response.raise_for_status()
            collections_data = collections_response.json()

            for collection_data in collections_data:
                try:
                    collection = MetabaseCollection.model_validate(collection_data)
                except ValidationError as e:
                    self.report.report_warning(
                        title="Invalid Collection Data",
                        message="Collection data from Metabase API failed validation.",
                        context=f"Data: {collection_data}, Error: {str(e)}",
                    )
                    continue

                collection_dashboards_response = self.session.get(
                    f"{self.config.connect_uri}/api/collection/{collection.id}/items?models=dashboard"
                )
                collection_dashboards_response.raise_for_status()

                try:
                    collection_dashboards = (
                        MetabaseCollectionItemsResponse.model_validate(
                            collection_dashboards_response.json()
                        )
                    )
                except ValidationError as e:
                    self.report.report_warning(
                        title="Invalid Collection Items Response",
                        message="Collection items response failed validation.",
                        context=f"Collection ID: {collection.id}, Error: {str(e)}",
                    )
                    continue

                if not collection_dashboards.data:
                    continue

                for dashboard_info in collection_dashboards.data:
                    yield from self._emit_dashboard_workunits(dashboard_info)

        except HTTPError as http_error:
            self.report.report_failure(
                title="Unable to Retrieve Dashboards",
                message="Request to retrieve dashboards from Metabase failed.",
                context=f"Error: {str(http_error)}",
            )

    @staticmethod
    def get_timestamp_millis_from_ts_string(ts_str: str) -> int:
        """Convert timestamp string to milliseconds, falling back to now on parse failure."""
        try:
            return int(dp.parse(ts_str).timestamp() * 1000)
        except (dp.ParserError, OverflowError) as e:
            logger.warning(
                f"Failed to parse timestamp '{ts_str}': {e}. Using current time instead."
            )
            return int(datetime.now(timezone.utc).timestamp() * 1000)

    def _emit_dashboard_workunits(
        self, dashboard_info: MetabaseDashboardListItem
    ) -> Iterable[MetadataWorkUnit]:
        dashboard_id = dashboard_info.id
        dashboard_url = f"{self.config.connect_uri}/api/dashboard/{dashboard_id}"
        try:
            dashboard_response = self.session.get(dashboard_url)
            dashboard_response.raise_for_status()
            dashboard = MetabaseDashboard.model_validate(dashboard_response.json())
        except HTTPError as http_error:
            self.report.report_warning(
                title="Unable to Retrieve Dashboard",
                message="Request to retrieve dashboards from Metabase failed.",
                context=f"Dashboard ID: {dashboard_id}, Error: {str(http_error)}",
            )
            return
        except ValidationError as e:
            self.report.report_warning(
                title="Invalid Dashboard Data",
                message="Dashboard data from Metabase API failed validation.",
                context=f"Dashboard ID: {dashboard_id}, Error: {str(e)}",
            )
            return

        dashboard_urn = builder.make_dashboard_urn(
            platform=self.platform, name=str(dashboard.id)
        )

        if dashboard.last_edit_info:
            email = dashboard.last_edit_info.email or "unknown"
            timestamp = dashboard.last_edit_info.timestamp
        else:
            email = "unknown"
            timestamp = None

        modified_actor = builder.make_user_urn(email)
        modified_ts = self.get_timestamp_millis_from_ts_string(f"{timestamp}")
        title = dashboard.name
        description = dashboard.description or ""
        last_modified = ChangeAuditStampsClass(
            created=AuditStampClass(time=modified_ts, actor=modified_actor),
            lastModified=AuditStampClass(time=modified_ts, actor=modified_actor),
        )

        chart_edges = []
        for dashcard in dashboard.dashcards:
            if not dashcard.card or not dashcard.card.id:
                continue

            card_id = dashcard.card.id

            if not card_id:
                continue
            chart_urn = builder.make_chart_urn(
                platform=self.platform, name=str(card_id)
            )
            chart_edges.append(
                EdgeClass(
                    destinationUrn=chart_urn,
                    lastModified=last_modified.lastModified,
                )
            )

        dataset_edges = self.construct_dashboard_lineage(
            dashboard=dashboard, last_modified=last_modified.lastModified
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=DashboardInfoClass(
                description=description,
                title=title,
                chartEdges=chart_edges,
                lastModified=last_modified,
                dashboardUrl=f"{self.config.display_uri}/dashboard/{dashboard_id}",
                customProperties={},
                datasetEdges=dataset_edges if dataset_edges else None,
            ),
        ).as_workunit()

        if dashboard.creator_id:
            ownership = self._get_ownership(dashboard.creator_id)
            if ownership is not None:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dashboard_urn,
                    aspect=ownership,
                ).as_workunit()

        tags = self._get_tags_from_collection(dashboard.collection_id)
        if tags:
            yield MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn,
                aspect=tags,
            ).as_workunit()

    def construct_dashboard_lineage(
        self, dashboard: MetabaseDashboard, last_modified: AuditStampClass
    ) -> Optional[List[EdgeClass]]:
        upstream_tables = []

        for dashcard in dashboard.dashcards:
            if not dashcard.card or not dashcard.card.id:
                continue

            card_id = dashcard.card.id

            if not card_id:
                continue

            card = self.get_card_details_by_id(card_id)
            if not card:
                continue

            table_urns = self._get_table_urns_from_card(card)
            if table_urns:
                upstream_tables.extend(table_urns)

        unique_table_urns = list(set(upstream_tables))

        if not unique_table_urns:
            return None

        dataset_edges = [
            EdgeClass(
                destinationUrn=table_urn,
                lastModified=last_modified,
            )
            for table_urn in unique_table_urns
        ]

        return dataset_edges

    def _check_recursion_limit(
        self, recursion_depth: int, context: str, card_id: int
    ) -> bool:
        if recursion_depth > DATASOURCE_URN_RECURSION_LIMIT:
            self.report.report_warning(
                title="Card Recursion Limit Exceeded",
                message=f"Unable to extract lineage for {context}. Nested card reference depth exceeded limit.",
                context=f"Card ID: {card_id}, Recursion Depth: {recursion_depth}, Limit: {DATASOURCE_URN_RECURSION_LIMIT}",
            )
            return True
        return False

    def _get_table_urns_from_card(
        self, card: MetabaseCard, recursion_depth: int = 0
    ) -> List[str]:
        if self._check_recursion_limit(
            recursion_depth=recursion_depth, context="card lineage", card_id=card.id
        ):
            return []

        if not card.dataset_query:
            return []

        query_type = card.dataset_query.type

        if query_type == _QUERY_TYPE_NATIVE:
            return self._get_table_urns_from_native_query(card)
        elif query_type == _QUERY_TYPE_QUERY:
            return self._get_table_urns_from_query_builder(
                card=card, recursion_depth=recursion_depth
            )

        return []

    def _extract_native_query(self, card: MetabaseCard) -> Optional[str]:
        if card.dataset_query and card.dataset_query.native:
            return card.dataset_query.native.query
        return None

    def _get_table_urns_from_native_query(self, card: MetabaseCard) -> List[str]:
        if not card.database_id:
            return []

        datasource = self.get_datasource_from_id(card.database_id)
        if not datasource or not datasource.platform:
            return []

        raw_query = self._extract_native_query(card)
        if not raw_query:
            return []

        raw_query_stripped = self.strip_template_expressions(raw_query)

        result = create_lineage_sql_parsed_result(
            query=raw_query_stripped,
            default_db=self._normalize(datasource.database_name)
            if datasource.database_name
            else None,
            default_schema=datasource.schema or self.config.default_schema,
            platform=datasource.platform,
            platform_instance=datasource.platform_instance,
            env=self.config.env,
            graph=self.ctx.graph,
        )

        if result.debug_info.table_error:
            logger.debug(
                "Failed to parse lineage from query: %s",
                result.debug_info.table_error,
            )

        return [str(t) for t in result.in_tables]

    def _get_table_urns_from_query_builder(
        self, card: MetabaseCard, recursion_depth: int = 0
    ) -> List[str]:
        """
        Metabase's query builder allows cards to reference other cards as their source:
        - source-table: 123 — direct table ID
        - source-table: "card__456" — nested card reference
        - joins[].source-table — additional tables via MBQL joins
        """
        if not card.dataset_query or not card.dataset_query.query:
            return []

        query = card.dataset_query.query
        source_table_ids = query.source_table_refs

        if not source_table_ids:
            return []

        table_urns: List[str] = []

        for source_table_id in source_table_ids:
            source_table_str = str(source_table_id)
            if source_table_str.startswith(_CARD_REF_PREFIX):
                referenced_card_id = source_table_str.replace(_CARD_REF_PREFIX, "")
                referenced_card = self.get_card_details_by_id(referenced_card_id)
                if referenced_card:
                    table_urns.extend(
                        self._get_table_urns_from_card(
                            card=referenced_card, recursion_depth=recursion_depth + 1
                        )
                    )
                continue

            if not card.database_id:
                continue

            datasource = self.get_datasource_from_id(card.database_id)
            if not datasource or not datasource.platform:
                continue

            schema_name, table_name = self.get_source_table_from_id(source_table_id)
            if not table_name:
                continue

            name_components = [
                datasource.database_name,
                schema_name if datasource.has_schema else None,
                table_name,
            ]
            table_urn = builder.make_dataset_urn_with_platform_instance(
                platform=datasource.platform,
                name=self._normalize(".".join([v for v in name_components if v])),
                platform_instance=datasource.platform_instance,
                env=self.config.env,
            )
            table_urns.append(table_urn)

        return table_urns

    def _field_urn(
        self, field: MetabaseField, datasource: DatasourceInfo
    ) -> Optional[str]:
        if not field.table_id:
            return None
        schema_name, table_name = self.get_source_table_from_id(field.table_id)
        if not table_name:
            return None
        name_components = [
            datasource.database_name,
            schema_name if datasource.has_schema else None,
            table_name,
        ]
        dataset_urn = builder.make_dataset_urn_with_platform_instance(
            platform=datasource.platform,
            name=self._normalize(".".join([v for v in name_components if v])),
            platform_instance=datasource.platform_instance,
            env=self.config.env,
        )
        return builder.make_schema_field_urn(
            parent_urn=dataset_urn,
            field_path=self._normalize(field.name),
        )

    def _upstream_urns_for_field_ids(
        self,
        field_ids: List[int],
        ctx: _MBQLContext,
    ) -> List[str]:
        urns = []
        for fid in field_ids:
            mbql_field = ctx.resolved.get(fid)
            if mbql_field:
                urn = self._field_urn(mbql_field, ctx.datasource)
                if urn:
                    urns.append(urn)
        return urns

    def _resolve_aggregation_upstream_urns(
        self,
        field_ref: List[object],
        ctx: _MBQLContext,
    ) -> List[str]:
        """
        Resolve upstream URNs for an aggregation field_ref ["aggregation", index].
        Returns all resolved field URNs for COUNT(*)-style aggregations with no
        explicit field argument (i.e. the aggregation references no specific field ID).
        """
        agg_index = field_ref[1] if len(field_ref) > 1 else None
        if agg_index is None or not ctx.query.aggregation:
            return []
        agg_clauses = ctx.query.aggregation
        # aggregation can be a single clause or a list-of-clauses
        if agg_clauses and isinstance(agg_clauses[0], list):
            agg_clause = (
                agg_clauses[agg_index]
                if isinstance(agg_index, int) and agg_index < len(agg_clauses)
                else None
            )
        else:
            agg_clause = agg_clauses
        if not isinstance(agg_clause, list):
            return []
        agg_field_ids = _extract_field_ids_from_mbql(agg_clause)
        if agg_field_ids:
            return self._upstream_urns_for_field_ids(agg_field_ids, ctx)
        # COUNT(*) — no explicit field; fan in all resolved upstream columns
        return [
            urn
            for mbql_field in ctx.resolved.values()
            for urn in [self._field_urn(mbql_field, ctx.datasource)]
            if urn
        ]

    def _resolve_field_ref_upstream_urns(
        self,
        field_ref: List[object],
        ctx: _MBQLContext,
    ) -> List[str]:
        """
        Resolve a result_metadata field_ref to a list of upstream DataHub field URNs.

        Handles the three MBQL field_ref types:
        - ["field", id, opts]      — direct column pass-through
        - ["expression", name]     — calculated column referencing query.expressions[name]
        - ["aggregation", index]   — metric referencing query.aggregation[index]
        """
        if not field_ref:
            return []
        ref_type = field_ref[0]

        if ref_type == _MBQL_REF_FIELD:
            return self._upstream_urns_for_field_ids(
                _extract_field_ids_from_mbql(field_ref), ctx
            )

        if ref_type == _MBQL_REF_EXPRESSION:
            expr_name = field_ref[1] if len(field_ref) > 1 else None
            if expr_name and ctx.query.expressions:
                expr_clause = ctx.query.expressions.get(str(expr_name))
                if isinstance(expr_clause, list):
                    return self._upstream_urns_for_field_ids(
                        _extract_field_ids_from_mbql(expr_clause), ctx
                    )
            return []

        if ref_type == _MBQL_REF_AGGREGATION:
            return self._resolve_aggregation_upstream_urns(field_ref, ctx)

        return []

    def _get_cll_from_query_builder(
        self,
        card: MetabaseCard,
        entity_urn: str,
    ) -> Optional[UpstreamLineageClass]:
        """
        Extract column-level lineage from an MBQL (query builder) card.

        Uses result_metadata.field_ref as a pre-built lineage map: Metabase records exactly
        which MBQL expression produced each output column, so we read that and resolve the
        referenced field IDs back to upstream column names via /api/field/{id}.
        """
        if not card.dataset_query or not card.dataset_query.query:
            return None
        if not card.result_metadata or not card.database_id:
            return None

        datasource = self.get_datasource_from_id(card.database_id)
        if not datasource or not datasource.platform:
            return None

        query = card.dataset_query.query

        resolved = {
            fid: f
            for fid in set(query.collect_field_ids())
            if (f := self.get_field_from_id(fid)) is not None
        }
        ctx = _MBQLContext(query=query, datasource=datasource, resolved=resolved)

        fine_grained: List[FineGrainedLineageClass] = []
        for meta in card.result_metadata:
            if (
                not meta.name
                or not meta.field_ref
                or not isinstance(meta.field_ref, list)
            ):
                continue
            upstream_urns = self._resolve_field_ref_upstream_urns(meta.field_ref, ctx)
            if upstream_urns:
                fine_grained.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        upstreams=upstream_urns,
                        downstreams=[
                            builder.make_schema_field_urn(
                                parent_urn=entity_urn, field_path=meta.name
                            )
                        ],
                    )
                )

        table_urns = self._get_table_urns_from_query_builder(card)
        if not table_urns:
            return None

        return UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=urn, type=DatasetLineageTypeClass.TRANSFORMED)
                for urn in table_urns
            ],
            fineGrainedLineages=fine_grained if fine_grained else None,
        )

    @lru_cache(maxsize=None)
    def _get_ownership(self, creator_id: int) -> Optional[OwnershipClass]:
        user_info_url = f"{self.config.connect_uri}/api/user/{creator_id}"
        try:
            user_info_response = self.session.get(user_info_url)
            user_info_response.raise_for_status()
            user = MetabaseUser.model_validate(user_info_response.json())
        except HTTPError as http_error:
            if (
                http_error.response is not None
                and http_error.response.status_code == 404
            ):
                self.report.report_warning(
                    title="Cannot find user",
                    message="User is blocked in Metabase or missing",
                    context=f"Creator ID: {creator_id}",
                )
                return None
            self.report.report_warning(
                title="Failed to retrieve user",
                message="Request to Metabase Failed",
                context=f"Creator ID: {creator_id}, Error: {str(http_error)}",
            )
            return None
        except ValidationError as e:
            self.report.report_warning(
                title="Invalid User Data",
                message="User data from Metabase API failed validation.",
                context=f"Creator ID: {creator_id}, Error: {str(e)}",
            )
            return None

        owner_urn = builder.make_user_urn(user.email)
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

    @lru_cache(maxsize=None)
    def _get_collections_map(self) -> Dict[str, MetabaseCollection]:
        """Cached to avoid N+1 API calls when tagging multiple entities."""
        try:
            collections_response = self.session.get(
                f"{self.config.connect_uri}/api/collection/"
                f"?exclude-other-user-collections={json.dumps(self.config.exclude_other_user_collections)}"
            )
            collections_response.raise_for_status()
            collections_data = collections_response.json()

            collections_dict = {}
            for coll_data in collections_data:
                try:
                    coll = MetabaseCollection.model_validate(coll_data)
                    collections_dict[str(coll.id)] = coll
                except ValidationError as e:
                    self.report.report_warning(
                        title="Invalid Collection Data",
                        message="Collection data from Metabase API failed validation.",
                        context=f"Data: {coll_data}, Error: {str(e)}",
                    )
                    continue
            return collections_dict
        except HTTPError as http_error:
            if (
                http_error.response is not None
                and http_error.response.status_code == 404
            ):
                # 404 is expected when collection features are disabled or unavailable
                logger.debug("Collections endpoint not found: %s", str(http_error))
                return {}
            self.report.report_warning(
                title="Failed to retrieve collections",
                message="Unable to fetch collections from Metabase API",
                context=f"Error: {str(http_error)} - Check API credentials and permissions",
            )
            return {}

    def _get_tags_from_collection(
        self, collection_id: Optional[Union[int, str]]
    ) -> Optional[GlobalTagsClass]:
        if not self.config.extract_collections_as_tags or not collection_id:
            return None

        collections_map = self._get_collections_map()
        collection = collections_map.get(str(collection_id))

        if not collection:
            logger.debug(
                f"Collection {collection_id} not found in available collections"
            )
            return None

        collection_slug = collection.tag_slug
        if not collection_slug:
            logger.debug(
                f"Collection {collection_id} has empty name after sanitization"
            )
            return None

        tag_urn = builder.make_tag_urn(f"metabase_collection_{collection_slug}")

        return GlobalTagsClass(tags=[TagAssociationClass(tag=tag_urn)])

    def emit_chart_workunits(self) -> Iterable[MetadataWorkUnit]:
        try:
            card_response = self.session.get(f"{self.config.connect_uri}/api/card")
            card_response.raise_for_status()
            cards = card_response.json()

            for card_data in cards:
                try:
                    card_info = MetabaseCardListItem.model_validate(card_data)
                    # Models are emitted as datasets by emit_model_workunits()
                    if self.config.extract_models and card_info.is_model:
                        continue

                    yield from self._emit_chart_workunits(card_info)
                except ValidationError as e:
                    self.report.report_warning(
                        title="Invalid Card List Item",
                        message="Card list item failed validation.",
                        context=f"Error: {str(e)}",
                    )

        except HTTPError as http_error:
            self.report.report_failure(
                title="Unable to Retrieve Cards",
                message="Request to retrieve cards from Metabase failed.",
                context=f"Error: {str(http_error)}",
            )
            return

    def get_card_details_by_id(
        self, card_id: Union[int, str]
    ) -> Optional[MetabaseCard]:
        card_url = f"{self.config.connect_uri}/api/card/{card_id}"
        try:
            # Use legacy-mbql=true to get MBQL 4 format for compatibility.
            # Metabase 0.57+ returns MBQL 5 by default which has a different structure.
            card_response = self.session.get(card_url, params={"legacy-mbql": "true"})
            card_response.raise_for_status()
            return MetabaseCard.model_validate(card_response.json())
        except HTTPError as http_error:
            self.report.report_warning(
                title="Unable to Retrieve Card",
                message="Request to retrieve Card from Metabase failed.",
                context=f"Card ID: {card_id}, Error: {str(http_error)}",
            )
            return None
        except ValidationError as e:
            self.report.report_warning(
                title="Invalid Card Data",
                message="Card data from Metabase API failed validation.",
                context=f"Card ID: {card_id}, Error: {str(e)}",
            )
            return None

    def _emit_chart_workunits(
        self, card_info: MetabaseCardListItem
    ) -> Iterable[MetadataWorkUnit]:
        card_details = self.get_card_details_by_id(card_info.id)
        if not card_details:
            return

        chart_urn = builder.make_chart_urn(
            platform=self.platform, name=str(card_details.id)
        )

        last_edit_info = card_details.last_edit_info
        if last_edit_info:
            email = last_edit_info.email or "unknown"
            timestamp = last_edit_info.timestamp
        else:
            email = "unknown"
            timestamp = None

        modified_actor = builder.make_user_urn(email)
        modified_ts = self.get_timestamp_millis_from_ts_string(f"{timestamp}")
        last_modified = ChangeAuditStampsClass(
            created=None,
            lastModified=AuditStampClass(time=modified_ts, actor=modified_actor),
        )

        chart_type = self._get_chart_type(
            card_id=card_details.id, display_type=card_details.display or ""
        )
        description = card_details.description or ""
        title = card_details.name
        datasource_urn = self.get_datasource_urn(card_details)
        custom_properties = card_details.custom_properties

        input_edges = (
            [
                EdgeClass(
                    destinationUrn=urn,
                    lastModified=last_modified.lastModified,
                )
                for urn in datasource_urn
            ]
            if datasource_urn
            else None
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=ChartInfoClass(
                type=chart_type,
                description=description,
                title=title,
                lastModified=last_modified,
                chartUrl=f"{self.config.display_uri}/card/{card_details.id}",
                inputEdges=input_edges,
                customProperties=custom_properties,
            ),
        ).as_workunit()

        if card_details.query_type == _QUERY_TYPE_NATIVE:
            raw_query = self._extract_native_query(card_details)
            if raw_query:
                yield MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=ChartQueryClass(
                        rawQuery=raw_query,
                        type=ChartQueryTypeClass.SQL,
                    ),
                ).as_workunit()

        if card_details.creator_id:
            ownership = self._get_ownership(card_details.creator_id)
            if ownership is not None:
                yield MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=ownership,
                ).as_workunit()

        tags = self._get_tags_from_collection(card_details.collection_id)
        if tags is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=tags,
            ).as_workunit()

    def _get_chart_type(self, card_id: int, display_type: str) -> Optional[str]:
        if not display_type:
            self.report.report_warning(
                title="Unrecognized Card Type",
                message=f"Unrecognized card type {display_type} found. Setting to None",
                context=f"Card ID: {card_id}",
            )
            return None
        if display_type not in METABASE_CHART_DISPLAY_TYPE_MAP:
            self.report.report_warning(
                title="Unrecognized Chart Type",
                message=f"Unrecognized chart type {display_type} found. Setting to None",
                context=f"Card ID: {card_id}",
            )
        return METABASE_CHART_DISPLAY_TYPE_MAP.get(display_type)

    def get_datasource_urn(
        self, card: MetabaseCard, recursion_depth: int = 0
    ) -> Optional[List[str]]:
        if self._check_recursion_limit(
            recursion_depth=recursion_depth,
            context="datasource URN extraction",
            card_id=card.id,
        ):
            return []

        table_urns = self._get_table_urns_from_card(
            card=card, recursion_depth=recursion_depth
        )
        return table_urns if table_urns else None if table_urns else None

    @staticmethod
    def strip_template_expressions(raw_query: str) -> str:
        """
        Strip Metabase template expressions before SQL parsing.

        [[optional]] clauses are removed; {{variable}} placeholders are replaced with "1".
        See: https://www.metabase.com/docs/latest/questions/native-editor/sql-parameters
        """
        query_patched = re.sub(r"\[\[.+?\]\]", r" ", raw_query)
        query_patched = re.sub(r"\{\{.+?\}\}", r"1", query_patched)
        return query_patched

    @lru_cache(maxsize=None)
    def get_source_table_from_id(
        self, table_id: Union[int, str]
    ) -> Tuple[Optional[str], Optional[str]]:
        try:
            dataset_response = self.session.get(
                f"{self.config.connect_uri}/api/table/{table_id}"
            )
            dataset_response.raise_for_status()
            table = MetabaseTable.model_validate(dataset_response.json())
            return table.schema_, table.name

        except HTTPError as http_error:
            self.report.report_warning(
                title="Failed to Retrieve Source Table",
                message="Request to retrieve source table from Metadabase failed",
                context=f"Table ID: {table_id}, Error: {str(http_error)}",
            )

        return None, None

    @lru_cache(maxsize=None)
    def get_field_from_id(self, field_id: int) -> Optional[MetabaseField]:
        try:
            response = self.session.get(
                f"{self.config.connect_uri}/api/field/{field_id}"
            )
            response.raise_for_status()
            return MetabaseField.model_validate(response.json())
        except HTTPError as http_error:
            self.report.report_warning(
                title="Failed to Retrieve Field",
                message="Request to retrieve field from Metabase failed",
                context=f"Field ID: {field_id}, Error: {str(http_error)}",
            )
            return None
        except ValidationError as e:
            self.report.report_warning(
                title="Invalid Field Data",
                message="Field data from Metabase API failed validation.",
                context=f"Field ID: {field_id}, Error: {str(e)}",
            )
            return None

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
        # Allows users to distinguish prod-clickhouse vs dev-clickhouse, or us-east-postgres vs eu-west-postgres
        if datasource_id is not None and self.config.database_id_to_instance_map:
            platform_instance = self.config.database_id_to_instance_map.get(
                str(datasource_id)
            )

        if platform and self.config.platform_instance_map and platform_instance is None:
            platform_instance = self.config.platform_instance_map.get(platform)

        return platform_instance

    @lru_cache(maxsize=None)
    def get_datasource_from_id(
        self, datasource_id: Union[int, str]
    ) -> Optional[DatasourceInfo]:
        try:
            dataset_response = self.session.get(
                f"{self.config.connect_uri}/api/database/{datasource_id}"
            )
            dataset_response.raise_for_status()
            database = MetabaseDatabase.model_validate(dataset_response.json())
        except HTTPError as http_error:
            self.report.report_warning(
                title="Unable to Retrieve Data Source",
                message="Request to retrieve data source from Metabase failed.",
                context=f"Data Source ID: {datasource_id}, Error: {str(http_error)}",
            )
            return None
        except ValidationError as e:
            self.report.report_warning(
                title="Invalid Database Data",
                message="Database data from Metabase API failed validation.",
                context=f"Database ID: {datasource_id}, Error: {str(e)}",
            )
            return None

        engine = database.engine

        engine_mapping = dict(METABASE_ENGINE_TO_DATAHUB_PLATFORM)
        if self.config.engine_platform_map is not None:
            engine_mapping.update(self.config.engine_platform_map)

        if engine in engine_mapping:
            platform = engine_mapping[engine]
        else:
            platform = engine
            self.report.report_warning(
                title="Unrecognized Data Platform found",
                message="Data Platform was not found. Using platform name as is",
                context=f"Platform: {platform}",
            )

        platform_instance = self.get_platform_instance(
            platform=platform, datasource_id=database.id
        )

        dbname = (
            database.details.get_database_name(engine) if database.details else None
        )
        schema = database.details.schema_ if database.details else None

        if (
            self.config.database_alias_map is not None
            and platform in self.config.database_alias_map
        ):
            dbname = self.config.database_alias_map[platform]

        if dbname is None:
            self.report.report_warning(
                title="Cannot resolve Database Name",
                message="Cannot determine database name for platform",
                context=f"Platform: {platform}",
            )

        return DatasourceInfo(
            platform=platform,
            database_name=dbname,
            schema=schema,
            platform_instance=platform_instance,
        )

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def emit_model_workunits(self) -> Iterable[MetadataWorkUnit]:
        if not self.config.extract_models:
            return

        try:
            card_response = self.session.get(f"{self.config.connect_uri}/api/card")
            card_response.raise_for_status()
            cards = card_response.json()

            for card_data in cards:
                try:
                    card_info = MetabaseCardListItem.model_validate(card_data)
                    if not card_info.is_model:
                        continue

                    yield from self._emit_model_workunits(card_info)
                except ValidationError as e:
                    self.report.report_warning(
                        title="Invalid Model List Item",
                        message="Model list item failed validation.",
                        context=f"Error: {str(e)}",
                    )

        except HTTPError as http_error:
            self.report.report_failure(
                title="Unable to Retrieve Models",
                message="Request to retrieve models from Metabase failed.",
                context=f"Error: {str(http_error)}",
            )

    def _map_metabase_type_to_datahub_type(
        self, metabase_type: str
    ) -> Union[
        NumberTypeClass,
        StringTypeClass,
        BooleanTypeClass,
        EnumTypeClass,
        BytesTypeClass,
        DateTypeClass,
        TimeTypeClass,
        NullTypeClass,
    ]:
        """Map Metabase base_type (e.g. "type/Integer") to DataHub type."""
        for type_keyword, datahub_class in METABASE_TYPE_TO_DATAHUB_TYPE.items():
            if type_keyword in metabase_type:
                return datahub_class()

        return NullTypeClass()

    def _get_schema_fields_from_result_metadata(
        self, result_metadata: List[MetabaseResultMetadata]
    ) -> List[SchemaFieldClass]:
        schema_fields: List[SchemaFieldClass] = []

        for field_meta in result_metadata:
            field_name = field_meta.name or field_meta.display_name or ""
            base_type = field_meta.base_type or ""
            display_name = field_meta.display_name or field_name

            if not field_name:
                continue

            data_type = self._map_metabase_type_to_datahub_type(base_type)

            schema_field = SchemaFieldClass(
                fieldPath=field_name,
                type=SchemaFieldDataTypeClass(type=data_type),
                nativeDataType=base_type,
                description=display_name,
                nullable=True,
            )
            schema_fields.append(schema_field)

        return schema_fields

    def _emit_model_workunits(
        self, card_info: MetabaseCardListItem
    ) -> Iterable[MetadataWorkUnit]:
        card = self.get_card_details_by_id(card_info.id)
        if not card:
            return

        model_urn = builder.make_dataset_urn(
            platform="metabase",
            name=f"model.{card.id}",
            env=self.config.env,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=DatasetPropertiesClass(
                name=card.name,
                description=card.description or "",
                customProperties={
                    "model_id": str(card.id),
                    "display_type": card.display or "",
                    "metabase_url": f"{self.config.display_uri}/model/{card.id}",
                },
            ),
        ).as_workunit()

        if card.result_metadata:
            schema_fields = self._get_schema_fields_from_result_metadata(
                card.result_metadata
            )
            if schema_fields:
                yield MetadataChangeProposalWrapper(
                    entityUrn=model_urn,
                    aspect=SchemaMetadataClass(
                        schemaName=card.name,
                        platform=f"urn:li:dataPlatform:{self.platform}",
                        version=0,
                        hash="",
                        platformSchema=MySqlDDLClass(tableSchema=""),
                        fields=schema_fields,
                    ),
                ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=SubTypesClass(typeNames=["Model", "View"]),
        ).as_workunit()

        if card.dataset_query and card.query_type == _QUERY_TYPE_NATIVE:
            raw_query = self._extract_native_query(card)
            if raw_query:
                yield MetadataChangeProposalWrapper(
                    entityUrn=model_urn,
                    aspect=ViewPropertiesClass(
                        materialized=False,
                        viewLogic=raw_query,
                        viewLanguage="SQL",
                    ),
                ).as_workunit()

        if card.dataset_query and card.dataset_query.type == _QUERY_TYPE_QUERY:
            cll = self._get_cll_from_query_builder(card=card, entity_urn=model_urn)
            if cll:
                yield MetadataChangeProposalWrapper(
                    entityUrn=model_urn,
                    aspect=cll,
                ).as_workunit()
        else:
            table_urns = self._get_table_urns_from_card(card)
            if table_urns:
                yield MetadataChangeProposalWrapper(
                    entityUrn=model_urn,
                    aspect=UpstreamLineageClass(
                        upstreams=[
                            UpstreamClass(
                                dataset=table_urn,
                                type=DatasetLineageTypeClass.TRANSFORMED,
                            )
                            for table_urn in table_urns
                        ]
                    ),
                ).as_workunit()

        if card.creator_id:
            ownership = self._get_ownership(card.creator_id)
            if ownership is not None:
                yield MetadataChangeProposalWrapper(
                    entityUrn=model_urn,
                    aspect=ownership,
                ).as_workunit()

        tags = self._get_tags_from_collection(card.collection_id)
        if tags:
            yield MetadataChangeProposalWrapper(
                entityUrn=model_urn,
                aspect=tags,
            ).as_workunit()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_chart_workunits()
        yield from self.emit_dashboard_workunits()
        yield from self.emit_model_workunits()

    def get_report(self) -> SourceReport:
        return self.report
