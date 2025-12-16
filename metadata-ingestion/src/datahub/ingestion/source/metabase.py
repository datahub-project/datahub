import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Pattern, Tuple, Type, Union

import dateutil.parser as dp
import pydantic
import requests
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
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


# ============================================================================
# Pydantic Models for Metabase API Responses
# ============================================================================


class MetabaseBaseModel(BaseModel):
    """Base model for Metabase API responses with common config."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class MetabaseUser(MetabaseBaseModel):
    """Metabase user model."""

    id: int
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    common_name: Optional[str] = None


class MetabaseResultMetadata(MetabaseBaseModel):
    """Metadata for query result fields."""

    name: Optional[str] = None
    display_name: Optional[str] = None
    base_type: Optional[str] = None
    semantic_type: Optional[str] = None
    field_ref: Optional[List] = None  # Heterogeneous list: ["field", "name", {...}]


class MetabaseNativeQuery(MetabaseBaseModel):
    """Native SQL query configuration."""

    query: Optional[str] = None
    template_tags: Optional[Dict] = Field(
        None, alias="template-tags"
    )  # Metabase template tag definitions


class MetabaseQuery(MetabaseBaseModel):
    """Query builder query configuration."""

    source_table: Optional[Union[int, str]] = Field(None, alias="source-table")
    filter: Optional[List] = None  # Metabase filter expression
    aggregation: Optional[List] = None  # Metabase aggregation expression
    breakout: Optional[List] = None  # Metabase breakout (group by) fields


class MetabaseDatasetQuery(MetabaseBaseModel):
    """Dataset query configuration."""

    type: str
    database: Optional[int] = None
    native: Optional[MetabaseNativeQuery] = None
    query: Optional[MetabaseQuery] = None


class MetabaseLastEditInfo(MetabaseBaseModel):
    """Information about last edit of a Metabase object."""

    id: Optional[int] = None
    email: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    timestamp: Optional[str] = None


class MetabaseCard(MetabaseBaseModel):
    """Metabase card (question/chart/model) model."""

    id: int
    name: str
    description: Optional[str] = None
    display: Optional[str] = None
    dataset_query: MetabaseDatasetQuery = Field(
        default_factory=lambda: MetabaseDatasetQuery(type="native")
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


class MetabaseCardInfo(MetabaseBaseModel):
    """Basic card info (used in dashcards)."""

    id: Optional[int] = None
    name: Optional[str] = None


class MetabaseDashCard(MetabaseBaseModel):
    """Dashboard card (panel) in a dashboard."""

    id: int
    card: MetabaseCardInfo
    dashboard_id: int


class MetabaseDashboard(MetabaseBaseModel):
    """Metabase dashboard model."""

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


class MetabaseCollectionItemsResponse(MetabaseBaseModel):
    """Response from /api/collection/{id}/items API."""

    data: List[MetabaseDashboardListItem] = Field(default_factory=list)
    total: Optional[int] = None


class MetabaseCollection(MetabaseBaseModel):
    """Metabase collection model."""

    id: int
    name: str
    slug: Optional[str] = None
    description: Optional[str] = None
    archived: Optional[bool] = None


class MetabaseLoginResponse(MetabaseBaseModel):
    """Response from /api/session API."""

    id: str  # Session token


class MetabaseDatabaseDetails(MetabaseBaseModel):
    """Database connection details."""

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


class MetabaseDatabase(MetabaseBaseModel):
    """Metabase database connection model."""

    id: int
    name: str
    engine: str
    details: MetabaseDatabaseDetails = Field(default_factory=MetabaseDatabaseDetails)


class MetabaseTable(MetabaseBaseModel):
    """Metabase table metadata."""

    id: int
    name: str
    display_name: Optional[str] = None
    schema_: Optional[str] = Field(None, alias="schema")
    db_id: int


class DatasourceInfo(MetabaseBaseModel):
    """Information about a datasource extracted from Metabase database."""

    platform: str
    database_name: Optional[str] = None
    schema: Optional[str] = None  # type: ignore[assignment]
    platform_instance: Optional[str] = None


# ============================================================================
# Constants
# ============================================================================

# Prevent stack overflow from circular card references
DATASOURCE_URN_RECURSION_LIMIT = 5

# Maps Metabase base_type to DataHub types
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

# Maps Metabase database engine to the connection detail field containing the database name
METABASE_DATABASE_FIELD_MAPPING: Dict[str, str] = {
    "athena": "catalog",
    "bigquery": "dataset_id",  # Python attribute (JSON: "dataset-id")
    "bigquery-cloud-sdk": "project_id",  # Python attribute (JSON: "project-id")
    "clickhouse": "dbname",
    "databricks": "catalog",
    "druid": "dbname",
    "h2": "db",
    "mongo": "dbname",
    "mysql": "dbname",
    "oracle": "service_name",  # Python attribute (JSON: "service-name")
    "postgres": "dbname",
    "presto": "catalog",
    "presto-jdbc": "catalog",
    "redshift": "db",
    "snowflake": "db",
    "sparksql": "dbname",
    "sqlserver": "db",
    "trino": "catalog",
    "vertica": "database",
}

# Pre-compiled regex patterns for collection name sanitization
_SPECIAL_CHARS_PATTERN: Pattern[str] = re.compile(r"[^a-zA-Z0-9_]")
_MULTIPLE_UNDERSCORES_PATTERN: Pattern[str] = re.compile(r"_+")


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
    password: Optional[pydantic.SecretStr] = Field(
        default=None,
        description="Metabase password, used when an API key is not provided.",
    )

    # https://www.metabase.com/learn/metabase-basics/administration/administration-and-operation/metabase-api#example-get-request
    api_key: Optional[pydantic.SecretStr] = Field(
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
    """Extracts dashboards, charts, models, and lineage from Metabase."""

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

    def emit_dashboard_mces(self) -> Iterable[MetadataWorkUnit]:
        """Emit dashboard entities using MCPs."""
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
                except ValidationError:
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
        """
        Converts the given timestamp string to milliseconds. If parsing fails,
        returns the utc-now in milliseconds.
        """
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
        """Emit workunits for a Metabase Dashboard using MCPs."""
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

        dashboard_urn = builder.make_dashboard_urn(self.platform, str(dashboard.id))

        # Handle last_edit_info
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
            chart_urn = builder.make_chart_urn(self.platform, str(card_id))
            chart_edges.append(
                EdgeClass(
                    destinationUrn=chart_urn,
                    lastModified=last_modified.lastModified,
                )
            )

        dataset_edges = self.construct_dashboard_lineage(
            dashboard, last_modified.lastModified
        )

        # Emit dashboard info
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

        # Emit ownership
        if dashboard.creator_id:
            ownership = self._get_ownership(dashboard.creator_id)
            if ownership is not None:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dashboard_urn,
                    aspect=ownership,
                ).as_workunit()

        # Emit collection tags
        tags = self._get_tags_from_collection(dashboard.collection_id)
        if tags:
            yield MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn,
                aspect=tags,
            ).as_workunit()

    def construct_dashboard_lineage(
        self, dashboard: MetabaseDashboard, last_modified: AuditStampClass
    ) -> Optional[List[EdgeClass]]:
        """
        Construct dashboard lineage by extracting table dependencies from all charts in the dashboard.
        This creates lineage from database tables to the dashboard using datasetEdges.
        """
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
        """Check if recursion limit is exceeded and report warning if so."""
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
        """
        Extract table URNs from a card, handling both native queries and query builder queries.
        Handles nested cards (cards referencing other cards) with recursion depth tracking.
        """
        if self._check_recursion_limit(recursion_depth, "card lineage", card.id):
            return []

        if not card.dataset_query:
            return []

        query_type = card.dataset_query.type

        if query_type == "native":
            return self._get_table_urns_from_native_query(card)
        elif query_type == "query":
            return self._get_table_urns_from_query_builder(card, recursion_depth)

        return []

    def _extract_native_query(self, card: MetabaseCard) -> Optional[str]:
        """Extract native SQL query from card details."""
        if card.dataset_query and card.dataset_query.native:
            return card.dataset_query.native.query
        return None

    def _get_table_urns_from_native_query(self, card: MetabaseCard) -> List[str]:
        """Extract table URNs from a native SQL query by parsing the SQL."""
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
            default_db=datasource.database_name,
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
        Extract table URNs from a query builder query by getting the source table ID.
        Handles nested card references with recursion depth tracking.

        Metabase's query builder allows cards to use other cards as data sources:
        - Direct table reference: source-table: 123 (table ID)
        - Card reference: source-table: "card__456" (references another card by ID)
        """
        if not card.dataset_query or not card.dataset_query.query:
            return []

        source_table_id = card.dataset_query.query.source_table

        if not source_table_id:
            return []

        source_table_str = str(source_table_id)
        if source_table_str.startswith("card__"):
            referenced_card_id = source_table_str.replace("card__", "")
            referenced_card = self.get_card_details_by_id(referenced_card_id)
            if referenced_card:
                return self._get_table_urns_from_card(
                    referenced_card, recursion_depth + 1
                )
            return []

        if not card.database_id:
            return []

        datasource = self.get_datasource_from_id(card.database_id)
        if not datasource or not datasource.platform:
            return []

        schema_name, table_name = self.get_source_table_from_id(source_table_id)

        if not table_name:
            return []

        name_components = [datasource.database_name, schema_name, table_name]
        table_urn = builder.make_dataset_urn_with_platform_instance(
            platform=datasource.platform,
            name=".".join([v for v in name_components if v]),
            platform_instance=datasource.platform_instance,
            env=self.config.env,
        )

        return [table_urn]

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
        """
        Fetch all collections once and return as a dict keyed by collection ID.
        Cached to avoid N+1 API calls when extracting tags for multiple entities.
        """
        try:
            collections_response = self.session.get(
                f"{self.config.connect_uri}/api/collection/"
                f"?exclude-other-user-collections={json.dumps(self.config.exclude_other_user_collections)}"
            )
            collections_response.raise_for_status()
            collections_data = collections_response.json()

            # Parse each collection
            collections_dict = {}
            for coll_data in collections_data:
                try:
                    coll = MetabaseCollection.model_validate(coll_data)
                    collections_dict[str(coll.id)] = coll
                except ValidationError:
                    # Skip invalid collections
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

    def _sanitize_collection_name(self, collection_name: str) -> str:
        """
        Sanitize collection name for use in tag URNs.
        Removes special characters that are invalid in tag URNs.
        """
        sanitized = collection_name.replace(" ", "_")
        sanitized = _SPECIAL_CHARS_PATTERN.sub("", sanitized)
        sanitized = sanitized.lower()
        sanitized = _MULTIPLE_UNDERSCORES_PATTERN.sub("_", sanitized)
        sanitized = sanitized.strip("_")
        return sanitized

    def _get_tags_from_collection(
        self, collection_id: Optional[Union[int, str]]
    ) -> Optional[GlobalTagsClass]:
        """
        Extract tags from a Metabase collection.
        Maps collection names to DataHub tags for better organization and searchability.
        Uses cached collection map for O(1) lookup instead of O(n) linear search.
        """
        if not self.config.extract_collections_as_tags or not collection_id:
            return None

        collections_map = self._get_collections_map()
        collection = collections_map.get(str(collection_id))

        if not collection:
            logger.debug(
                f"Collection {collection_id} not found in available collections"
            )
            return None

        collection_name = self._sanitize_collection_name(collection.name)
        if not collection_name:
            logger.debug(
                f"Collection {collection_id} has empty name after sanitization"
            )
            return None

        tag_urn = builder.make_tag_urn(f"metabase_collection_{collection_name}")

        return GlobalTagsClass(tags=[TagAssociationClass(tag=tag_urn)])

    def emit_card_mces(self) -> Iterable[MetadataWorkUnit]:
        """Emit chart entities for non-model cards using MCPs."""
        try:
            card_response = self.session.get(f"{self.config.connect_uri}/api/card")
            card_response.raise_for_status()
            cards = card_response.json()

            for card_data in cards:
                try:
                    card_info = MetabaseCardListItem.model_validate(card_data)
                    # Models are emitted as datasets by emit_model_mces()
                    if self.config.extract_models and card_info.type == "model":
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
        """Fetch card details by ID from Metabase API."""
        card_url = f"{self.config.connect_uri}/api/card/{card_id}"
        try:
            card_response = self.session.get(card_url)
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
        """Emit workunits for a Metabase Chart using MCPs."""
        # Fetch full card details
        card_details = self.get_card_details_by_id(card_info.id)
        if not card_details:
            return

        chart_urn = builder.make_chart_urn(self.platform, str(card_details.id))

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

        chart_type = self._get_chart_type(card_details.id, card_details.display or "")
        description = card_details.description or ""
        title = card_details.name
        datasource_urn = self.get_datasource_urn(card_details)
        custom_properties = self.construct_card_custom_properties(card_details)

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

        # Emit chart info
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

        # Emit chart query for native SQL
        if card_details.query_type == "native":
            raw_query = self._extract_native_query(card_details)
            if raw_query:
                yield MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=ChartQueryClass(
                        rawQuery=raw_query,
                        type=ChartQueryTypeClass.SQL,
                    ),
                ).as_workunit()

        # Emit ownership
        if card_details.creator_id:
            ownership = self._get_ownership(card_details.creator_id)
            if ownership is not None:
                yield MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=ownership,
                ).as_workunit()

        # Emit collection tags
        tags = self._get_tags_from_collection(card_details.collection_id)
        if tags is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=tags,
            ).as_workunit()

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
                title="Unrecognized Card Type",
                message=f"Unrecognized card type {display_type} found. Setting to None",
                context=f"Card ID: {card_id}",
            )
            return None
        try:
            chart_type = type_mapping[display_type]
        except KeyError:
            self.report.report_warning(
                title="Unrecognized Chart Type",
                message=f"Unrecognized chart type {display_type} found. Setting to None",
                context=f"Card ID: {card_id}",
            )
            chart_type = None

        return chart_type

    def construct_card_custom_properties(self, card: MetabaseCard) -> Dict[str, str]:
        """Construct custom properties from card for chart aspect."""
        result_metadata = card.result_metadata or []
        metrics, dimensions = [], []
        for meta_data in result_metadata:
            display_name = meta_data.display_name or ""
            field_ref = meta_data.field_ref or ""

            if "aggregation" in str(field_ref):
                metrics.append(display_name)
            else:
                dimensions.append(display_name)

        filters = []
        if card.dataset_query and card.dataset_query.query:
            filters = card.dataset_query.query.filter or []

        custom_properties = {
            "Metrics": ", ".join(metrics),
            "Filters": f"{filters}" if filters else "",
            "Dimensions": ", ".join(dimensions),
        }

        return custom_properties

    def get_datasource_urn(
        self, card: MetabaseCard, recursion_depth: int = 0
    ) -> Optional[List[str]]:
        """
        Extract datasource URNs from a card.
        This method now delegates to _get_table_urns_from_card() to avoid code duplication.
        Maintains backward compatibility with existing recursion_depth parameter.
        """
        if self._check_recursion_limit(
            recursion_depth, "datasource URN extraction", card.id
        ):
            return []

        table_urns = self._get_table_urns_from_card(card, recursion_depth)
        return table_urns if table_urns else None if table_urns else None

    @staticmethod
    def strip_template_expressions(raw_query: str) -> str:
        """
        Workarounds for metabase raw queries containing most commonly used template expressions:

        - strip conditional expressions "[[ .... ]]"
        - replace all {{ filter expressions }} with "1"

        reference: https://www.metabase.com/docs/latest/questions/native-editor/sql-parameters
        """

        # Metabase SQL templates use {{variable}} and [[optional clauses]] syntax
        # SQL parsers don't understand these, so we strip them to extract table lineage
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

        # Map engine names to what datahub expects in
        # https://github.com/datahub-project/datahub/blob/master/metadata-service/configuration/src/main/resources/bootstrap_mcps/data-platforms.yaml
        engine = database.engine

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
                title="Unrecognized Data Platform found",
                message="Data Platform was not found. Using platform name as is",
                context=f"Platform: {platform}",
            )

        platform_instance = self.get_platform_instance(platform, database.id)

        # Extract database name and schema from details
        dbname = None
        schema = None
        if database.details and engine in METABASE_DATABASE_FIELD_MAPPING:
            field_name = METABASE_DATABASE_FIELD_MAPPING[engine]
            dbname = getattr(database.details, field_name, None)

        if database.details:
            schema = database.details.schema_

        if (
            self.config.database_alias_map is not None
            and platform in self.config.database_alias_map
        ):
            dbname = self.config.database_alias_map[platform]
        else:
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

    def _is_metabase_model(self, card_info: MetabaseCardListItem) -> bool:
        """
        Check if a card is a Metabase Model.
        Models are special saved questions that can be used as data sources for other questions.
        They act like views - you can query a model instead of the underlying tables.
        Introduced in Metabase v0.41+
        """
        return card_info.type == "model"

    def emit_model_mces(self) -> Iterable[MetadataWorkUnit]:
        """Emit Metabase Models as Dataset entities using MCPs."""
        if not self.config.extract_models:
            return

        try:
            card_response = self.session.get(f"{self.config.connect_uri}/api/card")
            card_response.raise_for_status()
            cards = card_response.json()

            for card_data in cards:
                try:
                    card_info = MetabaseCardListItem.model_validate(card_data)
                    if card_info.type != "model":
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
        """Extract schema fields from Metabase result_metadata."""
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
        """Emit workunits for a Metabase Model using MCPs."""
        # Fetch full card details
        card = self.get_card_details_by_id(card_info.id)
        if not card:
            return

        model_urn = builder.make_dataset_urn(
            platform="metabase",
            name=f"model.{card.id}",
            env=self.config.env,
        )

        # Emit dataset properties
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

        # Emit schema metadata
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

        # Emit subtypes
        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=SubTypesClass(typeNames=["Model", "View"]),
        ).as_workunit()

        # Emit view properties if SQL query exists
        if card.dataset_query and card.query_type == "native":
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

        # Emit upstream lineage
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

        # Emit ownership
        if card.creator_id:
            ownership = self._get_ownership(card.creator_id)
            if ownership is not None:
                yield MetadataChangeProposalWrapper(
                    entityUrn=model_urn,
                    aspect=ownership,
                ).as_workunit()

        # Emit collection tags
        tags = self._get_tags_from_collection(card.collection_id)
        if tags:
            yield MetadataChangeProposalWrapper(
                entityUrn=model_urn,
                aspect=tags,
            ).as_workunit()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_card_mces()
        yield from self.emit_dashboard_mces()
        yield from self.emit_model_mces()

    def get_report(self) -> SourceReport:
        return self.report
