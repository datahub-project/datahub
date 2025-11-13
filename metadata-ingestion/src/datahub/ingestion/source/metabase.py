import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Tuple, Union

import dateutil.parser as dp
import pydantic
import requests
from pydantic import Field, field_validator, model_validator
from requests.models import HTTPError

import datahub.emitter.mce_builder as builder
from datahub.configuration.source_common import (
    DatasetLineageProviderConfigBase,
    LowerCaseDatasetUrnConfigMixin,
)
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
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    ChangeAuditStamps,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot,
    DashboardSnapshot,
    DatasetSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    ChartQueryClass,
    ChartQueryTypeClass,
    ChartTypeClass,
    DashboardInfoClass,
    DatasetPropertiesClass,
    EdgeClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result
from datahub.utilities import config_clean

logger = logging.getLogger(__name__)

DATASOURCE_URN_RECURSION_LIMIT = 5


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
    """
    This plugin extracts Charts, Dashboards, Models, and their associated metadata from Metabase.
    It supports comprehensive lineage extraction, including table-to-chart, table-to-dashboard, and nested query lineage.

    ### Entities Extracted

    The connector extracts the following Metabase entities into DataHub:
    - **Dashboards** → DataHub Dashboards
    - **Charts (Cards/Questions)** → DataHub Charts
    - **Models** → DataHub Datasets (with "Model" and "View" subtypes)
    - **Collections** → DataHub Tags

    ### Dashboard Extraction

    Dashboards are extracted using the [/api/dashboard](https://www.metabase.com/docs/latest/api/dashboard) endpoint.

    **Extracted Information:**
    - Title and description
    - Owner and last modified user
    - Link to the dashboard in Metabase
    - Associated charts (via chartEdges)
    - **Lineage to upstream database tables** (via datasetEdges)
    - Collection tags for organization

    **Dashboard Lineage:** The connector automatically aggregates table dependencies from all charts within a dashboard,
    creating direct table-to-dashboard lineage. This enables:
    - Impact analysis: See which dashboards are affected when a table changes
    - Data discovery: Find dashboards consuming specific datasets
    - Dependency tracking: Understand the complete data flow

    ### Chart Extraction

    Charts (Metabase Cards/Questions) are extracted using the [/api/card](https://www.metabase.com/docs/latest/api-documentation.html#card) endpoint.

    **Extracted Information:**
    - Title and description
    - Owner and last modified user
    - Link to the chart in Metabase
    - Chart type (bar, line, table, pie, etc.)
    - Source dataset lineage
    - SQL query (for native queries)
    - Collection tags

    **Custom Properties:**

    | Property      | Description                                     |
    | ------------- | ----------------------------------------------- |
    | `Dimensions`  | Column names used in the visualization          |
    | `Filters`     | Any filters applied to the chart                |
    | `Metrics`     | Columns used for aggregation (COUNT, SUM, etc.) |

    ### Lineage Extraction

    The connector provides comprehensive lineage extraction with support for multiple query types:

    #### 1. Native SQL Query Lineage
    - Parses SQL queries using DataHub's SQL parser
    - Extracts table references from SELECT, JOIN, and subqueries
    - Handles Metabase template variables (`{{variable}}`) and optional clauses (`[[WHERE ...]]`)
    - Creates lineage from source tables to charts

    #### 2. Query Builder Lineage
    - Extracts source tables from Metabase's visual query builder
    - Maps `source-table` IDs to actual database tables
    - Creates lineage from tables to charts

    #### 3. Nested Query Lineage
    - Handles charts built on top of other charts (using `card__` references)
    - Recursively resolves the chain to find ultimate source tables
    - Useful for multi-layered analysis workflows

    #### 4. Dashboard-Level Lineage
    - Aggregates lineage from all charts in a dashboard
    - Creates direct table-to-dashboard edges (via `datasetEdges`)
    - Automatically deduplicates tables referenced by multiple charts
    - Enables dashboard-level impact analysis

    #### 5. Model Lineage
    - Extracts upstream table dependencies for Metabase Models
    - Models appear as Dataset entities with lineage to their source tables
    - Supports both SQL-based and query builder-based models

    ### Collection Tags

    Metabase Collections are mapped to DataHub tags for better organization and discoverability:
    - Collection names are automatically converted to tags (e.g., "Sales Dashboard" → `metabase_collection_sales_dashboard`)
    - Tags are applied to dashboards, charts, and models within that collection
    - Enables filtering by collection in DataHub
    - Helps organize assets by team, department, or project

    **Configuration:** Set `extract_collections_as_tags: false` to disable this feature.

    ### Metabase Models

    Metabase Models (introduced in v0.41) are saved questions that can be used as data sources for other questions.
    The connector extracts Models as DataHub Dataset entities:

    **Model as Datasets:**
    - URN format: `urn:li:dataset:(urn:li:dataPlatform:metabase,model.{model_id},PROD)`
    - SubTypes: `["Model", "View"]` to indicate virtual dataset nature
    - ViewProperties: Preserves the underlying SQL query
    - Upstream lineage: Connects models to their source tables
    - Collection tags: Applied based on model's collection membership

    **Why Extract Models:**
    - **Lineage tracking**: See dependencies between models and source tables
    - **Impact analysis**: Understand which models are affected by table changes
    - **Documentation**: Model queries and descriptions are preserved
    - **Discovery**: Find reusable data models in your organization

    **Configuration:** Set `extract_models: false` to disable model extraction.

    ### Collections

    The [/api/collection](https://www.metabase.com/docs/latest/api/collection) endpoint is used to:
    - Retrieve available collections
    - List dashboards within each collection via [/api/collection/{id}/items?models=dashboard](https://www.metabase.com/docs/latest/api/collection#get-apicollectioniditems)
    - Map collections to tags on assets

    ### Configuration Examples

    ```yaml
    # Enable all features (default)
    source:
      type: metabase
      config:
        connect_uri: https://metabase.company.com
        username: datahub_user
        password: secure_password
        extract_collections_as_tags: true  # Map collections to tags
        extract_models: true                # Extract Metabase Models as datasets

    # Disable optional features
    source:
      type: metabase
      config:
        connect_uri: https://metabase.company.com
        api_key: your-api-key              # Recommended over username/password
        extract_collections_as_tags: false  # Don't create tags
        extract_models: false               # Don't extract models
    ```

    ### Compatibility

    - Tested with Metabase v0.41+ (Models require v0.41+)
    - Works with various database backends: PostgreSQL, MySQL, BigQuery, Snowflake, Redshift, and more
    - Supports both username/password and API key authentication (API key recommended)

    """

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
            # If no API key is provided, generate a session token using username and password.
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
            self.access_token = login_response.json().get("id", "")

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
                title="Unable to Retrieve Current User",
                message=f"Unable to retrieve user {self.config.username} information. %s"
                % str(e),
            )

    def close(self) -> None:
        # API key authentication does not require session closure.
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
        try:
            collections_response = self.session.get(
                f"{self.config.connect_uri}/api/collection/"
                f"?exclude-other-user-collections={json.dumps(self.config.exclude_other_user_collections)}"
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
        except (dp.ParserError, OverflowError):
            return int(datetime.now(timezone.utc).timestamp() * 1000)

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
            self.report.report_warning(
                title="Unable to Retrieve Dashboard",
                message="Request to retrieve dashboards from Metabase failed.",
                context=f"Dashboard ID: {dashboard_id}, Error: {str(http_error)}",
            )
            return None

        dashboard_urn = builder.make_dashboard_urn(
            self.platform, str(dashboard_details.get("id", ""))
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

        # Convert chart URNs to chart edges (instead of deprecated charts field)
        chart_edges = []
        cards_data = dashboard_details.get("dashcards", {})
        for card_info in cards_data:
            card_id = card_info.get("card").get("id", "")
            if not card_id:
                continue  # most likely a virtual card without an id (text or heading), not relevant.
            chart_urn = builder.make_chart_urn(self.platform, str(card_id))
            chart_edges.append(
                EdgeClass(
                    destinationUrn=chart_urn,
                    lastModified=last_modified.lastModified,
                )
            )

        # Lineage - extract table dependencies from dashboard charts
        dataset_edges = self.construct_dashboard_lineage(
            dashboard_details, last_modified.lastModified
        )

        dashboard_info_class = DashboardInfoClass(
            description=description,
            title=title,
            chartEdges=chart_edges,
            lastModified=last_modified,
            dashboardUrl=f"{self.config.display_uri}/dashboard/{dashboard_id}",
            customProperties={},
            datasetEdges=dataset_edges if dataset_edges else None,
        )
        dashboard_snapshot.aspects.append(dashboard_info_class)

        # Ownership
        ownership = self._get_ownership(dashboard_details.get("creator_id", ""))
        if ownership is not None:
            dashboard_snapshot.aspects.append(ownership)

        # Tags - extract from collection
        tags = self._get_tags_from_collection(dashboard_details.get("collection_id"))
        if tags is not None:
            dashboard_snapshot.aspects.append(tags)

        return dashboard_snapshot

    def construct_dashboard_lineage(
        self, dashboard_details: dict, last_modified: AuditStamp
    ) -> Optional[List[EdgeClass]]:
        """
        Construct dashboard lineage by extracting table dependencies from all charts in the dashboard.
        This creates lineage from database tables to the dashboard using datasetEdges.
        """
        upstream_tables = []
        cards_data = dashboard_details.get("dashcards", {})

        for card_info in cards_data:
            card_id = card_info.get("card", {}).get("id")
            if not card_id:
                continue

            # Get detailed card information
            card_details = self.get_card_details_by_id(card_id)
            if not card_details:
                continue

            # Extract table URNs from the card
            table_urns = self._get_table_urns_from_card(card_details)
            if table_urns:
                upstream_tables.extend(table_urns)

        # Remove duplicates
        unique_table_urns = list(set(upstream_tables))

        if not unique_table_urns:
            return None

        # Create dataset edges for dashboard lineage
        dataset_edges = [
            EdgeClass(
                destinationUrn=table_urn,
                lastModified=last_modified,
            )
            for table_urn in unique_table_urns
        ]

        return dataset_edges

    def _get_table_urns_from_card(self, card_details: dict) -> List[str]:
        """
        Extract table URNs from a card (question/chart) by analyzing its query.
        Supports both native SQL queries and query builder queries.
        """
        table_urns = []

        query_type = card_details.get("dataset_query", {}).get("type")

        if query_type == "native":
            # Handle native SQL queries
            table_urns = self._get_table_urns_from_native_query(card_details)
        elif query_type == "query":
            # Handle query builder queries
            table_urns = self._get_table_urns_from_query_builder(card_details)

        return table_urns

    def _get_table_urns_from_native_query(self, card_details: dict) -> List[str]:
        """
        Extract table URNs from a native SQL query by parsing the SQL.
        """
        datasource_id = card_details.get("database_id")
        if not datasource_id:
            return []

        (
            platform,
            database_name,
            database_schema,
            platform_instance,
        ) = self.get_datasource_from_id(datasource_id)

        if not platform:
            return []

        raw_query = (
            card_details.get("dataset_query", {}).get("native", {}).get("query", "")
        )

        if not raw_query:
            return []

        # Strip template expressions before parsing
        raw_query_stripped = self.strip_template_expressions(raw_query)

        # Parse SQL to extract table references
        result = create_lineage_sql_parsed_result(
            query=raw_query_stripped,
            default_db=database_name,
            default_schema=database_schema or self.config.default_schema,
            platform=platform,
            platform_instance=platform_instance,
            env=self.config.env,
            graph=self.ctx.graph,
        )

        if result.debug_info.table_error:
            logger.debug(
                f"Failed to parse lineage from query: {result.debug_info.table_error}"
            )

        return result.in_tables if result.in_tables else []

    def _get_table_urns_from_query_builder(self, card_details: dict) -> List[str]:
        """
        Extract table URNs from a query builder query by getting the source table ID.
        """
        source_table_id = (
            card_details.get("dataset_query", {}).get("query", {}).get("source-table")
        )

        if not source_table_id:
            return []

        # Check if this is a nested query (references another card)
        if str(source_table_id).startswith("card__"):
            # Recursively get the source table from the referenced card
            referenced_card_id = source_table_id.replace("card__", "")
            referenced_card = self.get_card_details_by_id(referenced_card_id)
            if referenced_card:
                return self._get_table_urns_from_card(referenced_card)
            return []

        # Get table details
        datasource_id = card_details.get("database_id")
        if not datasource_id:
            return []

        (
            platform,
            database_name,
            database_schema,
            platform_instance,
        ) = self.get_datasource_from_id(datasource_id)

        if not platform:
            return []

        schema_name, table_name = self.get_source_table_from_id(source_table_id)

        if not table_name:
            return []

        # Build the dataset URN
        name_components = [database_name, schema_name, table_name]
        table_urn = builder.make_dataset_urn_with_platform_instance(
            platform=platform,
            name=".".join([v for v in name_components if v]),
            platform_instance=platform_instance,
            env=self.config.env,
        )

        return [table_urn]

    @lru_cache(maxsize=None)
    def _get_ownership(self, creator_id: int) -> Optional[OwnershipClass]:
        user_info_url = f"{self.config.connect_uri}/api/user/{creator_id}"
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
                    title="Cannot find user",
                    message="User is blocked in Metabase or missing",
                    context=f"Creator ID: {creator_id}",
                )
                return None
            # For cases when the error is not 404 but something else
            self.report.report_warning(
                title="Failed to retrieve user",
                message="Request to Metabase Failed",
                context=f"Creator ID: {creator_id}, Error: {str(http_error)}",
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

    def _get_tags_from_collection(
        self, collection_id: Optional[Union[int, str]]
    ) -> Optional[GlobalTagsClass]:
        """
        Extract tags from a Metabase collection.
        Maps collection names to DataHub tags for better organization and searchability.
        """
        if not self.config.extract_collections_as_tags or not collection_id:
            return None

        # Find the collection by ID
        collection = None
        try:
            collections_response = self.session.get(
                f"{self.config.connect_uri}/api/collection/"
                f"?exclude-other-user-collections={json.dumps(self.config.exclude_other_user_collections)}"
            )
            collections_response.raise_for_status()
            collections = collections_response.json()

            # Find matching collection
            for coll in collections:
                if str(coll.get("id")) == str(collection_id):
                    collection = coll
                    break

        except HTTPError as http_error:
            logger.debug(
                f"Failed to retrieve collection {collection_id}: {str(http_error)}"
            )
            return None

        if not collection:
            return None

        # Create tag URN from collection name
        # Sanitize collection name for use in tag
        collection_name = collection.get("name", "").replace(" ", "_").lower()
        tag_urn = builder.make_tag_urn(f"metabase_collection_{collection_name}")

        return GlobalTagsClass(tags=[TagAssociationClass(tag=tag_urn)])

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
                title="Unable to Retrieve Cards",
                message="Request to retrieve cards from Metabase failed.",
                context=f"Error: {str(http_error)}",
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
        card_url = f"{self.config.connect_uri}/api/card/{card_id}"
        try:
            card_response = self.session.get(card_url)
            card_response.raise_for_status()
            return card_response.json()
        except HTTPError as http_error:
            self.report.report_warning(
                title="Unable to Retrieve Card",
                message="Request to retrieve Card from Metabase failed.",
                context=f"Card ID: {card_id}, Error: {str(http_error)}",
            )
            return {}

    def construct_card_from_api_data(self, card_data: dict) -> Optional[ChartSnapshot]:
        card_id = card_data.get("id")
        if card_id is None:
            self.report.report_warning(
                title="Card is missing 'id'",
                message="Unable to get field id from card data.",
                context=f"Card Details: {str(card_data)}",
            )
            return None

        card_details = self.get_card_details_by_id(card_id)
        if not card_details:
            self.report.report_warning(
                title="Missing Card Details",
                message="Unable to construct Card due to empty card details",
                context=f"Card ID: {card_id}",
            )
            return None

        chart_urn = builder.make_chart_urn(self.platform, str(card_id))
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
            created=None,
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )

        chart_type = self._get_chart_type(card_id, card_details.get("display") or "")
        description = card_details.get("description") or ""
        title = card_details.get("name") or ""
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

        chart_info = ChartInfoClass(
            type=chart_type,
            description=description,
            title=title,
            lastModified=last_modified,
            chartUrl=f"{self.config.display_uri}/card/{card_id}",
            inputEdges=input_edges,
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

        # Tags - extract from collection
        tags = self._get_tags_from_collection(card_details.get("collection_id"))
        if tags is not None:
            chart_snapshot.aspects.append(tags)

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

    def construct_card_custom_properties(self, card_details: dict) -> Dict:
        result_metadata = card_details.get("result_metadata") or []
        metrics, dimensions = [], []
        for meta_data in result_metadata:
            display_name = meta_data.get("display_name", "") or ""
            (
                metrics.append(display_name)
                if "aggregation" in meta_data.get("field_ref", "")
                else dimensions.append(display_name)
            )

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
                title="Unable to Retrieve Card Info",
                message="Unable to retrieve Card info. Source table recursion depth exceeded.",
                context=f"Card Details: {card_details}",
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
                title="Unable to find Data Platform",
                message="Unable to detect Data Platform for database id",
                context=f"Data Source ID: {datasource_id}",
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
            raw_query_stripped = self.strip_template_expressions(
                card_details.get("dataset_query", {}).get("native", {}).get("query", "")
            )

            result = create_lineage_sql_parsed_result(
                query=raw_query_stripped,
                default_db=database_name,
                default_schema=database_schema or self.config.default_schema,
                platform=platform,
                platform_instance=platform_instance,
                env=self.config.env,
                graph=self.ctx.graph,
            )
            if result.debug_info.table_error:
                logger.info(
                    f"Failed to parse lineage from query {raw_query_stripped}: "
                    f"{result.debug_info.table_error}"
                )
                self.report.report_warning(
                    title="Failed to Extract Lineage",
                    message="Unable to retrieve lineage from query",
                    context=f"Query: {raw_query_stripped}",
                )
            return result.in_tables

        return None

    @staticmethod
    def strip_template_expressions(raw_query: str) -> str:
        """
        Workarounds for metabase raw queries containing most commonly used template expressions:

        - strip conditional expressions "[[ .... ]]"
        - replace all {{ filter expressions }} with "1"

        reference: https://www.metabase.com/docs/latest/questions/native-editor/sql-parameters
        """

        # drop [[ WHERE {{FILTER}} ]]
        query_patched = re.sub(r"\[\[.+?\]\]", r" ", raw_query)

        # replace {{FILTER}} with 1
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
            dataset_json = dataset_response.json()
            schema = dataset_json.get("schema", "")
            name = dataset_json.get("name", "")
            return schema, name

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
                title="Unable to Retrieve Data Source",
                message="Request to retrieve data source from Metabase failed.",
                context=f"Data Source ID: {datasource_id}, Error: {str(http_error)}",
            )
            # returning empty string as `platform` because
            # `make_dataset_urn_with_platform_instance()` only accepts `str`
            return "", None, None, None

        # Map engine names to what datahub expects in
        # https://github.com/datahub-project/datahub/blob/master/metadata-service/configuration/src/main/resources/bootstrap_mcps/data-platforms.yaml
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
                title="Unrecognized Data Platform found",
                message="Data Platform was not found. Using platform name as is",
                context=f"Platform: {platform}",
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
                title="Cannot resolve Database Name",
                message="Cannot determine database name for platform",
                context=f"Platform: {platform}",
            )

        return platform, dbname, schema, platform_instance

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def _is_metabase_model(self, card_details: dict) -> bool:
        """
        Check if a card is a Metabase Model.
        Models are special saved questions that can be used as data sources.
        Introduced in Metabase v0.41+
        """
        return card_details.get("type") == "model"

    def emit_model_mces(self) -> Iterable[MetadataWorkUnit]:
        """
        Emit Metabase Models as Dataset entities.
        Models are saved questions that can be used as data sources for other questions.
        """
        if not self.config.extract_models:
            return

        try:
            card_response = self.session.get(f"{self.config.connect_uri}/api/card")
            card_response.raise_for_status()
            cards = card_response.json()

            for card_info in cards:
                card_id = card_info.get("id")
                if card_id is None:
                    continue

                # Get full card details
                card_details = self.get_card_details_by_id(card_id)
                if not card_details:
                    continue

                # Only process if it's a model
                if not self._is_metabase_model(card_details):
                    continue

                dataset_snapshot = self.construct_model_from_api_data(card_details)
                if dataset_snapshot is not None:
                    mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                    yield MetadataWorkUnit(id=dataset_snapshot.urn, mce=mce)

        except HTTPError as http_error:
            self.report.report_failure(
                title="Unable to Retrieve Models",
                message="Request to retrieve models from Metabase failed.",
                context=f"Error: {str(http_error)}",
            )

    def construct_model_from_api_data(
        self, card_details: dict
    ) -> Optional[DatasetSnapshot]:
        """
        Construct a Dataset entity from a Metabase Model.
        Models are virtual datasets created from saved questions.
        """
        card_id = card_details.get("id")
        if card_id is None:
            return None

        # Create dataset URN for the model
        # Use a special platform name to distinguish models from real tables
        model_urn = builder.make_dataset_urn(
            platform="metabase",
            name=f"model.{card_id}",
            env=self.config.env,
        )

        dataset_snapshot = DatasetSnapshot(
            urn=model_urn,
            aspects=[],
        )

        # Dataset properties
        dataset_properties = DatasetPropertiesClass(
            name=card_details.get("name", ""),
            description=card_details.get("description", ""),
            customProperties={
                "model_id": str(card_id),
                "display_type": card_details.get("display", ""),
                "metabase_url": f"{self.config.display_uri}/model/{card_id}",
            },
        )
        dataset_snapshot.aspects.append(dataset_properties)

        # Subtype to indicate this is a model/view
        subtypes = SubTypesClass(typeNames=["Model", "View"])
        dataset_snapshot.aspects.append(subtypes)  # type: ignore

        # View properties with the underlying query
        if card_details.get("dataset_query"):
            raw_query = None
            if card_details.get("query_type") == "native":
                raw_query = (
                    card_details.get("dataset_query", {})
                    .get("native", {})
                    .get("query", "")
                )

            if raw_query:
                view_properties = ViewPropertiesClass(
                    materialized=False,
                    viewLogic=raw_query,
                    viewLanguage="SQL",
                )
                dataset_snapshot.aspects.append(view_properties)

        # Upstream lineage - tables this model depends on
        table_urns = self._get_table_urns_from_card(card_details)
        if table_urns:
            upstream_lineage = UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=table_urn,
                        type="TRANSFORMED",
                    )
                    for table_urn in table_urns
                ]
            )
            dataset_snapshot.aspects.append(upstream_lineage)

        # Ownership
        ownership = self._get_ownership(card_details.get("creator_id", ""))
        if ownership is not None:
            dataset_snapshot.aspects.append(ownership)

        # Tags from collection
        tags = self._get_tags_from_collection(card_details.get("collection_id"))
        if tags is not None:
            dataset_snapshot.aspects.append(tags)

        return dataset_snapshot

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_card_mces()
        yield from self.emit_dashboard_mces()
        yield from self.emit_model_mces()

    def get_report(self) -> SourceReport:
        return self.report
