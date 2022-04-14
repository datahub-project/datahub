import json
import re
import unicodedata
from datetime import datetime
from functools import lru_cache
from typing import Dict, Iterable, Optional, List, Union, Any
from collections import namedtuple

import dateutil.parser as dp
import requests
from requests.adapters import HTTPAdapter, Retry
from pydantic import validator
from requests.models import HTTPError
from sqllineage.runner import LineageRunner

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp_builder import gen_containers, add_entity_to_container, FolderKey
from datahub.configuration.source_common import DatasetLineageProviderConfigBase
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    ChangeAuditStamps,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot,
    DashboardSnapshot,
    DataPlatformSnapshot
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    ChartInfoClass,
    ChartQueryClass,
    ChartQueryTypeClass,
    ChartTypeClass,
    DashboardInfoClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    DataPlatformInfoClass,
    ContainerPropertiesClass,
    ContainerClass,
    DataPlatformInstanceClass
)
from datahub.utilities import config_clean


FORMAT_STRING_REGEX_PATTERN = re.compile("[\[\]\.\-\(\)\s\/\,\"\'\+\!\?\=\*]+")

# create cache for card query lineage parsing
# Se usage on 'get_lineage_from_card_details'
query_parser_cache = dict()

#'collection_containers' will serve as a repository for containers that will be created based on Metabase collections.
# The key value pair has the following format:
# { <collection name>: {
#   "container_urn": <urn of the container>,
#   "container_properties": <object of ContainerPropertiesClass>,
#   "container_plataform_instance": <object of DataPlatformInstanceClass>
# }
# See '_create_collection_container' for more details
collection_containers = dict()

# Element used to store relationships of Dashboard and cards with created containers
# See 'generate_dashboard_urn' or 'generate_card_urn' for more details
add_entity_to_collection_containers = list()


class MetabaseConfig(DatasetLineageProviderConfigBase):
    # See the Metabase /api/session endpoint for details
    # https://www.metabase.com/docs/latest/api-documentation.html#post-apisession
    connect_uri: str = "localhost:3000"
    username: Optional[str] = None
    password: Optional[str] = None
    database_alias_map: Optional[dict] = None
    engine_platform_map: Optional[Dict[str, str]] = None
    default_schema: str = "public"
    inherit_collection: bool = False
    ignore_collection_patterns: Optional[List[str]] = None

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
        self.access_token = login_response.json().get("id", "")

        self.session = requests.session()
        session_adapter = HTTPAdapter(
            max_retries=Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[502, 503, 504],
                method_whitelist=["GET", "POST", "PUT"]
            )
        )
        self.session.mount('http://', session_adapter)
        self.session.mount('https://', session_adapter)
        self.session.headers.update(
            {
                "X-Metabase-Session": f"{self.access_token}",
                "Content-Type": "application/json",
                "Accept": "*/*",
                "User-Agent": "datahub/metadata-ingestion"
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

    def _create_collection_container(self, collection_id: int, collection_details: Dict[str, Any]):
        """
        Create a Container object from the definition of a Collection in Metabase
        Args:
            collection_id (int): The ID of a collection
            collection_details (dict): The collection details returned by the API
        Returns:
            No return - The function saves the Containers created in an object to be emitted later
        """
        collection_name = self._format_string(collection_details.get("name", ""))
        ancestors = [self._format_string(ancestor.get("name", "")) for ancestor in
                     collection_details.get("effective_ancestors", [])
                     if ancestor.get("id") != "root"]
        if not collection_name in collection_containers.keys():
            if ancestors:
                collection_path = "/".join(ancestors) + f"/{collection_name}"
            else:
                collection_path = collection_name
            folder_key = FolderKey(
                folder_abs_path=collection_path,
                platform=self.platform
            )
            container_urn = builder.make_container_urn(folder_key.guid())
            container_properties = ContainerPropertiesClass(
                name=collection_details.get("name", ""),
                description=collection_details.get("description", "") or "",
                externalUrl=f"{self.config.connect_uri}/collection/{collection_id}-{collection_name}"
            )
            container_plataform_instance = DataPlatformInstanceClass(
                platform=f"{builder.make_data_platform_urn(self.platform)}"
            )
            collection_containers[collection_name] = {
                "container_urn": container_urn,
                "container_properties": container_properties,
                "container_plataform_instance": container_plataform_instance
            }
            if ancestors:
                collection_containers[collection_name].update({
                    "parent": ancestors[-1]
                })

    def emit_containers_mces(self) -> Iterable[MetadataWorkUnit]:
        for collection, container in collection_containers.items():
            container_urn = container["container_urn"]
            mcp = MetadataChangeProposalWrapper(
                entityType="container",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=container_urn,
                aspectName="containerProperties",
                aspect=container["container_properties"]
            )
            wu = MetadataWorkUnit(id=f"container-info-{collection}-{container_urn}", mcp=mcp)
            yield wu
            mcp = MetadataChangeProposalWrapper(
                entityType="container",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=container["container_urn"],
                aspectName="dataPlatformInstance",
                aspect=container["container_plataform_instance"]
            )
            wu = MetadataWorkUnit(
                id=f"container-platform-instance-{collection}-{container_urn}", mcp=mcp
            )
            yield wu
            if container.get("parent"):
                parent_container = collection_containers.get(container.get("parent"), None)
                if parent_container:
                    parent_container_urn = parent_container["container_urn"]
                    parent_container_mcp = MetadataChangeProposalWrapper(
                        entityType="container",
                        changeType=ChangeTypeClass.UPSERT,
                        entityUrn=container_urn,
                        aspectName="container",
                        aspect=ContainerClass(container=parent_container_urn),
                    )
                    wu = MetadataWorkUnit(
                        id=f"container-parent-container-{collection}-{container_urn}-{parent_container_urn}",
                        mcp=parent_container_mcp
                    )
                    yield wu

    def emit_container_elements_mce(self) -> Iterable[MetadataWorkUnit]:
        for element in add_entity_to_collection_containers:
            mcp = MetadataChangeProposalWrapper(
                entityType=element.get("entity"),
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=element.get("entityUrn"),
                aspectName="container",
                aspect=element.get("aspect")
            )
            wu = MetadataWorkUnit(id=f"container-{element.get('aspect')}-to-{element.get('entityUrn')}", mcp=mcp)
            yield wu

    def emit_platform_mce(self) -> Iterable[MetadataWorkUnit]:
        platform_snapshot = DataPlatformSnapshot(
            builder.make_data_platform_urn(self.platform),
            aspects=[]
        )
        platform_snapshot.aspects.append(
            DataPlatformInfoClass(
                name="Metabase",
                logoUrl="https://www.metabase.com/images/logo.svg",
                displayName="Metabase",
                type="QUERY_ENGINE",
                datasetNameDelimiter="/"
            )
        )
        mce = MetadataChangeEvent(proposedSnapshot=platform_snapshot)
        wu = MetadataWorkUnit(id=platform_snapshot.urn, mce=mce)
        yield wu


    def emit_dashboard_mces(self) -> Iterable[MetadataWorkUnit]:
        try:
            dashboard_response = self.session.get(
                f"{self.config.connect_uri}/api/dashboard"
            )
            dashboard_response.raise_for_status()
            dashboards = dashboard_response.json()
            for dashboard_info in dashboards:
                dashboard_snapshot = self.construct_dashboard_from_api_data(
                    dashboard_info
                )
                if dashboard_snapshot is not None:
                    mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
                    wu = MetadataWorkUnit(id=dashboard_snapshot.urn, mce=mce)
                    self.report.report_workunit(wu)
                    yield wu

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
            return int(datetime.utcnow().timestamp() * 1000)

    @lru_cache(maxsize=None)
    def _check_colletion_ignore_pattern(self, name: str) -> bool:
        """
        Checks if a certain collection has been configured to be ignored
        """
        if self.config.ignore_collection_patterns:
            for pattern in self.config.ignore_collection_patterns:
                if re.match(pattern, name):
                    return True
                else:
                    return False
        else:
            return False

    @lru_cache(maxsize=None)
    def _format_string(self, name: str) -> str:
        """
        URNs are used as BrowserPaths when the 'datasetNameDelimiter' parameter is declared in the platform.
        So that there are no invalid paths, it is necessary to clean the strings that will be used as URN.
        Args:
            name (str): name/title of a dashboard or chart
        Returns:
            (str) - Returns the formatted name/title (or any other string)
        """
        def strip_accents(s):
            return ''.join(c for c in unicodedata.normalize('NFD', s)
                           if unicodedata.category(c) != 'Mn')
        # removes accents
        name = strip_accents(name)
        # removes emojis from name
        name = name.encode('ascii', 'ignore').decode('ascii')
        # removes any other special characters
        name = re.sub(FORMAT_STRING_REGEX_PATTERN, "_", name)
        if name.startswith("_") or name.startswith("-"):
            name = name[1:].lower().strip()
        if name.endswith("_") or name.endswith("-"):
            name = name[:-1].lower().strip()
        name = name.replace("--", "-").replace("__", "_").replace(" ", "")
        return name.lower().strip()

    @lru_cache(maxsize=None)
    def get_collection_path(self, collection_id: int) -> Union[str, None]:
        """
        Gets collection details from Metabase API to extract collection path info to use ate urn creation.
        Args:
            collection_id (int): The id of the collection to use as path parameter on API.
        Returns:
            (str) -> Return a string representation of collection path (dot separated). Example: "some_name.some_other_name".
        """
        collection_url = f"{self.config.connect_uri}/api/collection/{collection_id}"
        try:
            collection_response = self.session.get(collection_url)
            collection_response.raise_for_status()
            collection_details = collection_response.json()
        except HTTPError as http_error:
            self.report.report_warning(
                key=f"metabase-collection-{collection_id}",
                reason=f"Unable to retrieve collection. " f"Reason: {str(http_error)}",
            )
            return ""
        ancestors = collection_details.get("effective_ancestors", [])
        # Ignore collections. Example: Personal collections
        if self._check_colletion_ignore_pattern(collection_details.get("name", "")):
            return None
        # Create containers based on collections
        self._create_collection_container(collection_id, collection_details)
        collection_name = self._format_string(collection_details.get("name", ""))
        collection_path = []
        for element in ancestors:
            if element.get("id") != "root":
                if self._check_colletion_ignore_pattern(element.get("name", "")):
                    return None
                collection_path.append(self._format_string(element.get("name", "")))
        collection_path.append(collection_name)
        return "/".join(collection_path) if len(collection_path) > 0 else collection_name

    def generate_dashboard_urn(self, dashboard_id: int, dashboard_name: str = "", collection_id: int = None) -> str:
        """
        Generates Dashboard urn based on Dashboard id and collection id. Collection id
        is only used when the 'inherit_collection' config attribute is True.
        Args:
            dashboard_id (int): Dashboard id
            dashboard_name (str): Dashboard name/title
            collection_id (int): Collection id
        Returns:
            (str) -> Dashboard urn
        """
        if self.config.inherit_collection and collection_id:
            collection_path = self.get_collection_path(collection_id)
            if collection_path:
                dashboard_urn = builder.make_dashboard_urn(
                    self.platform, (collection_path + "/" + f"{self._format_string(dashboard_name)}_{dashboard_id}")
                )
                if len(collection_path.split("/")) > 1:
                    dashboard_container = collection_path.split("/")[-1]
                else:
                    dashboard_container = collection_path
                add_entity_to_collection_containers.append({
                    "entity": "dashboard",
                    "entityUrn": dashboard_urn,
                    "aspect": ContainerClass(container=collection_containers[dashboard_container]["container_urn"])
                })
                return dashboard_urn
            else:
                return None
        else:
            return builder.make_dashboard_urn(
                self.platform, f"{self._format_string(dashboard_name)}_{dashboard_id}"
            )

    def generate_chart_urn(self, card_id: int, card_name: str = "", collection_id: int = None) -> str:
        """
        Generates Chart urn based on Card id and collection id. Collection id
        is only used when the 'inherit_collection' config attribute is True.
        Args:
            card_id (int): Card id
            card_name (str): Card name/title
            collection_id (int): Collection id
        Returns:
            (str) -> Chart urn
        """
        if self.config.inherit_collection and collection_id:
            collection_path = self.get_collection_path(collection_id)
            if collection_path:
                card_urn = builder.make_chart_urn(
                    self.platform, (collection_path + "/" + f"{self._format_string(card_name)}_{card_id}")
                )
                if len(collection_path.split("/")) > 1:
                    card_container = collection_path.split("/")[-1]
                else:
                    card_container = collection_path
                add_entity_to_collection_containers.append({
                    "entity": "chart",
                    "entityUrn": card_urn,
                    "aspect": ContainerClass(container=collection_containers[card_container]["container_urn"])
                })
                return card_urn
            else:
                return None
        else:
            return builder.make_chart_urn(
                self.platform, f"{self._format_string(card_name)}_{card_id}"
            )

    def construct_dashboard_custom_properties(self, dashboard_details: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch elements returned by the Metabase API to create a Dashboard properties object.
        Args:
            dashboard_details (dict): JSON returned by Metabase API
        Returns:
            (dict) - Dictionary with Dashboard Properties
        """
        dashboard_cards = dashboard_details.get("ordered_cards", []) or []
        show_in_getting_started = dashboard_details.get("show_in_getting_started")
        dashboard_parameters = []
        if dashboard_details.get("parameters", []):
            for element in dashboard_details.get("parameters", []):
                dashboard_parameters.append(element.get("name"))
        return {
            "total_questions": str(len(dashboard_cards)),
            "show_in_getting_started": str(show_in_getting_started),
            "dashboard_parameters": ",".join(dashboard_parameters)
        }

    def construct_dashboard_from_api_data(
            self, dashboard_info: dict
    ) -> Optional[DashboardSnapshot]:
        """
        Use the data returned by the Metabase API to build Dashboard snapshots
        Args:
            dashboard_info (dict): JSON returned by Metabase API
        Returns:
            (DashboardSnapshot) - Returns a DashboardSnapshot object if no errors were detected otherwise returns None
        """

        dashboard_id = dashboard_info.get("id", "")
        dashboard_url = f"{self.config.connect_uri}/api/dashboard/{dashboard_id}"
        try:
            dashboard_response = self.session.get(dashboard_url)
            dashboard_response.raise_for_status()
            dashboard_details = dashboard_response.json()
        except HTTPError as http_error:
            self.report.report_failure(
                key=f"metabase-dashboard-{dashboard_id}",
                reason=f"Unable to retrieve dashboard. " f"Reason: {str(http_error)}",
            )
            return None

        collection_id = dashboard_info.get("collection_id")
        title = dashboard_details.get("name", "") or ""

        if not collection_id and self.config.inherit_collection:
            self.report.report_warning(
                key=f"metabase-dashboard-{dashboard_id}",
                reason=f"Collections cannot be inherited."
                       f"Reason: Unable to retrieve collection id. 'collection_id' is null or not found in dashboard details."
            )

        dashboard_urn = self.generate_dashboard_urn(dashboard_id, title, collection_id)
        if not dashboard_urn:
            return None

        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[],
        )
        last_edit_by = dashboard_details.get("last-edit-info") or {}
        modified_actor = builder.make_user_urn(last_edit_by.get("email", "unknown"))
        modified_ts = self.get_timestamp_millis_from_ts_string(
            f"{last_edit_by.get('timestamp')}"
        )
        description = dashboard_details.get("description", "") or ""
        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=modified_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )

        chart_urns = []
        cards_data = dashboard_details.get("ordered_cards", [])
        if cards_data:
            for card_info in cards_data:
                card_id = card_info.get("card_id")
                card_collection = card_info.get("card", {}).get("collection_id")
                card_name = card_info.get("card", {}).get("name", "")
                if card_id:
                    chart_urns.append(self.generate_chart_urn(card_id, card_name, card_collection))
                else:
                    self.report.report_warning(
                        key=f"metabase-dashboard-{dashboard_id}",
                        reason=f"Unable to retrieve card_id. "
                               f"Reason: 'card_id' not found in key 'ordered_cards'."
                    )
            chart_urns = list(filter(lambda x: True if x is not None else False, chart_urns))

        custom_properties = self.construct_dashboard_custom_properties(dashboard_details) or {}
        dashboard_info_class = DashboardInfoClass(
            description=description,
            title=title,
            charts=chart_urns,
            lastModified=last_modified,
            dashboardUrl=f"{self.config.connect_uri}/dashboard/{dashboard_id}",
            customProperties=custom_properties
        )
        dashboard_snapshot.aspects.append(dashboard_info_class)

        # Ownership
        ownership = self._get_ownership(dashboard_details.get("creator_id", ""))
        if ownership is not None:
            dashboard_snapshot.aspects.append(ownership)

        return dashboard_snapshot

    @lru_cache(maxsize=None)
    def _get_ownership(self, creator_id: int) -> Optional[OwnershipClass]:
        """
        Use user ID to fetch Owner information in Metabase API
        Args:
            creator_id (int): ID that will be used to fetch the user information in Metabase
        Returns:
            (OwnershipClass) - Returns an object OwnershipClass if user information was found otherwise returns None
        """
        user_info_url = f"{self.config.connect_uri}/api/user/{creator_id}"
        try:
            user_info_response = self.session.get(user_info_url)
            user_info_response.raise_for_status()
            user_details = user_info_response.json()
        except HTTPError as http_error:
            self.report.report_failure(
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
                    wu = MetadataWorkUnit(id=chart_snapshot.urn, mce=mce)
                    self.report.report_workunit(wu)
                    yield wu

        except HTTPError as http_error:
            self.report.report_failure(
                key="metabase-cards",
                reason=f"Unable to retrieve cards. " f"Reason: {str(http_error)}",
            )
            return None

    @lru_cache(maxsize=None)
    def _get_card_details(self, card_id: Union[str, int]) -> Dict[str, Any]:
        """
        Search card details in Metabase API
        Args:
            card_id (int): ID of the card that will be fetched in the API
        Returns:
            (dict) - JSON returned by the API
        """
        card_url = f"{self.config.connect_uri}/api/card/{card_id}"
        try:
            card_response = self.session.get(card_url)
            card_response.raise_for_status()
            return card_response.json()
        except HTTPError as http_error:
            self.report.report_failure(
                key=f"metabase-card-{card_id}",
                reason=f"Unable to retrieve Card info. " f"Reason: {str(http_error)}",
            )
            return None

    def construct_card_from_api_data(self, card_data: dict) -> Optional[ChartSnapshot]:
        """
        Builds the card snapshot based on the details returned by the API
        Args:
            card_data (dict): Content returned by card request in Metabase API
        Returns:
            (ChartSnapshot) - Returns a 'ChartSnapshot' object to be emitted
        """
        card_id = card_data.get("id", "")
        card_details = self._get_card_details(card_id)
        if not card_details:
            return None

        collection_id = card_details.get("collection_id")
        title = card_details.get("name") or ""

        if not collection_id and self.config.inherit_collection:
            self.report.report_warning(
                key=f"metabase-card-{card_id}",
                reason=f"Unable to retrieve collection id. "
                       f"Reason: 'collection_id' is null or not found in card details."
            )

        chart_urn = self.generate_chart_urn(card_id, title, collection_id)
        if not chart_urn:
            return None
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

        chart_type = self._get_chart_type(
            card_id, card_details.get("display")
        )
        description = card_details.get("description") or ""
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
        """
        Seeks to map the card type in Metabase with the types of Charts declared in DataHub
        Args:
            card_id (int): The ID of the card
            display_type (str): The chart type returned by the Metabase API
        Returns:
            Returns the card's chart type
        """
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
        """
        Fetch information from Card details to build custom properties
        Args:
            card_details (dict): JSON returned by Metabase API in Card request
        Returns:
            (dict) - A dictionaries of Card properties
        """
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
            "Dimensions": ", ".join(dimensions)
        }
        if card_details.get("average_query_time"):
            custom_properties.update({
                "average_query_time(ms)": str(round(card_details.get("average_query_time")))
            })

        return custom_properties

    def get_datasource_urn(self, card_details: Dict[str, Any]) -> List[str]:
        """
        Generate the URNs for the source datasets
        Args:
            card_details (dict): JSON returned by Metabase API in Card request
        Returns:
            (list) - Returns a list of URNs for the card's source tables
        """
        plataform_sources: dict = dict()
        self.get_lineage_from_card_details(card_details, plataform_sources)

        if not plataform_sources:
            return None

        # Create dataset URNs
        inputs_urn = []
        for data_source, tables in plataform_sources.items():
            platform, database_name, platform_instance = data_source[0], data_source[1], data_source[2]
            dbname = f"{database_name + '.' if database_name else ''}"
            source_tables = list(map(lambda tbl: f"{dbname}{tbl}", tables))
            inputs_urn = [
                builder.make_dataset_urn_with_platform_instance(
                    platform=platform,
                    name=name,
                    platform_instance=platform_instance,
                    env=self.config.env,
                )
                for name in source_tables
            ]
        if inputs_urn:
            return inputs_urn
        else:
            return None

    @lru_cache(maxsize=None)
    def get_source_table_details_by_id(self, source_table_id:int) -> set:
        """
        Fetch table information in Metabase
        Args:
            source_table_id (int): ID of the table that will be used to fetch the information in the API
        Returns:
            Returns the API response body
        """
        source_paths = set()
        dataset_json = {}
        try:
            dataset_response = self.session.get(
                f"{self.config.connect_uri}/api/table/{source_table_id}"
            )
            dataset_response.raise_for_status()
            dataset_json = dataset_response.json()
        except HTTPError as http_error:
            self.report.report_warning(
                key=f"metabase-table-{source_table_id}",
                reason=f"Unable to retrieve source table. "
                       f"Reason: {str(http_error)}",
            )
        schema = dataset_json.get("schema", "")
        table_name = dataset_json.get("name", "")
        if table_name:
            source_paths.add(
                f"{schema + '.' if schema else ''}{table_name}"
            )
            return source_paths

    def get_lineage_from_card_details(self, card_details: Dict[str, Any], plataform_sources: Dict[str, Any]) -> None:
        """
        Search the query performed by the card to find the source tables
        Args:
            card_details (dict): JSON returned by Metabase API
            plataform_sources (dict): A dictionary that maps the triple platform, database name and platform
                                      instance to the respective front tables found
        """
        if not card_details:
            return
        platform, database_name, platform_instance = self.get_datasource_from_id(
            card_details.get("database_id", "")
        )
        if not (platform, database_name, platform_instance) in plataform_sources.keys():
            plataform_sources[(platform, database_name, platform_instance)] = set()
        query_type = card_details.get("dataset_query", {}).get("type", "unknown")
        if query_type == "query":
            source_table_id = (
                card_details.get("dataset_query", {})
                    .get("query", {})
                    .get("source-table")
            )
            if source_table_id is not None:
                # It is possible that the source table is another card.
                # In this sense the 'source_table' field returns "card__{card_id}"
                if not str(source_table_id).find("card__") >= 0:
                    plataform_sources.update({
                        (platform, database_name, platform_instance): self.get_source_table_details_by_id(source_table_id)
                    })
                    return
                else:
                    # In case the source is a card, its necessary to extract the sources of the cards
                    # on which it depends since it is not possible to have a relationship of dependencies
                    # between chart entities
                    cards_id = set()
                    cards_id.add(str(source_table_id).replace("card__", "").strip())
                    joins = card_details.get("dataset_query", {}).get("query", {}).get("joins", [])
                    if joins:
                        for element in joins:
                            if element.get("source-table", "").find("card__") >= 0:
                                cards_id.add(str(element.get("source-table")).replace("card__", "").strip())
                    for id in cards_id:
                        details = self._get_card_details(id)
                        if details:
                            self.get_lineage_from_card_details(details, plataform_sources)
                    return
        else:
            try:
                raw_query = (
                    card_details.get("dataset_query", {})
                        .get("native", {})
                        .get("query", "")
                )
                if not card_details.get("id") in query_parser_cache.keys():
                    parser = LineageRunner(raw_query, verbose=True)
                    source_paths = set()
                    for table in parser.source_tables:
                        # It is necessary to use raw_name to enable uppercase
                        source = f"{table.schema.raw_name}.{table.raw_name}".split(".")
                        source_schema, source_table = source[-2], source[-1]
                        if source_schema == "<default>":
                            source_schema = str(self.config.default_schema)
                        source_paths.add(f"{source_schema}.{source_table}")
                    query_parser_cache[card_details.get("id")] = source_paths
                    plataform_sources.update({
                        (platform, database_name, platform_instance): source_paths
                    })
                else:
                    plataform_sources.update({
                        (platform, database_name, platform_instance): query_parser_cache[card_details.get("id")]
                    })
                return
            except Exception as e:
                self.report.report_warning(
                    key="metabase-query",
                    reason=f"Unable to retrieve lineage from query. "
                           f"Query: {raw_query} "
                           f"Reason: {str(e)} ",
                )

        return

    @lru_cache(maxsize=None)
    def get_datasource_from_id(self, datasource_id:int):
        """
        Fetch the information from the database in Metabase
        Args:
            datasource_id (int): The ID of the database to fetch in the API
        Returns:
            Returns a tuple containing the respective values: Database platform, database name and platform instance mapping
        """
        try:
            dataset_response = self.session.get(
                f"{self.config.connect_uri}/api/database/{datasource_id}"
            )
            dataset_response.raise_for_status()
            dataset_json = dataset_response.json()
        except HTTPError as http_error:
            self.report.report_failure(
                key=f"metabase-datasource-{datasource_id}",
                reason=f"Unable to retrieve Datasource. " f"Reason: {str(http_error)}",
            )
            return None, None, None

        # Map engine names to what datahub expects in
        # https://github.com/datahub-project/datahub/blob/master/metadata-service/war/src/main/resources/boot/data_platforms.json
        engine = dataset_json.get("engine", "")
        platform = engine

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
            self.report.report_warning(
                key=f"metabase-platform-{datasource_id}",
                reason=f"Platform was not found in DataHub. Using {platform} name as is",
            )
        # Set platform_instance if configuration provides a mapping from platform name to instance
        platform_instance = (
            self.config.platform_instance_map.get(platform)
            if self.config.platform_instance_map
            else None
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
            "sqlserver": "db"
        }

        if engine == "bigquery-cloud-sdk":
            project_id = dataset_json.get("details", {}).get("project-id")
            dbname = (
                f"{project_id}"
                if project_id
                else None
            )
        else:
            dbname = (
                dataset_json.get("details", {}).get(field_for_dbname_mapping[engine])
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

        return platform, dbname, platform_instance

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = MetabaseConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_platform_mce()
        yield from self.emit_dashboard_mces()
        yield from self.emit_card_mces()
        yield from self.emit_containers_mces()
        yield from self.emit_container_elements_mce()

    def get_report(self) -> SourceReport:
        return self.report
