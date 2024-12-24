# Looker SDK is imported here and higher level wrapper functions/classes are provided to interact with Looker Server
import json
import logging
import os
from functools import lru_cache
from typing import Dict, List, MutableMapping, Optional, Sequence, Set, Union, cast

import looker_sdk
import looker_sdk.rtl.requests_transport as looker_requests_transport
from looker_sdk.error import SDKError
from looker_sdk.rtl.transport import TransportOptions
from looker_sdk.sdk.api40.models import (
    Dashboard,
    DashboardBase,
    DBConnection,
    Folder,
    Look,
    LookmlModel,
    LookmlModelExplore,
    LookWithQuery,
    Query,
    User,
    WriteQuery,
)
from pydantic import BaseModel, Field
from requests.adapters import HTTPAdapter

from datahub.configuration import ConfigModel
from datahub.configuration.common import ConfigurationError

logger = logging.getLogger(__name__)


class TransportOptionsConfig(ConfigModel):
    timeout: int
    headers: MutableMapping[str, str]

    def get_transport_options(self) -> TransportOptions:
        return TransportOptions(timeout=self.timeout, headers=self.headers)


class LookerAPIConfig(ConfigModel):
    client_id: str = Field(description="Looker API client id.")
    client_secret: str = Field(description="Looker API client secret.")
    base_url: str = Field(
        description="Url to your Looker instance: `https://company.looker.com:19999` or `https://looker.company.com`, or similar. Used for making API calls to Looker and constructing clickable dashboard and chart urls."
    )
    transport_options: Optional[TransportOptionsConfig] = Field(
        None,
        description="Populates the [TransportOptions](https://github.com/looker-open-source/sdk-codegen/blob/94d6047a0d52912ac082eb91616c1e7c379ab262/python/looker_sdk/rtl/transport.py#L70) struct for looker client",
    )
    max_retries: int = Field(3, description="Number of retries for Looker API calls")


class LookerAPIStats(BaseModel):
    dashboard_calls: int = 0
    user_calls: int = 0
    explore_calls: int = 0
    query_calls: int = 0
    folder_calls: int = 0
    all_connections_calls: int = 0
    connection_calls: int = 0
    lookml_model_calls: int = 0
    all_dashboards_calls: int = 0
    all_looks_calls: int = 0
    all_models_calls: int = 0
    get_query_calls: int = 0
    get_look_calls: int = 0
    search_looks_calls: int = 0
    search_dashboards_calls: int = 0
    all_user_calls: int = 0


class LookerAPI:
    """A holder class for a Looker client"""

    def __init__(self, config: LookerAPIConfig) -> None:
        self.config = config
        # The Looker SDK looks wants these as environment variables
        os.environ["LOOKERSDK_CLIENT_ID"] = config.client_id
        os.environ["LOOKERSDK_CLIENT_SECRET"] = config.client_secret
        os.environ["LOOKERSDK_BASE_URL"] = config.base_url

        self.client = looker_sdk.init40()

        # Somewhat hacky mechanism for enabling retries on the Looker SDK.
        # Unfortunately, it doesn't expose a cleaner way to do this.
        if isinstance(
            self.client.transport, looker_requests_transport.RequestsTransport
        ):
            adapter = HTTPAdapter(
                max_retries=self.config.max_retries,
            )
            self.client.transport.session.mount("http://", adapter)
            self.client.transport.session.mount("https://", adapter)
        elif self.config.max_retries > 0:
            logger.warning("Unable to configure retries on the Looker SDK transport.")

        self.transport_options = (
            config.transport_options.get_transport_options()
            if config.transport_options is not None
            else None
        )
        # try authenticating current user to check connectivity
        # (since it's possible to initialize an invalid client without any complaints)
        try:
            self.me = self.client.me(
                transport_options=(
                    self.transport_options
                    if config.transport_options is not None
                    else None
                )
            )
        except SDKError as e:
            raise ConfigurationError(
                f"Failed to connect/authenticate with looker - check your configuration: {e}"
            ) from e

        self.client_stats = LookerAPIStats()

    @staticmethod
    def __fields_mapper(fields: Union[str, List[str]]) -> str:
        """Helper method to turn single string or list of fields into Looker API compatible fields param"""
        return fields if isinstance(fields, str) else ",".join(fields)

    def get_available_permissions(self) -> Set[str]:
        user_id = self.me.id
        assert user_id

        roles = self.client.user_roles(user_id)

        permissions: Set[str] = set()
        for role in roles:
            if role.permission_set and role.permission_set.permissions:
                permissions.update(role.permission_set.permissions)

        return permissions

    @lru_cache(maxsize=5000)
    def get_user(self, id_: str, user_fields: str) -> Optional[User]:
        self.client_stats.user_calls += 1
        try:
            return self.client.user(
                id_,
                fields=cast(str, user_fields),
                transport_options=self.transport_options,
            )
        except SDKError as e:
            if "Looker Not Found (404)" in str(e):
                # User not found
                logger.info(f"Could not find user with id {id_}: 404 error")
            else:
                logger.warning(f"Could not find user with id {id_}")
                logger.warning(f"Failure was {e}")
        # User not found
        return None

    def all_users(self, user_fields: str) -> Sequence[User]:
        self.client_stats.all_user_calls += 1
        try:
            return self.client.all_users(
                fields=cast(str, user_fields),
                transport_options=self.transport_options,
            )
        except SDKError as e:
            logger.warning(f"Failure was {e}")
        return []

    def execute_query(self, write_query: WriteQuery) -> List[Dict]:
        logger.debug(f"Executing query {write_query}")
        self.client_stats.query_calls += 1

        response_json = self.client.run_inline_query(
            result_format="json",
            body=write_query,
            transport_options=self.transport_options,
        )

        logger.debug("=================Response=================")
        data = json.loads(response_json)
        logger.debug("Length of response: %d", len(data))
        return data

    def dashboard(self, dashboard_id: str, fields: Union[str, List[str]]) -> Dashboard:
        self.client_stats.dashboard_calls += 1
        return self.client.dashboard(
            dashboard_id=dashboard_id,
            fields=self.__fields_mapper(fields),
            transport_options=self.transport_options,
        )

    def all_lookml_models(self) -> Sequence[LookmlModel]:
        self.client_stats.all_models_calls += 1
        return self.client.all_lookml_models(
            transport_options=self.transport_options,
        )

    def lookml_model_explore(self, model: str, explore_name: str) -> LookmlModelExplore:
        self.client_stats.explore_calls += 1
        return self.client.lookml_model_explore(
            model, explore_name, transport_options=self.transport_options
        )

    @lru_cache(maxsize=1000)
    def folder_ancestors(
        self,
        folder_id: str,
        fields: Union[str, List[str]] = ["id", "name", "parent_id"],
    ) -> Sequence[Folder]:
        self.client_stats.folder_calls += 1
        try:
            return self.client.folder_ancestors(
                folder_id,
                self.__fields_mapper(fields),
                transport_options=self.transport_options,
            )
        except SDKError as e:
            if "Looker Not Found (404)" in str(e):
                # Folder ancestors not found
                logger.info(
                    f"Could not find ancestors for folder with id {folder_id}: 404 error"
                )
            else:
                logger.warning(
                    f"Could not find ancestors for folder with id {folder_id}"
                )
                logger.warning(f"Failure was {e}")
        # Folder ancestors not found
        return []

    def all_connections(self):
        self.client_stats.all_connections_calls += 1
        return self.client.all_connections(transport_options=self.transport_options)

    def connection(self, connection_name: str) -> DBConnection:
        self.client_stats.connection_calls += 1
        return self.client.connection(
            connection_name, transport_options=self.transport_options
        )

    def lookml_model(
        self, model_name: str, fields: Union[str, List[str]]
    ) -> LookmlModel:
        self.client_stats.lookml_model_calls += 1
        return self.client.lookml_model(
            model_name,
            self.__fields_mapper(fields),
            transport_options=self.transport_options,
        )

    def compute_stats(self) -> Dict:
        return {
            "client_stats": self.client_stats,
            "folder_cache": self.folder_ancestors.cache_info(),
            "user_cache": self.get_user.cache_info(),
        }

    def all_dashboards(self, fields: Union[str, List[str]]) -> Sequence[DashboardBase]:
        self.client_stats.all_dashboards_calls += 1
        return self.client.all_dashboards(
            fields=self.__fields_mapper(fields),
            transport_options=self.transport_options,
        )

    def all_looks(
        self, fields: Union[str, List[str]], soft_deleted: bool
    ) -> List[Look]:
        self.client_stats.all_looks_calls += 1
        looks: List[Look] = list(
            self.client.all_looks(
                fields=self.__fields_mapper(fields),
                transport_options=self.transport_options,
            )
        )

        if soft_deleted:
            # Add soft deleted looks
            looks.extend(self.search_looks(fields=fields, deleted=True))

        return looks

    def get_query(self, query_id: str, fields: Union[str, List[str]]) -> Query:
        self.client_stats.get_query_calls += 1
        return self.client.query(
            query_id=query_id,
            fields=self.__fields_mapper(fields),
            transport_options=self.transport_options,
        )

    def get_look(self, look_id: str, fields: Union[str, List[str]]) -> LookWithQuery:
        self.client_stats.get_look_calls += 1
        return self.client.look(
            look_id=look_id,
            fields=self.__fields_mapper(fields),
            transport_options=self.transport_options,
        )

    def search_dashboards(
        self, fields: Union[str, List[str]], deleted: str
    ) -> Sequence[Dashboard]:
        self.client_stats.search_dashboards_calls += 1
        return self.client.search_dashboards(
            fields=self.__fields_mapper(fields),
            deleted=deleted,
            transport_options=self.transport_options,
        )

    def search_looks(
        self, fields: Union[str, List[str]], deleted: Optional[bool]
    ) -> List[Look]:
        self.client_stats.search_looks_calls += 1
        return list(
            self.client.search_looks(
                fields=self.__fields_mapper(fields),
                deleted=deleted,
                transport_options=self.transport_options,
            )
        )
