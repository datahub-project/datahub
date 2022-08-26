# Looker SDK is imported here and higher level wrapper functions/classes are provided to interact with Looker Server
import datetime
import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Dict, List, MutableMapping, Optional, cast

import looker_sdk
from looker_sdk.error import SDKError
from looker_sdk.rtl.transport import TransportOptions
from looker_sdk.sdk.api31.methods import Looker31SDK
from looker_sdk.sdk.api31.models import User, WriteQuery
from pydantic import Field

import datahub.emitter.mce_builder as builder
from datahub.configuration import ConfigModel
from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.looker.looker_common import (
    LookerCommonConfig,
    LookerExplore,
    ViewField,
)

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


class LookerAPI:
    """A holder class for a Looker client"""

    def __init__(self, config: LookerAPIConfig) -> None:
        self.config = config
        # The Looker SDK looks wants these as environment variables
        os.environ["LOOKERSDK_CLIENT_ID"] = config.client_id
        os.environ["LOOKERSDK_CLIENT_SECRET"] = config.client_secret
        os.environ["LOOKERSDK_BASE_URL"] = config.base_url

        self.client = looker_sdk.init31()
        self.transport_options = (
            config.transport_options.get_transport_options()
            if config.transport_options is not None
            else None
        )
        # try authenticating current user to check connectivity
        # (since it's possible to initialize an invalid client without any complaints)
        try:
            self.client.me(
                transport_options=self.transport_options
                if config.transport_options is not None
                else None
            )
        except SDKError as e:
            raise ConfigurationError(
                "Failed to initialize Looker client. Please check your configuration."
            ) from e

    def get_client(self) -> Looker31SDK:
        return self.client

    def get_user(self, id_: int, user_fields: str) -> Optional[User]:
        try:
            return self.client.user(
                id_,
                fields=cast(str, user_fields),
                transport_options=self.transport_options,
            )
        except SDKError as e:
            logger.warning(f"Could not find user with id {id}")
            logger.warning(f"Failure was {e}")
        # User not found
        return None

    def execute_query(self, write_query: WriteQuery) -> List[Dict]:
        response_sql = self.client.run_inline_query(
            result_format="sql",
            body=write_query,
            transport_options=self.transport_options,
        )
        logger.debug("=================Query=================")
        logger.debug(response_sql)

        response_json = self.client.run_inline_query(
            result_format="json",
            body=write_query,
            transport_options=self.transport_options,
        )

        logger.debug("=================Response=================")
        data = json.loads(response_json)
        logger.debug(f"length {len(data)}")
        return data


# These function will avoid to create LookerDashboard object to get the Looker Dashboard urn id part
def get_urn_looker_dashboard_id(id_: str) -> str:
    return f"dashboards.{id_}"


def get_urn_looker_element_id(id_: str) -> str:
    return f"dashboard_elements.{id_}"


@dataclass
class LookerUser:
    id: int
    email: Optional[str]
    display_name: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]

    @classmethod
    def create_looker_user(cls, raw_user: User) -> "LookerUser":
        assert raw_user.id is not None
        return LookerUser(
            raw_user.id,
            raw_user.email,
            raw_user.display_name,
            raw_user.first_name,
            raw_user.last_name,
        )

    def get_urn(self, strip_user_ids_from_email: bool) -> Optional[str]:
        if self.email is None:
            return None
        if strip_user_ids_from_email:
            return builder.make_user_urn(self.email.split("@")[0])
        else:
            return builder.make_user_urn(self.email)


@dataclass
class InputFieldElement:
    name: str
    view_field: Optional[ViewField]
    model: str = ""
    explore: str = ""


@dataclass
class LookerDashboardElement:
    id: str
    title: str
    query_slug: str
    upstream_explores: List[LookerExplore]
    look_id: Optional[str]
    type: Optional[str] = None
    description: Optional[str] = None
    input_fields: Optional[List[InputFieldElement]] = None

    def url(self, base_url: str) -> str:
        # A dashboard element can use a look or just a raw query against an explore
        # If the base_url contains a port number (like https://company.looker.com:19999) remove the port number
        m = re.match("^(.*):([0-9]+)$", base_url)
        if m is not None:
            base_url = m[1]
        if self.look_id is not None:
            return f"{base_url}/looks/{self.look_id}"
        else:
            return f"{base_url}/x/{self.query_slug}"

    def get_urn_element_id(self):
        # A dashboard element can use a look or just a raw query against an explore
        return f"dashboard_elements.{self.id}"

    def get_view_urns(self, config: LookerCommonConfig) -> List[str]:
        return [v.get_explore_urn(config) for v in self.upstream_explores]


@dataclass
class LookerDashboard:
    id: str
    title: str
    dashboard_elements: List[LookerDashboardElement]
    created_at: Optional[datetime.datetime]
    description: Optional[str] = None
    folder_path: Optional[str] = None
    is_deleted: bool = False
    is_hidden: bool = False
    owner: Optional[LookerUser] = None
    strip_user_ids_from_email: Optional[bool] = True
    last_updated_at: Optional[datetime.datetime] = None
    last_updated_by: Optional[LookerUser] = None
    deleted_at: Optional[datetime.datetime] = None
    deleted_by: Optional[LookerUser] = None
    favorite_count: Optional[int] = None
    view_count: Optional[int] = None
    last_viewed_at: Optional[datetime.datetime] = None

    def url(self, base_url):
        # If the base_url contains a port number (like https://company.looker.com:19999) remove the port number
        m = re.match("^(.*):([0-9]+)$", base_url)
        if m is not None:
            base_url = m[1]
        return f"{base_url}/dashboards/{self.id}"

    def get_urn_dashboard_id(self):
        return get_urn_looker_dashboard_id(self.id)


class LookerUserRegistry:
    user_map: Dict[int, LookerUser]
    looker_api_wrapper: LookerAPI
    fields: str = ",".join(["id", "email", "display_name", "first_name", "last_name"])

    def __init__(self, looker_api: LookerAPI):
        self.looker_api_wrapper = looker_api
        self.user_map = {}

    def get_by_id(self, id_: int) -> Optional[LookerUser]:
        logger.debug(f"Will get user {id_}")
        if id_ in self.user_map:
            return self.user_map[id_]

        raw_user: Optional[User] = self.looker_api_wrapper.get_user(
            id_, user_fields=self.fields
        )
        if raw_user is None:
            return None

        looker_user = LookerUser.create_looker_user(raw_user)
        self.user_map[id_] = looker_user
        return looker_user
