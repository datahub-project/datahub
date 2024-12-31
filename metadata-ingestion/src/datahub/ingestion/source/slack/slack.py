import logging
import textwrap
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

from pydantic import Field, SecretStr
from slack_sdk import WebClient

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    CorpUserEditableInfoClass,
    DatasetPropertiesClass,
    DeprecationClass,
    SubTypesClass,
)
from datahub.utilities.ratelimiter import RateLimiter
from datahub.utilities.urns.urn import Urn

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class CorpUser:
    urn: Optional[str] = None
    email: Optional[str] = None
    slack_id: Optional[str] = None
    title: Optional[str] = None
    image_url: Optional[str] = None
    phone: Optional[str] = None
    real_name: Optional[str] = None
    slack_display_name: Optional[str] = None


class SlackSourceConfig(ConfigModel):
    bot_token: SecretStr = Field(
        description="Bot token for the Slack workspace. Needs `users:read`, `users:read.email` and `users.profile:read` scopes.",
    )
    enrich_user_metadata: bool = Field(
        type=bool,
        default=True,
        description="Whether to enrich user metadata.",
    )
    api_requests_per_min: int = Field(
        type=int,
        default=10,
        description="Number of API requests per minute. Low-level config. Do not tweak unless you are facing any issues.",
    )
    ingest_public_channels = Field(
        type=bool,
        default=False,
        description="Whether to ingest public channels. If set to true needs `channels:read` scope.",
    )
    channels_iteration_limit = Field(
        type=int,
        default=200,
        description="Limit the number of channels to be ingested in a iteration. Low-level config. Do not tweak unless you are facing any issues.",
    )
    channel_min_members = Field(
        type=int,
        default=2,
        description="Ingest channels with at least this many members.",
    )
    should_ingest_archived_channels = Field(
        type=bool,
        default=False,
        description="Whether to ingest archived channels.",
    )


@dataclass
class SlackSourceReport(SourceReport):
    channels_reported: int = 0
    archived_channels_reported: int = 0


PLATFORM_NAME = "slack"


@platform_name("Slack")
@config_class(SlackSourceConfig)
@support_status(SupportStatus.TESTING)
class SlackSource(Source):
    def __init__(self, ctx: PipelineContext, config: SlackSourceConfig):
        self.ctx = ctx
        self.config = config
        self.report = SlackSourceReport()
        self.workspace_base_url: Optional[str] = None
        self.rate_limiter = RateLimiter(
            max_calls=self.config.api_requests_per_min, period=60
        )
        self._use_users_info = False

    @classmethod
    def create(cls, config_dict, ctx):
        config = SlackSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_slack_client(self) -> WebClient:
        return WebClient(token=self.config.bot_token.get_secret_value())

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        assert self.ctx.graph is not None
        auth_resp = self.get_slack_client().auth_test()
        assert isinstance(auth_resp.data, dict)
        self.workspace_base_url = str(auth_resp.data.get("url"))
        logger.info("Successfully connected to Slack")
        logger.info(auth_resp.data)
        if self.config.ingest_public_channels:
            yield from self.get_public_channels()
        if self.config.enrich_user_metadata:
            yield from self.get_user_info()

    def get_user_info(self) -> Iterable[MetadataWorkUnit]:
        assert self.ctx.graph is not None
        for user_obj in self.get_user_to_be_updated():
            self.populate_slack_id_from_email(user_obj)
            if user_obj.slack_id is None:
                continue
            self.populate_user_profile(user_obj)
            if user_obj.urn is None:
                continue
            logger.info(f"User: {user_obj}")
            corpuser_editable_info = (
                self.ctx.graph.get_aspect(
                    entity_urn=user_obj.urn, aspect_type=CorpUserEditableInfoClass
                )
                or CorpUserEditableInfoClass()
            )
            corpuser_editable_info.email = user_obj.email
            corpuser_editable_info.slack = user_obj.slack_id
            corpuser_editable_info.title = user_obj.title
            if user_obj.image_url:
                corpuser_editable_info.pictureLink = user_obj.image_url
            if user_obj.phone:
                corpuser_editable_info.phone = user_obj.phone
            if (
                not corpuser_editable_info.displayName
                or corpuser_editable_info.displayName == corpuser_editable_info.email
            ):
                # let's fill out a real name
                corpuser_editable_info.displayName = user_obj.real_name
            yield MetadataWorkUnit(
                id=f"{user_obj.urn}",
                mcp=MetadataChangeProposalWrapper(
                    entityUrn=user_obj.urn,
                    aspect=corpuser_editable_info,
                ),
            )

    def _get_channel_info(
        self, cursor: Optional[str]
    ) -> Tuple[List[MetadataWorkUnit], Optional[str]]:
        result_channels: List[MetadataWorkUnit] = []
        with self.rate_limiter:
            response = self.get_slack_client().conversations_list(
                types="public_channel",
                limit=self.config.channels_iteration_limit,
                cursor=cursor,
            )
        assert isinstance(response.data, dict)
        if not response.data["ok"]:
            self.report.report_failure(
                "public_channel", "Failed to fetch public channels"
            )
            return result_channels, None
        for channel in response.data["channels"]:
            num_members = channel["num_members"]
            if num_members < self.config.channel_min_members:
                continue
            channel_id = channel["id"]
            urn_channel = builder.make_dataset_urn(
                platform=PLATFORM_NAME, name=channel_id
            )
            name = channel["name"]
            is_archived = channel.get("is_archived", False)
            if is_archived:
                if not self.config.should_ingest_archived_channels:
                    continue
                self.report.archived_channels_reported += 1
                logger.info(f"Archived channel: {name}")
                result_channels.append(
                    MetadataWorkUnit(
                        id=f"{urn_channel}-deprecation",
                        mcp=MetadataChangeProposalWrapper(
                            entityUrn=urn_channel,
                            aspect=DeprecationClass(
                                deprecated=True,
                                note="This channel is archived",
                                actor="urn:li:corpuser:datahub",
                            ),
                        ),
                    )
                )

            topic = channel.get("topic", {}).get("value")
            purpose = channel.get("purpose", {}).get("value")
            if self.workspace_base_url:
                external_url = f"{self.workspace_base_url}/archives/{channel_id}"
            result_channels.append(
                MetadataWorkUnit(
                    id=f"{urn_channel}-datasetproperties",
                    mcp=MetadataChangeProposalWrapper(
                        entityUrn=urn_channel,
                        aspect=DatasetPropertiesClass(
                            name=name,
                            externalUrl=external_url,
                            description=f"Topic: {topic}\nPurpose: {purpose}",
                        ),
                    ),
                )
            )
            result_channels.append(
                MetadataWorkUnit(
                    id=f"{urn_channel}-subtype",
                    mcp=MetadataChangeProposalWrapper(
                        entityUrn=urn_channel,
                        aspect=SubTypesClass(
                            typeNames=["Slack Channel"],
                        ),
                    ),
                )
            )
        cursor = str(response.data["response_metadata"]["next_cursor"])
        return result_channels, cursor

    def get_public_channels(self) -> Iterable[MetadataWorkUnit]:
        cursor = None
        while True:
            with self.rate_limiter:
                channels, cursor = self._get_channel_info(cursor=cursor)
            for channel in channels:
                self.report.channels_reported += 1
                yield channel
            if not cursor:
                break

    def populate_user_profile(self, user_obj: CorpUser) -> None:
        if not user_obj.slack_id:
            return
        try:
            # https://api.slack.com/methods/users.profile.get
            with self.rate_limiter:
                if self._use_users_info:
                    user_profile_res = self.get_slack_client().users_info(
                        user=user_obj.slack_id
                    )
                    user_profile_res = user_profile_res.get("user", {})
                else:
                    user_profile_res = self.get_slack_client().users_profile_get(
                        user=user_obj.slack_id
                    )
            logger.debug(f"User profile: {user_profile_res}")
            user_profile = user_profile_res.get("profile", {})
            user_obj.title = user_profile.get("title")
            user_obj.image_url = user_profile.get("image_192")
            user_obj.phone = user_profile.get("phone")
            user_obj.real_name = user_profile.get("real_name")
            user_obj.slack_display_name = user_profile.get("display_name")

        except Exception as e:
            if "missing_scope" in str(e):
                if self._use_users_info:
                    raise e
                self._use_users_info = True
                self.populate_user_profile(user_obj)
            return

    def populate_slack_id_from_email(self, user_obj: CorpUser) -> None:
        if user_obj.email is None:
            return
        try:
            # https://api.slack.com/methods/users.lookupByEmail
            with self.rate_limiter:
                user_info_res = self.get_slack_client().users_lookupByEmail(
                    email=user_obj.email
                )
            user_info = user_info_res.get("user", {})
            user_obj.slack_id = user_info.get("id")
        except Exception as e:
            if "users_not_found" in str(e):
                return
            raise e

    def get_user_to_be_updated(self) -> Iterable[CorpUser]:
        graphql_query = textwrap.dedent(
            """
            query listUsers($input: ListUsersInput!) {
                listUsers(input: $input) {
                    total
                    users {
                        urn
                        editableProperties {
                            email
                            slack
                        }
                    }
                }
            }
        """
        )
        start = 0
        count = 10
        total = count

        assert self.ctx.graph is not None

        while start < total:
            variables = {"input": {"start": start, "count": count}}
            response = self.ctx.graph.execute_graphql(
                query=graphql_query, variables=variables
            )
            list_users = response.get("listUsers", {})
            total = list_users.get("total", 0)
            users = list_users.get("users", [])
            for user in users:
                user_obj = CorpUser()
                editable_properties = user.get("editableProperties", {})
                user_obj.urn = user.get("urn")
                if user_obj.urn is None:
                    continue
                if editable_properties is not None:
                    user_obj.email = editable_properties.get("email")
                if user_obj.email is None:
                    urn_id = Urn.from_string(user_obj.urn).get_entity_id_as_string()
                    if "@" in urn_id:
                        user_obj.email = urn_id
                if user_obj.email is not None:
                    yield user_obj
            start += count

    def get_report(self) -> SourceReport:
        return self.report
