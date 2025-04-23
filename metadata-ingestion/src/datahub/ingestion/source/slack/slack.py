import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pydantic import BaseModel, Field, SecretStr
from slack_sdk import WebClient
from tenacity import retry, wait_exponential
from tenacity.before_sleep import before_sleep_log

import datahub.emitter.mce_builder as builder
from datahub.emitter.mce_builder import datahub_guid, make_dataplatform_instance_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
    SourceReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    CorpUserEditableInfoClass,
    CorpUserSettingsClass,
    DataPlatformInstanceClass,
    DataPlatformInstancePropertiesClass,
    DatasetPropertiesClass,
    DeprecationClass,
    NotificationSettingsClass,
    PlatformResourceInfoClass,
    SerializedValueClass,
    SerializedValueContentTypeClass,
    SerializedValueSchemaTypeClass,
    SlackNotificationSettingsClass,
    SlackUserInfoClass as SlackUserInfo,
    StatusClass,
    SubTypesClass,
    _Aspect,
)
from datahub.utilities.ratelimiter import RateLimiter
from datahub.utilities.str_enum import StrEnum
from datahub.utilities.urns.urn import Urn

logger: logging.Logger = logging.getLogger(__name__)


# TODO: Relocate this function to a utility module
def is_picture_default_or_missing(picture_link: Optional[str]) -> bool:
    if not picture_link:
        return True
    return picture_link.endswith("default_avatar.png")


def is_slack_image(picture_link: Optional[str]) -> bool:
    """
    Guesses if the picture link is a slack image.
    """
    if not picture_link:
        return False
    return "slack-edge.com" in picture_link


class ResourceType(StrEnum):
    USER_INFO = "user-info"
    CHANNEL_INFO = "channel-info"


class SlackInstance(BaseModel):
    id: str
    name: Optional[str] = None
    description: Optional[str] = None
    external_url: Optional[str] = None
    custom_properties: Optional[Dict[str, str]] = None

    def to_platform_instance_urn(self) -> str:
        return make_dataplatform_instance_urn(
            platform=DATA_PLATFORM_SLACK_URN, instance=self.id
        )

    def with_slack_team_info(self, team_info: dict) -> "SlackInstance":
        """
        team_info looks like this
        {'id': 'T22BUCL1LKW', 'name': 'DataHub', 'url': 'https://datahubspace.slack.com/', 'domain': 'datahub', 'email_domain': '', 'icon': {'image_default': False, 'image_34': 'https://avatars.slack-edge.com/2021-07-05/2228585180071_63e6f300a919abc488bb_34.png', 'image_44': 'https://avatars.slack-edge.com/2021-07-05/2228585180071_63e6f300a919abc488bb_44.png', 'image_68': 'https://avatars.slack-edge.com/2021-07-05/2228585180071_63e6f300a919abc488bb_68.png', 'image_88': 'https://avatars.slack-edge.com/2021-07-05/2228585180071_63e6f300a919abc488bb_88.png', 'image_102': 'https://avatars.slack-edge.com/2021-07-05/2228585180071_63e6f300a919abc488bb_102.png', 'image_230': 'https://avatars.slack-edge.com/2021-07-05/2228585180071_63e6f300a919abc488bb_230.png', 'image_132': 'https://avatars.slack-edge.com/2021-07-05/2228585180071_63e6f300a919abc488bb_132.png'}, 'avatar_base_url': 'https://ca.slack-edge.com/', 'is_verified': False, 'external_org_migrations': {'date_updated': 1722672564, 'current': []}, 'discoverable': 'closed', 'enterprise_id': 'E06TPM5T1G9', 'enterprise_name': 'DataHub', 'enterprise_domain': 'datahubspace', 'lob_sales_home_enabled': False}
        """
        self.name = team_info.get("name")
        self.description = team_info.get("name")
        self.external_url = team_info.get("url")
        self.custom_properties = {
            k: v
            for k, v in {
                "domain": team_info.get("domain"),
                "enterprise_id": team_info.get("enterprise_id"),
                "enterprise_name": team_info.get("enterprise_name"),
                "enterprise_domain": team_info.get("enterprise_domain"),
                "icon": team_info.get("icon", {}).get("image_102"),
            }.items()
            if v is not None
        }
        return self

    def to_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.to_platform_instance_urn(),
                aspect=DataPlatformInstancePropertiesClass(
                    name=self.name or self.id,
                    description=self.description,
                    externalUrl=self.external_url or None,
                    customProperties=self.custom_properties or {},
                ),
            )
        ]


def to_serialized_value(value: _Aspect) -> SerializedValueClass:
    # HACK: we remove the .pegasus2avro from the schema type since we want to refer to
    # the original pdl type
    schema_type = value.RECORD_SCHEMA.fullname.replace(".pegasus2avro", "")
    serialized_value = SerializedValueClass(
        blob=json.dumps(value.to_obj()).encode("utf-8"),
        contentType=SerializedValueContentTypeClass.JSON,
        schemaType=SerializedValueSchemaTypeClass.PEGASUS,
        schemaRef=schema_type,
    )
    return serialized_value


class SlackUserDetails:
    def __init__(self, slack_user_info: SlackUserInfo):
        self.slack_user_info = slack_user_info

    def to_guid(self) -> str:
        """
        A slack user is uniquely identified by the combination of their id and teamId.
        """
        return datahub_guid(
            {"id": self.slack_user_info.id, "dpi": self.slack_user_info.teamId}
        )

    def get_resource_urn(self) -> str:
        return f"urn:li:platformResource:{self.to_guid()}"

    def to_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:
        resource_urn = self.get_resource_urn()

        dpi = DataPlatformInstanceClass(
            platform=DATA_PLATFORM_SLACK_URN,
            instance=self.slack_user_info.slackInstance,
        )

        secondary_keys = []
        if self.slack_user_info.email:
            secondary_keys.append(self.slack_user_info.email)

        resource_info = PlatformResourceInfoClass(
            resourceType=ResourceType.USER_INFO.value,
            value=to_serialized_value(self.slack_user_info),
            primaryKey=self.slack_user_info.id,
            secondaryKeys=secondary_keys,
        )

        status = StatusClass(
            removed=self.slack_user_info.isDeleted,
        )

        yield from MetadataChangeProposalWrapper.construct_many(
            resource_urn, aspects=[dpi, resource_info, status]
        )


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
    team_id: Optional[str] = None
    team_domain: Optional[str] = None
    is_team_enterprise: Optional[bool] = None


class SlackSourceConfig(
    StatefulIngestionConfigBase,
):
    bot_token: SecretStr = Field(
        description="Bot token for the Slack workspace. Needs `users:read`, `users:read.email`, `users.profile:read`, and `team:read` scopes.",
    )
    enrich_user_metadata: bool = Field(
        type=bool,
        default=True,
        description="When enabled, will enrich provisioned DataHub users' metadata with information from Slack.",
    )
    ingest_users: bool = Field(
        type=bool,
        default=True,
        description="Whether to ingest users. When set to true, will ingest all users in the Slack workspace (as platform resources) to simplify user enrichment after they are provisioned on DataHub.",
    )
    api_requests_per_min: int = Field(
        type=int,
        default=10,
        description="Number of API requests per minute. Low-level config. Do not tweak unless you are facing any issues.",
    )
    ingest_public_channels: bool = Field(
        type=bool,
        default=False,
        description="Whether to ingest public channels. If set to true needs `channels:read` scope.",
    )
    channels_iteration_limit: int = Field(
        type=int,
        default=200,
        description="Limit the number of channels to be ingested in a iteration. Low-level config. Do not tweak unless you are facing any issues.",
    )
    channel_min_members: int = Field(
        type=int,
        default=2,
        description="Ingest channels with at least this many members.",
    )
    should_ingest_archived_channels: bool = Field(
        type=bool,
        default=False,
        description="Whether to ingest archived channels.",
    )


@dataclass
class SlackSourceReport(StaleEntityRemovalSourceReport):
    channels_reported: int = 0
    archived_channels_reported: int = 0
    users_reported: int = 0


PLATFORM_NAME = "slack"
DATA_PLATFORM_SLACK_URN: str = builder.make_data_platform_urn(PLATFORM_NAME)


@platform_name("Slack")
@config_class(SlackSourceConfig)
@support_status(SupportStatus.TESTING)
class SlackSource(StatefulIngestionSourceBase):
    def __init__(self, ctx: PipelineContext, config: SlackSourceConfig):
        super().__init__(config, ctx)
        self.ctx = ctx
        self.config = config
        self.report: SlackSourceReport = SlackSourceReport()
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

    @staticmethod
    def populate_slack_member_from_response(
        user: Dict[str, Any], slack_instance: SlackInstance
    ) -> SlackUserDetails:
        profile = user.get("profile", {})

        user_info = SlackUserInfo(
            slackInstance=slack_instance.to_platform_instance_urn(),
            id=user["id"],
            name=user["name"],
            realName=user.get("real_name", ""),
            displayName=profile.get("display_name", ""),
            email=profile.get("email"),
            teamId=user["team_id"],
            isDeleted=user.get("deleted", False),
            isAdmin=user.get("is_admin", False),
            isOwner=user.get("is_owner", False),
            isPrimaryOwner=user.get("is_primary_owner", False),
            isBot=user.get("is_bot", False),
            timezone=user.get("tz"),
            timezoneOffset=user.get("tz_offset"),
            title=profile.get("title"),
            phone=profile.get("phone"),
            profilePictureUrl=profile.get(
                "image_192"
            ),  # Using 192px image as an example
            statusText=profile.get("status_text"),
            statusEmoji=profile.get("status_emoji"),
            lastUpdatedSeconds=user.get("updated"),
        )
        return SlackUserDetails(slack_user_info=user_info)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

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
        if self.config.enrich_user_metadata or self.config.ingest_users:
            yield from self.get_user_info()

    def _get_datahub_user_info(
        self,
    ) -> Dict[str, Tuple[CorpUser, Optional[CorpUserEditableInfoClass]]]:
        # get_user_to_be_updated ensures that the email field is not None
        users = {
            user_obj.email: (user_obj, editable_properties)
            for user_obj, editable_properties in self.get_user_to_be_updated()
            if user_obj.email
        }
        return users

    def get_user_info(self) -> Iterable[MetadataWorkUnit]:
        # Get team information to populate for all users
        slack_instance: Optional[SlackInstance] = None
        with self.rate_limiter:
            team_response = self.get_slack_client().team_info()
            if team_response and "team" in team_response:
                team_info = team_response["team"]
                slack_instance = SlackInstance(id=team_info.get("id"))
                slack_instance = slack_instance.with_slack_team_info(team_info)

        if slack_instance:
            for mcp in slack_instance.to_mcps():
                yield mcp.as_workunit()
        else:
            logger.error("Failed to fetch team information")
            self.report.report_failure(
                "team_info", "Failed to fetch team information for users"
            )

        assert slack_instance

        # Fetch all DataHub users that need to be updated
        if self.config.enrich_user_metadata:
            datahub_users = self._get_datahub_user_info()
        else:
            datahub_users = {}
        cursor = None
        while True:
            with self.rate_limiter:
                response = self.get_slack_client().users_list(cursor=cursor)
            assert isinstance(response.data, dict)
            if not response.data["ok"]:
                self.report.report_failure("users", "Failed to fetch users")
                return

            assert self.ctx.graph is not None
            for user in response.data["members"]:
                # Query all slack users and ingest them into the generic
                # slackMember aspect
                slack_user_details: SlackUserDetails = (
                    self.populate_slack_member_from_response(user, slack_instance)
                )
                if self.config.ingest_users:
                    for mcp in slack_user_details.to_mcps():
                        yield mcp.as_workunit()

                platform_resource_urn = slack_user_details.get_resource_urn()
                # If user is in DataHub, compute and emit CorpUserEditableInfo
                # aspect. This code will be removed once we have server side
                # processing of raw slackMember aspects. This code path can also
                # be turned off by setting enrich_user_metadata to False.
                user_obj_props_tuple = datahub_users.get(user["profile"].get("email"))
                if user_obj_props_tuple is None:
                    # User is not in DataHub or enrichment is disabled
                    continue
                user_obj, editable_properties = user_obj_props_tuple
                slack_user_profile = user.get("profile", {})
                user_obj.slack_id = user.get("id")
                user_obj.title = slack_user_profile.get("title")
                user_obj.image_url = slack_user_profile.get("image_192")
                user_obj.phone = slack_user_profile.get("phone")
                user_obj.real_name = slack_user_profile.get("real_name")
                user_obj.slack_display_name = slack_user_profile.get("display_name")
                corpuser_editable_info = editable_properties or (
                    CorpUserEditableInfoClass()
                )
                emittable_corpuser_editable_info = self.populate_corpuser_editable_info(
                    corpuser_editable_info,
                    user_obj,
                    platform_resource_urn=platform_resource_urn,
                    slack_instance=slack_instance,
                )
                if emittable_corpuser_editable_info:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=user_obj.urn, aspect=emittable_corpuser_editable_info
                    ).as_workunit()
                    # if we update corpusereditable info, we also update
                    # slackuserinfo. This will be removed once we have server
                    # side processing of raw slackMember aspects.
                    yield MetadataChangeProposalWrapper(
                        entityUrn=user_obj.urn,
                        aspect=slack_user_details.slack_user_info,
                    ).as_workunit()
                yield from self.emit_corp_user_slack_settings(user_obj)
            cursor = str(response.data["response_metadata"]["next_cursor"])
            if not cursor:
                break

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

    def populate_corpuser_editable_info(
        self,
        corpuser_editable_info: CorpUserEditableInfoClass,
        user_obj: CorpUser,
        platform_resource_urn: str,
        slack_instance: SlackInstance,
    ) -> Optional[CorpUserEditableInfoClass]:
        """
        Populate CorpUserEditableInfo aspect with user information from Slack.
        If changes are not required, None is returned.
        If changes are required, the updated aspect is returned.
        """
        mutation_required = False
        if not corpuser_editable_info.email and user_obj.email:
            mutation_required = True
            corpuser_editable_info.email = user_obj.email
        if not corpuser_editable_info.slack and user_obj.slack_id:
            mutation_required = True
            corpuser_editable_info.slack = user_obj.slack_id
        if not corpuser_editable_info.title and user_obj.title:
            mutation_required = True
            corpuser_editable_info.title = user_obj.title
        if user_obj.image_url and (
            is_picture_default_or_missing(corpuser_editable_info.pictureLink)
            or (
                is_slack_image(corpuser_editable_info.pictureLink)
                and user_obj.image_url != corpuser_editable_info.pictureLink
            )
        ):
            mutation_required = True
            corpuser_editable_info.pictureLink = user_obj.image_url
        if user_obj.phone and not corpuser_editable_info.phone:
            mutation_required = True
            corpuser_editable_info.phone = user_obj.phone
        if (
            not corpuser_editable_info.displayName
            or corpuser_editable_info.displayName == corpuser_editable_info.email
        ) and user_obj.real_name:
            mutation_required = True
            corpuser_editable_info.displayName = user_obj.real_name
        if mutation_required:
            # update informationSources
            corpuser_editable_info.informationSources = (
                []
                if not corpuser_editable_info.informationSources
                else corpuser_editable_info.informationSources
            )
            if platform_resource_urn not in corpuser_editable_info.informationSources:
                corpuser_editable_info.informationSources.append(platform_resource_urn)
            return corpuser_editable_info
        return None

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

    def emit_slack_member_aspect(
        self, user: SlackUserInfo
    ) -> Iterable[MetadataWorkUnit]:
        slack_user = SlackUserDetails(slack_user_info=user)
        for mcp in slack_user.to_mcps():
            yield mcp.as_workunit()

    def emit_corp_user_slack_settings(
        self, user_obj: CorpUser
    ) -> Iterable[MetadataWorkUnit]:
        assert self.ctx.graph is not None

        if not user_obj.urn:
            return

        corp_user_settings = self.ctx.graph.get_aspect(
            user_obj.urn, CorpUserSettingsClass
        )
        if not corp_user_settings:
            return

        notification_settings = corp_user_settings.notificationSettings

        if not notification_settings:
            corp_user_settings.notificationSettings = NotificationSettingsClass(
                sinkTypes=[],
                slackSettings=SlackNotificationSettingsClass(
                    userHandle=user_obj.slack_id
                ),
            )
        elif (
            not notification_settings.slackSettings
            or not notification_settings.slackSettings.userHandle
        ):
            notification_settings.slackSettings = SlackNotificationSettingsClass(
                userHandle=user_obj.slack_id
            )
        else:
            return

        yield MetadataWorkUnit(
            id=f"{user_obj.urn}",
            mcp=MetadataChangeProposalWrapper(
                entityUrn=user_obj.urn,
                aspect=corp_user_settings,
            ),
        )

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=60),
        before_sleep=before_sleep_log(logger, logging.ERROR, True),
    )
    def get_user_to_be_updated(
        self,
    ) -> Iterable[Tuple[CorpUser, Optional[CorpUserEditableInfoClass]]]:
        assert self.ctx.graph is not None
        for urn in self.ctx.graph.get_urns_by_filter(
            entity_types=["corpuser"], query="*"
        ):
            user_obj = CorpUser()
            user_obj.urn = urn
            editable_properties = self.ctx.graph.get_aspect(
                urn, CorpUserEditableInfoClass
            )
            if editable_properties and editable_properties.email:
                user_obj.email = editable_properties.email
            else:
                urn_id = Urn.from_string(user_obj.urn).get_entity_id_as_string()
                if "@" in urn_id:
                    user_obj.email = urn_id
            if user_obj.email is not None:
                yield (user_obj, editable_properties)

    def get_report(self) -> SourceReport:
        return self.report
