import logging
import textwrap
from dataclasses import dataclass
from typing import Iterable, Optional

from pydantic import Field, SecretStr
from slack_sdk import WebClient

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import CorpUserEditableInfoClass
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


class SlackSourceConfig(ConfigModel):
    bot_token: SecretStr = Field(
        description="Bot token for the Slack workspace. Needs `users:read`, `users:read.email` and `users.profile:read` scopes.",
    )


@platform_name("Slack")
@config_class(SlackSourceConfig)
@support_status(SupportStatus.TESTING)
class SlackSource(TestableSource):
    def __init__(self, ctx: PipelineContext, config: SlackSourceConfig):
        self.ctx = ctx
        self.config = config
        self.report = SourceReport()

    @classmethod
    def create(cls, config_dict, ctx):
        config = SlackSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        raise NotImplementedError("This class does not implement this method")

    def get_slack_client(self) -> WebClient:
        return WebClient(token=self.config.bot_token.get_secret_value())

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        assert self.ctx.graph is not None
        auth_resp = self.get_slack_client().auth_test()
        logger.info("Successfully connected to Slack")
        logger.info(auth_resp.data)
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
            yield MetadataWorkUnit(
                id=f"{user_obj.urn}",
                mcp=MetadataChangeProposalWrapper(
                    entityUrn=user_obj.urn,
                    aspect=corpuser_editable_info,
                ),
            )

    def populate_user_profile(self, user_obj: CorpUser) -> None:
        try:
            # https://api.slack.com/methods/users.profile.get
            user_profile_res = self.get_slack_client().users_profile_get(
                user=user_obj.slack_id
            )
            user_profile = user_profile_res.get("profile", {})
            user_obj.title = user_profile.get("title")
            user_obj.image_url = user_profile.get("image_192")
            user_obj.phone = user_profile.get("phone")
        except Exception as e:
            if "missing_scope" in str(e):
                raise e
            return

    def populate_slack_id_from_email(self, user_obj: CorpUser) -> None:
        if user_obj.email is None:
            return
        try:
            # https://api.slack.com/methods/users.lookupByEmail
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
