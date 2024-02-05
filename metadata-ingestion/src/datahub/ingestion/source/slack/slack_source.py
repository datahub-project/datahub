from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.source import (
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
import textwrap
from datahub.ingestion.api.common import PipelineContext
from pydantic import Field, SecretStr
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from slack_sdk import WebClient
from datahub.utilities.urns.urn import Urn
from typing import Iterable, Optional, Tuple
from datahub.ingestion.api.workunit import MetadataWorkUnit


class SlackSourceConfig(ConfigModel):
    bot_token: SecretStr = Field(
        default=None,
        description="Bot token for the Slack workspace.",
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

    def test_connection(config_dict: dict) -> TestConnectionReport:
        raise NotImplementedError("This class does not implement this method")

    def get_slack_client(self) -> None:
        return WebClient(token=self.config.bot_token.get_secret_value())

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        for ret_val in self.get_urn_email_without_slack_id():
            urn, email = ret_val
            slack_id = self.get_slack_id_from_email(email)
            if slack_id is None:
                continue
            # TODO Actually yield the workunit for adding slack id to URN
            print(f"urn: {urn}, Email: {email}, Slack ID: {slack_id}")
        return
        yield

    def get_slack_id_from_email(self, email: str) -> Optional[str]:
        try:
            user_info_res = self.get_slack_client().users_lookupByEmail(
                token=self.config.bot_token.get_secret_value(), email=email
            )
            user_info = user_info_res.get("user", {})
            return user_info.get("id")
        except Exception as e:
            if "users_not_found" in str(e):
                return None
            raise e

    def get_urn_email_without_slack_id(self) -> Iterable[Tuple[str, str]]:
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

        while start < total:
            variables = {"input": {"start": start, "count": count}}
            response = self.ctx.graph.execute_graphql(
                query=graphql_query, variables=variables
            )
            list_users = response.get("listUsers", {})
            total = list_users.get("total", 0)
            users = list_users.get("users", [])
            for user in users:
                editable_properties = user.get("editableProperties", {})
                email = None
                urn = user.get("urn")
                if editable_properties is not None:
                    slack = editable_properties.get("slack")
                    if slack is not None:
                        continue
                    email = editable_properties.get("email")
                if email is None:
                    urn_id = Urn.from_string(urn).get_entity_id_as_string()
                    if "@" in urn_id:
                        email = urn_id
                if email is not None:
                    yield urn, email
            start += count

    def get_report(self) -> SourceReport:
        return self.report
