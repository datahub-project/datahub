import json
import logging
from datetime import datetime
from typing import Iterable, List, Optional

from acryl_datahub_cloud.acryl_cs_issues.acryl_customer import (
    AcrylCustomerRepository,
    IssueSummarizer,
    generate_markdown_with_logo,
    get_linear_issue_url_from_comment,
    get_platform_logo_url,
    needs_summarization,
)
from acryl_datahub_cloud.acryl_cs_issues.acryl_linear import LinearAPIClient
from acryl_datahub_cloud.acryl_cs_issues.acryl_slack import AcrylSlack
from acryl_datahub_cloud.acryl_cs_issues.acryl_zendesk import ZendeskClient
from acryl_datahub_cloud.acryl_cs_issues.models import (
    DataHubProductGlossary,
    ExternalUser,
    Issue,
    LinkedResource,
    Project,
    TicketComment,
)
from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.emitter.mce_builder import make_data_platform_urn
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
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import DataPlatformInfoClass, PlatformTypeClass

logger = logging.getLogger(__name__)


class ZendeskConfig(ConfigModel):
    email: str
    token: str
    subdomain: str = "acrylsupport"
    lookback_days: int = 30


class SlackConfig(ConfigModel):
    bot_token: str


class OpenAIConfig(ConfigModel):
    api_key: str


class LinearConfig(ConfigModel):
    api_key: str


class AcrylCSIssuesSourceConfig(ConfigModel):
    server: Optional[DatahubClientConfig] = None
    zendesk: ZendeskConfig
    slack: SlackConfig
    openai: Optional[OpenAIConfig] = None
    customer_names: Optional[AllowDenyPattern] = None
    ticket_subjects: Optional[AllowDenyPattern] = None
    linear: Optional[LinearConfig] = None


@platform_name(id="acryl", platform_name="Acryl")
@config_class(AcrylCSIssuesSourceConfig)
@support_status(SupportStatus.INCUBATING)
class AcrylCSIssuesSource(Source):
    def __init__(self, config: AcrylCSIssuesSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config: AcrylCSIssuesSourceConfig = config
        self.zendesk_client = ZendeskClient(
            email=config.zendesk.email,
            token=config.zendesk.token,
            subdomain=config.zendesk.subdomain,
        )
        self.slack_client = AcrylSlack(slack_token=config.slack.bot_token)
        self.issue_summarizer = (
            IssueSummarizer(api_key=config.openai.api_key) if config.openai else None
        )
        if self.config.linear:
            self.linear_client: Optional[LinearAPIClient] = LinearAPIClient(
                api_key=self.config.linear.api_key
            )
        else:
            self.linear_client = None
        ctx.require_graph()
        self.graph = ctx.graph

    def get_report(self) -> SourceReport:
        return SourceReport()

    def _provision_platform(
        self, platform: str, logo_url: str, graph: DataHubGraph
    ) -> None:

        platform_urn = make_data_platform_urn(platform)
        if not graph.exists(platform_urn):
            platform_info = DataPlatformInfoClass(
                name=platform,
                type=PlatformTypeClass.OTHERS,
                datasetNameDelimiter=".",
                displayName=platform.title(),
                logoUrl=logo_url,
            )
            graph.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=platform_urn, aspect=platform_info
                )
            )

    def _get_status_html(self, status: str) -> str:
        color_map = {
            "DONE": "#4CAF50",
            "PENDING": "#FFC107",
            "WAITING": "#F44336",
            "OPEN": "#FFC107",
            "CLOSED": "#4CAF50",
        }
        color = color_map.get(
            status.upper(), "#FFC107"
        )  # Default to yellow if status is unknown
        return f'<span style="display: inline-block; padding: 5px 10px; border-radius: 5px; color: white; font-weight: bold; background-color: {color};">{status}</span>'

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:  # noqa: C901
        assert self.graph, "CS Issues Source requires a DataHub graph"

        # Provision zendesk platform
        self._provision_platform(
            platform="zendesk",
            logo_url=get_platform_logo_url("zendesk"),
            graph=self.graph,
        )

        acryl_customer_repository = AcrylCustomerRepository(self.graph)
        for organization in self.zendesk_client.get_organizations():
            print(organization.name)
            acryl_customer_repository.create_customer(organization.name)

        customers = [c for c in acryl_customer_repository.customers()]
        for customer in customers:
            print(customer.name)
            assert customer.name, "Customer must have a name"
            if self.config.customer_names and not self.config.customer_names.allowed(
                customer.name
            ):
                logger.info(f"Skipping customer {customer.name} as per config")
                continue
            organization_id = self.zendesk_client.get_organization_id(customer.name)
            if organization_id:
                project = Project(
                    project_id=Project.ProjectId(
                        platform="zendesk", id=str(organization_id)
                    ),
                    name=customer.name,
                    type="Organization",
                    description="Zendesk issues for " + customer.name,
                    external_url=f"https://acrylsupport.zendesk.com/agent/organizations/{organization_id}/tickets",
                )
                for ticket in self.zendesk_client.get_tickets_last_period(
                    organization_id, lookback_days=self.config.zendesk.lookback_days
                ):
                    (
                        ticket,
                        org,
                        comments,
                    ) = self.zendesk_client.get_ticket_with_organization(ticket.id)
                    if (
                        self.config.ticket_subjects
                        and not self.config.ticket_subjects.allowed(ticket.subject)
                    ):
                        logger.info(
                            f"Skipping ticket {ticket.id} as per config since it doesn't match the subject rules"
                        )
                        continue
                    linked_resources = []
                    ticket_comments: List[TicketComment] = []
                    other_assignees = []
                    doc_string = "## Description\n\n"
                    if not comments:
                        doc_string += ticket.description
                    else:
                        for comment in comments:
                            (
                                comment_body,
                                linear_issue,
                            ) = get_linear_issue_url_from_comment(
                                comment.body, self.linear_client
                            )
                            ticket_comments.append(
                                TicketComment(
                                    created_at=datetime.fromisoformat(
                                        comment.created_at[:-1]
                                    ),
                                    public=comment.public,
                                    body=comment_body,
                                    platform="zendesk",
                                    author=ExternalUser(
                                        platform="zendesk",
                                        id=str(comment.author_id),
                                        name=comment.author.name,
                                        email=comment.author.email,
                                    ),
                                )
                            )
                            if linear_issue:
                                if linear_issue.assignee:
                                    other_assignees.append(linear_issue.assignee)
                                if linear_issue.external_url:
                                    linked_resources.append(
                                        LinkedResource(
                                            platform="linear",
                                            url=linear_issue.external_url,
                                            name=f"Linear ({linear_issue.issue_id.id})",
                                        )
                                    )
                                ticket_comments.extend(linear_issue.conversations())

                            slack_conversations = (
                                self.slack_client.get_slack_conversations_from_text(
                                    comment.body
                                )
                            )
                            if slack_conversations:
                                message_url = slack_conversations.get("url")
                                slack_messages = slack_conversations.get("messages")
                                is_thread = slack_conversations.get("is_thread")
                                if message_url:
                                    assert isinstance(message_url, str)
                                    linked_resources.append(
                                        LinkedResource(
                                            platform="slack",
                                            url=message_url,
                                            name=f"Slack {'Message' if not is_thread else 'Thread'}",
                                        )
                                    )
                                if slack_messages:
                                    assert isinstance(slack_messages, list)
                                    ticket_comments.extend(slack_messages)  # type: ignore

                    ticket_comments = sorted(
                        ticket_comments, key=lambda x: x.created_at
                    )
                    last_commented_at = (
                        ticket_comments[-1].created_at
                        if ticket_comments
                        else ticket.updated_at
                    )

                    for comment in ticket_comments:
                        if "```" in comment.body:
                            # Replace ``` with \n\n```\n\n to avoid markdown
                            # issues with code blocks
                            comment.body = comment.body.replace("```", "\n\n```\n\n")
                        doc_string += f"""----------------------------------------

{generate_markdown_with_logo(comment.author, comment.created_at, comment.platform, comment.public)}

{comment.body}

"""
                    assert customer.id, "Customer must have an ID"
                    issue = Issue(
                        project=project,
                        issue_id=Issue.IssueId(platform="zendesk", id=ticket.id),
                        customer_id=customer.id,
                        status=ticket.status,
                        subject=ticket.subject,
                        created_at=ticket.created_at,
                        updated_at=ticket.updated_at,
                        last_commented_at=last_commented_at,
                        description=doc_string,
                        linked_resources=linked_resources,
                        creator=ExternalUser(
                            platform="zendesk",
                            id=str(ticket.requester_id),
                            name=ticket.requester.name,
                            email=ticket.requester.email,
                        ),
                        assignee=(
                            ExternalUser(
                                platform="zendesk",
                                id=str(ticket.assignee_id),
                                name=ticket.assignee.name,
                                email=ticket.assignee.email,
                            )
                            if ticket.assignee
                            else None
                        ),
                        other_assignees=other_assignees,
                        external_url=f"https://acrylsupport.zendesk.com/agent/tickets/{ticket.id}",
                        conversation_list=ticket_comments,
                    )
                    if self.issue_summarizer and needs_summarization(
                        issue, self.graph, self.issue_summarizer
                    ):
                        logger.info(f"Summarizing issue {issue.issue_id.id}")
                        issue_summary = self.issue_summarizer.summarize(issue)
                        if "category" in issue_summary:
                            issue.category = DataHubProductGlossary.IssueCategory(
                                issue_summary["category"]
                            )
                        if "feature_area" in issue_summary:
                            try:
                                issue.feature_area = next(
                                    c
                                    for c in DataHubProductGlossary.FEATURE_AREAS
                                    if c.name == issue_summary["feature_area"]
                                )
                            except Exception as e:
                                logger.warning(
                                    f"Could not find feature area {issue_summary['feature_area']}: {str(e)}"
                                )
                                issue.feature_area = issue_summary["feature_area"]
                        if "platforms" in issue_summary:
                            if isinstance(issue_summary["platforms"], str):
                                try:
                                    issue_summary["platforms"] = json.loads(
                                        issue_summary["platforms"]
                                    )
                                except Exception as e:
                                    logger.warning(
                                        f"Could not parse platforms: {str(e)}"
                                    )
                            # check if platform is present in the enum before
                            # assigning
                            issue.platforms = []
                            for platform_response in issue_summary["platforms"]:
                                try:
                                    platform_response = (
                                        DataHubProductGlossary.IntegrationPlatform(
                                            platform_response
                                        )
                                    )
                                    issue.platforms.append(platform_response)
                                except Exception as e:
                                    logger.warning(
                                        f"Could not parse platform: {str(e)}"
                                    )
                                    issue.platforms.append(platform_response)
                                    continue
                        if "summary" in issue_summary:
                            issue.description = f"""## Summary

<p><strong>Status: {self._get_status_html(issue.status)}</strong></p>

{issue_summary["summary"]}

{issue.description}"""
                    for mcp in customer.add_or_update_issue(issue):
                        if mcp:
                            yield mcp.as_workunit()
