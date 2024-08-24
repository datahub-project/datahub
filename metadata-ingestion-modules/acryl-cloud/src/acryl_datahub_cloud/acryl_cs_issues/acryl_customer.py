import json
import logging
import re
import time
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import jinja2
import openai

from acryl_datahub_cloud.acryl_cs_issues.acryl_linear import LinearAPIClient
from acryl_datahub_cloud.acryl_cs_issues.models import (
    DataHubProductGlossary,
    ExternalUser,
    Issue,
)
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ContainerClass,
    CorpUserInfoClass,
    DatasetPropertiesClass,
    DomainPropertiesClass,
    DomainsClass,
    GlobalTagsClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    OwnershipTypeClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
    TimeStampClass,
)
from datahub.metadata.urns import Urn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def needs_properties_update(
    issue: Issue, issue_properties: Optional[DatasetPropertiesClass]
) -> bool:
    """
    Check if the issue properties needs to be updated in the DataHub graph.
    1. If the issue has been updated since the last time it was updated in
    the graph
    2. If the summary algorithm version has changed
    3. If the length of the conversation list has changed
    4. If the glossary version has changed
    """
    if not issue_properties:
        return True
    if (
        issue.last_activity
        and issue_properties.lastModified
        and issue.last_activity
        > datetime.fromtimestamp(issue_properties.lastModified.time / 1000.0)
    ):
        return True
    if (
        issue_properties.customProperties.get("summarizer_algo_version")
        != IssueSummarizer.ALGO_VERSION
    ):
        return True
    if (
        issue_properties.customProperties.get("summarizer_glossary_version")
        != DataHubProductGlossary.VERSION
    ):
        return True
    if issue_properties.customProperties.get(
        "num_comments"
    ) and issue_properties.customProperties.get("num_comments") != str(
        len(issue.conversation_list)
    ):
        return True
    return False


class IssueSummarizer:
    ALGO_VERSION = "v0.3"  # Update this when the summarization algorithm changes

    def __init__(self, api_key: str):
        self.openai_client = openai.Client(api_key=api_key)
        self.prompt_template = jinja2.Template(
            """Categorize this description into either a {{ IssueCategory.BUG.value }} or a {{ IssueCategory.FEATURE_REQUEST.value }}.
Also provide a feature area that this description corresponds to.
Options are:
{% for fa in FEATURE_AREAS %}
{{ fa.name }}: {{ fa.description }}
Examples:
{% for example in fa.examples %}
- {{ example }}
{% endfor %}
{% endfor %}

If this is an Ingestion issue, please provide the platforms that this issue is
related to.
Options are
{% for platform in IntegrationPlatform %}{{ platform.value }}{% if not loop.last %}, {% endif %}{% endfor %}

If you are not sure about the platform, please don't provide any platforms.

Also, summarize the issue in a few words. Indicate if the issue is still
unresolved or closed, what is the current action item and who owns it.
Include names of all participants in the discussion and the last activity date
in the summary.

The issue is as follows:

Subject: {{ issue.subject }}
Creator: {{ issue.creator.name  if issue.creator else "Unknown" }}
Assignee: {{ issue.assignee.name if issue.assignee else "Unassigned" }}
Description: {{ issue.description }}

e.g.
{
  "category": "{{ IssueCategory.BUG.value }}",
  "feature_area": "SEARCH",
  "summary": "The autocomplete search is resulting in errors for some users. The issue is still unresolved and last activity was 5 days ago."
}
or
{
  "category": "{{ IssueCategory.FEATURE_REQUEST.value }}",
  "feature_area": "BUSINESS_GLOSSARY",
  "summary": "Add support for custom attributes in the business glossary. The discussion started 3 months ago and is still ongoing. Participants are @user1, @user2, and @user3. Main action item is to finalize the schema."
}
or
{
  "category": "{{ IssueCategory.BUG.value }}",
  "feature_area": "INGESTION",
  "platforms": ["{{ IntegrationPlatform.SNOWFLAKE.value }}"],
  "summary": "The ingestion job is failing with a connection error. The issue was debugged by @user1 and @user2. The issue is still unresolved."
}
or
{
  "category": "{{ IssueCategory.FEATURE_REQUEST.value }}",
  "feature_area": "INGESTION",
  "platforms": ["{{ IntegrationPlatform.SNOWFLAKE.value }}", "{{ IntegrationPlatform.BIGQUERY.value }}"],
  "summary": "Add support for profiling in the ingestion job. The action item is on @user1 who has promised to deliver by the end of the week. The issue is stale and needs a follow-up."
}
"""
        )

    def prompt(self, issue: "Issue") -> str:
        return self.prompt_template.render(
            issue=issue,
            IssueCategory=DataHubProductGlossary.IssueCategory,
            FEATURE_AREAS=DataHubProductGlossary.FEATURE_AREAS,
            IntegrationPlatform=DataHubProductGlossary.IntegrationPlatform,
        )

    def summarize(self, issue: "Issue") -> dict:
        prompt_string = self.prompt(issue=issue)
        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful assistant that categorizes issues.",
                    },
                    {"role": "user", "content": prompt_string},
                ],
            )
            assert isinstance(response.choices[0].message.content, str)
            return json.loads(response.choices[0].message.content)
        except json.JSONDecodeError:
            return {"error": "Could not parse JSON response"}
        except openai.OpenAIError as e:
            return {"error": f"OpenAI API error: {str(e)}"}
        except Exception as e:
            return {"error": f"Unexpected error: {str(e)}"}


class AcrylCustomer:
    @classmethod
    def create_or_get(
        cls, graph: DataHubGraph, id: Optional[str] = None, name: Optional[str] = None
    ) -> "AcrylCustomer":
        return cls(graph, id, name)

    def __init__(self, graph: DataHubGraph, id: Optional[str], name: Optional[str]):
        self.graph = graph
        if not name and id:
            domain_properties = self.graph.get_aspect(
                "urn:li:domain:" + id,
                DomainPropertiesClass,
            )
            if not domain_properties:
                raise ValueError(f"Customer {id} not found")
            name = domain_properties.name
        if name and not id:
            urns = [
                u
                for u in self.graph.get_urns_by_filter(
                    entity_types=["domain"],
                    extraFilters=[
                        {
                            "field": "name.keyword",
                            "values": [name],
                            "condition": "EQUAL",
                        }
                    ],
                )
            ]
            if not urns:
                raise ValueError(f"Customer {name} not found")
            if len(urns) > 1:
                raise ValueError(f"Multiple customers found with name {name}")
            id = Urn.from_string(urns[0]).entity_ids[0]

        assert id, "Customer must have an id"
        assert name, "Customer must have a name"
        self.id = id
        self.name = name
        # A record of all the containers emitted for this customer
        self._emitted_containers: Dict[str, bool] = {}

    def _get_owner_from_assignee(self, assignee: ExternalUser) -> str:
        owner_urn_options = [
            urn
            for urn in self.graph.get_urns_by_filter(
                entity_types=["corpuser"],
                extraFilters=[
                    {
                        "field": "email",
                        "values": [assignee.email],
                        "condition": "EQUAL",
                    }
                ],
            )
        ]
        if not owner_urn_options:
            assert assignee.email, "Assignee must have an email"
            owner_urn = "urn:li:corpuser:" + assignee.email
            self.graph.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=owner_urn,
                    aspect=CorpUserInfoClass(
                        active=False,
                        email=assignee.email,
                        displayName=assignee.name,
                        fullName=assignee.name,
                    ),
                )
            )
            return owner_urn

        return owner_urn_options[0]

    def add_or_update_issue(
        self, issue: Issue
    ) -> Iterable[MetadataChangeProposalWrapper]:
        from datahub.emitter.mce_builder import (
            make_container_urn,
            make_data_platform_urn,
        )
        from datahub.metadata.schema_classes import (
            ContainerPropertiesClass,
            DataPlatformInstanceClass,
            OwnerClass,
            OwnershipClass,
        )

        if issue.project:
            container_urn = make_container_urn(
                f"{issue.project.project_id.platform}-{issue.project.project_id.id}"
            )
            if not self._emitted_containers.get(container_urn):
                yield from MetadataChangeProposalWrapper.construct_many(
                    entityUrn=container_urn,
                    aspects=[
                        ContainerPropertiesClass(
                            name=issue.project.name,
                            description=issue.project.description,
                            externalUrl=issue.project.external_url,
                        ),
                        DataPlatformInstanceClass(
                            platform=make_data_platform_urn(
                                issue.project.project_id.platform.lower()
                            ),
                        ),
                        SubTypesClass(
                            typeNames=[issue.project.type],
                        ),
                    ],
                )
                self._emitted_containers[container_urn] = True
        issue_urn = make_dataset_urn("zendesk", issue.issue_id.id)
        tags = []
        if issue.category:
            tags.append(issue.category.value)
        if issue.feature_area:
            tags.append(
                "feat:"
                + (
                    issue.feature_area
                    if isinstance(issue.feature_area, str)
                    else issue.feature_area.name
                ).lower()
            )
        if issue.platforms:
            for platform in issue.platforms:
                tags.append(
                    "integ:"
                    + (
                        platform if isinstance(platform, str) else platform.value
                    ).lower()
                )
        tags = ["urn:li:tag:" + tag for tag in tags]
        if issue.assignee:
            owner_urn = self._get_owner_from_assignee(issue.assignee)
        else:
            owner_urn = None

        other_assignees = list(
            set(
                [
                    self._get_owner_from_assignee(other_assignee)
                    for other_assignee in issue.other_assignees
                ]
            )
            - {owner_urn}
            if issue.other_assignees
            else set()
        )

        assert issue.updated_at, "Issue must have an updated_at timestamp"
        assert issue.created_at, "Issue must have a created_at timestamp"
        assert issue.last_activity, "Issue must have a last_activity timestamp"
        yield from MetadataChangeProposalWrapper.construct_many(
            entityUrn=issue_urn,
            aspects=[
                StatusClass(
                    removed=False,
                ),
                SubTypesClass(
                    typeNames=["Issue"],
                ),
                (
                    DatasetPropertiesClass(
                        name=issue.subject,
                        description=issue.description,
                        customProperties={
                            "status": issue.status,
                            "last_summarized_at": issue.last_activity.isoformat(),
                            "num_comments": str(len(issue.conversation_list)),
                            "summarizer_algo_version": IssueSummarizer.ALGO_VERSION,
                            "summarizer_glossary_version": DataHubProductGlossary.VERSION,
                        },
                        created=TimeStampClass(
                            time=int(issue.created_at.timestamp() * 1000.0)
                        ),
                        lastModified=TimeStampClass(
                            time=int(issue.last_activity.timestamp() * 1000.0)
                        ),
                        externalUrl=issue.external_url,
                    )
                    if needs_properties_update(
                        issue,
                        self.graph.get_aspect(issue_urn, DatasetPropertiesClass),
                    )
                    else None
                ),
                DomainsClass(
                    domains=["urn:li:domain:" + self.id],
                ),
                (
                    GlobalTagsClass(
                        tags=[TagAssociationClass(tag=t) for t in tags]
                        + [
                            TagAssociationClass(
                                tag="urn:li:tag:status:" + issue.status.lower()
                            )
                        ]
                    )
                    if tags
                    else None
                ),
                ContainerClass(container=container_urn) if issue.project else None,
                (
                    InstitutionalMemoryClass(
                        elements=(
                            [
                                InstitutionalMemoryMetadataClass(
                                    url=linked_resource.url,
                                    description=linked_resource.description
                                    or linked_resource.name,
                                    createStamp=AuditStampClass(
                                        time=int(issue.updated_at.timestamp() * 1000.0),
                                        actor="urn:li:corpuser:datahub",  # TODO: get the actual user who linked the resource
                                    ),
                                )
                                for linked_resource in issue.linked_resources
                            ]
                        )
                    )
                    if issue.linked_resources
                    else None
                ),
                (
                    OwnershipClass(
                        owners=(
                            [
                                OwnerClass(
                                    owner=owner_urn,
                                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                                )
                            ]
                            + (
                                [
                                    OwnerClass(
                                        owner=other_assignee,
                                        type=OwnershipTypeClass.STAKEHOLDER,
                                    )
                                    for other_assignee in other_assignees
                                ]
                                if other_assignees
                                else []
                            )
                        )
                    )
                    if owner_urn
                    else None
                ),
            ],
        )

    def save(self) -> None:
        root_domains = None
        root_domain_attempts = 5
        while not root_domains and root_domain_attempts > 0:
            root_domains = [
                urn
                for urn in self.graph.get_urns_by_filter(
                    entity_types=["domain"],
                    extraFilters=[
                        {
                            "field": "name.keyword",
                            "values": ["Customers"],
                            "condition": "EQUAL",
                        }
                    ],
                )
            ]
            root_domain_attempts -= 1
            if not root_domains:
                # create root domain
                self.graph.emit(
                    MetadataChangeProposalWrapper(
                        entityUrn="urn:li:domain:Customers",
                        aspect=DomainPropertiesClass(
                            name="Customers",
                        ),
                    )
                )
                logger.info("Root domain not found. Creating and retrying...")
                time.sleep(1)

        if not root_domains:
            raise ValueError("Root domain not found")
        root_domain = root_domains[0]
        assert self.id, "Customer must have an id"
        assert self.name, "Customer must have a name"
        domain_urn = "urn:li:domain:" + self.id
        if not self.graph.exists(domain_urn):
            self.graph.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=domain_urn,
                    aspect=DomainPropertiesClass(
                        name=self.name,
                        parentDomain=root_domain,
                    ),
                )
            )


class AcrylCustomerRepository:
    def __init__(self, graph: DataHubGraph):
        self.graph: DataHubGraph = graph
        self._customers: List[AcrylCustomer] = []
        self._customers = [c for c in self.customers()]
        self._customers_last_updated = datetime.now()
        self._customers_server_last_updated = self._customers_last_updated

    def customers(self, customer_id: Optional[str] = None) -> Iterable[AcrylCustomer]:
        if (
            self._customers
            and self._customers_last_updated >= self._customers_server_last_updated
        ):
            if customer_id:
                yield from [c for c in self._customers if c.id == customer_id]
            logger.debug(
                "Returning cached customers of size {}".format(len(self._customers))
            )
            yield from self._customers

        root_domain_urns = [
            urn
            for urn in self.graph.get_urns_by_filter(
                entity_types=["domain"],
                extraFilters=[
                    {
                        "field": "name.keyword",
                        "values": ["Customers"],
                        "condition": "EQUAL",
                    }
                ],
            )
        ]
        for urn in root_domain_urns:
            logger.debug(f"Discovered root urn {urn}")

        all_domain_urns = [
            urn
            for urn in self.graph.get_urns_by_filter(
                entity_types=["domain"],
            )
        ]
        customer_urns = []
        for urn in all_domain_urns:
            domain_properties = self.graph.get_aspect(urn, DomainPropertiesClass)
            if (
                domain_properties
                and domain_properties.parentDomain == root_domain_urns[0]
            ):
                logger.debug(f"Found child domain: {urn}")
                customer_urns.append(urn)

        for urn in customer_urns:
            id = Urn.from_string(urn).entity_ids[0]
            yield AcrylCustomer.create_or_get(id=id, graph=self.graph)

    def create_customer(self, name: str) -> AcrylCustomer:
        # first check if the customer already exists
        if next((c for c in self.customers() if c.name == name), None):
            return next(c for c in self.customers() if c.name == name)

        customer_id = "customer-" + name.lower().replace(" ", "-")
        customer = AcrylCustomer.create_or_get(
            graph=self.graph, id=customer_id, name=name
        )
        customer.save()
        self._customers.append(customer)
        self._customers_server_last_updated = datetime.now()
        self._customers_last_updated = self._customers_server_last_updated
        return customer

    def get(self, name: str) -> Optional[AcrylCustomer]:
        maybe_customer = [c for c in self._customers if c.name == name]
        if maybe_customer:
            return maybe_customer[0]
        return None

    def list(self) -> Iterable[AcrylCustomer]:
        return self._customers

    def delete(self, name: str) -> None:
        self._customers = [c for c in self._customers if c.name != name]


def needs_summarization(
    issue: Issue, graph: DataHubGraph, issue_summarizer: IssueSummarizer
) -> bool:
    issue_urn = make_dataset_urn("zendesk", issue.issue_id.id)
    issue_tags = graph.get_aspect(issue_urn, GlobalTagsClass)
    issue_properties = graph.get_aspect(issue_urn, DatasetPropertiesClass)
    if not issue_tags or not issue_tags.tags:
        # no tags present, so we need to summarize
        return True
    if (
        issue_properties
        and issue_properties.customProperties.get("summarizer_algo_version")
        != IssueSummarizer.ALGO_VERSION
    ):
        # issue was not summarized by the current version of the summarizer
        return True
    if (
        issue_properties
        and issue_properties.customProperties.get("summarizer_glossary_version")
        != DataHubProductGlossary.VERSION
    ):
        # issue was not summarized by the current version of the glossary
        return True
    if issue_properties and issue_properties.customProperties.get("last_summarized_at"):
        last_summarized_at = datetime.fromisoformat(
            issue_properties.customProperties["last_summarized_at"]
        )
        if issue.updated_at and last_summarized_at < issue.updated_at:
            # issue was updated since last summarization
            return True
        if issue.last_commented_at and last_summarized_at < issue.last_commented_at:
            # issue was commented on since last summarization
            return True

    return False


def get_linear_issue_url_from_comment(
    comment_body: str, linear_client: Optional[LinearAPIClient]
) -> Tuple[str, Optional[Issue]]:
    """
    Comment body looks like: Issue CUS-2296 was linked to the ticket
    Return the issue id from the comment. (in this case, CUS-2296)
    URL to the comment: https://linear.app/acryl-data/issue/CUS-2296
    """

    if "was linked to the ticket" in comment_body:
        match = re.search(r"Issue ([A-Z]+-\d+) was", comment_body)
        if match:
            issue_id = match.group(1)
            url = f"https://linear.app/acryl-data/issue/{issue_id}"
            comment_body = comment_body.replace(
                f"Issue {issue_id}", f"[Issue {issue_id}]({url})"
            )
            if linear_client:
                linear_issue_data: Issue = linear_client.get_issue(issue_id)
            else:
                # fill out a bare bones issue object
                linear_issue_data = Issue(
                    issue_id=Issue.IssueId(platform="linear", id=issue_id),
                    customer_id="",
                    external_url=url,
                )

            return comment_body, linear_issue_data
        else:
            logger.warning(f"Could not find issue id in comment {comment_body}")

    return comment_body, None


def get_platform_logo_url(platform: str) -> str:
    return {
        "slack": "/assets/platforms/slacklogo.png",
        "zendesk": "https://upload.wikimedia.org/wikipedia/commons/4/4c/Zendesk_logo_RGB.png",
        "linear": "https://linear.app/favicon.ico",
    }.get(platform.lower(), "")


# https://linear.app/cdn-cgi/imagedelivery/fO02fVwohEs9s9UHFwon6A/82d07241-84b3-4cdf-33b5-a09b8d169300/f=auto,dpr=2,q=95,fit=scale-down,metadata=none


def generate_markdown_with_logo(
    author: ExternalUser,
    date: datetime,
    platform: str,
    is_public: bool,
    logo_size: int = 20,
) -> str:
    """
    Generate a markdown string with an inline logo next to the author and date.

    :param author: The name of the author
    :param date: A datetime object representing the date
    :param logo_url: The URL of the logo image
    :param logo_size: The size (width and height) of the logo in pixels
    :return: A markdown string with the inline logo and text
    """
    logo_url = get_platform_logo_url(platform)
    formatted_date = date.strftime("%B %d, %Y")
    if logo_url:
        markdown = f'<img src="{logo_url}" alt="Logo" width="{logo_size}" height="{logo_size}" style="vertical-align: middle;"> by {author.name} on {formatted_date} {""if is_public else "(Internal)"}'
    else:
        markdown = f":{platform}: by {author.name} on {formatted_date} {'' if is_public else '(Internal)'}"
    return markdown
