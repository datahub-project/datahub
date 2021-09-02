import json
import logging
import re
import urllib
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Union

import click
import requests

from datahub.configuration import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    CorpGroupSnapshot,
    CorpUserSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    CorpGroupInfoClass,
    CorpUserInfoClass,
    GroupMembershipClass,
)

logger = logging.getLogger(__name__)


class AzureADConfig(ConfigModel):
    """Config to create a token and connect to Azure AD instance"""

    # Required
    client_id: str
    tenant_id: str
    client_secret: str
    redirect: str
    authority: str
    token_url: str
    graph_url: str

    # Optional: Customize the mapping to DataHub Username from an attribute in the REST API response
    # Reference: https://docs.microsoft.com/en-us/graph/api/user-list?view=graph-rest-1.0&tabs=http#response-1
    azure_ad_response_to_username_attr: str = "mail"
    azure_ad_response_to_username_regex: str = "([^@]+)"

    # Optional: Customize the mapping to DataHub Groupname from an attribute in the REST API response
    # Reference: https://docs.microsoft.com/en-us/graph/api/group-list?view=graph-rest-1.0&tabs=http#response-1
    azure_ad_response_to_groupname_attr: str = "displayName"
    azure_ad_response_to_groupname_regex: str = "(.*)"

    # Optional: to ingest users, groups or both
    ingest_users: bool = True
    ingest_groups: bool = True
    ingest_group_membership: bool = True


@dataclass
class AzureADSourceReport(SourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_filtered(self, name: str) -> None:
        self.filtered.append(name)


# Source that extracts Azure AD users, groups and group memberships using Microsoft Graph REST API
#
# Validated against load:
# - user count: 1000
# - group count: 100
# - group membership edges: 1000 (1 per user)


class AzureADSource(Source):
    """Ingest Azure AD Users and Groups into DataHub"""

    @classmethod
    def create(cls, config_dict, ctx):
        config = AzureADConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def __init__(self, config: AzureADConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = AzureADSourceReport()
        self.token_data = {
            "grant_type": "client_credentials",
            "client_id": self.config.client_id,
            "tenant_id": self.config.tenant_id,
            "client_secret": self.config.client_secret,
            "resource": "https://graph.microsoft.com",
            "scope": "https://graph.microsoft.com/.default",
        }
        self.token = self.get_token()

    def get_token(self):
        token_response = requests.post(self.config.token_url, data=self.token_data)
        if token_response.status_code == 200:
            token = token_response.json().get("access_token")
            return token
        else:
            error_str = f"Token response status code: {str(token_response.status_code)}. Token response content: {str(token_response.content)}"
            logger.error(error_str)
            self.report.report_failure("get_token", error_str)
            click.echo("Error: Token response invalid")
            exit()

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        # Create MetadataWorkUnits for CorpGroups
        if self.config.ingest_groups:
            azure_ad_groups = next(self._get_azure_ad_groups())
            datahub_corp_group_snapshots = self._map_azure_ad_groups(azure_ad_groups)
            for datahub_corp_group_snapshot in datahub_corp_group_snapshots:
                mce = MetadataChangeEvent(proposedSnapshot=datahub_corp_group_snapshot)
                wu = MetadataWorkUnit(id=datahub_corp_group_snapshot.urn, mce=mce)
                self.report.report_workunit(wu)
                yield wu
        # Populate GroupMembership Aspects for CorpUsers
        datahub_corp_user_urn_to_group_membership: Dict[str, GroupMembershipClass] = {}
        if self.config.ingest_group_membership and azure_ad_groups:
            # Fetch membership for each group
            for azure_ad_group in azure_ad_groups:
                datahub_corp_group_urn = self._map_azure_ad_group_to_urn(azure_ad_group)
                if not datahub_corp_group_urn:
                    error_str = "Failed to extract DataHub Group Name from Azure AD Group named {}. Skipping...".format(
                        azure_ad_group.get("displayName")
                    )
                    self.report.report_failure("azure_ad_group_mapping", error_str)
                    continue
                # Extract and map users for each group
                azure_ad_group_users = next(
                    self._get_azure_ad_group_users(azure_ad_group)
                )
                # if group doesn't have any members, continue
                if not azure_ad_group_users:
                    continue
                for azure_ad_user in azure_ad_group_users:
                    datahub_corp_user_urn = self._map_azure_ad_user_to_urn(
                        azure_ad_user
                    )
                    if not datahub_corp_user_urn:
                        error_str = "Failed to extract DataHub Username from Azure ADUser {}. Skipping...".format(
                            azure_ad_user.get("displayName")
                        )
                        self.report.report_failure("azure_ad_user_mapping", error_str)
                        continue

                    # update/create the GroupMembership aspect for this group member.
                    if (
                        datahub_corp_user_urn
                        in datahub_corp_user_urn_to_group_membership
                    ):
                        datahub_corp_user_urn_to_group_membership[
                            datahub_corp_user_urn
                        ].groups.append(datahub_corp_group_urn)
                    else:
                        datahub_corp_user_urn_to_group_membership[
                            datahub_corp_user_urn
                        ] = GroupMembershipClass(groups=[datahub_corp_group_urn])

        # Create MetadatWorkUnits for CorpUsers
        if self.config.ingest_users:
            azure_ad_users = next(self._get_azure_ad_users())
            datahub_corp_user_snapshots = self._map_azure_ad_users(azure_ad_users)
            for datahub_corp_user_snapshot in datahub_corp_user_snapshots:
                # Add GroupMembership if applicable
                if (
                    datahub_corp_user_snapshot.urn
                    in datahub_corp_user_urn_to_group_membership
                ):
                    datahub_group_membership = (
                        datahub_corp_user_urn_to_group_membership.get(
                            datahub_corp_user_snapshot.urn
                        )
                    )
                    assert datahub_group_membership
                    datahub_corp_user_snapshot.aspects.append(datahub_group_membership)
                mce = MetadataChangeEvent(proposedSnapshot=datahub_corp_user_snapshot)
                wu = MetadataWorkUnit(id=datahub_corp_user_snapshot.urn, mce=mce)
                self.report.report_workunit(wu)
                yield wu

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass

    def _get_azure_ad_groups(self):
        headers = {"Authorization": "Bearer {}".format(self.token)}
        url = self.config.graph_url + "/groups"
        while True:
            if not url:
                break
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                json_data = json.loads(response.text)
                url = json_data.get("@odata.nextLink", None)
                yield json_data["value"]
            else:
                error_str = f"Response status code: {str(response.status_code)}. Response content: {str(response.content)}"
                logger.error(error_str)
                self.report.report_failure("_get_azure_ad_groups", error_str)
                continue

    def _get_azure_ad_users(self):
        headers = {"Authorization": "Bearer {}".format(self.token)}
        url = self.config.graph_url + "/users"
        while True:
            if not url:
                break
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                json_data = json.loads(response.text)
                url = json_data.get("@odata.nextLink", None)
                yield json_data["value"]
            else:
                error_str = f"Response status code: {str(response.status_code)}. Response content: {str(response.content)}"
                logger.error(error_str)
                self.report.report_failure("_get_azure_ad_groups", error_str)
                continue

    def _get_azure_ad_group_users(self, azure_ad_group):
        headers = {"Authorization": "Bearer {}".format(self.token)}
        url = "{0}/groups/{1}/members".format(
            self.config.graph_url, azure_ad_group.get("id")
        )
        while True:
            if not url:
                break
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                json_data = json.loads(response.text)
                url = json_data.get("@odata.nextLink", None)
                yield json_data["value"]
            else:
                error_str = f"Response status code: {str(response.status_code)}. Response content: {str(response.content)}"
                logger.error(error_str)
                self.report.report_failure("_get_azure_ad_groups", error_str)
                continue

    def _map_azure_ad_groups(self, azure_ad_groups):
        for azure_ad_group in azure_ad_groups:
            corp_group_urn = self._map_azure_ad_group_to_urn(azure_ad_group)
            if not corp_group_urn:
                error_str = "Failed to extract DataHub Group Name from Azure Group for group named {}. Skipping...".format(
                    azure_ad_group.get("displayName")
                )
                logger.error(error_str)
                self.report.report_failure("azure_ad_group_mapping", error_str)
                continue
            corp_group_snapshot = CorpGroupSnapshot(
                urn=corp_group_urn,
                aspects=[],
            )
            corp_group_info = self._map_azure_ad_group_to_corp_group(azure_ad_group)
            corp_group_snapshot.aspects.append(corp_group_info)
            yield corp_group_snapshot

    # Converts Azure group profile into DataHub CorpGroupInfoClass Aspect
    def _map_azure_ad_group_to_corp_group(self, group):
        return CorpGroupInfoClass(
            displayName=self._map_azure_ad_group_to_group_name(group),
            description=group.get("description"),
            email=group.get("mail"),
            members=[],
            groups=[],
            admins=[],
        )

    # Creates Datahub CorpGroup Urn from Azure AD Group object
    def _map_azure_ad_group_to_urn(self, azure_ad_group):
        group_name = self._map_azure_ad_group_to_group_name(azure_ad_group)
        if not group_name:
            return None
        # URL encode the group name to deal with potential spaces
        url_encoded_group_name = urllib.parse.quote(group_name)
        return self._make_corp_group_urn(url_encoded_group_name)

    def _map_azure_ad_group_to_group_name(self, azure_ad_group):
        return self._extract_regex_match_from_dict_value(
            azure_ad_group,
            self.config.azure_ad_response_to_groupname_attr,
            self.config.azure_ad_response_to_groupname_regex,
        )

    def _map_azure_ad_users(self, azure_ad_users):
        for user in azure_ad_users:
            corp_user_urn = self._map_azure_ad_user_to_urn(user)
            if not corp_user_urn:
                error_str = "Failed to extract DataHub Username from Azure AD User {}. Skipping...".format(
                    user.get("displayName")
                )
                logger.error(error_str)
                self.report.report_failure("azure_ad_user_mapping", error_str)
                continue
            corp_user_snapshot = CorpUserSnapshot(
                urn=corp_user_urn,
                aspects=[],
            )
            corp_user_info = self._map_azure_ad_user_to_corp_user(user)
            corp_user_snapshot.aspects.append(corp_user_info)
            yield corp_user_snapshot

    def _map_azure_ad_user_to_user_name(self, azure_ad_user):
        return self._extract_regex_match_from_dict_value(
            azure_ad_user,
            self.config.azure_ad_response_to_username_attr,
            self.config.azure_ad_response_to_username_regex,
        )

    # Creates DataHub CorpUser Urn from Azure AD User object
    def _map_azure_ad_user_to_urn(self, azure_ad_user):
        user_name = self._map_azure_ad_user_to_user_name(azure_ad_user)
        if not user_name:
            return None
        return self._make_corp_user_urn(user_name)

    def _map_azure_ad_user_to_corp_user(self, azure_ad_user):
        full_name = (
            str(azure_ad_user.get("givenName", ""))
            + " "
            + str(azure_ad_user.get("surname", ""))
        )
        return CorpUserInfoClass(
            active=True,
            displayName=azure_ad_user.get("displayName", full_name),
            firstName=azure_ad_user.get("givenName", None),
            lastName=azure_ad_user.get("surname", None),
            fullName=full_name,
            email=azure_ad_user.get("mail"),
            title=azure_ad_user.get("jobTitle", None),
            countryCode=azure_ad_user.get("mobilePhone", None),
        )

    def _make_corp_group_urn(self, groupname: str) -> str:
        return f"urn:li:corpGroup:{groupname}"

    def _make_corp_user_urn(self, username: str) -> str:
        return f"urn:li:corpuser:{username}"

    def _extract_regex_match_from_dict_value(
        self, str_dict: Dict[str, str], key: str, pattern: str
    ) -> Union[str, None]:
        raw_value = str_dict.get(key)
        if raw_value is None:
            return None
        match = re.search(pattern, raw_value)
        if match is None:
            return None
        return match.group()
