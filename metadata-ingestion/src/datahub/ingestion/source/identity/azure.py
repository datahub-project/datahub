import json
import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List

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


class AzureConfig(ConfigModel):
    """Config to create a token and connect to Azure instance"""

    # Required
    client_id: str
    tenant_id: str
    client_secret: str
    redirect: str
    authority: str
    token_url: str
    graph_url: str

    # Optional: to ingest users, groups or both
    ingest_users: bool = True
    ingest_groups: bool = True
    ingest_group_membership: bool = True


@dataclass
class AzureSourceReport(SourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_filtered(self, name: str) -> None:
        self.filtered.append(name)


# Source that extracts Azure users, groups and group memberships using Microsoft Graph REST API
#
# Validated against load:
# - user count: 1000
# - group count: 100
# - group membership edges: 1000 (1 per user)


class AzureSource(Source):
    """Ingest Azure Uses and Groups into Datahub"""

    @classmethod
    def create(cls, config_dict, ctx):
        config = AzureConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def __init__(self, config: AzureConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = AzureSourceReport()
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
        token_request = requests.post(self.config.token_url, data=self.token_data)
        token = token_request.json().get("access_token")
        return token

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        # Create MetadataWorkUnits for CorpGroups
        if self.config.ingest_groups:
            azure_groups = self._get_azure_groups()
            datahub_corp_group_snapshots = self._map_azure_groups(azure_groups)
            for datahub_corp_group_snapshot in datahub_corp_group_snapshots:
                mce = MetadataChangeEvent(proposedSnapshot=datahub_corp_group_snapshot)
                wu = MetadataWorkUnit(id=datahub_corp_group_snapshot.urn, mce=mce)
                self.report.report_workunit(wu)
                yield wu
        # Populate GroupMembership Aspects for CorpUsers
        datahub_corp_user_urn_to_group_membership: Dict[str, GroupMembershipClass] = {}
        if self.config.ingest_group_membership and azure_groups:
            # Fetch membership for each group
            for azure_group in azure_groups:
                datahub_corp_group_urn = self._map_azure_group_to_urn(azure_group)
                if not datahub_corp_group_urn:
                    error_str = "Failed to extract DataHub Group Name from Azure Group: Invalid regex pattern provided or missing profile attribute for group named {0}. Skipping...".format(
                        azure_group.get("displayName")
                    )
                    logger.error(error_str)
                    self.report.report_failure("azure_group_mapping", error_str)
                    continue
                # Extract and map users for each group
                azure_group_users = self._get_azure_group_users(azure_group)
                # if group doesn't have any members, continue
                if not azure_group_users:
                    continue
                for azure_user in azure_group_users:
                    datahub_corp_user_urn = self._map_azure_user_to_urn(azure_user)
                    if not datahub_corp_user_urn:
                        error_str = "Failed to extract DataHub Username from Azure User: Invalid regex pattern provided or missing profile attribute for User with login {0}. Skipping...".format(
                            azure_user.get("displayName")
                        )
                        logger.error(error_str)
                        self.report.report_failure("azure_user_mapping", error_str)
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
            azure_users = self._get_azure_users()
            datahub_corp_user_snapshots = self._map_azure_users(azure_users)
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

    def _get_azure_groups(self):
        headers = {"Authorization": "Bearer {0}".format(self.token)}
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
                error_str = "Response status code: {0}. Response content: {1}".format(response.status_code, response.content)
                logger.error(error_str)
                self.report.report_failure("_get_azure_groups", error_str)
                continue

    def _get_azure_users(self):
        headers = {"Authorization": "Bearer {0}".format(self.token)}
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
                error_str = "Response status code: {0}. Response content: {1}".format(response.status_code, response.content)
                logger.error(error_str)
                self.report.report_failure("_get_azure_groups", error_str)
                continue

    def _get_azure_group_users(self, azure_group):
        headers = {"Authorization": "Bearer {0}".format(self.token)}
        url = "{0}/groups/{1}/members".format(
            self.config.graph_url, azure_group.get("id")
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
                error_str = "Response status code: {0}. Response content: {1}".format(response.status_code, response.content)
                logger.error(error_str)
                self.report.report_failure("_get_azure_groups", error_str)
                continue

    def _map_azure_groups(self, azure_groups):
        for azure_group in azure_groups:
            corp_group_urn = self._map_azure_group_to_urn(azure_group)
            if not corp_group_urn:
                error_str = "Failed to extract DataHub Group Name from Azure Group: Invalid regex pattern provided or missing profile attribute for group named {0}. Skipping...".format(
                    azure_group.get("displayName")
                )
                logger.error(error_str)
                self.report.report_failure("azure_group_mapping", error_str)
                continue
            corp_group_snapshot = CorpGroupSnapshot(
                urn=corp_group_urn,
                aspects=[],
            )
            corp_group_info = self._map_azure_group_to_corp_group(azure_group)
            corp_group_snapshot.aspects.append(corp_group_info)
            yield corp_group_snapshot

    # Converts Azure group profile into DataHub CorpGroupInfoClass Aspect
    def _map_azure_group_to_corp_group(self, group):
        return CorpGroupInfoClass(
            displayName=group.get("displayName"),
            description=group.get("description"),
            email=group.get("displayName") + "@chinmayacryl.onmicrosoft.com",
            members=[],
            groups=[],
            admins=[],
        )

    def _map_azure_users(self, azure_users):
        for user in azure_users:
            corp_user_urn = self._map_azure_user_to_urn(user)
            if not corp_user_urn:
                error_str = "Failed to extract DataHub Username from Okta User: Invalid regex pattern provided or missing profile attribute for User with login {0}. Skipping...".format(
                    user.get("displayName")
                )
                logger.error(error_str)
                self.report.report_failure("azure_user_mapping", error_str)
                continue
            corp_user_snapshot = CorpUserSnapshot(
                urn=corp_user_urn,
                aspects=[],
            )
            corp_user_info = self._map_azure_user_to_corp_user(user)
            corp_user_snapshot.aspects.append(corp_user_info)
            yield corp_user_snapshot

    def _map_azure_user_to_corp_user(self, azure_user):
        return CorpUserInfoClass(
            active=True,
            displayName=azure_user.get("displayName"),
            firstName=azure_user.get("givenName", None),
            lastName=azure_user.get("surname", None),
            fullName="{0} {1}".format(
                azure_user.get("givenName", ""), azure_user.get("surname", "")
            ),
            # TODO: migrate to None once email is optional
            # email=azure_user.get("mail", ""),
            email=azure_user.get("displayName") + "@chinmayacryl.onmicrosoft.com",
            title=azure_user.get("jobTitle", None),
            countryCode=azure_user.get("mobilePhone", None),
        )

    # Creates Datahub CorpGroup Urn from Azure Group object
    def _map_azure_group_to_urn(self, azure_group):
        if not azure_group.get("displayName"):
            return None
        return self._make_corp_group_urn(groupname=azure_group.get("displayName"))

    # Creates Datahub CorpUser Urn from Azure User object
    def _map_azure_user_to_urn(self, azure_user):
        if not azure_user.get("displayName"):
            return None
        return self._make_corp_user_urn(username=azure_user.get("displayName"))

    def _make_corp_group_urn(self, groupname: str) -> str:
        return f"urn:li:corpGroup:{groupname}"

    def _make_corp_user_urn(self, username: str) -> str:
        return f"urn:li:corpuser:{username}"
