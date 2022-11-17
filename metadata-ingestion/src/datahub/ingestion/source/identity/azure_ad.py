import json
import logging
import re
import urllib
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Generator, Iterable, List

import click
import requests
from pydantic.fields import Field

from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import make_group_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (  # SourceCapability,; capability,
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    CorpGroupSnapshot,
    CorpUserSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    CorpGroupInfoClass,
    CorpUserInfoClass,
    GroupMembershipClass,
    OriginClass,
    OriginTypeClass,
    StatusClass,
)

logger = logging.getLogger(__name__)


class AzureADConfig(ConfigModel):
    """Config to create a token and connect to Azure AD instance"""

    # Required
    client_id: str = Field(
        description="Application ID. Found in your app registration on Azure AD Portal"
    )
    tenant_id: str = Field(
        description="Directory ID. Found in your app registration on Azure AD Portal"
    )
    client_secret: str = Field(
        description="Client secret. Found in your app registration on Azure AD Portal"
    )
    authority: str = Field(
        description="The authority (https://docs.microsoft.com/en-us/azure/active-directory/develop/msal-client-application-configuration) is a URL that indicates a directory that MSAL can request tokens from."
    )
    token_url: str = Field(
        description="The token URL that acquires a token from Azure AD for authorizing requests.  This source will only work with v1.0 endpoint."
    )
    # Optional: URLs for redirect and hitting the Graph API
    redirect: str = Field(
        "https://login.microsoftonline.com/common/oauth2/nativeclient",
        description="Redirect URI.  Found in your app registration on Azure AD Portal.",
    )

    graph_url: str = Field(
        "https://graph.microsoft.com/v1.0",
        description="[Microsoft Graph API endpoint](https://docs.microsoft.com/en-us/graph/use-the-api)",
    )

    # Optional: Customize the mapping to DataHub Username from an attribute in the REST API response
    # Reference: https://docs.microsoft.com/en-us/graph/api/user-list?view=graph-rest-1.0&tabs=http#response-1
    azure_ad_response_to_username_attr: str = Field(
        default="userPrincipalName",
        description="Which Azure AD User Response attribute to use as input to DataHub username mapping.",
    )
    azure_ad_response_to_username_regex: str = Field(
        default="(.*)",
        description="A regex used to parse the DataHub username from the attribute specified in `azure_ad_response_to_username_attr`.",
    )

    # Optional: Customize the mapping to DataHub Groupname from an attribute in the REST API response
    # Reference: https://docs.microsoft.com/en-us/graph/api/group-list?view=graph-rest-1.0&tabs=http#response-1
    azure_ad_response_to_groupname_attr: str = Field(
        default="displayName",
        description="Which Azure AD Group Response attribute to use as input to DataHub group name mapping.",
    )
    azure_ad_response_to_groupname_regex: str = Field(
        default="(.*)",
        description="A regex used to parse the DataHub group name from the attribute specified in `azure_ad_response_to_groupname_attr`.",
    )

    # Optional: to ingest users, groups or both
    ingest_users: bool = Field(
        default=True, description="Whether users should be ingested into DataHub."
    )
    ingest_groups: bool = Field(
        default=True, description="Whether groups should be ingested into DataHub."
    )
    ingest_group_membership: bool = Field(
        default=True,
        description="Whether group membership should be ingested into DataHub. ingest_groups must be True if this is True.",
    )

    ingest_groups_users: bool = Field(
        default=True,
        description="This option is useful only when `ingest_users` is set to False and `ingest_group_membership` to True. As effect, only the users which belongs to the selected groups will be ingested.",
    )
    users_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for users to filter in ingestion.",
    )
    groups_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for groups to include in ingestion.",
    )

    # If enabled, report will contain names of filtered users and groups.
    filtered_tracking: bool = Field(
        default=True,
        description="If enabled, report will contain names of filtered users and groups.",
    )

    # Optional: Whether to mask sensitive information from workunit ID's. On by default.
    mask_group_id: bool = Field(
        True,
        description="Whether workunit ID's for groups should be masked to avoid leaking sensitive information.",
    )
    mask_user_id: bool = Field(
        True,
        description="Whether workunit ID's for users should be masked to avoid leaking sensitive information.",
    )


@dataclass
class AzureADSourceReport(SourceReport):
    filtered: List[str] = field(default_factory=list)
    filtered_tracking: bool = field(default=True, repr=False)
    filtered_count: int = field(default=0)

    def report_filtered(self, name: str) -> None:
        self.filtered_count += 1
        if self.filtered_tracking:
            self.filtered.append(name)


# Source that extracts Azure AD users, groups and group memberships using Microsoft Graph REST API


@platform_name("Azure AD")
@config_class(AzureADConfig)
@support_status(SupportStatus.CERTIFIED)
class AzureADSource(Source):
    """
    This plugin extracts the following:

    - Users
    - Groups
    - Group Membership

    from your Azure AD instance.

    Note that any users ingested from this connector will not be able to log into DataHub unless you have Azure AD OIDC
    SSO enabled. You can, however, have these users ingested into DataHub before they log in for the first time if you
    would like to take actions like adding them to a group or assigning them a role.

    For instructions on how to do configure Azure AD OIDC SSO, please read the documentation
    [here](https://datahubproject.io/docs/authentication/guides/sso/configure-oidc-react-azure).

    ### Extracting DataHub Users

    #### Usernames

    Usernames serve as unique identifiers for users on DataHub. This connector extracts usernames using the
    "userPrincipalName" field of an [Azure AD User Response](https://docs.microsoft.com/en-us/graph/api/user-list?view=graph-rest-1.0&tabs=http#response-1),
    which is the unique identifier for your Azure AD users.

    If this is not how you wish to map to DataHub usernames, you can provide a custom mapping using the configurations options detailed below. Namely, `azure_ad_response_to_username_attr`
    and `azure_ad_response_to_username_regex`.

    #### Responses

    This connector also extracts basic user response information from Azure. The following fields of the Azure User Response are extracted
    and mapped to the DataHub `CorpUserInfo` aspect:

    - display name
    - first name
    - last name
    - email
    - title
    - country

    ### Extracting DataHub Groups

    #### Group Names

    Group names serve as unique identifiers for groups on DataHub. This connector extracts group names using the "name" attribute of an Azure Group Response.
    By default, a URL-encoded version of the full group name is used as the unique identifier (CorpGroupKey) and the raw "name" attribute is mapped
    as the display name that will appear in DataHub's UI.

    If this is not how you wish to map to DataHub group names, you can provide a custom mapping using the configurations options detailed below. Namely, `azure_ad_response_to_groupname_attr`
    and `azure_ad_response_to_groupname_regex`.

    #### Responses

    This connector also extracts basic group information from Azure. The following fields of the [Azure AD Group Response](https://docs.microsoft.com/en-us/graph/api/group-list?view=graph-rest-1.0&tabs=http#response-1) are extracted and mapped to the
    DataHub `CorpGroupInfo` aspect:

    - name
    - description

    ### Extracting Group Membership

    This connector additional extracts the edges between Users and Groups that are stored in [Azure AD](https://docs.microsoft.com/en-us/graph/api/group-list-members?view=graph-rest-1.0&tabs=http#response-1). It maps them to the `GroupMembership` aspect
    associated with DataHub users (CorpUsers).

    ### Prerequisite

    [Create a DataHub Application](https://docs.microsoft.com/en-us/graph/toolkit/get-started/add-aad-app-registration) within the Azure AD Portal with the permissions
    to read your organization's Users and Groups. The following permissions are required, with the `Application` permission type:

    - `Group.Read.All`
    - `GroupMember.Read.All`
    - `User.Read.All`

    """

    @classmethod
    def create(cls, config_dict, ctx):
        config = AzureADConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def __init__(self, config: AzureADConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = AzureADSourceReport(
            filtered_tracking=self.config.filtered_tracking
        )
        self.token_data = {
            "grant_type": "client_credentials",
            "client_id": self.config.client_id,
            "tenant_id": self.config.tenant_id,
            "client_secret": self.config.client_secret,
            "resource": "https://graph.microsoft.com",
            "scope": "https://graph.microsoft.com/.default",
        }
        self.token = self.get_token()
        self.selected_azure_ad_groups: list = []
        self.azure_ad_groups_users: list = []

    def get_token(self):
        token_response = requests.post(self.config.token_url, data=self.token_data)
        if token_response.status_code == 200:
            token = token_response.json().get("access_token")
            return token
        else:
            error_str = (
                f"Token response status code: {str(token_response.status_code)}. "
                f"Token response content: {str(token_response.content)}"
            )
            logger.error(error_str)
            self.report.report_failure("get_token", error_str)
            click.echo("Error: Token response invalid")
            exit()

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        # for future developers: The actual logic of this ingestion wants to be executed, in order:
        # 1) the groups
        # 2) the groups' memberships
        # 3) the users

        # Create MetadataWorkUnits for CorpGroups
        if self.config.ingest_groups:
            # 1) the groups
            for azure_ad_groups in self._get_azure_ad_groups():
                logger.info("Processing another groups batch...")
                datahub_corp_group_snapshots = self._map_azure_ad_groups(
                    azure_ad_groups
                )
                for group_count, datahub_corp_group_snapshot in enumerate(
                    datahub_corp_group_snapshots
                ):
                    mce = MetadataChangeEvent(
                        proposedSnapshot=datahub_corp_group_snapshot
                    )
                    wu_id = (
                        f"group-{group_count + 1}"
                        if self.config.mask_group_id
                        else datahub_corp_group_snapshot.urn
                    )
                    wu = MetadataWorkUnit(id=wu_id, mce=mce)
                    self.report.report_workunit(wu)
                    yield wu

                    group_origin_mcp = MetadataChangeProposalWrapper(
                        entityType="corpGroup",
                        entityUrn=datahub_corp_group_snapshot.urn,
                        changeType=ChangeTypeClass.UPSERT,
                        aspectName="origin",
                        aspect=OriginClass(OriginTypeClass.EXTERNAL, "AZURE_AD"),
                    )
                    group_origin_wu_id = f"group-origin-{group_count + 1 if self.config.mask_group_id else datahub_corp_group_snapshot.urn}"
                    group_origin_wu = MetadataWorkUnit(
                        id=group_origin_wu_id, mcp=group_origin_mcp
                    )
                    self.report.report_workunit(group_origin_wu)
                    yield group_origin_wu

                    group_status_mcp = MetadataChangeProposalWrapper(
                        entityType="corpGroup",
                        entityUrn=datahub_corp_group_snapshot.urn,
                        changeType=ChangeTypeClass.UPSERT,
                        aspectName="status",
                        aspect=StatusClass(removed=False),
                    )
                    group_status_wu_id = f"group-status-{group_count + 1 if self.config.mask_group_id else datahub_corp_group_snapshot.urn}"
                    group_status_wu = MetadataWorkUnit(
                        id=group_status_wu_id, mcp=group_status_mcp
                    )
                    self.report.report_workunit(group_status_wu)
                    yield group_status_wu

        # Populate GroupMembership Aspects for CorpUsers
        datahub_corp_user_urn_to_group_membership: Dict[
            str, GroupMembershipClass
        ] = defaultdict(lambda: GroupMembershipClass(groups=[]))
        if (
            self.config.ingest_group_membership
            and len(self.selected_azure_ad_groups) > 0
        ):
            # 2) the groups' membership
            for azure_ad_group in self.selected_azure_ad_groups:
                # Azure supports nested groups, but not DataHub.  We need to explode the nested groups into a flat list.
                datahub_corp_group_urn = self._map_azure_ad_group_to_urn(azure_ad_group)
                if not datahub_corp_group_urn:
                    error_str = f"Failed to extract DataHub Group Name from Azure AD Group named {azure_ad_group.get('displayName')}. Skipping..."
                    self.report.report_failure("azure_ad_group_mapping", error_str)
                    continue
                self._add_group_members_to_group_membership(
                    datahub_corp_group_urn,
                    azure_ad_group,
                    datahub_corp_user_urn_to_group_membership,
                )

        if (
            self.config.ingest_groups_users
            and self.config.ingest_group_membership
            and not self.config.ingest_users
        ):
            # 3) the users
            # getting infos about the users belonging to the found groups
            datahub_corp_user_snapshots = self._map_azure_ad_users(
                self.azure_ad_groups_users
            )
            yield from self.ingest_ad_users(
                datahub_corp_user_snapshots, datahub_corp_user_urn_to_group_membership
            )

        # Create MetadataWorkUnits for CorpUsers
        if self.config.ingest_users:
            # 3) the users
            for azure_ad_users in self._get_azure_ad_users():
                # azure_ad_users = next(self._get_azure_ad_users())
                datahub_corp_user_snapshots = self._map_azure_ad_users(azure_ad_users)
                yield from self.ingest_ad_users(
                    datahub_corp_user_snapshots,
                    datahub_corp_user_urn_to_group_membership,
                )

    def _add_group_members_to_group_membership(
        self,
        parent_corp_group_urn: str,
        azure_ad_group: dict,
        user_urn_to_group_membership: Dict[str, GroupMembershipClass],
    ) -> None:
        # Extract and map members for each group
        for azure_ad_group_members in self._get_azure_ad_group_members(azure_ad_group):
            # if group doesn't have any members, continue
            if not azure_ad_group_members:
                continue
            for azure_ad_member in azure_ad_group_members:
                odata_type = azure_ad_member.get("@odata.type")
                if odata_type == "#microsoft.graph.user":
                    self._add_user_to_group_membership(
                        parent_corp_group_urn,
                        azure_ad_member,
                        user_urn_to_group_membership,
                    )
                elif odata_type == "#microsoft.graph.group":
                    # Since DataHub does not support nested group, we add the members to the parent group and not the nested one.
                    self._add_group_members_to_group_membership(
                        parent_corp_group_urn,
                        azure_ad_member,
                        user_urn_to_group_membership,
                    )
                else:
                    # Unless told otherwise, we only care about users and groups.  Silently skip other object types.
                    logger.warning(
                        f"Unsupported @odata.type '{odata_type}' found in Azure group member. Skipping...."
                    )

    def _add_user_to_group_membership(
        self,
        group_urn: str,
        azure_ad_user: dict,
        user_urn_to_group_membership: Dict[str, GroupMembershipClass],
    ) -> None:
        user_urn = self._map_azure_ad_user_to_urn(azure_ad_user)
        if not user_urn:
            error_str = f"Failed to extract DataHub Username from Azure ADUser {azure_ad_user.get('displayName')}. Skipping..."
            self.report.report_failure("azure_ad_user_mapping", error_str)
        else:
            self.azure_ad_groups_users.append(azure_ad_user)
            # update/create the GroupMembership aspect for this group member.
            if group_urn not in user_urn_to_group_membership[user_urn].groups:
                user_urn_to_group_membership[user_urn].groups.append(group_urn)

    def ingest_ad_users(
        self,
        datahub_corp_user_snapshots: Generator[CorpUserSnapshot, Any, None],
        datahub_corp_user_urn_to_group_membership: dict,
    ) -> Generator[MetadataWorkUnit, Any, None]:
        for user_count, datahub_corp_user_snapshot in enumerate(
            datahub_corp_user_snapshots
        ):
            # Add GroupMembership if applicable
            if (
                datahub_corp_user_snapshot.urn
                in datahub_corp_user_urn_to_group_membership.keys()
            ):
                datahub_group_membership = (
                    datahub_corp_user_urn_to_group_membership.get(
                        datahub_corp_user_snapshot.urn
                    )
                )
                assert datahub_group_membership
                datahub_corp_user_snapshot.aspects.append(datahub_group_membership)
            mce = MetadataChangeEvent(proposedSnapshot=datahub_corp_user_snapshot)
            wu_id = f"user-snapshot-{user_count + 1 if self.config.mask_user_id else datahub_corp_user_snapshot.urn}"
            wu = MetadataWorkUnit(id=wu_id, mce=mce)
            self.report.report_workunit(wu)
            yield wu

            user_origin_mcp = MetadataChangeProposalWrapper(
                entityType="corpuser",
                entityUrn=datahub_corp_user_snapshot.urn,
                changeType=ChangeTypeClass.UPSERT,
                aspectName="origin",
                aspect=OriginClass(OriginTypeClass.EXTERNAL, "AZURE_AD"),
            )
            user_origin_wu_id = f"user-origin-{user_count + 1 if self.config.mask_user_id else datahub_corp_user_snapshot.urn}"
            user_origin_wu = MetadataWorkUnit(id=user_origin_wu_id, mcp=user_origin_mcp)
            self.report.report_workunit(user_origin_wu)
            yield user_origin_wu

            user_status_mcp = MetadataChangeProposalWrapper(
                entityType="corpuser",
                entityUrn=datahub_corp_user_snapshot.urn,
                changeType=ChangeTypeClass.UPSERT,
                aspectName="status",
                aspect=StatusClass(removed=False),
            )
            user_status_wu_id = f"user-status-{user_count + 1 if self.config.mask_user_id else datahub_corp_user_snapshot.urn}"
            user_status_wu = MetadataWorkUnit(id=user_status_wu_id, mcp=user_status_mcp)
            self.report.report_workunit(user_status_wu)
            yield user_status_wu

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass

    def _get_azure_ad_groups(self) -> Iterable[List]:
        yield from self._get_azure_ad_data(kind="/groups")

    def _get_azure_ad_users(self) -> Iterable[List]:
        yield from self._get_azure_ad_data(kind="/users")

    def _get_azure_ad_group_members(self, azure_ad_group: dict) -> Iterable[List]:
        group_id = azure_ad_group.get("id")
        kind = f"/groups/{group_id}/members"
        yield from self._get_azure_ad_data(kind=kind)

    def _get_azure_ad_data(self, kind: str) -> Iterable[List]:
        headers = {"Authorization": "Bearer {}".format(self.token)}
        #           'ConsistencyLevel': 'eventual'}
        url = self.config.graph_url + kind
        while True:
            if not url:
                break
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                json_data = json.loads(response.text)
                try:
                    url = json_data["@odata.nextLink"]
                except KeyError:
                    # no more data will follow
                    url = False  # type: ignore
                yield json_data["value"]
            else:
                error_str = (
                    f"Response status code: {str(response.status_code)}. "
                    f"Response content: {str(response.content)}"
                )
                logger.error(error_str)
                self.report.report_failure("_get_azure_ad_data_", error_str)
                continue

    def _map_identity_to_urn(self, func, id_to_extract, mapping_identifier, id_type):
        result, error_str = None, None
        try:
            result = func(id_to_extract)
        except Exception as e:
            error_str = "Failed to extract DataHub {} from Azure AD {} with name {} due to '{}'".format(
                id_type, id_type, id_to_extract.get("displayName"), repr(e)
            )
        if not result:
            error_str = "Failed to extract DataHub {} from Azure AD {} with name {} due to unknown reason".format(
                id_type, id_type, id_to_extract.get("displayName")
            )
        if error_str is not None:
            logger.error(error_str)
            self.report.report_failure(mapping_identifier, error_str)
        return result, error_str

    def _map_azure_ad_groups(self, azure_ad_groups):
        for azure_ad_group in azure_ad_groups:
            corp_group_urn, error_str = self._map_identity_to_urn(
                self._map_azure_ad_group_to_urn,
                azure_ad_group,
                "azure_ad_group_mapping",
                "group",
            )
            if error_str is not None:
                continue
            group_name = self._extract_regex_match_from_dict_value(
                azure_ad_group,
                self.config.azure_ad_response_to_groupname_attr,
                self.config.azure_ad_response_to_groupname_regex,
            )
            if not self.config.groups_pattern.allowed(group_name):
                self.report.report_filtered(f"{corp_group_urn}")
                continue
            self.selected_azure_ad_groups.append(azure_ad_group)
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
        # decode the group name to deal with URL encoding, and replace spaces with '_'
        url_encoded_group_name = urllib.parse.quote(group_name)
        return make_group_urn(url_encoded_group_name)

    def _map_azure_ad_group_to_group_name(self, azure_ad_group):
        return self._extract_regex_match_from_dict_value(
            azure_ad_group,
            self.config.azure_ad_response_to_groupname_attr,
            self.config.azure_ad_response_to_groupname_regex,
        )

    def _map_azure_ad_users(self, azure_ad_users):
        for user in azure_ad_users:
            corp_user_urn, error_str = self._map_identity_to_urn(
                self._map_azure_ad_user_to_urn, user, "azure_ad_user_mapping", "user"
            )
            if error_str is not None:
                continue
            if not self.config.users_pattern.allowed(corp_user_urn):
                self.report.report_filtered(f"{corp_user_urn}.*")
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
        return make_user_urn(user_name)

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

    def _extract_regex_match_from_dict_value(
        self, str_dict: Dict[str, str], key: str, pattern: str
    ) -> str:
        raw_value = str_dict.get(key)
        if raw_value is None:
            raise ValueError(f"Unable to find the key {key} in Group. Is it wrong?")
        match = re.search(pattern, raw_value)
        if match is None:
            raise ValueError(
                f"Unable to extract a name from {raw_value} with the pattern {pattern}"
            )
        return match.group()
