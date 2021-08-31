import asyncio
import logging
import re
import urllib
from dataclasses import dataclass, field
from time import sleep
from typing import Dict, Iterable, List, Union

from okta.client import Client as OktaClient
from okta.models import Group, GroupProfile, User, UserProfile, UserStatus

from datahub.configuration import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    CorpGroupSnapshot,
    CorpUserSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (  # GroupMembershipClass,
    CorpGroupInfoClass,
    CorpUserInfoClass,
    GroupMembershipClass,
)

logger = logging.getLogger(__name__)


class OktaConfig(ConfigModel):

    # Required: Domain of the Okta deployment. Example: dev-33231928.okta.com
    okta_domain = "dev-44231988.okta.com"
    # Required: An API token generated from Okta.
    okta_api_token = "00be4R_M2MzDqXawbWgfKGpKee0kuEOfX1RCQSRx00"

    # Optional: Whether to ingest users, groups, or both.
    ingest_users: bool = True
    ingest_groups: bool = True
    ingest_group_membership: bool = True

    # Optional: Customize the mapping to DataHub Username from an attribute appearing in the Okta User
    # profile. Reference: https://developer.okta.com/docs/reference/api/users/
    okta_profile_to_username_attr: str = "login"
    okta_profile_to_username_regex: str = "([^@]+)"

    # Optional: Customize the mapping to DataHub Group from an attribute appearing in the Okta Group
    # profile. Reference: https://developer.okta.com/docs/reference/api/groups/
    okta_profile_to_group_name_attr: str = "name"
    okta_profile_to_group_name_regex: str = "(.*)"

    # Optional: Include deprovisioned or suspended Okta users in the ingestion.
    include_deprovisioned_users = False
    include_suspended_users = False

    # Optional: Page size for reading groups and users from Okta API.
    page_size = 100

    # Optional: Set the delay for fetching batches of entities from Okta. Okta has rate limiting in place.
    delay_seconds = 0.01


@dataclass
class OktaSourceReport(SourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_filtered(self, name: str) -> None:
        self.filtered.append(name)


#
# Source Connector that extracts Users, Groups, and Group Membership in batch using the Okta Python SDK.
#
# Validated against Okta API Versions:
#   - 2021.07.2
#
# Validated against load:
#   - User Count: 1000
#   - Group Count: 100
#   - Group Membership Edges: 1000 (1 per User)
#   - Run Time (Wall Clock): 2min 7sec
#
class OktaSource(Source):
    """Ingest Okta Users & Groups into Datahub"""

    @classmethod
    def create(cls, config_dict, ctx):
        config = OktaConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def __init__(self, config: OktaConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = OktaSourceReport()
        self.okta_client = self._create_okta_client()

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:

        # Step 1: Produce MetadataWorkUnits for CorpGroups.
        if self.config.ingest_groups:
            okta_groups = list(self._get_okta_groups())
            datahub_corp_group_snapshots = self._map_okta_groups(okta_groups)
            for datahub_corp_group_snapshot in datahub_corp_group_snapshots:
                mce = MetadataChangeEvent(proposedSnapshot=datahub_corp_group_snapshot)
                wu = MetadataWorkUnit(id=datahub_corp_group_snapshot.urn, mce=mce)
                self.report.report_workunit(wu)
                yield wu

        # Step 2: Populate GroupMembership Aspects for CorpUsers
        datahub_corp_user_urn_to_group_membership: Dict[str, GroupMembershipClass] = {}
        if self.config.ingest_group_membership and okta_groups is not None:

            # Fetch membership for each group.
            for okta_group in okta_groups:
                datahub_corp_group_urn = self._map_okta_group_profile_to_urn(
                    okta_group.profile
                )
                if datahub_corp_group_urn is None:
                    error_str = f"Failed to extract DataHub Group Name from Okta Group: Invalid regex pattern provided or missing profile attribute for group named {okta_group.profile.name}. Skipping..."
                    logger.error(error_str)
                    self.report.report_failure("okta_group_mapping", error_str)
                    continue

                # Extract and map users for each group.
                okta_group_users = self._get_okta_group_users(okta_group)
                for okta_user in okta_group_users:
                    datahub_corp_user_urn = self._map_okta_user_profile_to_urn(
                        okta_user.profile
                    )
                    if datahub_corp_user_urn is None:
                        error_str = f"Failed to extract DataHub Username from Okta User: Invalid regex pattern provided or missing profile attribute for User with login {okta_user.profile.login}. Skipping..."
                        logger.error(error_str)
                        self.report.report_failure("okta_user_mapping", error_str)
                        continue

                    # Either update or create the GroupMembership aspect for this group member.
                    # TODO: Production of the GroupMembership aspect will overwrite the existing
                    # group membership for the DataHub user.
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

        # Step 3: Produce MetadataWorkUnits for CorpUsers.
        if self.config.ingest_users:
            okta_users = self._get_okta_users()
            filtered_okta_users = filter(self._filter_okta_user, okta_users)
            datahub_corp_user_snapshots = self._map_okta_users(filtered_okta_users)
            for datahub_corp_user_snapshot in datahub_corp_user_snapshots:

                # Add GroupMembership aspect populated in Step 2 if applicable.
                if (
                    datahub_corp_user_snapshot.urn
                    in datahub_corp_user_urn_to_group_membership
                ):
                    datahub_group_membership = (
                        datahub_corp_user_urn_to_group_membership.get(
                            datahub_corp_user_snapshot.urn
                        )
                    )
                    assert datahub_group_membership is not None
                    datahub_corp_user_snapshot.aspects.append(datahub_group_membership)
                mce = MetadataChangeEvent(proposedSnapshot=datahub_corp_user_snapshot)
                wu = MetadataWorkUnit(id=datahub_corp_user_snapshot.urn, mce=mce)
                self.report.report_workunit(wu)
                yield wu

    def get_report(self):
        return self.report

    def close(self):
        pass

    # Instantiates Okta SDK Client.
    def _create_okta_client(self):
        config = {
            "orgUrl": f"https://{self.config.okta_domain}",
            "token": f"{self.config.okta_api_token}",
        }
        return OktaClient(config)

    # Retrieves all Okta Group Objects in batches.
    def _get_okta_groups(self) -> Iterable[Group]:
        # Note that this is not taking full advantage of Python AsyncIO, as we are blocking on calls.
        query_parameters = {"limit": self.config.page_size}
        groups, resp, err = asyncio.get_event_loop().run_until_complete(
            self.okta_client.list_groups(query_parameters)
        )
        while True:
            if err is not None:
                self.report.report_failure(
                    "okta_groups", f"Failed to fetch Groups from Okta API: {err}"
                )
            if groups is not None:
                for group in groups:
                    yield group
            if resp is not None and resp.has_next():
                sleep(self.config.delay_seconds)
                groups, err = asyncio.get_event_loop().run_until_complete(resp.next())
            else:
                break

    # Retrieves Okta User Objects in a particular Okta Group in batches.
    def _get_okta_group_users(self, group: Group) -> Iterable[User]:
        # Note that this is not taking full advantage of Python AsyncIO; we are blocking on calls.
        query_parameters = {"limit": self.config.page_size}
        users, resp, err = asyncio.get_event_loop().run_until_complete(
            self.okta_client.list_group_users(group.id, query_parameters)
        )
        while True:
            if err is not None:
                self.report.report_failure(
                    "okta_group_users",
                    f"Failed to fetch Users of Group {group.profile.name} from Okta API: {err}",
                )
            if users is not None:
                for user in users:
                    yield user
            if resp is not None and resp.has_next():
                sleep(self.config.delay_seconds)
                users, err = asyncio.get_event_loop().run_until_complete(resp.next())
            else:
                break

    # Retrieves all Okta User Objects in batches.
    def _get_okta_users(self) -> Iterable[User]:
        query_parameters = {"limit": self.config.page_size}
        users, resp, err = asyncio.get_event_loop().run_until_complete(
            self.okta_client.list_users(query_parameters)
        )
        while True:
            if err is not None:
                self.report.report_failure(
                    "okta_users", f"Failed to fetch Users from Okta API: {err}"
                )
            if users is not None:
                for user in users:
                    yield user
            if resp is not None and resp.has_next():
                sleep(self.config.delay_seconds)
                users, err = asyncio.get_event_loop().run_until_complete(resp.next())
            else:
                break

    # Filters Okta User Objects based on provided configuration.
    def _filter_okta_user(self, okta_user: User) -> bool:
        if (
            self.config.include_deprovisioned_users is False
            and okta_user.status == UserStatus.DEPROVISIONED
        ):
            return False
        elif (
            self.config.include_suspended_users is False
            and okta_user.status == UserStatus.SUSPENDED
        ):
            return False
        return True

    # Converts Okta Group Objects into DataHub CorpGroupSnapshots.
    def _map_okta_groups(
        self, okta_groups: Iterable[Group]
    ) -> Iterable[CorpGroupSnapshot]:
        for okta_group in okta_groups:
            corp_group_urn = self._map_okta_group_profile_to_urn(okta_group.profile)
            if corp_group_urn is None:
                error_str = f"Failed to extract DataHub Group Name from Okta Group: Invalid regex pattern provided or missing profile attribute for group named {okta_group.profile.name}. Skipping..."
                logger.error(error_str)
                self.report.report_failure("okta_group_mapping", error_str)
                continue
            corp_group_snapshot = CorpGroupSnapshot(
                urn=corp_group_urn,
                aspects=[],
            )
            corp_group_info = self._map_okta_group_profile(okta_group.profile)
            corp_group_snapshot.aspects.append(corp_group_info)
            yield corp_group_snapshot

    # Creates DataHub CorpGroup Urn from Okta Group Object.
    def _map_okta_group_profile_to_urn(
        self, okta_group_profile: GroupProfile
    ) -> Union[str, None]:
        # Profile is a required field as per https://developer.okta.com/docs/reference/api/groups/#group-attributes
        group_name = self._map_okta_group_profile_to_group_name(okta_group_profile)
        if group_name is None:
            return None
        # URL Encode the Group Name to deal with potential spaces.
        # TODO: Modeling - Need to figure out a better way to generate a stable identifier for the group.
        url_encoded_group_name = urllib.parse.quote(group_name)
        return self._make_corp_group_urn(url_encoded_group_name)

    # Converts Okta Group Profile Object into a DataHub CorpGroupInfo Aspect.
    def _map_okta_group_profile(self, profile: GroupProfile) -> CorpGroupInfoClass:
        return CorpGroupInfoClass(
            displayName=self._map_okta_group_profile_to_group_name(profile),
            description=profile.description,
            members=[],
            groups=[],
            admins=[],
        )

    # Converts Okta Group Profile Object into a DataHub Group Name.
    def _map_okta_group_profile_to_group_name(
        self, okta_group_profile: GroupProfile
    ) -> Union[str, None]:
        # Profile is a required field as per https://developer.okta.com/docs/reference/api/groups/#group-attributes
        return self._extract_regex_match_from_dict_value(
            okta_group_profile.as_dict(),
            self.config.okta_profile_to_group_name_attr,
            self.config.okta_profile_to_group_name_regex,
        )

    # Converts Okta User Objects into DataHub CorpUserSnapshots.
    def _map_okta_users(self, okta_users: Iterable[User]) -> Iterable[CorpUserSnapshot]:
        for okta_user in okta_users:
            corp_user_urn = self._map_okta_user_profile_to_urn(okta_user.profile)
            if corp_user_urn is None:
                error_str = f"Failed to extract DataHub Username from Okta User: Invalid regex pattern provided or missing profile attribute for User with login {okta_user.profile.login}. Skipping..."
                logger.error(error_str)
                self.report.report_failure("okta_user_mapping", error_str)
                continue
            corp_user_snapshot = CorpUserSnapshot(
                urn=corp_user_urn,
                aspects=[],
            )
            corp_user_info = self._map_okta_user_profile(okta_user.profile)
            corp_user_snapshot.aspects.append(corp_user_info)
            yield corp_user_snapshot

    # Creates DataHub CorpUser Urn from Okta User Profile
    def _map_okta_user_profile_to_urn(
        self, okta_user_profile: UserProfile
    ) -> Union[str, None]:
        # Profile is a required field as per https://developer.okta.com/docs/reference/api/users/#user-attributes
        username = self._map_okta_user_profile_to_username(okta_user_profile)
        if username is None:
            return None
        return self._make_corp_user_urn(username)

    # Converts Okta User Profile Object into a DataHub User name.
    def _map_okta_user_profile_to_username(
        self, okta_user_profile: UserProfile
    ) -> Union[str, None]:
        # Profile is a required field as per https://developer.okta.com/docs/reference/api/users/#user-attributes
        return self._extract_regex_match_from_dict_value(
            okta_user_profile.as_dict(),
            self.config.okta_profile_to_username_attr,
            self.config.okta_profile_to_username_regex,
        )

    # Converts Okta User Profile into a CorpUserInfo.
    def _map_okta_user_profile(self, profile: UserProfile) -> CorpUserInfoClass:
        # TODO: Extract user's manager if provided.
        # Source: https://developer.okta.com/docs/reference/api/users/#default-profile-properties
        full_name = f"{profile.firstName} {profile.lastName}"
        return CorpUserInfoClass(
            active=True,
            displayName=profile.displayName
            if profile.displayName is not None
            else full_name,
            firstName=profile.firstName,
            lastName=profile.lastName,
            fullName=full_name,
            email=profile.email,
            title=profile.title,
            countryCode=profile.countryCode,
            departmentName=profile.department,
        )

    def _make_corp_group_urn(self, name: str) -> str:
        return f"urn:li:corpGroup:{name}"

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
