import asyncio
import logging
import re
import urllib
from dataclasses import dataclass, field
from time import sleep
from typing import Dict, Iterable, List, Optional, Union

from okta.client import Client as OktaClient
from okta.exceptions import OktaAPIException
from okta.models import Group, GroupProfile, User, UserProfile, UserStatus
from pydantic import validator
from pydantic.fields import Field

from datahub.configuration import ConfigModel
from datahub.configuration.common import ConfigurationError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
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
from datahub.metadata.schema_classes import (  # GroupMembershipClass,
    ChangeTypeClass,
    CorpGroupInfoClass,
    CorpUserInfoClass,
    GroupMembershipClass,
    OriginClass,
    OriginTypeClass,
)

logger = logging.getLogger(__name__)


class OktaConfig(ConfigModel):
    # Required: Domain of the Okta deployment. Example: dev-33231928.okta.com
    okta_domain: str = Field(
        description="The location of your Okta Domain, without a protocol. Can be found in Okta Developer console. e.g. dev-33231928.okta.com",
    )
    # Required: An API token generated from Okta.
    okta_api_token: str = Field(
        description="An API token generated for the DataHub application inside your Okta Developer Console. e.g. 00be4R_M2MzDqXawbWgfKGpKee0kuEOfX1RCQSRx00",
    )

    # Optional: Whether to ingest users, groups, or both.
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

    # Optional: Customize the mapping to DataHub Username from an attribute appearing in the Okta User
    # profile. Reference: https://developer.okta.com/docs/reference/api/users/
    okta_profile_to_username_attr: str = Field(
        default="login",
        description="Which Okta User Profile attribute to use as input to DataHub username mapping.",
    )
    okta_profile_to_username_regex: str = Field(
        default="([^@]+)",
        description="A regex used to parse the DataHub username from the attribute specified in `okta_profile_to_username_attr`.",
    )

    # Optional: Customize the mapping to DataHub Group from an attribute appearing in the Okta Group
    # profile. Reference: https://developer.okta.com/docs/reference/api/groups/
    okta_profile_to_group_name_attr: str = Field(
        default="name",
        description="Which Okta Group Profile attribute to use as input to DataHub group name mapping.",
    )
    okta_profile_to_group_name_regex: str = Field(
        default="(.*)",
        description="A regex used to parse the DataHub group name from the attribute specified in `okta_profile_to_group_name_attr`.",
    )

    # Optional: Include deprovisioned or suspended Okta users in the ingestion.
    include_deprovisioned_users: bool = Field(
        default=False,
        description="Whether to ingest users in the DEPROVISIONED state from Okta.",
    )
    include_suspended_users: bool = Field(
        default=False,
        description="Whether to ingest users in the SUSPENDED state from Okta.",
    )

    # Optional: Page size for reading groups and users from Okta API.
    page_size: int = Field(
        default=100,
        description="The number of entities requested from Okta's REST APIs in one request.",
    )

    # Optional: Set the delay for fetching batches of entities from Okta. Okta has rate limiting in place.
    delay_seconds: Union[float, int] = Field(
        default=0.01,
        description="Number of seconds to wait between calls to Okta's REST APIs. (Okta rate limits). Defaults to 10ms.",
    )

    # Optional: Filter and search expression for ingesting a subset of users. Only one can be specified at a time.
    okta_users_filter: Optional[str] = Field(
        default=None,
        description="Okta filter expression (not regex) for ingesting users. Only one of `okta_users_filter` and `okta_users_search` can be set. See (https://developer.okta.com/docs/reference/api/users/#list-users-with-a-filter) for more info.",
    )
    okta_users_search: Optional[str] = Field(
        default=None,
        description="Okta search expression (not regex) for ingesting users. Only one of `okta_users_filter` and `okta_users_search` can be set. See (https://developer.okta.com/docs/reference/api/users/#list-users-with-search) for more info.",
    )

    # Optional: Filter and search expression for ingesting a subset of groups. Only one can be specified at a time.
    okta_groups_filter: Optional[str] = Field(
        default=None,
        description="Okta filter expression (not regex) for ingesting groups. Only one of `okta_groups_filter` and `okta_groups_search` can be set. See (https://developer.okta.com/docs/reference/api/groups/#filters) for more info.",
    )
    okta_groups_search: Optional[str] = Field(
        default=None,
        description="Okta search expression (not regex) for ingesting groups. Only one of `okta_groups_filter` and `okta_groups_search` can be set. See (https://developer.okta.com/docs/reference/api/groups/#list-groups-with-search) for more info.",
    )

    # Optional: Whether to mask sensitive information from workunit ID's. On by default.
    mask_group_id: bool = True
    mask_user_id: bool = True

    @validator("okta_users_search")
    def okta_users_one_of_filter_or_search(cls, v, values):
        if v and values["okta_users_filter"]:
            raise ConfigurationError(
                "Only one of okta_users_filter or okta_users_search can be set"
            )
        return v

    @validator("okta_groups_search")
    def okta_groups_one_of_filter_or_search(cls, v, values):
        if v and values["okta_groups_filter"]:
            raise ConfigurationError(
                "Only one of okta_groups_filter or okta_groups_search can be set"
            )
        return v


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


@platform_name("Okta")
@config_class(OktaConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DESCRIPTIONS, "Optionally enabled via configuration")
class OktaSource(Source):
    """
    This plugin extracts the following:

    - Users
    - Groups
    - Group Membership

    from your Okta instance.

    ### Extracting DataHub Users

    #### Usernames

    Usernames serve as unique identifiers for users on DataHub. This connector extracts usernames using the
    "login" field of an [Okta User Profile](https://developer.okta.com/docs/reference/api/users/#profile-object).
    By default, the 'login' attribute, which contains an email, is parsed to extract the text before the "@" and map that to the DataHub username.

    If this is not how you wish to map to DataHub usernames, you can provide a custom mapping using the configurations options detailed below. Namely, `okta_profile_to_username_attr`
    and `okta_profile_to_username_regex`.

    #### Profiles

    This connector also extracts basic user profile information from Okta. The following fields of the Okta User Profile are extracted
    and mapped to the DataHub `CorpUserInfo` aspect:

    - display name
    - first name
    - last name
    - email
    - title
    - department
    - country code

    ### Extracting DataHub Groups

    #### Group Names

    Group names serve as unique identifiers for groups on DataHub. This connector extracts group names using the "name" attribute of an Okta Group Profile.
    By default, a URL-encoded version of the full group name is used as the unique identifier (CorpGroupKey) and the raw "name" attribute is mapped
    as the display name that will appear in DataHub's UI.

    If this is not how you wish to map to DataHub group names, you can provide a custom mapping using the configurations options detailed below. Namely, `okta_profile_to_group_name_attr`
    and `okta_profile_to_group_name_regex`.

    #### Profiles

    This connector also extracts basic group information from Okta. The following fields of the Okta Group Profile are extracted and mapped to the
    DataHub `CorpGroupInfo` aspect:

    - name
    - description

    ### Extracting Group Membership

    This connector additional extracts the edges between Users and Groups that are stored in Okta. It maps them to the `GroupMembership` aspect
    associated with DataHub users (CorpUsers). Today this has the unfortunate side effect of **overwriting** any Group Membership information that
    was created outside of the connector. That means if you've used the DataHub REST API to assign users to groups, this information will be overridden
    when the Okta source is executed. If you intend to *always* pull users, groups, and their relationships from your Identity Provider, then
    this should not matter.

    This is a known limitation in our data model that is being tracked by [this ticket](https://github.com/datahub-project/datahub/issues/3065).

    ### Filtering and Searching
    You can also choose to ingest a subset of users or groups to Datahub by adding flags for filtering or searching. For
    users, set either the `okta_users_filter` or `okta_users_search` flag (only one can be set at a time). For groups, set
    either the `okta_groups_filter` or `okta_groups_search` flag. Note that these are not regular expressions. See [below](#config-details) for full configuration
    options.


    """

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

        # Step 0: get or create the event loop
        # This method can be called on the main thread or an async thread, so we must create a new loop if one doesn't exist
        # See https://docs.python.org/3/library/asyncio-eventloop.html for more info.

        try:
            event_loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        except RuntimeError:
            event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(event_loop)

        # Step 1: Produce MetadataWorkUnits for CorpGroups.
        if self.config.ingest_groups:
            okta_groups = list(self._get_okta_groups(event_loop))
            datahub_corp_group_snapshots = self._map_okta_groups(okta_groups)
            for group_count, datahub_corp_group_snapshot in enumerate(
                datahub_corp_group_snapshots
            ):
                mce = MetadataChangeEvent(proposedSnapshot=datahub_corp_group_snapshot)
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
                    aspect=OriginClass(OriginTypeClass.EXTERNAL, "OKTA"),
                )
                group_origin_wu_id = (
                    f"group-origin-{group_count + 1}"
                    if self.config.mask_group_id
                    else datahub_corp_group_snapshot.urn
                )
                group_origin_wu = MetadataWorkUnit(
                    id=group_origin_wu_id, mcp=group_origin_mcp
                )
                self.report.report_workunit(group_origin_wu)
                yield group_origin_wu

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
                okta_group_users = self._get_okta_group_users(okta_group, event_loop)
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
            okta_users = self._get_okta_users(event_loop)
            filtered_okta_users = filter(self._filter_okta_user, okta_users)
            datahub_corp_user_snapshots = self._map_okta_users(filtered_okta_users)
            for user_count, datahub_corp_user_snapshot in enumerate(
                datahub_corp_user_snapshots
            ):

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
                wu_id = (
                    f"user-{user_count + 1}"
                    if self.config.mask_user_id
                    else datahub_corp_user_snapshot.urn
                )
                wu = MetadataWorkUnit(id=wu_id, mce=mce)
                self.report.report_workunit(wu)
                yield wu

                user_origin_mcp = MetadataChangeProposalWrapper(
                    entityType="corpuser",
                    entityUrn=datahub_corp_user_snapshot.urn,
                    changeType=ChangeTypeClass.UPSERT,
                    aspectName="origin",
                    aspect=OriginClass(OriginTypeClass.EXTERNAL, "OKTA"),
                )
                user_origin_wu_id = (
                    f"user-origin-{user_count + 1}"
                    if self.config.mask_user_id
                    else datahub_corp_user_snapshot.urn
                )
                user_origin_wu = MetadataWorkUnit(
                    id=user_origin_wu_id, mcp=user_origin_mcp
                )
                self.report.report_workunit(user_origin_wu)
                yield user_origin_wu

        # Step 4: Close the event loop
        event_loop.close()

    def get_report(self):
        return self.report

    def close(self):
        pass

    # Instantiates Okta SDK Client.
    def _create_okta_client(self):
        config = {
            "orgUrl": f"https://{self.config.okta_domain}",
            "token": f"{self.config.okta_api_token}",
            "raiseException": True,
        }
        return OktaClient(config)

    # Retrieves all Okta Group Objects in batches.
    def _get_okta_groups(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> Iterable[Group]:
        logger.debug("Extracting all Okta groups")

        # Note that this is not taking full advantage of Python AsyncIO, as we are blocking on calls.
        query_parameters: Dict[str, Union[str, int]] = {"limit": self.config.page_size}
        if self.config.okta_groups_filter:
            query_parameters.update({"filter": self.config.okta_groups_filter})
        if self.config.okta_groups_search:
            query_parameters.update({"search": self.config.okta_groups_search})
        groups = resp = err = None
        try:
            groups, resp, err = event_loop.run_until_complete(
                self.okta_client.list_groups(query_parameters)
            )
        except OktaAPIException as api_err:
            self.report.report_failure(
                "okta_groups", f"Failed to fetch Groups from Okta API: {api_err}"
            )
        while True:
            if err:
                self.report.report_failure(
                    "okta_groups", f"Failed to fetch Groups from Okta API: {err}"
                )
            if groups:
                for group in groups:
                    yield group
            if resp and resp.has_next():
                sleep(self.config.delay_seconds)
                try:
                    groups, err = event_loop.run_until_complete(resp.next())
                except OktaAPIException as api_err:
                    self.report.report_failure(
                        "okta_groups",
                        f"Failed to fetch Groups from Okta API: {api_err}",
                    )
            else:
                break

    # Retrieves Okta User Objects in a particular Okta Group in batches.
    def _get_okta_group_users(
        self, group: Group, event_loop: asyncio.AbstractEventLoop
    ) -> Iterable[User]:
        logger.debug(f"Extracting users from Okta group named {group.profile.name}")

        # Note that this is not taking full advantage of Python AsyncIO; we are blocking on calls.
        query_parameters = {"limit": self.config.page_size}
        users = resp = err = None
        try:
            users, resp, err = event_loop.run_until_complete(
                self.okta_client.list_group_users(group.id, query_parameters)
            )
        except OktaAPIException as api_err:
            self.report.report_failure(
                "okta_group_users",
                f"Failed to fetch Users of Group {group.profile.name} from Okta API: {api_err}",
            )
        while True:
            if err:
                self.report.report_failure(
                    "okta_group_users",
                    f"Failed to fetch Users of Group {group.profile.name} from Okta API: {err}",
                )
            if users:
                for user in users:
                    yield user
            if resp and resp.has_next():
                sleep(self.config.delay_seconds)
                try:
                    users, err = event_loop.run_until_complete(resp.next())
                except OktaAPIException as api_err:
                    self.report.report_failure(
                        "okta_group_users",
                        f"Failed to fetch Users of Group {group.profile.name} from Okta API: {api_err}",
                    )
            else:
                break

    # Retrieves all Okta User Objects in batches.
    def _get_okta_users(self, event_loop: asyncio.AbstractEventLoop) -> Iterable[User]:
        logger.debug("Extracting all Okta users")

        query_parameters: Dict[str, Union[str, int]] = {"limit": self.config.page_size}
        if self.config.okta_users_filter:
            query_parameters.update({"filter": self.config.okta_users_filter})
        if self.config.okta_users_search:
            query_parameters.update({"search": self.config.okta_users_search})
        users = resp = err = None
        try:
            users, resp, err = event_loop.run_until_complete(
                self.okta_client.list_users(query_parameters)
            )
        except OktaAPIException as api_err:
            self.report.report_failure(
                "okta_users", f"Failed to fetch Users from Okta API: {api_err}"
            )
        while True:
            if err:
                self.report.report_failure(
                    "okta_users", f"Failed to fetch Users from Okta API: {err}"
                )
            if users:
                for user in users:
                    yield user
            if resp and resp.has_next():
                sleep(self.config.delay_seconds)
                try:
                    users, err = event_loop.run_until_complete(resp.next())
                except OktaAPIException as api_err:
                    self.report.report_failure(
                        "okta_users", f"Failed to fetch Users from Okta API: {api_err}"
                    )
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
