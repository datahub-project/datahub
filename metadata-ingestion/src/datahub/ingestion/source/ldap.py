"""LDAP Source"""

import contextlib
import dataclasses
from typing import Any, Dict, Iterable, List, Optional

import ldap
from ldap.controls import SimplePagedResultsControl
from pydantic.fields import Field

from datahub.configuration.common import ConfigurationError
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    CorpGroupInfoClass,
    CorpGroupSnapshotClass,
    CorpUserInfoClass,
    CorpUserSnapshotClass,
    GroupMembershipClass,
)
from datahub.utilities.lossy_collections import LossyList

# default mapping for attrs
user_attrs_map: Dict[str, Any] = {}
group_attrs_map: Dict[str, Any] = {}

# general attrs
user_attrs_map["urn"] = "sAMAccountName"

# user related attrs
user_attrs_map["fullName"] = "cn"
user_attrs_map["lastName"] = "sn"
user_attrs_map["firstName"] = "givenName"
user_attrs_map["displayName"] = "displayName"
user_attrs_map["managerUrn"] = "manager"
user_attrs_map["email"] = "mail"
user_attrs_map["departmentId"] = "departmentNumber"
user_attrs_map["title"] = "title"
user_attrs_map["departmentName"] = "departmentNumber"
user_attrs_map["countryCode"] = "countryCode"
user_attrs_map["memberOf"] = "memberOf"

# group related attrs
group_attrs_map["urn"] = "cn"
group_attrs_map["email"] = "mail"
group_attrs_map["admins"] = "owner"
group_attrs_map["members"] = "uniqueMember"
group_attrs_map["displayName"] = "name"
group_attrs_map["description"] = "info"


def create_controls(pagesize: int) -> SimplePagedResultsControl:
    """
    Create an LDAP control with a page size of "pagesize".
    """
    return SimplePagedResultsControl(True, size=pagesize, cookie="")


def get_pctrls(
    serverctrls: List[SimplePagedResultsControl],
) -> List[SimplePagedResultsControl]:
    """
    Lookup an LDAP paged control object from the returned controls.
    """
    return [
        c for c in serverctrls if c.controlType == SimplePagedResultsControl.controlType
    ]


def set_cookie(
    lc_object: SimplePagedResultsControl,
    pctrls: List[SimplePagedResultsControl],
) -> bool:
    """
    Push latest cookie back into the page control.
    """

    cookie = pctrls[0].cookie
    lc_object.cookie = cookie
    return bool(cookie)


class LDAPSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    """Config used by the LDAP Source."""

    # Server configuration.
    ldap_server: str = Field(description="LDAP server URL.")
    ldap_user: str = Field(description="LDAP user.")
    ldap_password: str = Field(description="LDAP password.")

    # Custom Stateful Ingestion settings
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    # Extraction configuration.
    base_dn: str = Field(description="LDAP DN.")
    filter: str = Field(default="(objectClass=*)", description="LDAP extractor filter.")
    attrs_list: Optional[List[str]] = Field(
        default=None, description="Retrieved attributes list"
    )

    custom_props_list: Optional[List[str]] = Field(
        default=None,
        description="A list of custom attributes to extract from the LDAP provider.",
    )

    # If set to true, any users without first and last names will be dropped.
    drop_missing_first_last_name: bool = Field(
        default=True,
        description="If set to true, any users without first and last names will be dropped.",
    )

    page_size: int = Field(
        default=20, description="Size of each page to fetch when extracting metadata."
    )

    manager_filter_enabled: bool = Field(
        default=True,
        description="Use LDAP extractor filter to search managers.",
    )

    manager_pagination_enabled: bool = Field(
        default=True,
        description="[deprecated] Use pagination_enabled ",
    )
    _deprecate_manager_pagination_enabled = pydantic_renamed_field(
        "manager_pagination_enabled", "pagination_enabled"
    )
    pagination_enabled: bool = Field(
        default=True,
        description="Use pagination while do search query (enabled by default).",
    )

    use_email_as_username: bool = Field(
        default=False,
        description="Use email for users' usernames instead of username (disabled by default). \
            If enabled, the user and group urn would be having email as the id part of the urn.",
    )
    # default mapping for attrs
    user_attrs_map: Dict[str, Any] = {}
    group_attrs_map: Dict[str, Any] = {}


@dataclasses.dataclass
class LDAPSourceReport(StaleEntityRemovalSourceReport):
    dropped_dns: LossyList[str] = dataclasses.field(default_factory=LossyList)

    def report_dropped(self, dn: str) -> None:
        self.dropped_dns.append(dn)


def guess_person_ldap(
    attrs: Dict[str, Any], config: LDAPSourceConfig, report: LDAPSourceReport
) -> Optional[str]:
    """Determine the user's LDAP based on the DN and attributes."""
    if config.user_attrs_map["urn"] in attrs:
        return attrs[config.user_attrs_map["urn"]][0].decode()
    else:  # for backward compatiblity
        if "sAMAccountName" in attrs:
            report.report_warning(
                "<general>",
                "Defaulting to sAMAccountName as it was found in attrs and not set in user_attrs_map in recipe",
            )
            return attrs["sAMAccountName"][0].decode()
        if "uid" in attrs:
            report.report_warning(
                "<general>",
                "Defaulting to uid as it was found in attrs and not set in user_attrs_map in recipe",
            )
            return attrs["uid"][0].decode()
        return None


@platform_name("LDAP")
@config_class(LDAPSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@dataclasses.dataclass
class LDAPSource(StatefulIngestionSourceBase):
    """
    This plugin extracts the following:
    - People
    - Names, emails, titles, and manager information for each person
    - List of groups
    """

    config: LDAPSourceConfig
    report: LDAPSourceReport
    platform: str = "ldap"

    def __init__(self, ctx: PipelineContext, config: LDAPSourceConfig):
        """Constructor."""
        super().__init__(config, ctx)
        self.config = config

        # ensure prior defaults are in place
        for k in user_attrs_map:
            if k not in self.config.user_attrs_map:
                self.config.user_attrs_map[k] = user_attrs_map[k]

        for k in group_attrs_map:
            if k not in self.config.group_attrs_map:
                self.config.group_attrs_map[k] = group_attrs_map[k]

        self.report = LDAPSourceReport()

        ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_ALLOW)
        ldap.set_option(ldap.OPT_REFERRALS, 0)

        self.ldap_client = ldap.initialize(self.config.ldap_server)
        self.ldap_client.protocol_version = 3

        try:
            self.ldap_client.simple_bind_s(
                self.config.ldap_user, self.config.ldap_password
            )
        except ldap.LDAPError as e:
            raise ConfigurationError("LDAP connection failed") from e

        if self.config.pagination_enabled:
            self.lc = create_controls(self.config.page_size)
        else:
            self.lc = None

    @classmethod
    def create(cls, config_dict: Dict[str, Any], ctx: PipelineContext) -> "LDAPSource":
        """Factory method."""
        config = LDAPSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Returns an Iterable containing the workunits to ingest LDAP users or groups."""
        cookie = True
        while cookie:
            try:
                msgid = self.ldap_client.search_ext(
                    self.config.base_dn,
                    ldap.SCOPE_SUBTREE,
                    self.config.filter,
                    self.config.attrs_list,
                    serverctrls=[self.lc] if self.lc else [],
                )
                _rtype, rdata, _rmsgid, serverctrls = self.ldap_client.result3(msgid)
            except ldap.LDAPError as e:
                self.report.report_failure("ldap-control", f"LDAP search failed: {e}")
                break

            for dn, attrs in rdata:
                if dn is None:
                    continue

                if not attrs or "objectClass" not in attrs:
                    self.report.report_warning(
                        "<general>",
                        f"skipping {dn} because attrs ({attrs}) does not contain expected data; "
                        f"check your permissions if this is unexpected",
                    )
                    continue

                if (
                    b"inetOrgPerson" in attrs["objectClass"]
                    or b"posixAccount" in attrs["objectClass"]
                    or b"person" in attrs["objectClass"]
                ):
                    yield from self.handle_user(dn, attrs)
                elif (
                    b"posixGroup" in attrs["objectClass"]
                    or b"organizationalUnit" in attrs["objectClass"]
                    or b"groupOfNames" in attrs["objectClass"]
                    or b"group" in attrs["objectClass"]
                ):
                    yield from self.handle_group(dn, attrs)
                else:
                    self.report.report_dropped(dn)

            if self.lc:
                pctrls = get_pctrls(serverctrls)
                if not pctrls:
                    self.report.report_failure(
                        "ldap-control", "Server ignores RFC 2696 control."
                    )
                    break
                cookie = set_cookie(self.lc, pctrls)
            else:
                break

    def handle_user(self, dn: str, attrs: Dict[str, Any]) -> Iterable[MetadataWorkUnit]:
        """
        Handle a DN and attributes by adding manager info and constructing a
        work unit based on the information.
        """
        manager_ldap = None
        make_manager_urn = None
        if self.config.user_attrs_map["managerUrn"] in attrs:
            try:
                m_cn = attrs[self.config.user_attrs_map["managerUrn"]][0].decode()
                if self.config.manager_filter_enabled:
                    manager_filter = self.config.filter
                else:
                    manager_filter = None
                manager_msgid = self.ldap_client.search_ext(
                    m_cn,
                    ldap.SCOPE_BASE,
                    manager_filter,
                    serverctrls=[self.lc] if self.lc else [],
                )
                result = self.ldap_client.result3(manager_msgid)
                if result[1]:
                    _m_dn, m_attrs = result[1][0]

                    manager_ldap = guess_person_ldap(m_attrs, self.config, self.report)

                    m_email = get_attr_or_none(
                        m_attrs, self.config.user_attrs_map["email"]
                    )
                    make_manager_urn = (
                        m_email
                        if m_email and self.config.use_email_as_username
                        else manager_ldap
                    )

            except ldap.LDAPError as e:
                self.report.report_warning(dn, f"manager LDAP search failed: {e}")
        mce = self.build_corp_user_mce(dn, attrs, make_manager_urn)
        if mce:
            yield MetadataWorkUnit(dn, mce)
        else:
            self.report.report_dropped(dn)

    def handle_group(
        self, dn: str, attrs: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        """Creates a workunit for LDAP groups."""

        mce = self.build_corp_group_mce(attrs)
        if mce:
            yield MetadataWorkUnit(dn, mce)
        else:
            self.report.report_dropped(dn)

    def build_corp_user_mce(
        self, dn: str, attrs: dict, manager_ldap: Optional[str]
    ) -> Optional[MetadataChangeEvent]:
        """
        Create the MetadataChangeEvent via DN and attributes.
        """
        ldap_user = guess_person_ldap(attrs, self.config, self.report)

        if self.config.drop_missing_first_last_name and (
            self.config.user_attrs_map["firstName"] not in attrs
            or self.config.user_attrs_map["lastName"] not in attrs
        ):
            return None
        full_name = attrs[self.config.user_attrs_map["fullName"]][0].decode()
        first_name = attrs[self.config.user_attrs_map["firstName"]][0].decode()
        last_name = attrs[self.config.user_attrs_map["lastName"]][0].decode()
        groups = parse_groups(attrs, self.config.user_attrs_map["memberOf"])

        email = get_attr_or_none(attrs, self.config.user_attrs_map["email"])
        display_name = get_attr_or_none(
            attrs, self.config.user_attrs_map["displayName"], full_name
        )
        title = get_attr_or_none(attrs, self.config.user_attrs_map["title"])
        department_id_str = get_attr_or_none(
            attrs, self.config.user_attrs_map["departmentId"]
        )
        department_name = get_attr_or_none(
            attrs, self.config.user_attrs_map["departmentName"]
        )
        country_code = get_attr_or_none(
            attrs, self.config.user_attrs_map["countryCode"]
        )
        department_id = None
        with contextlib.suppress(ValueError):
            if department_id_str:
                department_id = int(department_id_str)

        custom_props_map = {}
        if self.config.custom_props_list:
            for prop in self.config.custom_props_list:
                if prop in attrs:
                    custom_props_map[prop] = (attrs[prop][0]).decode()

        manager_urn = f"urn:li:corpuser:{manager_ldap}" if manager_ldap else None

        make_user_urn = (
            email if email and self.config.use_email_as_username else ldap_user
        )

        user_snapshot = CorpUserSnapshotClass(
            urn=f"urn:li:corpuser:{make_user_urn}",
            aspects=[
                CorpUserInfoClass(
                    active=True,
                    email=email,
                    fullName=full_name,
                    firstName=first_name,
                    lastName=last_name,
                    departmentId=department_id,
                    departmentName=department_name,
                    displayName=display_name,
                    countryCode=country_code,
                    title=title,
                    managerUrn=manager_urn,
                    customProperties=custom_props_map,
                ),
            ],
        )

        user_snapshot.aspects.append(GroupMembershipClass(groups=groups))

        return MetadataChangeEvent(proposedSnapshot=user_snapshot)

    def build_corp_group_mce(self, attrs: dict) -> Optional[MetadataChangeEvent]:
        """Creates a MetadataChangeEvent for LDAP groups."""
        cn = attrs.get(self.config.group_attrs_map["urn"])
        if cn:
            full_name = cn[0].decode()
            admins = parse_users(attrs, self.config.group_attrs_map["admins"])
            members = parse_users(attrs, self.config.group_attrs_map["members"])

            email = get_attr_or_none(attrs, self.config.group_attrs_map["email"])
            description = get_attr_or_none(
                attrs, self.config.group_attrs_map["description"]
            )
            displayName = get_attr_or_none(
                attrs, self.config.group_attrs_map["displayName"]
            )

            make_group_urn = (
                email if email and self.config.use_email_as_username else full_name
            )

            group_snapshot = CorpGroupSnapshotClass(
                urn=f"urn:li:corpGroup:{make_group_urn}",
                aspects=[
                    CorpGroupInfoClass(
                        email=email,
                        admins=admins,
                        members=members,
                        groups=[],
                        description=description,
                        displayName=displayName,
                    ),
                ],
            )
            return MetadataChangeEvent(proposedSnapshot=group_snapshot)
        return None

    def get_report(self) -> LDAPSourceReport:
        """Returns the source report."""
        return self.report

    def close(self) -> None:
        self.ldap_client.unbind()
        super().close()


def parse_users(attrs: Dict[str, Any], filter_key: str) -> List[str]:
    """Converts a list of LDAP DNs to Datahub corpuser strings."""
    if filter_key in attrs:
        return [
            f"urn:li:corpuser:{parse_ldap_dn(ldap_user)}"
            for ldap_user in attrs[filter_key]
        ]
    return []


def parse_groups(attrs: Dict[str, Any], filter_key: str) -> List[str]:
    """Converts a list of LDAP DNs to Datahub corpgroup strings"""
    if filter_key in attrs:
        return [
            f"urn:li:corpGroup:{parse_ldap_dn(ldap_group)}"
            for ldap_group in attrs[filter_key]
        ]
    return []


def parse_ldap_dn(input_clean: bytes) -> str:
    """
    Converts an LDAP DN of format b'cn=group_name,ou=Groups,dc=internal,dc=machines'
    or b'uid=username,ou=Groups,dc=internal,dc=machines' to group name or username.
    Inputs which are not valid LDAP DNs are simply decoded and returned as strings.
    """
    if ldap.dn.is_dn(input_clean):
        return ldap.dn.str2dn(input_clean, flags=ldap.DN_FORMAT_LDAPV3)[0][0][1]
    else:
        return input_clean.decode()


def get_attr_or_none(
    attrs: Dict[str, Any], key: str, default: Optional[str] = None
) -> Optional[str]:
    return attrs[key][0].decode() if attrs.get(key) else default
