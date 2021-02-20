from dataclasses import dataclass
from typing import Iterable, Optional

import ldap
from ldap.controls import SimplePagedResultsControl

from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import CorpUserInfoClass, CorpUserSnapshotClass


def create_controls(pagesize):
    """
    Create an LDAP control with a page size of "pagesize".
    """
    return SimplePagedResultsControl(True, size=pagesize, cookie="")


def get_pctrls(serverctrls):
    """
    Lookup an LDAP paged control object from the returned controls.
    """
    return [
        c for c in serverctrls if c.controlType == SimplePagedResultsControl.controlType
    ]


def set_cookie(lc_object, pctrls, pagesize):
    """
    Push latest cookie back into the page control.
    """
    cookie = pctrls[0].cookie
    lc_object.cookie = cookie
    return cookie


def guess_person_ldap(dn, attrs) -> Optional[str]:
    """Determine the user's LDAP based on the DN and attributes."""
    if "sAMAccountName" in attrs:
        return attrs["sAMAccountName"][0].decode()
    if "uid" in attrs:
        return attrs["uid"][0].decode()
    return None


class LDAPSourceConfig(ConfigModel):
    # Server configuration.
    ldap_server: str
    ldap_user: str
    ldap_password: str

    # Extraction configuration.
    base_dn: str
    filter: str = "(objectClass=*)"

    page_size: int = 20


@dataclass
class LDAPSource(Source):
    config: LDAPSourceConfig
    report: SourceReport

    def __init__(self, ctx: PipelineContext, config: LDAPSourceConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

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

        self.lc = create_controls(self.config.page_size)

    @classmethod
    def create(cls, config_dict, ctx):
        config = LDAPSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        cookie = True
        while cookie:
            try:
                msgid = self.ldap_client.search_ext(
                    self.config.base_dn,
                    ldap.SCOPE_SUBTREE,
                    self.config.filter,
                    serverctrls=[self.lc],
                )
                rtype, rdata, rmsgid, serverctrls = self.ldap_client.result3(msgid)
            except ldap.LDAPError as e:
                self.report.report_failure(
                    "ldap-control", "LDAP search failed: {}".format(e)
                )
                break

            for dn, attrs in rdata:
                # TODO: create groups if 'organizationalUnit' in attrs['objectClass']

                if (
                    b"inetOrgPerson" in attrs["objectClass"]
                    or b"posixAccount" in attrs["objectClass"]
                ):
                    yield from self.handle_user(dn, attrs)

            pctrls = get_pctrls(serverctrls)
            if not pctrls:
                self.report.report_failure(
                    "ldap-control", "Server ignores RFC 2696 control."
                )
                break

            cookie = set_cookie(self.lc, pctrls, self.config.page_size)

    def handle_user(self, dn, attrs) -> Iterable[MetadataWorkUnit]:
        """
        Handle a DN and attributes by adding manager info and constructing a
        work unit based on the information.
        """
        manager_ldap = None
        if "manager" in attrs:
            try:
                m_cn = attrs["manager"][0].split(b",")[0]
                manager_msgid = self.ldap_client.search_ext(
                    self.config.base_dn,
                    ldap.SCOPE_SUBTREE,
                    f"({m_cn.decode()})",
                    serverctrls=[self.lc],
                )
                m_dn, m_attrs = self.ldap_client.result3(manager_msgid)[1][0]
                manager_ldap = guess_person_ldap(m_dn, m_attrs)
            except ldap.LDAPError as e:
                self.report.report_warning(
                    dn, "manager LDAP search failed: {}".format(e)
                )

        mce = self.build_corp_user_mce(dn, attrs, manager_ldap)
        if mce:
            wu = MetadataWorkUnit(dn, mce)
            self.report.report_workunit(wu)
            yield wu
        yield from []

    def build_corp_user_mce(
        self, dn, attrs, manager_ldap
    ) -> Optional[MetadataChangeEvent]:
        """
        Create the MetadataChangeEvent via DN and attributes.
        """
        ldap = guess_person_ldap(dn, attrs)
        full_name = attrs["cn"][0].decode()
        first_name = attrs["givenName"][0].decode()
        last_name = attrs["sn"][0].decode()
        email = (attrs["mail"][0]).decode() if "mail" in attrs else None
        display_name = (
            (attrs["displayName"][0]).decode() if "displayName" in attrs else full_name
        )
        department = (
            (attrs["departmentNumber"][0]).decode()
            if "departmentNumber" in attrs
            else None
        )
        title = attrs["title"][0].decode() if "title" in attrs else None
        manager_urn = f"urn:li:corpuser:{manager_ldap}" if manager_ldap else None

        mce = MetadataChangeEvent(
            proposedSnapshot=CorpUserSnapshotClass(
                urn=f"urn:li:corpuser:{ldap}",
                aspects=[
                    CorpUserInfoClass(
                        active=True,
                        email=email,
                        fullName=full_name,
                        firstName=first_name,
                        lastName=last_name,
                        departmentName=department,
                        displayName=display_name,
                        title=title,
                        managerUrn=manager_urn,
                    )
                ],
            )
        )

        return mce

    def get_report(self):
        return self.report

    def close(self):
        self.ldap_client.unbind()
