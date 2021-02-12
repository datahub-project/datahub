#! /usr/bin/python
import sys
import ldap
from ldap.controls import SimplePagedResultsControl
from distutils.version import LooseVersion

LDAP24API = LooseVersion(ldap.__version__) >= LooseVersion('2.4')

ATTRLIST = ['cn', 'title', 'mail', 'sAMAccountName', 'department', 'manager']


class LDAPSourceConfig(ConfigModel):
    server: str
    base_dn: str
    user: str
    password: str
    search_filter: Optional[str] = None
    page_size: Optional[int] = 20


class LDAPSource(Source):
    def __init__(self, config_dict):
        self.config = LDAPSourceConfig.parse_obj(config_dict)
        ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_ALLOW)
        ldap.set_option(ldap.OPT_REFERRALS, 0)

        self.l = ldap.initialize(self.config.server)
        self.l.protocol_version = 3

        try:
            self.l.simple_bind_s(self.config.user, self.config.password)
        except ldap.LDAPError as e:
            exit('LDAP bind failed: %s' % e)

        self.lc = self.create_controls(self.config.page_size)
        self.mce_list = []
        self.download_data()

    def extract_record(self):
        return self.mce_list

    def create_controls(self, pagesize):
        """
            Create an LDAP control with a page size of "pagesize".
            """
        if LDAP24API:
            return SimplePagedResultsControl(True, size=pagesize, cookie='')
        else:
            return SimplePagedResultsControl(ldap.LDAP_CONTROL_PAGE_OID, True, (pagesize, ''))

    def get_pctrls(self, serverctrls):
        """
            Lookup an LDAP paged control object from the returned controls.
            """
        if LDAP24API:
            return [c for c in serverctrls if c.controlType == SimplePagedResultsControl.controlType]
        else:
            return [c for c in serverctrls if c.controlType == ldap.LDAP_CONTROL_PAGE_OID]

    def set_cookie(self, lc_object, pctrls, pagesize):
        """
            Push latest cookie back into the page control.
            """
        if LDAP24API:
            cookie = pctrls[0].cookie
            lc_object.cookie = cookie
            return cookie
        else:
            est, cookie = pctrls[0].controlValue
            lc_object.controlValue = (pagesize, cookie)
            return cookie

    def build_corp_user_mce(self, dn, attrs, manager_ldap):
        """
            Create the MetadataChangeEvent via DN and return of attributes.
            """
        ldap = attrs['sAMAccountName'][0]
        full_name = dn.split(',')[0][3:]
        first_mame = full_name.split(' ')[0]
        last_name = full_name.split(' ')[-1]
        email = attrs['mail'][0]
        display_name = attrs['cn'][0] if 'cn' in attrs else None
        department = attrs['department'][0] if 'department' in attrs else None
        title = attrs['title'][0] if 'title' in attrs else None
        manager_urn = ("urn:li:corpuser:" + manager_ldap) if manager_ldap else None

        corp_user_info = {
            "active": True,
            "email": email,
            "fullName": full_name,
            "firstName": first_mame,
            "lastName": last_name,
            "departmentName": department,
            "displayName": display_name,
            "title": title,
            "managerUrn": manager_urn,
        }

        mce = {
            "auditHeader": None,
            "proposedSnapshot": (
                "com.linkedin.pegasus2avro.metadata.snapshot.CorpUserSnapshot",
                {"urn": "urn:li:corpuser:" + ldap, "aspects": [corp_user_info]},
            ),
            "proposedDelta": None,
        }
        return mce

    def download_data(self):
        try:
            msgid = self.l.search_ext(
                self.config.base_dn, ldap.SCOPE_SUBTREE, self.config.search_filter, ATTRLIST, serverctrls=[self.lc]
            )
        except ldap.LDAPError as e:
            sys.stdout.write('LDAP search failed: %s' % e)
            continue

        try:
            rtype, rdata, rmsgid, serverctrls = self.l.result3(msgid)
        except ldap.LDAPError as e:
            sys.stdout.write('Could not pull LDAP results: %s' % e)
            continue

        for dn, attrs in rdata:
            if (
                len(attrs) == 0
                or 'mail' not in attrs
                or 'OU=Staff Users' not in dn
                or 'sAMAccountName' not in attrs
                or len(attrs['sAMAccountName']) == 0
            ):
                continue
            manager_ldap = None
            if 'manager' in attrs:
                try:
                    manager_msgid = self.l.search_ext(
                        self.config.base_dn,
                        ldap.SCOPE_SUBTREE,
                        '(&(objectCategory=Person)(cn=%s))' % attrs['manager'][0].split(',')[0][3:],
                        ['sAMAccountName'],
                        serverctrls=[lc],
                    )
                except ldap.LDAPError as e:
                    sys.stdout.write('manager LDAP search failed: %s' % e)
                    continue
                try:
                    manager_ldap = l.result3(manager_msgid)[1][0][1]['sAMAccountName'][0]
                except ldap.LDAPError as e:
                    sys.stdout.write('Could not pull managerLDAP results: %s' % e)
                    continue
            self.mce_list.add(build_corp_user_mce(dn, attrs, manager_ldap))

        self.cursor = 0
        self.num_elements = len(self.mce_list)

    def close(self):
        self.l.unbind()

    while True:
        try:
            msgid = l.search_ext(BASEDN, ldap.SCOPE_SUBTREE, SEARCHFILTER, ATTRLIST, serverctrls=[lc])
        except ldap.LDAPError as e:
            sys.stdout.write('LDAP search failed: %s' % e)
            continue

        pctrls = get_pctrls(serverctrls)
        if not pctrls:
            print >>sys.stderr, 'Warning: Server ignores RFC 2696 control.'
            break

        cookie = set_cookie(lc, pctrls, PAGESIZE)
        if not cookie:
            break
