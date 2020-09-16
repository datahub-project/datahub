#! /usr/bin/python
import sys
import ldap
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from ldap.controls import SimplePagedResultsControl
from distutils.version import LooseVersion

LDAP24API = LooseVersion(ldap.__version__) >= LooseVersion('2.4')

LDAPSERVER ='LDAPSERVER'
BASEDN ='BASEDN'
LDAPUSER = 'LDAPUSER'
LDAPPASSWORD = 'LDAPPASSWORD'
PAGESIZE = 20
ATTRLIST = ['cn', 'title', 'mail', 'sAMAccountName', 'department','manager']
SEARCHFILTER='SEARCHFILTER'

AVROLOADPATH = '../../metadata-events/mxe-schemas/src/renamed/avro/com/linkedin/mxe/MetadataChangeEvent.avsc'
KAFKATOPIC = 'MetadataChangeEvent_v4'
BOOTSTRAP = 'localhost:9092'
SCHEMAREGISTRY = 'http://localhost:8081'

def create_controls(pagesize):
    """
    Create an LDAP control with a page size of "pagesize".
    """
    if LDAP24API:
        return SimplePagedResultsControl(True, size=pagesize, cookie='')
    else:
        return SimplePagedResultsControl(ldap.LDAP_CONTROL_PAGE_OID, True,
                                         (pagesize,''))

def get_pctrls(serverctrls):
    """
    Lookup an LDAP paged control object from the returned controls.
    """
    if LDAP24API:
        return [c for c in serverctrls
                if c.controlType == SimplePagedResultsControl.controlType]
    else:
        return [c for c in serverctrls
                if c.controlType == ldap.LDAP_CONTROL_PAGE_OID]

def set_cookie(lc_object, pctrls, pagesize):
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

def build_corp_user_mce(dn, attrs, manager_ldap):
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

    corp_user_info = \
        {"active":True, "email": email, "fullName": full_name, "firstName": first_mame, "lastName": last_name,
         "departmentName": department, "displayName": display_name,"title": title, "managerUrn": manager_urn}

    mce = {"auditHeader": None, "proposedSnapshot":
        ("com.linkedin.pegasus2avro.metadata.snapshot.CorpUserSnapshot",{"urn": "urn:li:corpuser:" + ldap, "aspects": [corp_user_info]}),
         "proposedDelta": None}

    produce_corp_user_mce(mce)

def produce_corp_user_mce(mce):
    """
    Produce MetadataChangeEvent records
    """
    conf = {'bootstrap.servers': BOOTSTRAP,
            'schema.registry.url': SCHEMAREGISTRY}
    record_schema = avro.load(AVROLOADPATH)
    producer = AvroProducer(conf, default_value_schema=record_schema)

    try:
        producer.produce(topic=KAFKATOPIC, value=mce)
        producer.poll(0)
        sys.stdout.write('\n%s has been successfully produced!\n' % mce)
    except ValueError as e:
        sys.stdout.write('Message serialization failed %s' % e)
    producer.flush()

ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_ALLOW)
ldap.set_option(ldap.OPT_REFERRALS, 0)

l = ldap.initialize(LDAPSERVER)
l.protocol_version = 3

try:
    l.simple_bind_s(LDAPUSER, LDAPPASSWORD)
except ldap.LDAPError as e:
    exit('LDAP bind failed: %s' % e)

lc = create_controls(PAGESIZE)

while True:
    try:
        msgid = l.search_ext(BASEDN, ldap.SCOPE_SUBTREE, SEARCHFILTER,
                             ATTRLIST, serverctrls=[lc])
    except ldap.LDAPError as e:
        sys.stdout.write('LDAP search failed: %s' % e)
        continue

    try:
        rtype, rdata, rmsgid, serverctrls = l.result3(msgid)
    except ldap.LDAPError as e:
        sys.stdout.write('Could not pull LDAP results: %s' % e)
        continue

    for dn, attrs in rdata:
        if len(attrs) == 0 or 'mail' not in attrs \
                or 'OU=Staff Users' not in dn or 'sAMAccountName' not in attrs \
                or len(attrs['sAMAccountName']) == 0:
            continue
        manager_ldap = None
        if 'manager' in attrs:
            try:
                manager_msgid = l.search_ext(BASEDN, ldap.SCOPE_SUBTREE,
                                             '(&(objectCategory=Person)(cn=%s))' % attrs['manager'][0].split(',')[0][3:],
                                             ['sAMAccountName'], serverctrls=[lc])
            except ldap.LDAPError as e:
                sys.stdout.write('manager LDAP search failed: %s' % e)
                continue
            try:
                manager_ldap = l.result3(manager_msgid)[1][0][1]['sAMAccountName'][0]
            except ldap.LDAPError as e:
                sys.stdout.write('Could not pull managerLDAP results: %s' % e)
                continue
        build_corp_user_mce(dn, attrs, manager_ldap)

    pctrls = get_pctrls(serverctrls)
    if not pctrls:
        print >> sys.stderr, 'Warning: Server ignores RFC 2696 control.'
        break

    cookie = set_cookie(lc, pctrls, PAGESIZE)
    if not cookie:
        break

l.unbind()
sys.exit(0)
