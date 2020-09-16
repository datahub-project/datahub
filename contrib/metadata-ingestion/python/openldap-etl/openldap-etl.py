#! /usr/bin/python
import sys
import ldap
from ldap.controls import SimplePagedResultsControl
from distutils.version import LooseVersion

LDAP24API = LooseVersion(ldap.__version__) >= LooseVersion('2.4')

LDAPSERVER ='ldap://localhost'
BASEDN ='dc=example,dc=org'
LDAPUSER = 'cn=admin,dc=example,dc=org'
LDAPPASSWORD = 'admin'
PAGESIZE = 10
ATTRLIST = ['cn', 'title', 'mail', 'displayName', 'departmentNumber','manager']
SEARCHFILTER='givenname=Homer'

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
    ldap = str(attrs['displayName'][0])[1:]
    full_name = ldap
    first_mame = full_name.split(' ')[0]
    last_name = full_name.split(' ')[-1]
    email = str(attrs['mail'][0])[1:]
    display_name = ldap if 'displayName' in attrs else None
    department = str(attrs['departmentNumber'][0])[1:] if 'departmentNumber' in attrs else None
    title = str(attrs['title'][0])[1:] if 'title' in attrs else None
    manager_urn = ("urn:li:corpuser:" + str(manager_ldap)[1:]) if manager_ldap else None
    
    corp_user_info = \
        {"active":True, "email": email, "fullName": full_name, "firstName": first_mame, "lastName": last_name,
         "departmentNumber": department, "displayName": display_name,"title": title, "managerUrn": manager_urn}
    # sys.stdout.write('cor user info: %s\n' % corp_user_info)

    mce = {"auditHeader": None, "proposedSnapshot":
        ("com.linkedin.pegasus2avro.metadata.snapshot.CorpUserSnapshot",{"urn": "urn:li:corpuser:" + ldap, "aspects": [corp_user_info]}),
         "proposedDelta": None}

    produce_corp_user_mce(mce)

def produce_corp_user_mce(mce):
    """
    Produce MetadataChangeEvent records
    """
    from confluent_kafka import avro
    from confluent_kafka.avro import AvroProducer

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
        sys.stdout.write('LDAP searched\n')
    except ldap.LDAPError as e:
        sys.stdout.write('LDAP search failed: %s' % e)
        continue

    try:
        rtype, rdata, rmsgid, serverctrls = l.result3(msgid)
    except ldap.LDAPError as e:
        sys.stdout.write('Could not pull LDAP results: %s' % e)
        continue

    for dn, attrs in rdata:
        sys.stdout.write('found attrs result: %s\n' % attrs)
        if len(attrs) == 0 or 'mail' not in attrs \
                or 'displayName' not in attrs \
                or len(attrs['displayName']) == 0:
            continue
        manager_ldap = None
        if 'manager' in attrs:
            try:
                manager = attrs['manager'][0]
                manager_name = str(manager).split(',')[0][5:]
                manager_search_filter = 'displayName=%s' % manager_name
                manager_msgid = l.search_ext(BASEDN, ldap.SCOPE_SUBTREE,
                                             manager_search_filter, serverctrls=[lc])
            except ldap.LDAPError as e:
                sys.stdout.write('manager LDAP search failed: %s' % e)
                continue
            try:
                
                manager_ldap = l.result3(manager_msgid)[1][0][1]['displayName'][0]
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
