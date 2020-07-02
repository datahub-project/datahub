# About this OpenLDAP ETL
The openldap-etl provides you ETL channel to communicate with an OpenLDAP server.

# OpenLDAP Docker Image
**Attention**
> The docker compose is for macOS environment. If you are running in a Linux environment, use the offical [osxia/docker-openldap](https://github.com/osixia/docker-openldap)
This docker compose file comes with a `OpenLDAP server` and `Php LDAP Admin` portal, and it is based on [this](https://gist.github.com/thomasdarimont/d22a616a74b45964106461efb948df9c) with modification. 

# Start OpenLDAP and Php LDAP admin
```
docker-compose up 
```
# Login via ldapadmin
Head to `localhost:7080` with your browser, enter the following credential to login
``` 
Login:cn=admin,dc=example,dc=org
Password:admin
```

# Seed Group, Users
Import `sample-ldif.txt` to come up with your organization from PhpLDAPAdmin portal. 
`sample-ldif.txt` contains information about 
 * group: we set up a `people` group
 * peoples under `people` group: here are `Simpons` family member under `people` group. 

# Run ETL Script
Once we finish setting up our organization, we are about to run `openldap-etl.py` script. 
In this script, we query a user by his given name: Homer, we also filter result attributes to a few. We also look for Homer's manager, if there is one.
This script is mostly based on `ldap-etl.py`. However, there is an important attribute `sAMAccountName` which is not exist in OpenLDAP. So we have to modify it a little bit.
Once we find Homer, we assemble his information and his manager's name to `corp_user_info`, as a message of `MetadataChangeEvent` topic, publish it. 
After Run `pip install --user -r requirements.txt`, then run `python openldap-etl.py`, you are expected to see
```
{'auditHeader': None, 'proposedSnapshot': ('com.linkedin.pegasus2avro.metadata.snapshot.CorpUserSnapshot', {'urn': "urn:li:corpuser:'Homer Simpson'", 'aspects': [{'active': True, 'email': 'hsimpson', 'fullName': "'Homer Simpson'", 'firstName': "b'Homer", 'lastName': "Simpson'", 'departmentNumber': '1001', 'displayName': 'Homer Simpson', 'title': 'Mr. Everything', 'managerUrn': "urn:li:corpuser:'Bart Simpson'"}]}), 'proposedDelta': None} has been successfully produced! 
```

 
