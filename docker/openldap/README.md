# About this Open-LDAP Docker Compose
This docker compose  comes with a `OpenLDAP server` and `Php LDAP Admin` portal, and it is based on [this]
(https://gist.github.com/thomasdarimont/d22a616a74b45964106461efb948df9c) with modification. 

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

# Query
```
 docker exec openldap ldapsearch -x -H ldap://localhost -b dc=example,dc=org -D "cn=admin,dc=example,dc=org" -w admin
 ```

 