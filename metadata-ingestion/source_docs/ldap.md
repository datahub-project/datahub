# LDAP

To install this plugin, run `pip install 'acryl-datahub[ldap]'`.

This plugin extracts the following:

- List of people
- Names, emails, titles, and manager information for each person
- List of groups

```yml
source:
  type: "ldap"
  config:
    ldap_server: ldap://localhost
    ldap_user: "cn=admin,dc=example,dc=org"
    ldap_password: "admin"

    # Extraction configuration.
    base_dn: "dc=example,dc=org"
    filter: "(objectClass=*)" # optional field

    # If set to true, any users without first and last names will be dropped.
    drop_missing_first_last_name: False # optional

    # For creating LDAP controls
    page_size: # default is 20
```

The `drop_missing_first_last_name` should be set to true if you've got many "headless" user LDAP accounts
for devices or services should be excluded when they do not contain a first and last name. This will only
impact the ingestion of LDAP users, while LDAP groups will be unaffected by this config option.
