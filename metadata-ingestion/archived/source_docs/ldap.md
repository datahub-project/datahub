# LDAP

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[ldap]'`.

## Capabilities

This plugin extracts the following:

- People
- Names, emails, titles, and manager information for each person
- List of groups

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: "ldap"
  config:
    # Coordinates
    ldap_server: ldap://localhost

    # Credentials
    ldap_user: "cn=admin,dc=example,dc=org"
    ldap_password: "admin"

    # Options
    base_dn: "dc=example,dc=org"

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                          | Required | Default             | Description                                                             |
| ------------------------------ | -------- | ------------------- | ----------------------------------------------------------------------- |
| `ldap_server`                  | ✅       |                     | LDAP server URL.                                                        |
| `ldap_user`                    | ✅       |                     | LDAP user.                                                              |
| `ldap_password`                | ✅       |                     | LDAP password.                                                          |
| `base_dn`                      | ✅       |                     | LDAP DN.                                                                |
| `filter`                       |          | `"(objectClass=*)"` | LDAP extractor filter.                                                  |
| `drop_missing_first_last_name` |          | `True`              | If set to true, any users without first and last names will be dropped. |
| `page_size`                    |          | `20`                | Size of each page to fetch when extracting metadata.                    |

The `drop_missing_first_last_name` should be set to true if you've got many "headless" user LDAP accounts
for devices or services should be excluded when they do not contain a first and last name. This will only
impact the ingestion of LDAP users, while LDAP groups will be unaffected by this config option.

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
