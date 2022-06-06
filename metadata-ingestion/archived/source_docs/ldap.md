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

    # Optional attribute mapping to allow ldap config differences across orgs
    attrs_mapping:
      sAMAccountName: sAMAccountName
      uid: uid
      objectClass: objectClass
      manager: manager
      givenName: givenName
      sn: sn
      cn: cn
      mail: mail
      displayName: displayName
      departmentNumber: departmentNumber
      title: title
      owner: owner
      managedBy: managedBy
      uniqueMember: uniqueMember
      member: member

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
| `attrs_mapping.sAMAccountName` |          | `sAMAccountName`    | Alternate attrs key representing same information as sAMAccountName in the organization. |
| `attrs_mapping.uid`            |          | `uid`               | Alternate attrs key representing same information as uid in the organization. |
| `attrs_mapping.objectClass`    |          | `objectClass`       | Alternate attrs key representing same information as objectClass in the organization. |
| `attrs_mapping.manager`        |          | `manager`           | Alternate attrs key representing same information as manager in the organization. |
| `attrs_mapping.givenName`      |          | `givenName`         | Alternate attrs key representing same information as givenName in the organization. |
| `attrs_mapping.sn`             |          | `sn`                | Alternate attrs key representing same information as sn in the organization. |
| `attrs_mapping.cn`             |          | `cn`                | Alternate attrs key representing same information as cn in the organization. |
| `attrs_mapping.mail`           |          | `mail`              | Alternate attrs key representing same information as mail in the organization. |
| `attrs_mapping.displayName`    |          | `displayName`       | Alternate attrs key representing same information as displayName in the organization. |
| `attrs_mapping.departmentNumber`|          | `departmentNumber` | Alternate attrs key representing same information as departmentNumber in the organization. |
| `attrs_mapping.title`          |          | `title`             | Alternate attrs key representing same information as title in the organization. |
| `attrs_mapping.owner`          |          | `owner`             | Alternate attrs key representing same information as owner in the organization. |
| `attrs_mapping.managedBy`      |          | `managedBy`         | Alternate attrs key representing same information as managedBy in the organization. |
| `attrs_mapping.uniqueMember`   |          | `uniqueMember`      | Alternate attrs key representing same information as uniqueMember in the organization. |
| `attrs_mapping.member`         |          | `member`            | Alternate attrs key representing same information as member in the organization. |

The `drop_missing_first_last_name` should be set to true if you've got many "headless" user LDAP accounts
for devices or services should be excluded when they do not contain a first and last name. This will only
impact the ingestion of LDAP users, while LDAP groups will be unaffected by this config option.

### Configurable LDAP

Every organization may implement LDAP slightly differently based on their needs. The makes a standard LDAP recipe ineffecvtive due to missing data during LDAP ingestion. For instance, LDAP recipe assumes department information for a CorpUser would be present in the `departmentNumber` attribute. If an organization chose not to implement that attribute or rather capture similar imformation in the `department` attribute, that information can be missed during LDAP ingestion (even though the information may be present in LDAP in a slightly different form). LDAP source provides flexibility to provide optional mapping for such variations to be reperesented under attrs_mapping. So if an organization represented `departmentNumber` as `department` and `mail` as `email`, the recipe can be adapted to customiza that mapping based on need. An example is show below. If `attrs_mapping` section is not provided, the default mapping will apply.

```yaml
# in config section
    attrs_mapping:
      departmentNumber: department
      mail: email     
```

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
