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

    # Optional: Map LDAP User Attributes to DataHub User Attributes
    user_attrs_map:
      urn: sAMAccountName # A unique, stable ID for the User
      fullName: cn
      lastName: sn
      firstName: givenName
      displayName: displayName
      manager: manager
      mail: mail
      departmentNumber: departmentNumber
      title: title

    # Optional: Map LDAP Group Attributes to DataHub Group Attributes
    group_attrs_map:   
      urn: cn # A unique, stable ID for the Group
      admins: owner
      members: uniqueMember
      displayName: name

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                           | Required | Default             | Description                                                                                                                                                                                                                                                                       |
| ------------------------------- | -------- | ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ldap_server`                   | ✅       |                     | LDAP server URL.                                                                                                                                                                                                                                                                  |
| `ldap_user`                     | ✅       |                     | LDAP user.                                                                                                                                                                                                                                                                        |
| `ldap_password`                 | ✅       |                     | LDAP password.                                                                                                                                                                                                                                                                    |
| `base_dn`                       | ✅       |                     | LDAP DN.                                                                                                                                                                                                                                                                          |
| `filter`                        |          | `"(objectClass=*)"` | LDAP extractor filter.                                                                                                                                                                                                                                                            |
| `drop_missing_first_last_name`  |          | `True`              | If set to true, any users without first and last names will be dropped.                                                                                                                                                                                                           |
| `page_size`                     |          | `20`                | Size of each page to fetch when extracting metadata.                                                                                                                                                                                                                              |
| `user_attrs_map.urn`            |          | `sAMAccountName`    | An attribute to use in constructing the DataHub User urn. This should be something that uniquely identifies the user and is stable over time.                                                                                                                                     |
| `user_attrs_map.managerUrn`     |          | `manager`           | Alternate attrs key representing same information as user's manager in the organization.                                                                                                                                                                                          |
| `user_attrs_map.firstName`      |          | `givenName`         | Alternate attrs key representing same information as user's givenName in the organization.                                                                                                                                                                                        |
| `user_attrs_map.lastName`       |          | `sn`                | Alternate attrs key representing same information as user's sn (surname) in the organization.                                                                                                                                                                                     |
| `user_attrs_map.fullName`       |          | `cn`                | Alternate attrs key representing same information as user's cn (common name) in the organization.                                                                                                                                                                                 |
| `user_attrs_map.email`          |          | `mail`              | Alternate attrs key representing same information as user's mail in the organization.                                                                                                                                                                                             |
| `user_attrs_map.displayName`    |          | `displayName`       | Alternate attrs key representing same information as user's displayName in the organization.                                                                                                                                                                                      |
| `user_attrs_map.departmentId`   |          | `departmentNumber`  | Alternate attrs key representing same information as user's departmentNumber in the organization.                                                                                                                                                                                 |
| `user_attrs_map.departmentName` |          | `departmentNumber`  | Alternate attrs key representing same information as user's departmentName in the organization. It is defaulted to `departmentNumber` to not impact existing users. New users are recommended to use descriptive attributes like `department` or `departmantName` that may exist. |
| `user_attrs_map.title`          |          | `title`             | Alternate attrs key representing same information as user's title in the organization.                                                                                                                                                                                            |
| `user_attrs_map.countryCode`    |          | `countryCode`       | Alternate attrs key representing same information as user's countryCode in the organization.                                                                                                                                                                                      |
| `group_attrs_map.urn`           |          | `cn`                | Alternate attrs key representing same information as the group's cn (common name) for the LDAP group.                                                                                                                                                                             |
| `group_attrs_map.email`         |          | `mail`              | Alternate attrs key representing same information as group's mail in the organization.                                                                                                                                                                                            |
| `group_attrs_map.admins`        |          | `owner`             | Alternate attrs key representing same information as group's owner in the organization.                                                                                                                                                                                           |
| `group_attrs_map.members`       |          | `uniqueMember`      | Alternate attrs key representing same information as group's members in the organization.                                                                                                                                                                                         |
| `group_attrs_map.displayName`   |          | `name`              | Alternate attrs key representing same information as group's display name in the organization.                                                                                                                                                                                    |
| `group_attrs_map.description`   |          | `info`              | Alternate attrs key representing same information as group's description in the organization.                                                                                                                                                                                     |

The `drop_missing_first_last_name` should be set to true if you've got many "headless" user LDAP accounts
for devices or services should be excluded when they do not contain a first and last name. This will only
impact the ingestion of LDAP users, while LDAP groups will be unaffected by this config option.

### Configurable LDAP

Every organization may implement LDAP slightly differently based on their needs. The makes a standard LDAP recipe ineffective due to missing data during LDAP ingestion. For instance, LDAP recipe assumes department information for a CorpUser would be present in the `departmentNumber` attribute. If an organization chose not to implement that attribute or rather capture similar imformation in the `department` attribute, that information can be missed during LDAP ingestion (even though the information may be present in LDAP in a slightly different form). LDAP source provides flexibility to provide optional mapping for such variations to be reperesented under `user_attrs_map` and `group_attrs_map`. So if an organization represented `departmentNumber` as `department` and `mail` as `email`, the recipe can be adapted to customize that mapping based on need. An example is show below. If `user_attrs_map` section is not provided, the default mapping will apply.

```yaml
# in config section
user_attrs_map:
  departmentNumber: department
  mail: email
```

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
