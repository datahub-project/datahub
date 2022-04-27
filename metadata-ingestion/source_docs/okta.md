# Okta 

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[okta]'`.

## Capabilities

This plugin extracts the following:

- Users
- Groups
- Group Membership

from your Okta instance. 

### Extracting DataHub Users

#### Usernames

Usernames serve as unique identifiers for users on DataHub. This connector extracts usernames using the 
"login" field of an [Okta User Profile](https://developer.okta.com/docs/reference/api/users/#profile-object). 
By default, the 'login' attribute, which contains an email, is parsed to extract the text before the "@" and map that to the DataHub username.

If this is not how you wish to map to DataHub usernames, you can provide a custom mapping using the configurations options detailed below. Namely, `okta_profile_to_username_attr` 
and `okta_profile_to_username_regex`. 

#### Profiles

This connector also extracts basic user profile information from Okta. The following fields of the Okta User Profile are extracted
and mapped to the DataHub `CorpUserInfo` aspect:

- display name 
- first name
- last name
- email
- title
- department
- country code

### Extracting DataHub Groups

#### Group Names

Group names serve as unique identifiers for groups on DataHub. This connector extracts group names using the "name" attribute of an Okta Group Profile.
By default, a URL-encoded version of the full group name is used as the unique identifier (CorpGroupKey) and the raw "name" attribute is mapped
as the display name that will appear in DataHub's UI. 

If this is not how you wish to map to DataHub group names, you can provide a custom mapping using the configurations options detailed below. Namely, `okta_profile_to_group_name_attr`
and `okta_profile_to_group_name_regex`.

#### Profiles 

This connector also extracts basic group information from Okta. The following fields of the Okta Group Profile are extracted and mapped to the
DataHub `CorpGroupInfo` aspect:

- name
- description

### Extracting Group Membership

This connector additional extracts the edges between Users and Groups that are stored in Okta. It maps them to the `GroupMembership` aspect
associated with DataHub users (CorpUsers). Today this has the unfortunate side effect of **overwriting** any Group Membership information that
was created outside of the connector. That means if you've used the DataHub REST API to assign users to groups, this information will be overridden
when the Okta source is executed. If you intend to *always* pull users, groups, and their relationships from your Identity Provider, then
this should not matter. 

This is a known limitation in our data model that is being tracked by [this ticket](https://github.com/datahub-project/datahub/issues/3065).

### Filtering and Searching
You can also choose to ingest a subset of users or groups to Datahub by adding flags for filtering or searching. For
users, set either the `okta_users_filter` or `okta_users_search` flag (only one can be set at a time). For groups, set
either the `okta_groups_filter` or `okta_groups_search` flag. Note that these are not regular expressions. See [below](#config-details) for full configuration
options.


## Quickstart recipe

As a prerequisite, you should [create a DataHub Application](https://developer.okta.com/docs/guides/sign-into-web-app/aspnet/create-okta-application/) within the Okta Developer Console with full permissions
to read your organization's Users and Groups. 

You can use the following recipe to get started with Okta ingestion! See [below](#config-details) for full configuration options.

```yml
source:
  type: okta
  config:
    # Coordinates
    okta_domain: "dev-35531955.okta.com"

    # Credentials
    okta_api_token: "11be4R_M2MzDqXawbTHfKGpKee0kuEOfX1RCQSRx99"

sink:
  # sink configs
```

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).


## Configuration

Note that a `.` is used to denote nested fields in the YAML configuration block.

| Field                              | Type   | Required | Default     | Description                                                                                                                                                                                                                                          |
|------------------------------------|--------|----------|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `okta_domain`                      | string | ✅        |             | The location of your Okta Domain, without a protocol. Can be found in Okta Developer console.                                                                                                                                                        |
| `okta_api_token`                   | string | ✅        |             | An API token generated for the DataHub application inside your Okta Developer Console.                                                                                                                                                               |
| `ingest_users`                     | bool   |          | `True`      | Whether users should be ingested into DataHub.                                                                                                                                                                                                       |
| `ingest_groups`                    | bool   |          | `True`      | Whether groups should be ingested into DataHub.                                                                                                                                                                                                      |
| `ingest_group_membership`          | bool   |          | `True`      | Whether group membership should be ingested into DataHub. ingest_groups must be True if this is True.                                                                                                                                                |
| `okta_profile_to_username_attr`    | string |          | `"login"`   | Which Okta User Profile attribute to use as input to DataHub username mapping.                                                                                                                                                                       |
| `okta_profile_to_username_regex`   | string |          | `"([^@]+)"` | A regex used to parse the DataHub username from the attribute specified in `okta_profile_to_username_attr`.                                                                                                                                          |
| `okta_profile_to_group_name_attr`  | string |          | `"name"`    | Which Okta Group Profile attribute to use as input to DataHub group name mapping.                                                                                                                                                                    |
| `okta_profile_to_group_name_regex` | string |          | `"(.*)"`    | A regex used to parse the DataHub group name from the attribute specified in `okta_profile_to_group_name_attr`.                                                                                                                                      |
| `include_deprovisioned_users`      | bool   |          | `False`     | Whether to ingest users in the DEPROVISIONED state from Okta.                                                                                                                                                                                        |
| `include_suspended_users`          | bool   |          | `False`     | Whether to ingest users in the SUSPENDED state from Okta.                                                                                                                                                                                            |
| `page_size`                        | number |          | `100`       | The number of entities requested from Okta's REST APIs in one request.                                                                                                                                                                               |
| `delay_seconds`                    | number |          | `0.01`      | Number of seconds to wait between calls to Okta's REST APIs. (Okta rate limits). Defaults to 10ms.                                                                                                                                                   |
| `okta_users_filter`                | string |          | `None`      | Okta filter expression (not regex) for ingesting users. Only one of `okta_users_filter` and `okta_users_search` can be set. See the [Okta API docs](https://developer.okta.com/docs/reference/api/users/#list-users-with-a-filter) for more info.    |
| `okta_users_search`                | string |          | `None`      | Okta search expression (not regex) for ingesting users. Only one of `okta_users_filter` and `okta_users_search` can be set. See the [Okta API docs](https://developer.okta.com/docs/reference/api/users/#list-users-with-search) for more info.      |
| `okta_groups_filter`               | string |          | `None`      | Okta filter expression (not regex) for ingesting groups. Only one of `okta_groups_filter` and `okta_groups_search` can be set. See the [Okta API docs](https://developer.okta.com/docs/reference/api/groups/#filters) for more info.                 |
| `okta_users_search`                | string |          | `None`      | Okta search expression (not regex) for ingesting groups. Only one of `okta_groups_filter` and `okta_groups_search` can be set. See the [Okta API docs](https://developer.okta.com/docs/reference/api/groups/#list-groups-with-search) for more info. |
| `mask_group_id`                    | bool   |          | `True`      | Whether workunit ID's for groups should be masked.                                                                                                                                                                                                   |
| `mask_user_id`                     | bool   |          | `True`      | Whether workunit ID's for users should be masked.                                                                                                                                                                                                    |

## Compatibility

 Validated against Okta API Versions:
   - `2021.07.2`

 Validated against load:
   - User Count: `1000`
   - Group Count: `100`
   - Group Membership Edges: `1000` (1 per User)
   - Run Time (Wall Clock): `2min 7sec`

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
