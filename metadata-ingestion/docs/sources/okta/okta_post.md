### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Extracting DataHub Users

User entities are extracted from Okta users APIs and mapped to DataHub `CorpUser` entities.

##### Usernames

Usernames serve as unique identifiers for users on DataHub. This connector extracts usernames using the "login" field of an [Okta User Profile](https://developer.okta.com/docs/reference/api/users/#profile-object). By default, the 'login' attribute, which contains an email, is parsed to extract the text before the "@" and map that to the DataHub username.

If this is not how you wish to map to DataHub usernames, you can provide a custom mapping using the configurations options detailed below. Namely, `okta_profile_to_username_attr` and `okta_profile_to_username_regex`. e.g. if you want to map emails to urns then you may use the following configuration:

```
okta_profile_to_username_attr: "email"
okta_profile_to_username_regex: ".*"
```

##### Profiles

This connector also extracts basic user profile information from Okta. The following fields of the Okta User Profile are extracted and mapped to the DataHub `CorpUserInfo` aspect:

- display name
- first name
- last name
- email
- title
- department
- country code

#### Extracting DataHub Groups

Group entities are extracted from Okta groups APIs and mapped to DataHub `CorpGroup` entities.

##### Group Names

Group names serve as unique identifiers for groups on DataHub. This connector extracts group names using the "name" attribute of an Okta Group Profile. By default, a URL-encoded version of the full group name is used as the unique identifier (`CorpGroupKey`) and the raw "name" attribute is mapped as the display name that will appear in DataHub's UI.

If this is not how you wish to map to DataHub group names, you can provide a custom mapping using the configurations options detailed below. Namely, `okta_profile_to_group_name_attr` and `okta_profile_to_group_name_regex`.

##### Profiles

This connector also extracts basic group information from Okta. The following fields of the Okta Group Profile are extracted and mapped to the DataHub `CorpGroupInfo` aspect:

- name
- description

#### Extracting Group Membership

User-to-group membership edges are extracted and emitted as DataHub group membership relationships.

This connector additional extracts the edges between Users and Groups that are stored in Okta. It maps them to the `GroupMembership` aspect associated with DataHub users (`CorpUsers`).

#### Filtering and Searching

Use connector filter/search configuration to scope user and group extraction to relevant identities and reduce ingestion load.

You can also choose to ingest a subset of users or groups to Datahub by adding flags for filtering or searching. For users, set either the `okta_users_filter` or `okta_users_search` flag (only one can be set at a time). For groups, set either the `okta_groups_filter` or `okta_groups_search` flag. Note that these are not regular expressions.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
