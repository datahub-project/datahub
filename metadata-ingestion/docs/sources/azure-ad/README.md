### Extracting DataHub Users

#### Usernames

Usernames serve as unique identifiers for users on DataHub. This connector extracts usernames using the
"userPrincipalName" field of an [Azure AD User Response](https://docs.microsoft.com/en-us/graph/api/user-list?view=graph-rest-1.0&tabs=http#response-1),
which is the unique identifier for your Azure AD users.

If this is not how you wish to map to DataHub usernames, you can provide a custom mapping using the configurations options detailed below. Namely, `azure_ad_response_to_username_attr`
and `azure_ad_response_to_username_regex`.

#### Responses

This connector also extracts basic user response information from Azure. The following fields of the Azure User Response are extracted
and mapped to the DataHub `CorpUserInfo` aspect:

- display name
- first name
- last name
- email
- title
- country

### Extracting DataHub Groups

#### Group Names

Group names serve as unique identifiers for groups on DataHub. This connector extracts group names using the "name" attribute of an Azure Group Response.
By default, a URL-encoded version of the full group name is used as the unique identifier (CorpGroupKey) and the raw "name" attribute is mapped
as the display name that will appear in DataHub's UI.

If this is not how you wish to map to DataHub group names, you can provide a custom mapping using the configurations options detailed below. Namely, `azure_ad_response_to_groupname_attr`
and `azure_ad_response_to_groupname_regex`.

#### Responses

This connector also extracts basic group information from Azure. The following fields of the [Azure AD Group Response](https://docs.microsoft.com/en-us/graph/api/group-list?view=graph-rest-1.0&tabs=http#response-1) are extracted and mapped to the
DataHub `CorpGroupInfo` aspect:

- name
- description

### Extracting Group Membership

This connector additional extracts the edges between Users and Groups that are stored in [Azure AD](https://docs.microsoft.com/en-us/graph/api/group-list-members?view=graph-rest-1.0&tabs=http#response-1). It maps them to the `GroupMembership` aspect
associated with DataHub users (CorpUsers).
