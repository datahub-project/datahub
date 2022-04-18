# Azure AD

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[azure-ad]'`.

## Capabilities

This plugin extracts the following:

- Users
- Groups
- Group Membership

from your Azure AD instance.

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
associated with DataHub users (CorpUsers). Today this has the unfortunate side effect of **overwriting** any Group Membership information that
was created outside of the connector. That means if you've used the DataHub REST API to assign users to groups, this information will be overridden
when the Azure AD Source is executed. If you intend to *always* pull users, groups, and their relationships from your Identity Provider, then
this should not matter. 

This is a known limitation in our data model that is being tracked by [this ticket](https://github.com/datahub-project/datahub/issues/3065).


## Quickstart recipe

As a prerequisite, you should [create a DataHub Application](https://docs.microsoft.com/en-us/graph/toolkit/get-started/add-aad-app-registration) within the Azure AD Portal with the permissions
to read your organization's Users and Groups. The following permissions are required, with the `Application` permission type:

- `Group.Read.All`
- `GroupMember.Read.All`
- `User.Read.All`

You can use the following recipe to get started with Azure ingestion! See [below](#config-details) for full configuration options.

```yml
---
source:
  type: "azure-ad"
  config:
    client_id: "00000000-0000-0000-0000-000000000000"
    tenant_id: "00000000-0000-0000-0000-000000000000"
    client_secret: "xxxxx"
    redirect: "https://login.microsoftonline.com/common/oauth2/nativeclient"
    authority: "https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000"
    token_url: "https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000/oauth2/token"
    graph_url: "https://graph.microsoft.com/v1.0"
    ingest_users: True
    ingest_groups: True
    groups_pattern:
      allow:
        - ".*"
    users_pattern:
      allow:
        - ".*"

sink:
  # sink configs
```

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).


## Configuration

Note that a `.` is used to denote nested fields in the YAML configuration block.

| Field                              | Type   | Required | Default     | Description                                                                                                     |
|------------------------------------|--------|----------|-------------|-----------------------------------------------------------------------------------------------------------------|
| `client_id`                     | string   | ✅          | |  Application ID. Found in your app registration on Azure AD Portal       |                                                         
| `tenant_id`                     | string   | ✅          |       | Directory ID. Found in your app registration on Azure AD Portal       |                                     
| `client_secret`                     | string   | ✅           |       | Client secret. Found in your app registration on Azure AD Portal       |                                  
| `redirect`                     | string   | ✅          |       | Redirect URI.  Found in your app registration on Azure AD Portal       |                                  
| `authority`                     | string   | ✅          |       | The [authority](https://docs.microsoft.com/en-us/azure/active-directory/develop/msal-client-application-configuration) is a URL that indicates a directory that MSAL can request tokens from. |
| `token_url`                     | string   | ✅          |       | The token URL that acquires a token from Azure AD for authorizing requests.  This source will only work with v1.0 endpoint. |
| `graph_url`                     | string   | ✅          |       | [Microsoft Graph API endpoint](https://docs.microsoft.com/en-us/graph/use-the-api)
| `ingest_users`                     | bool   |          | `True`      | Whether users should be ingested into DataHub.                                                                  |
| `ingest_groups`                    | bool   |          | `True`      | Whether groups should be ingested into DataHub.                                                                 |
| `ingest_group_membership`          | bool   |          | `True`      | Whether group membership should be ingested into DataHub. ingest_groups must be True if this is True.           |
| `azure_ad_response_to_username_attr`    | string |          | `"userPrincipalName"`   | Which Azure AD User Response attribute to use as input to DataHub username mapping.                                  |
| `azure_ad_response_to_username_regex`   | string |          | `"(.*)"` | A regex used to parse the DataHub username from the attribute specified in `azure_ad_response_to_username_attr`.     |
| `users_pattern.allow`                 |  list of strings    |             |       | List of regex patterns for users to include in ingestion. The name against which compare the regexp is the DataHub user name, i.e. the one resulting from the action of `azure_ad_response_to_username_attr` and `azure_ad_response_to_username_regex`   |
| `users_pattern.deny`                  | list of strings     |             |       | As above, but for excluding users from ingestion.                                                                                                                           |
| `azure_ad_response_to_groupname_attr`  | string |          | `"name"`    | Which Azure AD Group Response attribute to use as input to DataHub group name mapping.                               |
| `azure_ad_response_to_groupname_regex` | string |          | `"(.*)"`    | A regex used to parse the DataHub group name from the attribute specified in `azure_ad_response_to_groupname_attr`. |
| `groups_pattern.allow`                 |  list of strings  |             |       | List of regex patterns for groups to include in ingestion. The name against which compare the regexp is the DataHub group name, i.e. the one resulting from the action of `azure_ad_response_to_groupname_attr` and `azure_ad_response_to_groupname_regex`   |
| `groups_pattern.deny`                  |  list of strings  |             |       | As above, but for exculing groups from ingestion.                                                                                                                           |
| `ingest_groups_users`                  | bool              |             | `True`             | This option is useful only when `ingest_users` is set to False and `ingest_group_membership` to True. As effect, only the users which belongs to the selected groups will be ingested. |

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
