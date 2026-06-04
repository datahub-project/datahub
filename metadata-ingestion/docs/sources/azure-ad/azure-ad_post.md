As a prerequisite, you should [create a DataHub Application](https://docs.microsoft.com/en-us/graph/toolkit/get-started/add-aad-app-registration) within the Azure AD Portal with the permissions
to read your organization's Users and Groups. The following permissions are required, with the `Application` permission type:

- `Group.Read.All`
- `GroupMember.Read.All`
- `User.Read.All`

You can add a permission by navigating to the permissions tab in your DataHub application on the Azure AD portal.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/azure-ad/azure_ad_api_permissions.png"/>
</p>

You can view the necessary endpoints to configure by clicking on the Endpoints button in the Overview tab.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/azure-ad/azure_ad_endpoints.png"/>
</p>

### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Extracting DataHub Users

##### Usernames

Usernames serve as unique identifiers for users on DataHub. This connector extracts usernames using the
"userPrincipalName" field of an [Azure AD User Response](https://docs.microsoft.com/en-us/graph/api/user-list?view=graph-rest-1.0&tabs=http#response-1),
which is the unique identifier for your Azure AD users.

If this is not how you wish to map to DataHub usernames, you can provide a custom mapping using the configurations options detailed below. Namely, `azure_ad_response_to_username_attr`
and `azure_ad_response_to_username_regex`.

##### Responses

This connector also extracts basic user response information from Azure. The following fields of the Azure User Response are extracted
and mapped to the DataHub `CorpUserInfo` aspect:

- display name
- first name
- last name
- email
- title
- country

#### Extracting DataHub Groups

##### Group Names

Group names serve as unique identifiers for groups on DataHub. This connector extracts group names using the "name" attribute of an Azure Group Response.
By default, a URL-encoded version of the full group name is used as the unique identifier (CorpGroupKey) and the raw "name" attribute is mapped
as the display name that will appear in DataHub's UI.

If this is not how you wish to map to DataHub group names, you can provide a custom mapping using the configurations options detailed below. Namely, `azure_ad_response_to_groupname_attr`
and `azure_ad_response_to_groupname_regex`.

##### Responses

This connector also extracts basic group information from Azure. The following fields of the [Azure AD Group Response](https://docs.microsoft.com/en-us/graph/api/group-list?view=graph-rest-1.0&tabs=http#response-1) are extracted and mapped to the
DataHub `CorpGroupInfo` aspect:

- name
- description

#### Extracting Group Membership

This connector additional extracts the edges between Users and Groups that are stored in [Azure AD](https://docs.microsoft.com/en-us/graph/api/group-list-members?view=graph-rest-1.0&tabs=http#response-1). It maps them to the `GroupMembership` aspect
associated with DataHub users (CorpUsers).

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
