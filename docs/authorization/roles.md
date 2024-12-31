import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Roles

<FeatureAvailability/>

DataHub provides the ability to use **Roles** to manage permissions.

:::tip **Roles** are the recommended way to manage permissions on DataHub. This should suffice for most use cases, but advanced users can use **Policies** if needed.

## Roles Setup, Prerequisites, and Permissions

The out-of-the-box Roles represent the most common types of DataHub users. Currently, the supported Roles are **Admin**, **Editor** and **Reader**.

| Role Name | Description                                                                             |
| --------- | --------------------------------------------------------------------------------------- |
| Admin     | Can do everything on the platform.                                                      |
| Editor    | Can read and edit all metadata. Cannot take administrative actions.                     |
| Reader    | Can read all metadata. Cannot edit anything by default, or take administrative actions. |

:::note To manage roles, including viewing roles, or editing a user's role, you must either be an **Admin**, or have the **Manage Policies** privilege.

## Using Roles

### Viewing Roles

You can view the list of existing Roles under **Settings > Permissions > Roles**. You can click into a Role to see details about
it, like which users have that Role, and which Policies correspond to that Role.

<p align="center">
 <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/roles/view-roles-list.png" />
</p>

### Assigning Roles

Roles can be assigned in two different ways.

#### Assigning a New Role to a Single User

If you go to **Settings > Users & Groups > Users**, you will be able to view your full list of users, as well as which Role they are currently
assigned to, including if they don't have a Role.

<p align="center">
 <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/roles/user-list-roles.png" />
</p>

You can simply assign a new Role to a user by clicking on the drop-down that appears on their row and selecting the desired Role.

<p align="center">
 <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/roles/user-list-select-role.png" />
</p>


#### Batch Assigning a Role

When viewing the full list of roles at **Settings > Permissions > Roles**, you will notice that each role has an `Add Users` button next to it. Clicking this button will
lead you to a search box where you can search through your users, and select which users you would like to assign this role to.

<p align="center">
 <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/roles/batch-assign-role.png" />
</p>

### How do Roles interact with Policies?

Roles actually use Policies under-the-hood, and come prepackaged with corresponding policies to control what a Role can do, which you can view in the
Policies tab. Note that these Role-specific policies **cannot** be changed. You can find the full list of policies corresponding to each Role at the bottom of this
[file](https://github.com/datahub-project/datahub/blob/master/metadata-service/war/src/main/resources/boot/policies.json).

If you would like to have finer control over what a user on your DataHub instance can do, the Roles system interfaces cleanly
with the Policies system. For example, if you would like to give a user a **Reader** role, but also allow them to edit metadata
for certain domains, you can add a policy that will allow them to do. Note that adding a policy like this will only add to what a user can do
in DataHub.

### Role Privileges

#### Self-Hosted DataHub and DataHub Cloud

These privileges are common to both Self-Hosted DataHub and DataHub Cloud.

##### Platform Privileges

| Privilege                                 | Admin              | Editor             | Reader | Description                                                                                                                                                                |
|-------------------------------------------|--------------------|--------------------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Generate Personal Access Tokens           | :heavy_check_mark: | :heavy_check_mark: | :x:    | Generate personal access tokens for use with DataHub APIs.                                                                                                                 |
| Manage Domains                            | :heavy_check_mark: | :heavy_check_mark: | :x:    | Create and remove Asset Domains.                                                                                                                                           |
| Manage Home Page Posts                    | :heavy_check_mark: | :heavy_check_mark: | :x:    | Create and delete home page posts                                                                                                                                          |
| Manage Glossaries                         | :heavy_check_mark: | :heavy_check_mark: | :x:    | Create, edit, and remove Glossary Entities                                                                                                                                 |
| Manage Tags                               | :heavy_check_mark: | :heavy_check_mark: | :x:    | Create and remove Tags.                                                                                                                                                    |
| Manage Business Attribute                 | :heavy_check_mark: | :heavy_check_mark: | :x:    | Create, update, delete Business Attribute                                                                                                                                  |
| Manage Documentation Forms                | :heavy_check_mark: | :heavy_check_mark: | :x:    | Manage forms assigned to assets to assist in documentation efforts.                                                                                                        |
| Manage Policies                           | :heavy_check_mark: | :x:                | :x:    | Create and remove access control policies. Be careful - Actors with this privilege are effectively super users.                                                            |
| Manage Metadata Ingestion                 | :heavy_check_mark: | :x:                | :x:    | Create, remove, and update Metadata Ingestion sources.                                                                                                                     |
| Manage Secrets                            | :heavy_check_mark: | :x:                | :x:    | Create & remove Secrets stored inside DataHub.                                                                                                                             |
| Manage Users & Groups                     | :heavy_check_mark: | :x:                | :x:    | Create, remove, and update users and groups on DataHub.                                                                                                                    |
| View Analytics                            | :heavy_check_mark: | :x:                | :x:    | View the DataHub analytics dashboard.                                                                                                                                      |
| Manage All Access Tokens                  | :heavy_check_mark: | :x:                | :x:    | Create, list and revoke access tokens on behalf of users in DataHub. Be careful - Actors with this privilege are effectively super users that can impersonate other users. |
| Manage User Credentials                   | :heavy_check_mark: | :x:                | :x:    | Manage credentials for native DataHub users, including inviting new users and resetting passwords                                                                          |
| Manage Public Views                       | :heavy_check_mark: | :x:                | :x:    | Create, update, and delete any Public (shared) Views.                                                                                                                      |
| Manage Ownership Types                    | :heavy_check_mark: | :x:                | :x:    | Create, update and delete Ownership Types.                                                                                                                                 |
| Create Business Attribute                 | :heavy_check_mark: | :x:                | :x:    | Create new Business Attribute.                                                                                                                                             |
| Manage Connections                        | :heavy_check_mark: | :x:                | :x:    | Manage connections to external DataHub platforms.                                                                                                                          |
| Restore Indices API                       | :heavy_check_mark: | :x:                | :x:    | The ability to use the Restore Indices API.                                                                                                                                |
| Get Timeseries index sizes API            | :heavy_check_mark: | :x:                | :x:    | The ability to use the get Timeseries indices size API.                                                                                                                    |
| Truncate timeseries aspect index size API | :heavy_check_mark: | :x:                | :x:    | The ability to use the API to truncate a timeseries index.                                                                                                                 |
| Get ES task status API                    | :heavy_check_mark: | :x:                | :x:    | The ability to use the get task status API for an ElasticSearch task.                                                                                                      |
| Enable/Disable Writeability API           | :heavy_check_mark: | :x:                | :x:    | The ability to enable or disable GMS writeability for data migrations.                                                                                                     |
| Apply Retention API                       | :heavy_check_mark: | :x:                | :x:    | The ability to apply retention using the API.                                                                                                                              |
| Analytics API access                      | :heavy_check_mark: | :x:                | :x:    | API read access to raw analytics data.                                                                                                                                     |

##### Metadata Privileges

| Privilege                          | Admin              | Editor             | Reader             | Description                                                                                      |
|------------------------------------|--------------------|--------------------|--------------------|--------------------------------------------------------------------------------------------------|
| View Entity Page                   | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | The ability to view the entity page.                                                             |
| View Dataset Usage                 | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | The ability to access dataset usage information (includes usage statistics and queries).         |
| View Dataset Profile               | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | The ability to access dataset profile (snapshot statistics)                                      |
| Edit Tags                          | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to add and remove tags to an asset.                                                  |
| Edit Glossary Terms                | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to add and remove glossary terms to an asset.                                        |
| Edit Description                   | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to edit the description (documentation) of an entity.                                |
| Edit Links                         | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to edit links associated with an entity.                                             |
| Edit Status                        | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to edit the status of an entity (soft deleted or not).                               |
| Edit Domain                        | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to edit the Domain of an entity.                                                     |
| Edit Data Product                  | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to edit the Data Product of an entity.                                               |
| Edit Deprecation                   | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to edit the Deprecation status of an entity.                                         |
| Edit Assertions                    | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to add and remove assertions from an entity.                                         |
| Edit Incidents                     | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to create and remove incidents for an entity.                                        |
| Edit Entity                        | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to edit any information about an entity. Super user privileges for the entity.       |
| Edit Dataset Column Tags           | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to edit the column (field) tags associated with a dataset schema.                    |
| Edit Dataset Column Glossary Terms | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to edit the column (field) glossary terms associated with a dataset schema.          |
| Edit Dataset Column Descriptions   | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to edit the column (field) descriptions associated with a dataset schema.            |
| Edit Tag Color                     | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to change the color of a Tag.                                                        |
| Edit Lineage                       | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to add and remove lineage edges for this entity.                                     |
| Edit Dataset Queries               | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to edit the Queries for a Dataset.                                                   |
| Manage Data Products               | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to create, edit, and delete Data Products within a Domain                            |
| Edit Properties                    | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to edit the properties for an entity.                                                |
| Edit Owners                        | :heavy_check_mark: | :x:                | :x:                | The ability to add and remove owners of an entity.                                               |
| Edit Group Members                 | :heavy_check_mark: | :x:                | :x:                | The ability to add and remove members to a group.                                                |
| Edit User Profile                  | :heavy_check_mark: | :x:                | :x:                | The ability to change the user's profile including display name, bio, title, profile image, etc. |
| Edit Contact Information           | :heavy_check_mark: | :x:                | :x:                | The ability to change the contact information such as email & chat handles.                      |
| Delete                             | :heavy_check_mark: | :x:                | :x:                | The ability to delete this entity.                                                               |
| Search API                         | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | The ability to access search APIs.                                                               |
| Get Aspect/Entity Count APIs       | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | The ability to use the GET Aspect/Entity Count APIs.                                             |
| Get Timeseries Aspect API          | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | The ability to use the GET Timeseries Aspect API.                                                |
| Get Entity + Relationships API     | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | The ability to use the GET Entity and Relationships API.                                         |
| Get Timeline API                   | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | The ability to use the GET Timeline API.                                                         |
| Explain ElasticSearch Query API    | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | The ability to use the Operations API explain endpoint.                                          |
| Produce Platform Event API         | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to produce Platform Events using the API.                                            |

#### DataHub Cloud

These privileges are only relevant to DataHub Cloud.

##### Platform Privileges

| Privilege                   | Admin              | Editor             | Reader | Description                                                                                         |
|-----------------------------|--------------------|--------------------|--------|-----------------------------------------------------------------------------------------------------|
| Manage Tests                | :heavy_check_mark: | :heavy_check_mark: | :x:    | Create and remove Asset Tests.                                                                      |
| View Metadata Proposals     | :heavy_check_mark: | :heavy_check_mark: | :x:    | View the requests tab for viewing metadata proposals.                                               |
| Create metadata constraints[^1] | :heavy_check_mark: | :heavy_check_mark: | :x:    | Create metadata constraints.                                                                        |
| Manage Platform Settings    | :heavy_check_mark: | :x:                | :x:    | View and change platform-level settings, like integrations & notifications.                         |
| Manage Monitors             | :heavy_check_mark: | :x:                | :x:    | Create, update, and delete any data asset monitors, including Custom SQL monitors. Grant with care. |

[^1]: Deprecated feature

##### Metadata Privileges

| Privilege                             | Admin              | Editor             | Reader             | Description                                                                                    |
|---------------------------------------|--------------------|--------------------|--------------------|------------------------------------------------------------------------------------------------|
| View Entity                           | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | The ability to view the entity in search results.                                              |
| Propose Tags                          | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | The ability to propose adding a tag to an asset.                                               |
| Propose Glossary Terms                | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | The ability to propose adding a glossary term to an asset.                                     |
| Propose Documentation                 | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | The ability to propose updates to an asset's documentation.                                    |
| Propose Dataset Column Glossary Terms | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | The ability to propose column (field) glossary terms associated with a dataset schema.         |
| Propose Dataset Column Tags           | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | The ability to propose new column (field) tags associated with a dataset schema.               |
| Manage Tag Proposals                  | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to manage a proposal to add a tag to an asset.                                     |
| Manage Glossary Term Proposals        | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to manage a proposal to add a glossary term to an asset.                           |
| Manage Dataset Column Glossary Terms  | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to manage column (field) glossary term proposals associated with a dataset schema. |
| Manage Dataset Column Tag Proposals   | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to manage column (field) tag proposals associated with a dataset schema.           |
| Manage Documentation Proposals        | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to manage a proposal update an asset's documentation                               |
| Manage Group Notification Settings    | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to manage notification settings for a group.                                       |
| Manage Group Subscriptions            | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to manage subscriptions for a group.                                               |
| Manage User Subscriptions             | :heavy_check_mark: | :x:                | :x:                | The ability to manage subscriptions for another user.                                          |
| Manage Data Contract Proposals        | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to manage a proposal for a Data Contract                                           |
| Share Entity                          | :heavy_check_mark: | :heavy_check_mark: | :x:                | The ability to share an entity with another DataHub Cloud instance.                                    |

## Additional Resources

### GraphQL

* [acceptRole](../../graphql/mutations.md#acceptrole)
* [batchAssignRole](../../graphql/mutations.md#batchassignrole)
* [listRoles](../../graphql/queries.md#listroles)

## FAQ and Troubleshooting

## What updates are planned for Roles?

In the future, the DataHub team is looking into adding the following features to Roles.

- Defining a role mapping from OIDC identity providers to DataHub that will grant users a DataHub role based on their IdP role
- Allowing Admins to set a default role on DataHub so all users are assigned a role
- Building custom roles
