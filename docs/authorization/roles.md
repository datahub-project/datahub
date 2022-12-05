import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Roles

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

#### Self-Hosted DataHub and Managed DataHub

These privileges are common to both Self-Hosted DataHub and Managed DataHub.

##### Platform Privileges

| Privilege                       | Admin              | Editor             | Reader |
|---------------------------------|--------------------|--------------------|--------|
| Generate Personal Access Tokens | :heavy_check_mark: | :heavy_check_mark: | :x:    |
| Manage Domains                  | :heavy_check_mark: | :heavy_check_mark: | :x:    |
| Manage Glossaries               | :heavy_check_mark: | :heavy_check_mark: | :x:    |
| Manage Tags                     | :heavy_check_mark: | :heavy_check_mark: | :x:    |
| Manage Policies                 | :heavy_check_mark: | :x:                | :x:    |
| Manage Ingestion                | :heavy_check_mark: | :x:                | :x:    |
| Manage Secrets                  | :heavy_check_mark: | :x:                | :x:    |
| Manage Users and Groups         | :heavy_check_mark: | :x:                | :x:    |
| Manage Access Tokens            | :heavy_check_mark: | :x:                | :x:    |
| Manage User Credentials         | :heavy_check_mark: | :x:                | :x:    |
| View Analytics                  | :heavy_check_mark: | :x:                | :x:    |

##### Metadata Privileges

| Privilege                            | Admin              | Editor             | Reader             |
|--------------------------------------|--------------------|--------------------|--------------------|
| View Entity Page                     | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: |
| View Dataset Usage                   | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: |
| View Dataset Profile                 | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: |
| Edit Entity                          | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Edit Entity Tags                     | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Edit Entity Glossary Terms           | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Edit Entity Owners                   | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Edit Entity Docs                     | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Edit Entity Doc Links                | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Edit Entity Status                   | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Edit Entity Assertions               | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Manage Entity Tags                   | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Manage Entity Glossary Terms         | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Edit Dataset Column Tags             | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Edit Dataset Column Glossary Terms   | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Edit Dataset Column Descriptions     | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Manage Dataset Column Tags           | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Manage Dataset Column Glossary Terms | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Edit Tag Color                       | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Edit User Profile                    | :heavy_check_mark: | :heavy_check_mark: | :x:                |
| Edit Contact Info                    | :heavy_check_mark: | :heavy_check_mark: | :x:                |

#### Managed DataHub

These privileges are only relevant to Managed DataHub.

##### Platform Privileges

| Privilege               | Admin              | Editor             | Reader |
|-------------------------|--------------------|--------------------|--------|
| Create Constraints      | :heavy_check_mark: | :heavy_check_mark: | :x:    |
| View Metadata Proposals | :heavy_check_mark: | :heavy_check_mark: | :x:    |
| Manage Tests            | :heavy_check_mark: | :x:                | :x:    |
| Manage Global Settings  | :heavy_check_mark: | :x:                | :x:    |

##### Metadata Privileges

| Privilege                             | Admin              | Editor             | Reader             |
|---------------------------------------|--------------------|--------------------|--------------------|
| Propose Entity Tags                   | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: |
| Propose Entity Glossary Terms         | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: |
| Propose Dataset Column Tags           | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: |
| Propose Dataset Column Glossary Terms | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: |
| Edit Entity Operations                | :heavy_check_mark: | :heavy_check_mark: | :x:                |

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
