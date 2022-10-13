# Authorization using Roles

## Introduction

DataHub provides the ability to use **Roles** to manage permissions.

:::tip
**Roles** are the recommended way to manage permissions on DataHub. This should suffice for most use cases, but advanced users can use **Policies** if needed.

## Roles

### Why use Roles?

Roles are a simple set of defaults for the types of users that are likely to exist on DataHub. Currently, the supported Roles are **Admin**, **Editor** and **Reader**.

| Group Name | Description                                                                             |
| ---------- | --------------------------------------------------------------------------------------- |
| Admin      | Can do everything on the platform.                                                      |
| Editor     | Can read and edit all metadata. Cannot take administrative actions.                     |
| Reader     | Can read all metadata. Cannot edit anything by default, or take administrative actions. |

:::note
To manage roles, including viewing roles, or editing a user's role, you must either be an **Admin**, or have the **Manage Policies** privilege.

### Viewing Roles

You can view the list of Roles under **Settings > Permissions > Roles**. You can click into a Role to see details about
it, like which users have that Role, and which Policies correspond to that Role.

![](../imgs/view-roles-list.png)

### How do I assign a Role to a User?

Roles can be assigned in two different ways.

#### Assigning a New Role to a Single User

If you go to **Settings > Users & Groups > Users**, you will be able to view your full list of DataHub users, as well as which Role they are currently
assigned to, including if they don't have a Role.

![](../imgs/user-list-roles.png)

You can simply assign a new Role to a user by clicking on the drop-down that appears on their row and selecting the desired Role.

![](../imgs/user-list-select-role.png)

#### Batch Assigning a Role

When viewing the full list of roles at **Settings > Permissions > Roles**, you will notice that each role has an `Add Users` button next to it. Clicking this button will
lead you to a search box where you can search through your users, and select which users you would like to assign this role to.

![](../imgs/batch-assign-role.png)

### How do Roles interact with Policies?

Roles actually use Policies under-the-hood, and come pre-packaged with corresponding policies to control what a Role can do, which you can view in the
Policies tab. Note that these Role-specific policies **cannot** be changed. You can find the full list of policies corresponding to each Role at the bottom of this
[file](https://github.com/datahub-project/datahub/blob/master/metadata-service/war/src/main/resources/boot/policies.json).

If you would like to have finer control over what a user on your DataHub instance can do, the Roles system interfaces cleanly
with the Policies system. For example, if you would like to give a user a **Reader** role, but also allow them to edit metadata
for certain domains, you can add a policy that will allow them to do. Note that adding a policy like this will only add to what a user can do
in DataHub.

## What's coming next?

In the future, the DataHub team is looking into adding the following features to Roles.

- Sharing invite links to other users that will assign them a specific role
- Defining a role mapping from OIDC identity providers to DataHub that will grant users a DataHub role based on their IdP role
- Allowing Admins to set a default role to DataHub so that all users are assigned a role
- Building custom roles

## Feedback / Questions / Concerns

We want to hear from you! For any inquiries, including Feedback, Questions, or Concerns, reach out on Slack!
