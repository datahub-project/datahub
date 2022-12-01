---
description: >-
  DataHub supports Fine Grained Access Policies that determine who can perform
  specific actions on the platform.
---

# Access Policies

## Overview&#x20;

Before proceeding, we recommend you check out the [Policies Guide doc](https://datahubproject.io/docs/authorization/access-policies-guide), which provides a detailed overview about the new DataHub Access Policies feature

In a nutshell, access policies are a tool for DataHub administrators to define who can perform what actions on the platform.

DataHub comes out of the box with a default set of Access Policies. These grant "super user" privileges to the "admin" user account. Today, these include the ability to&#x20;

* Edit all Metadata Entities (Datasets, Charts, Dashboards, Pipelines, Tasks, etc)&#x20;
* Manage Access Policies
* View Analytics

Expect this set of privileges to grow as capabilities are added to the platform!

:::info
**Pro-Tip**: You can access the "admin" account at your-datahub-domain.com/login with the password provided to you by the Acryl team.&#x20;
:::

## Creating an Access Policy&#x20;

In order to customize the permissions that individual Users & Groups have, you can using the **Policies Builder** feature. This is located in the top-right navigation bar when logged in as any user that has the "Manage Policies" Platform Privilege. By default, the "admin" account is the only account that has this privilege.

This means that to grant additional privileges to your users & groups, you'll first need to log in as the "admin" user. From there, click the **Policies** button. This will show you all of your Access Policies. From here, you can click "New Policy" to add a custom Access Policy.&#x20;

Inside the Policy builder form, you'll complete 3 steps before saving your Policy. These 3 steps determine a) the type of Policy you wish to create, b) the privileges you wish to grant via the Policy, and c) the recipients of those privileges. The final step is the place where you can search users and groups that the Policy should apply to.&#x20;

Once a new policy has been created, it will immediately go into effect. Policies can be deactivated and deleted using the "Deactivate" and "Delete" buttons located on the Policy information view, which can be accessed by clicking on a Policy in the "Your Policies" list.&#x20;

## Default Groups

Acryl DataHub provides some pre-defined groups for you to get started, along with some pre-configured policies.

| Group Name          | Description                                                                                                                                                                                      |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Organization Admins | Top level DataHub administrators. They have **all** privileges.                                                                                                                                  |
| Access Admins       | Able to manage users (including resetting passwords for native users) and groups. Can also edit policies and access tokens.                                                                      |
| Metadata Stewards   | Able to both read and modify **all** assets across DataHub. They **cannot** make changes to users, group, policies and credentials.                                                              |
| Metadata Readers    | <p>Able to read metadata, but cannot change it by default (<strong>unless</strong> they are an owner of an asset).<br></br>Otherwise, they must request to make changes using approval flows.</p> |
