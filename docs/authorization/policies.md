# Policies Guide

## Introduction 

DataHub provides the ability to declare fine-grained access control Policies via the UI & GraphQL API.
Access policies in DataHub define *who* can *do what* to *which resources*. A few policies in plain English include

- Dataset Owners should be allowed to edit documentation, but not Tags. 
- Jenny, our Data Steward, should be allowed to edit Tags for any Dashboard, but no other metadata.
- James, a Data Analyst, should be allowed to edit the Links for a specific Data Pipeline he is a downstream consumer of.
- The Data Platform team should be allowed to manage users & groups, view platform analytics, & manage policies themselves.

In this document, we'll take a deeper look at DataHub Policies & how to use them effectively. 

## What is a Policy?

There are 2 types of Policy within DataHub:

1. Platform Policies
2. Metadata Policies

We'll briefly describe each. 

### Platform Policies

**Platform** policies determine who has platform-level privileges on DataHub. These privileges include

- Managing Users & Groups
- Viewing the DataHub Analytics Page
- Managing Policies themselves

Platform policies can be broken down into 2 parts:

1. **Actors**: Who the policy applies to (Users or Groups)
2. **Privileges**: Which privileges should be assigned to the Actors (e.g. "View Analytics")

Note that platform policies do not include a specific "target resource" against which the Policies apply. Instead,
they simply serve to assign specific privileges to DataHub users and groups.

### Metadata Policies

**Metadata** policies determine who can do what to which Metadata Entities. For example, 

- Who can edit Dataset Documentation & Links?
- Who can add Owners to a Chart?
- Who can add Tags to a Dashboard?

and so on. 

A Metadata Policy can be broken down into 3 parts:

1. **Resources**: The 'which'. Resources that the policy applies to, e.g. "All Datasets".
2. **Privileges**: The 'what'. What actions are being permitted by a policy, e.g. "Add Tags".
3. **Actors**: The 'who'. Specific users, groups that the policy applies to.

#### Resources

Resources can be associated with the policy in a number of ways.

1. List of resource types - The entity's type for example: dataset, chart, dashboard
2. List of resource URNs 
3. List of tags
4. List of domains

:::note Important Note
The associations in the list above are an *intersection* or an _AND_ operation. For example, if the policy targets
`1. resource type: dataset` and `3. resources tagged: 'myTag'`, it will apply to datasets that are tagged with tag 'myTag'.
:::

#### Privileges

Check out the list of
privileges [here](https://github.com/datahub-project/datahub/blob/master/metadata-utils/src/main/java/com/linkedin/metadata/authorization/PoliciesConfig.java)
. Note, the privileges are semantic by nature, and does not tie in 1-to-1 with the aspect model.

All edits on the UI are covered by a privilege, to make sure we have the ability to restrict write access. See the
[Reference](#Reference) section below.

#### Actors

We currently support 3 ways to define the set of actors the policy applies to: 

1. list of users (or all users)
2. list of groups (or all groups)
3. owners of the entity

:::note Important Note
Unlike resources, the definitions for actors are a union of the actors. For example, if user `1. Alice` is associated
with the policy as well as `3. owners of the entity`. This means that Alice _OR_ any owner of
the targeted resource(s) will be included in the policy.
:::

## Managing Policies

Policies can be managed on the page **Settings > Permissions > Policies** page. The `Policies` tab will only 
be visible to those users having the `Manage Policies` privilege.

Out of the box, DataHub is deployed with a set of pre-baked Policies. The set of default policies are created at deploy 
time and can be found inside the `policies.json` file within `metadata-service/war/src/main/resources/boot`. This set of policies serves the 
following purposes:

1. Assigns immutable super-user privileges for the root `datahub` user account (Immutable)
2. Assigns all Platform privileges for all Users by default (Editable)

The reason for #1 is to prevent people from accidentally deleting all policies and getting locked out (`datahub` super user account can be a backup)
The reason for #2 is to permit administrators to log in via OIDC or another means outside of the `datahub` root account
when they are bootstrapping with DataHub. This way, those setting up DataHub can start managing policies without friction. 
Note that these privilege *can* and likely *should* be altered inside the **Policies** page of the UI.

:::note Pro-Tip
To login using the `datahub` account, simply navigate to `<your-datahub-domain>/login` and enter `datahub`, `datahub`. Note that the password can be customized for your
deployment by changing the `user.props` file within the `datahub-frontend` module. Notice that JaaS authentication must be enabled. 
:::note

## Configuration 

By default, the Policies feature is *enabled*. This means that the deployment will support creating, editing, removing, and 
most importantly enforcing fine-grained access policies.

In some cases, these capabilities are not desirable. For example, if your company's users are already used to having free reign, you
may want to keep it that way. Or perhaps it is only your Data Platform team who actively uses DataHub, in which case Policies may be overkill.

For these scenarios, we've provided a back door to disable Policies in your deployment of DataHub. This will completely hide
the policies management UI and by default will allow all actions on the platform. It will be as though
each user has *all* privileges, both of the **Platform** & **Metadata** flavor.

To disable Policies, you can simply set the `AUTH_POLICIES_ENABLED` environment variable for the `datahub-gms` service container
to `false`. For example in your `docker/datahub-gms/docker.env`, you'd place

```
AUTH_POLICIES_ENABLED=false
```

### REST API Authorization

Policies only affect REST APIs when the environment variable `REST_API_AUTHORIZATION` is set to `true` for GMS. Some policies only apply when this setting is enabled, marked above, and other Metadata and Platform policies apply to the APIs where relevant, also specified in the table above.

## Reference

For a complete list of privileges see the
privileges [here](https://github.com/datahub-project/datahub/blob/master/metadata-utils/src/main/java/com/linkedin/metadata/authorization/PoliciesConfig.java).

### Platform-level privileges

These privileges are for DataHub operators to access & manage the administrative functionality of the system.

#### Access & Credentials

| Platform Privileges             | Description                                                                                                                                                                               |
|---------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Generate Personal Access Tokens | Allow actor to generate personal access tokens for use with DataHub APIs.                                                                                                                 |
| Manage Policies                 | Allow actor to create and remove access control policies. Be careful - Actors with this privilege are effectively super users.                                                            |
| Manage Secrets                  | Allow actor to create & remove Secrets stored inside DataHub.                                                                                                                             |
| Manage Users & Groups           | Allow actor to create, remove, and update users and groups on DataHub.                                                                                                                    |
| Manage All Access Tokens        | Allow actor to create, list and revoke access tokens on behalf of users in DataHub. Be careful - Actors with this privilege are effectively super users that can impersonate other users. |
| Manage User Credentials         | Allow actor to manage credentials for native DataHub users, including inviting new users and resetting passwords                                                                          |                                                                                                                                  |
| Manage Connections              | Allow actor to manage connections to external DataHub platforms.                                                                                                                          |

#### Product Features

| Platform Privileges                 | Description                                                                                                        |
|-------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| Manage Home Page Posts              | Allow actor to create and delete home page posts                                                                   |
| Manage Business Attribute           | Allow actor to create, update, delete Business Attribute                                                           |
| Manage Documentation Forms          | Allow actor to manage forms assigned to assets to assist in documentation efforts.                                 |
| Manage Metadata Ingestion           | Allow actor to create, remove, and update Metadata Ingestion sources.                                              |
| Manage Features                     | Umbrella privilege to manage all features.                                                                         |
| View Analytics                      | Allow actor to view the DataHub analytics dashboard.                                                               |
| Manage Public Views                 | Allow actor to create, update, and delete any Public (shared) Views.                                               |
| Manage Ownership Types              | Allow actor to create, update and delete Ownership Types.                                                          |
| Create Business Attribute           | Allow actor to create new Business Attribute.                                                                      |
| Manage Structured Properties        | Manage structured properties in your instance.                                                                     |
| View Tests                          | View Asset Tests.                                                                                                  |
| Manage Tests[^2]                    | Allow actor to create and remove Asset Tests.                                                                      |
| View Metadata Proposals[^2]         | Allow actor to view the requests tab for viewing metadata proposals.                                               |
| Create metadata constraints[^3]     | Allow actor to create metadata constraints.                                                                        |
| Manage Platform Settings[^2]        | Allow actor to view and change platform-level settings, like integrations & notifications.                         |
| Manage Monitors[^2]                 | Allow actor to create, update, and delete any data asset monitors, including Custom SQL monitors. Grant with care. |

[^1]: Only active if REST_API_AUTHORIZATION_ENABLED is true
[^2]: DataHub Cloud only
[^3]: Deprecated feature

#### Entity Management

| Platform Privileges                 | Description                                                                        |
|-------------------------------------|------------------------------------------------------------------------------------|
| Manage Domains                      | Allow actor to create and remove Asset Domains.                                    |
| Manage Glossaries                   | Allow actor to create, edit, and remove Glossary Entities                          |
| Manage Tags                         | Allow actor to create and remove Tags.                                             |

#### System Management

| Platform Privileges                           | Description                                                                                              |
|-----------------------------------------------|----------------------------------------------------------------------------------------------------------|
| Restore Indices API[^1]                       | Allow actor to use the Restore Indices API.                                                              |                                                                                                                                                                                           |
| Get Timeseries index sizes API[^1]            | Allow actor to use the get Timeseries indices size API.                                                  |
| Truncate timeseries aspect index size API[^1] | Allow actor to use the API to truncate a timeseries index.                                               |
| Get ES task status API[^1]                    | Allow actor to use the get task status API for an ElasticSearch task.                                    |
| Enable/Disable Writeability API[^1]           | Allow actor to enable or disable GMS writeability for data migrations.                                   |
| Apply Retention API[^1]                       | Allow actor to apply retention using the API.                                                            |
| Analytics API access[^1]                      | Allow actor to use API read access to raw analytics data.                                                |
| Explain ElasticSearch Query API[^1]           | Allow actor to use the Operations API explain endpoint.                                                  |
| Produce Platform Event API[^1]                | Allow actor to produce Platform Events using the API.                                                    |
| Manage System Operations                      | Allow actor to manage system operation controls. This setting includes all System Management privileges. |

[^1]: Only active if REST_API_AUTHORIZATION_ENABLED is true
[^2]: DataHub Cloud only

### Common Metadata Privileges

These privileges are to view & modify any entity within DataHub.

#### Entity Privileges

| Entity Privileges                   | Description                                                                                |
|-------------------------------------|--------------------------------------------------------------------------------------------|
| View Entity Page                    | Allow actor to view the entity page.                                                       |
| Edit Entity                         | Allow actor to edit any information about an entity. Super user privileges for the entity. |
| Delete                              | Allow actor to delete this entity.                                                         |
| Create Entity                       | Allow actor to create an entity if it doesn't exist.                                       |
| Entity Exists                       | Allow actor to determine whether the entity exists.                                        |
| Get Timeline API[^1]                | Allow actor to use the GET Timeline API.                                                   |
| Get Entity + Relationships API[^1]  | Allow actor to use the GET Entity and Relationships API.                                   |
| Get Aspect/Entity Count APIs[^1]    | Allow actor to use the GET Aspect/Entity Count APIs.                                       |
| View Entity[^2]                     | Allow actor to view the entity in search results.                                          |
| Share Entity[^2]                    | Allow actor to share an entity with another DataHub Cloud instance.                        |

[^1]: Only active if REST_API_AUTHORIZATION_ENABLED is true
[^2]: DataHub Cloud only

#### Aspect Privileges

| Aspect Privileges                   | Description                                                                                |
|-------------------------------------|--------------------------------------------------------------------------------------------|
| Edit Tags                           | Allow actor to add and remove tags to an asset.                                            |
| Edit Glossary Terms                 | Allow actor to add and remove glossary terms to an asset.                                  |
| Edit Description                    | Allow actor to edit the description (documentation) of an entity.                          |
| Edit Links                          | Allow actor to edit links associated with an entity.                                       |
| Edit Status                         | Allow actor to edit the status of an entity (soft deleted or not).                         |
| Edit Domain                         | Allow actor to edit the Domain of an entity.                                               |
| Edit Data Product                   | Allow actor to edit the Data Product of an entity.                                         |
| Edit Deprecation                    | Allow actor to edit the Deprecation status of an entity.                                   |
| Edit Incidents                      | Allow actor to create and remove incidents for an entity.                                  |
| Edit Lineage                        | Allow actor to add and remove lineage edges for this entity.                               |
| Edit Properties                     | Allow actor to edit the properties for an entity.                                          |
| Edit Owners                         | Allow actor to add and remove owners of an entity.                                         |
| Get Timeseries Aspect API[^1]       | Allow actor to use the GET Timeseries Aspect API.                                          |

[^1]: Only active if REST_API_AUTHORIZATION_ENABLED is true
[^2]: DataHub Cloud only

#### Proposals

| Proposals Privileges               | Description                                                                                |
|------------------------------------|--------------------------------------------------------------------------------------------|
| Propose Tags[^2]                   | Allow actor to propose adding a tag to an asset.                                           |
| Propose Glossary Terms[^2]         | Allow actor to propose adding a glossary term to an asset.                                 |
| Propose Documentation[^2]          | Allow actor to propose updates to an asset's documentation.                                |
| Manage Tag Proposals[^2]           | Allow actor to manage a proposal to add a tag to an asset.                                 |
| Manage Glossary Term Proposals[^2] | Allow actor to manage a proposal to add a glossary term to an asset.                       |
| Manage Documentation Proposals[^2] | Allow actor to manage a proposal update an asset's documentation                           |

[^1]: Only active if REST_API_AUTHORIZATION_ENABLED is true
[^2]: DataHub Cloud only

### Specific Entity-level Privileges
These privileges are not generalizable.

#### Users & Groups

| Entity       | Privilege                                 | Description                                                                                                                                                                       |
|--------------|-------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Group        | Edit Group Members                        | Allow actor to add and remove members to a group.                                                                                                                                 |
| Group        | Manage Group Notification Settings[^2]    | Allow actor to manage notification settings for a group.                                                                                                                          |
| Group        | Manage Group Subscriptions[^2]            | Allow actor to manage subscriptions for a group.                                                                                                                                  |
| Group        | Edit Contact Information                  | Allow actor to change the contact information such as email & chat handles.                                                                                                       |
| User         | Edit Contact Information                  | Allow actor to change the contact information such as email & chat handles.                                                                                                       |
| User         | Edit User Profile                         | Allow actor to change the user's profile including display name, bio, title, profile image, etc.                                                                                  |

[^1]: Only active if REST_API_AUTHORIZATION_ENABLED is true
[^2]: DataHub Cloud only

#### Dataset

| Entity       | Privilege                                 | Description                                                                                                                                                                       |
|--------------|-------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Dataset      | View Dataset Usage                        | Allow actor to access dataset usage information (includes usage statistics and queries).                                                                                          |
| Dataset      | View Dataset Profile                      | Allow actor to access dataset profile (snapshot statistics)                                                                                                                       |
| Dataset      | Edit Dataset Column Descriptions          | Allow actor to edit the column (field) descriptions associated with a dataset schema.                                                                                             |
| Dataset      | Edit Dataset Column Tags                  | Allow actor to edit the column (field) tags associated with a dataset schema.                                                                                                     |
| Dataset      | Edit Dataset Column Glossary Terms        | Allow actor to edit the column (field) glossary terms associated with a dataset schema.                                                                                           |
| Dataset      | Propose Dataset Column Glossary Terms[^2] | Allow actor to propose column (field) glossary terms associated with a dataset schema.                                                                                            |
| Dataset      | Propose Dataset Column Tags[^2]           | Allow actor to propose new column (field) tags associated with a dataset schema.                                                                                                  |
| Dataset      | Manage Dataset Column Glossary Terms[^2]  | Allow actor to manage column (field) glossary term proposals associated with a dataset schema.                                                                                    |
| Dataset      | Propose Dataset Column Descriptions[^2]   | Allow actor to propose new descriptions associated with a dataset schema.                                                                                                         |
| Dataset      | Manage Dataset Column Tag Proposals[^2]   | Allow actor to manage column (field) tag proposals associated with a dataset schema.                                                                                              |
| Dataset      | Edit Assertions                           | Allow actor to add and remove assertions from an entity.                                                                                                                          |
| Dataset      | Edit Dataset Queries                      | Allow actor to edit the Queries for a Dataset.                                                                                                                                    |
| Dataset      | Create erModelRelationship                | Allow actor to add erModelRelationship on a dataset.                                                                                                                              |
| Dataset      | Edit Monitors[^2]                         | Allow actor to edit monitors for the entity.                                                                                                                                      |
| Dataset      | Edit SQL Assertion Monitors[^2]           | Allow actor to edit custom SQL assertion monitors for the entity. Note that this gives read query access to users with through the Custom SQL assertion builder. Grant with care. |
| Dataset      | Edit Data Contract[^2]                    | Allow actor to edit the Data Contract for an entity.                                                                                                                              |
| Dataset      | Manage Data Contract Proposals[^2]        | Allow actor to manage a proposal for a Data Contract                                                                                                                              |
| Tag          | Edit Tag Color                            | Allow actor to change the color of a Tag.                                                                                                                                         |
| Domain       | Manage Data Products                      | Allow actor to create, edit, and delete Data Products within a Domain                                                                                                             |
| GlossaryNode | Manage Direct Glossary Children           | Allow actor to create and delete the direct children of this entity.                                                                                                              |
| GlossaryNode | Manage All Glossary Children              | Allow actor to create and delete everything underneath this entity.                                                                                                               |


[^1]: Only active if REST_API_AUTHORIZATION_ENABLED is true
[^2]: DataHub Cloud only

#### Misc

| Entity       | Privilege                                 | Description                                                                                                                                                                       |
|--------------|-------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Tag          | Edit Tag Color                            | Allow actor to change the color of a Tag.                                                                                                                                         |
| Domain       | Manage Data Products                      | Allow actor to create, edit, and delete Data Products within a Domain                                                                                                             |
| GlossaryNode | Manage Direct Glossary Children           | Allow actor to create and delete the direct children of this entity.                                                                                                              |
| GlossaryNode | Manage All Glossary Children              | Allow actor to create and delete everything underneath this entity.                                                                                                               |

[^1]: Only active if REST_API_AUTHORIZATION_ENABLED is true
[^2]: DataHub Cloud only

## Coming Soon

The DataHub team is hard at work trying to improve the Policies feature. We are planning on building out the following:

- Hide edit action buttons on Entity pages to reflect user privileges

Under consideration

- Ability to define Metadata Policies against multiple resources scoped to particular "Containers" (e.g. A "schema", "database", or "collection")

## Feedback / Questions / Concerns

We want to hear from you! For any inquiries, including Feedback, Questions, or Concerns, reach out on Slack!
