# Policies Guide

## Introduction

DataHub provides the ability to declare fine-grained access control Policies via the UI & GraphQL API.
Access policies in DataHub define _who_ can _do what_ to _which resources_. A few policies in plain English include

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

Resources can be associated with the policy in a number of ways:

1. **Resource types** - The entity's type, for example: dataset, chart, dashboard
2. **Resource URNs** - Specific entity URNs to target
3. **Tags** - Assets tagged with specific tags
4. **Domains** - Assets within specific domains
5. **Containers** - Assets within specific containers (e.g., databases, schemas)
6. **Glossary Terms or Term Groups** - Assets annotated with specific glossary terms or any term within a term group (see [Glossary-Based Policy Targeting](#glossary-based-policy-targeting) below)

:::note Important Note
The associations in the list above are an _intersection_ or an _AND_ operation. For example, if the policy targets
`1. resource type: dataset` and `3. resources tagged: 'myTag'`, it will apply to datasets that are tagged with tag 'myTag'.
:::

##### Domain-Based Policy Targeting

Policies can be targeted to assets based on **Domains**. This allows you to apply permissions to all assets within a specific business domain.

When you target a policy by domain, the policy applies recursively to any asset that belongs to that domain as well as any assets in nested child domains.

**Example**: A policy targeting the "Marketing" domain will apply to all datasets, dashboards, and other assets assigned to that domain, as well as assets in child domains like "Marketing Analytics" or "Marketing Campaigns".

##### Container-Based Policy Targeting

Policies can be targeted to assets based on **Containers** (e.g., databases, schemas, projects). This allows you to apply permissions based on technical organization.

When you target a policy by container, the policy applies recursively to all assets within that container as well as any assets in nested child containers.

**Example**: A policy targeting a "production" database container will apply to all schemas within that database and all tables within those schemas.

##### Glossary-Based Policy Targeting

Policies can be targeted to assets based on **Glossary Terms** or **Glossary Term Groups**. This allows you to apply permissions based on business vocabulary rather than technical properties.

When you target a policy by glossary terms or groups:

- **Individual Terms**: The policy applies to any asset annotated with that specific glossary term
- **Term Groups**: The policy applies recursively to assets annotated with any term within that group, including all nested child terms and groups

This works similarly to domain and container-based policies, automatically covering assets as your glossary evolves.

**Example**: A policy targeting the "Sensitive Data" term group will apply to all assets tagged with child terms like "PII", "PHI", or any terms in nested groups, without requiring policy updates when new terms are added to the hierarchy.

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
Note that these privilege _can_ and likely _should_ be altered inside the **Policies** page of the UI.

:::note Pro-Tip
To login using the `datahub` account, simply navigate to `<your-datahub-domain>/login` and enter `datahub`, `datahub`. Note that the password can be customized for your
deployment by changing the `user.props` file within the `datahub-frontend` module. Notice that JaaS authentication must be enabled.
:::note

## Configuration

By default, the Policies feature is _enabled_. This means that the deployment will support creating, editing, removing, and
most importantly enforcing fine-grained access policies.

In some cases, these capabilities are not desirable. For example, if your company's users are already used to having free reign, you
may want to keep it that way. Or perhaps it is only your Data Platform team who actively uses DataHub, in which case Policies may be overkill.

For these scenarios, we've provided a back door to disable Policies in your deployment of DataHub. This will completely hide
the policies management UI and by default will allow all actions on the platform. It will be as though
each user has _all_ privileges, both of the **Platform** & **Metadata** flavor.

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
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --- |
| Generate Personal Access Tokens | Allow actor to generate personal access tokens for use with DataHub APIs.                                                                                                                 |
| Manage Policies                 | Allow actor to create and remove access control policies. Be careful - Actors with this privilege are effectively super users.                                                            |
| Manage Secrets                  | Allow actor to create & remove Secrets stored inside DataHub.                                                                                                                             |
| Manage Users & Groups           | Allow actor to create, remove, and update users and groups on DataHub.                                                                                                                    |
| Manage All Access Tokens        | Allow actor to create, list and revoke access tokens on behalf of users in DataHub. Be careful - Actors with this privilege are effectively super users that can impersonate other users. |
| Manage User Credentials         | Allow actor to manage credentials for native DataHub users, including inviting new users and resetting passwords                                                                          |     |
| Manage Connections              | Allow actor to manage connections to external DataHub platforms.                                                                                                                          |

#### Product Features

| Platform Privileges             | Description                                                                                                        |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Manage Home Page Posts          | Allow actor to create and delete home page posts                                                                   |
| Manage Business Attribute       | Allow actor to create, update, delete Business Attribute                                                           |
| Manage Documentation Forms      | Allow actor to manage forms assigned to assets to assist in documentation efforts.                                 |
| Manage Metadata Ingestion       | Allow actor to create, remove, and update Metadata Ingestion sources.                                              |
| Manage Features                 | Umbrella privilege to manage all features.                                                                         |
| View Analytics                  | Allow actor to view the DataHub analytics dashboard.                                                               |
| Manage Public Views             | Allow actor to create, update, and delete any Public (shared) Views.                                               |
| Manage Ownership Types          | Allow actor to create, update and delete Ownership Types.                                                          |
| Create Business Attribute       | Allow actor to create new Business Attribute.                                                                      |
| Manage Structured Properties    | Manage structured properties in your instance.                                                                     |
| View Tests                      | View Asset Tests.                                                                                                  |
| Manage Tests[^1]                | Allow actor to create and remove Asset Tests.                                                                      |
| View Metadata Proposals[^1]     | Allow actor to view the requests tab for viewing metadata proposals.                                               |
| Create metadata constraints[^2] | Allow actor to create metadata constraints.                                                                        |
| Manage Platform Settings[^1]    | Allow actor to view and change platform-level settings, like integrations & notifications.                         |
| Manage Monitors[^1]             | Allow actor to create, update, and delete any data asset monitors, including Custom SQL monitors. Grant with care. |
| View Manage Tags                | Allow the actor to view the Manage Tags page.                                                                      |

#### Entity Management

| Platform Privileges | Description                                               |
| ------------------- | --------------------------------------------------------- |
| Manage Domains      | Allow actor to create and remove Asset Domains.           |
| Manage Glossaries   | Allow actor to create, edit, and remove Glossary Entities |
| Manage Tags         | Allow actor to create and remove Tags.                    |

#### System Management

| Platform Privileges                           | Description                                                                                              |
| --------------------------------------------- | -------------------------------------------------------------------------------------------------------- | --- |
| Restore Indices API[^3]                       | Allow actor to use the Restore Indices API.                                                              |     |
| Get Timeseries index sizes API[^3]            | Allow actor to use the get Timeseries indices size API.                                                  |
| Truncate timeseries aspect index size API[^3] | Allow actor to use the API to truncate a timeseries index.                                               |
| Get ES task status API[^3]                    | Allow actor to use the get task status API for an ElasticSearch task.                                    |
| Enable/Disable Writeability API[^3]           | Allow actor to enable or disable GMS writeability for data migrations.                                   |
| Apply Retention API[^3]                       | Allow actor to apply retention using the API.                                                            |
| Analytics API access[^3]                      | Allow actor to use API read access to raw analytics data.                                                |
| Explain ElasticSearch Query API[^3]           | Allow actor to use the Operations API explain endpoint.                                                  |
| Produce Platform Event API[^3]                | Allow actor to produce Platform Events using the API.                                                    |
| Manage System Operations                      | Allow actor to manage system operation controls. This setting includes all System Management privileges. |

### Common Metadata Privileges

These privileges are to view & modify any entity within DataHub.

#### Entity Privileges

| Entity Privileges                  | Description                                                                                                                                          |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| View Entity Page                   | Allow actor to view the entity page.                                                                                                                 |
| Edit Entity                        | Allow actor to edit any information about an entity. Super user privileges for the entity.                                                           |
| Delete                             | Allow actor to delete this entity.                                                                                                                   |
| Create Entity                      | Allow actor to create an entity if it doesn't exist.                                                                                                 |
| Entity Exists                      | Allow actor to determine whether the entity exists.                                                                                                  |
| Execute Entity                     | Allow actor to execute entity ingestion.                                                                                                             |
| Get Timeline API[^3]               | Allow actor to use the GET Timeline API.                                                                                                             |
| Get Entity + Relationships API[^3] | Allow actor to use the GET Entity and Relationships API.                                                                                             |
| Get Aspect/Entity Count APIs[^3]   | Allow actor to use the GET Aspect/Entity Count APIs.                                                                                                 |
| View Entity[^1]                    | Allow actor to view the entity in search results. This privilege can be explicitly granted, but is also implied by the `View Entity Page` privilege. |
| Share Entity[^1]                   | Allow actor to share an entity with another DataHub Cloud instance.                                                                                  |

#### Aspect Privileges

| Aspect Privileges             | Description                                                        |
| ----------------------------- | ------------------------------------------------------------------ |
| Edit Tags                     | Allow actor to add and remove tags to an asset.                    |
| Edit Glossary Terms           | Allow actor to add and remove glossary terms to an asset.          |
| Edit Description              | Allow actor to edit the description (documentation) of an entity.  |
| Edit Links                    | Allow actor to edit links associated with an entity.               |
| Edit Status                   | Allow actor to edit the status of an entity (soft deleted or not). |
| Edit Domain                   | Allow actor to edit the Domain of an entity.                       |
| Edit Data Product             | Allow actor to edit the Data Product of an entity.                 |
| Edit Deprecation              | Allow actor to edit the Deprecation status of an entity.           |
| Edit Incidents                | Allow actor to create and remove incidents for an entity.          |
| Edit Lineage                  | Allow actor to add and remove lineage edges for this entity.       |
| Edit Properties               | Allow actor to edit the properties for an entity.                  |
| Edit Owners                   | Allow actor to add and remove owners of an entity.                 |
| Get Timeseries Aspect API[^3] | Allow actor to use the GET Timeseries Aspect API.                  |

#### Proposals

| Proposals Privileges                              | Description                                                                                     |
| ------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Propose Tags[^1]                                  | Allow actor to propose adding a tag to an asset.                                                |
| Propose Glossary Terms[^1]                        | Allow actor to propose adding a glossary term to an asset.                                      |
| Propose Owners[^1]                                | Allow actor to propose adding an owner to an asset.                                             |
| Propose Domains[^1]                               | Allow actor to propose adding a domain to an asset.                                             |
| Propose Data Contract[^1]                         | Allow actor to propose adding a data contract to a dataset.                                     |
| Propose Structured properties[^1]                 | Allow actor to propose adding a structured property to an asset.                                |
| Propose Documentation[^1]                         | Allow actor to propose updates to an asset's documentation.                                     |
| Propose Dataset Column Glossary Terms[^1]         | Allow actor to propose a glossary term to a dataset schema column (field).                      |
| Propose Dataset Column Tags[^1]                   | Allow actor to propose a tag to a dataset schema column (field).                                |
| Propose Dataset Column Descriptions[^1]           | Allow actor to propose a updates to dataset's schema column (field) description                 |
| Propose Dataset Column Structured Properties[^1]  | Allow actor to propose a structured property to a dataset schema column (field).                |
| Propose Create Glossary Term[^1]                  | Allow actor to propose creation of a new glossary term.                                         |
| Propose Create Glossary Node[^1]                  | Allow actor to propose creation of a new glossary node.                                         |
| Manage Tag Proposals[^1]                          | Allow actor to manage a proposal to add a tag to an asset.                                      |
| Manage Glossary Term Proposals[^1]                | Allow actor to manage a proposal to add a glossary term to an asset.                            |
| Manage Domain Proposals[^1]                       | Allow actor to manage a proposal to add a domain to an asset.                                   |
| Manage Owner Proposals[^1]                        | Allow actor to manage a proposal to add an owner to an asset.                                   |
| Manage Property Proposals[^1]                     | Allow actor to manage a proposal to add a structured property to an asset.                      |
| Manage Data Contract Proposals[^1]                | Allow actor to manage a proposal to add a data contract to a dataset.                           |
| Manage Documentation Proposals[^1]                | Allow actor to manage updates to asset's documentation.                                         |
| Manage Dataset Column Tag Proposals[^1]           | Allow actor to manage a proposal to add a tag to dataset schema field (column).                 |
| Manage Dataset Column Glossary Term Proposals[^1] | Allow actor to manage a proposal to add a glossary term to dataset schema field (column).       |
| Manage Dataset Column Property Proposals[^1]      | Allow actor to manage a proposal to add a structured property to dataset schema field (column). |

### Specific Entity-level Privileges

These privileges are not generalizable.

#### Users & Groups

| Entity | Privilege                              | Description                                                                                      |
| ------ | -------------------------------------- | ------------------------------------------------------------------------------------------------ |
| Group  | Edit Group Members                     | Allow actor to add and remove members to a group.                                                |
| Group  | Manage Group Notification Settings[^1] | Allow actor to manage notification settings for a group.                                         |
| Group  | Manage Group Subscriptions[^1]         | Allow actor to manage subscriptions for a group.                                                 |
| Group  | Edit Contact Information               | Allow actor to change the contact information such as email & chat handles.                      |
| User   | Edit Contact Information               | Allow actor to change the contact information such as email & chat handles.                      |
| User   | Edit User Profile                      | Allow actor to change the user's profile including display name, bio, title, profile image, etc. |

#### Dataset

| Entity       | Privilege                                 | Description                                                                                                                                                                       |
| ------------ | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Dataset      | View Dataset Usage                        | Allow actor to access dataset usage information (includes usage statistics and queries).                                                                                          |
| Dataset      | View Dataset Profile                      | Allow actor to access dataset profile (snapshot statistics)                                                                                                                       |
| Dataset      | Edit Dataset Column Descriptions          | Allow actor to edit the column (field) descriptions associated with a dataset schema.                                                                                             |
| Dataset      | Edit Dataset Column Tags                  | Allow actor to edit the column (field) tags associated with a dataset schema.                                                                                                     |
| Dataset      | Edit Dataset Column Glossary Terms        | Allow actor to edit the column (field) glossary terms associated with a dataset schema.                                                                                           |
| Dataset      | Edit Dataset Column Properties            | Allow actor to edit the column (field) properties associated with a dataset schema.                                                                                               |
| Dataset      | Propose Dataset Column Glossary Terms[^1] | Allow actor to propose column (field) glossary terms associated with a dataset schema.                                                                                            |
| Dataset      | Propose Dataset Column Tags[^1]           | Allow actor to propose new column (field) tags associated with a dataset schema.                                                                                                  |
| Dataset      | Manage Dataset Column Glossary Terms[^1]  | Allow actor to manage column (field) glossary term proposals associated with a dataset schema.                                                                                    |
| Dataset      | Propose Dataset Column Descriptions[^1]   | Allow actor to propose new descriptions associated with a dataset schema.                                                                                                         |
| Dataset      | Manage Dataset Column Tag Proposals[^1]   | Allow actor to manage column (field) tag proposals associated with a dataset schema.                                                                                              |
| Dataset      | Edit Assertions                           | Allow actor to add and remove assertions from an entity.                                                                                                                          |
| Dataset      | Edit Dataset Queries                      | Allow actor to edit the Queries for a Dataset.                                                                                                                                    |
| Dataset      | View Dataset Operations                   | Allow actor to view operations on a Dataset.                                                                                                                                      |
| Dataset      | Create erModelRelationship                | Allow actor to add erModelRelationship on a dataset.                                                                                                                              |
| Dataset      | Edit Monitors[^1]                         | Allow actor to edit monitors for the entity.                                                                                                                                      |
| Dataset      | Edit SQL Assertion Monitors[^1]           | Allow actor to edit custom SQL assertion monitors for the entity. Note that this gives read query access to users with through the Custom SQL assertion builder. Grant with care. |
| Dataset      | Edit Data Contract[^1]                    | Allow actor to edit the Data Contract for an entity.                                                                                                                              |
| Dataset      | Manage Data Contract Proposals[^1]        | Allow actor to manage a proposal for a Data Contract                                                                                                                              |
| Tag          | Edit Tag Color                            | Allow actor to change the color of a Tag.                                                                                                                                         |
| Domain       | Manage Data Products                      | Allow actor to create, edit, and delete Data Products within a Domain                                                                                                             |
| GlossaryNode | Manage Direct Glossary Children           | Allow actor to create and delete the direct children of this entity.                                                                                                              |
| GlossaryNode | Manage All Glossary Children              | Allow actor to create and delete everything underneath this entity.                                                                                                               |

#### Misc

| Entity       | Privilege                       | Description                                                           |
| ------------ | ------------------------------- | --------------------------------------------------------------------- |
| Tag          | Edit Tag Color                  | Allow actor to change the color of a Tag.                             |
| Domain       | Manage Data Products            | Allow actor to create, edit, and delete Data Products within a Domain |
| GlossaryNode | Manage Direct Glossary Children | Allow actor to create and delete the direct children of this entity.  |
| GlossaryNode | Manage All Glossary Children    | Allow actor to create and delete everything underneath this entity.   |

## Coming Soon

### Experimental

Support for Policy Constraints based on entity sub-resources (tags, glossary terms, domains, containers, etc.) is currently in development and in an experimental phase.

Currently the only supported sub-resources are tags. These are supported through an additional parameter in DataHubPolicyInfo which is currently only modifiable via API, there is no UI option to configure it. Specifically the
option is `privilegeConstraints` which takes a `PolicyMatchFilter` within the existing `DataHubResourceFilter` for a policy. This works similarly to the existing resource filter, but instead of applying to the main entity being acted on
it applies to the subResource targeted in the action. For example, if the policy specifies it is constrained to tags that equal `urn:li:tag:tag1` or `urn:li:tag:tag2` for `EDIT_DATASET_TAGS` privilege, then assuming no other policies match,
a user would only be able to apply those tags to the dataset. This is also supported with the `NOT_EQUALS` condition for preventing certain tags from being added/removed. These policies apply by default in the UI and can be configured to apply
to API operations as well through the `MCP_VALIDATION_PRIVILEGE_CONSTRAINTS` environment variable which should be applied globally (GMS, MCE Consumer, and DataHub Upgrade specifically), which is enabled by default.

Example JSON of a policy with constraints:

```json
{
  "actors": {
    "resourceOwners": false,
    "groups": [],
    "allGroups": false,
    "allUsers": false,
    "users": ["urn:li:corpuser:ryan@email.com"]
  },
  "lastUpdatedTimestamp": 0,
  "privileges": ["EDIT_ENTITY_TAGS", "EDIT_DATASET_COL_TAGS"],
  "editable": true,
  "displayName": "Ryan Policy",
  "resources": {
    "filter": { "criteria": [] },
    "allResources": false,
    "privilegeConstraints": {
      "criteria": [
        {
          "field": "URN",
          "condition": "EQUALS",
          "values": ["urn:li:tag:PII", "urn:li:tag:Business Critical"]
        }
      ]
    }
  },
  "description": "",
  "state": "ACTIVE",
  "type": "METADATA"
}
```

```graphql
mutation {
  createPolicy(
    input: {
      type: METADATA
      name: "my-policy"
      state: ACTIVE
      description: "My policy"
      privileges: ["EDIT_ENTITY_TAGS"]
      actors: {
        allUsers: true
        users: []
        groups: []
        resourceOwners: true
        allGroups: true
      }
      resources: {
        allResources: true
        resources: []
        filter: { criteria: [] }
        policyConstraints: {
          criteria: [
            {
              field: "URN"
              values: ["urn:li:tag:PII", "urn:li:tag:Business Critical"]
              condition: EQUALS
            }
          ]
        }
      }
    }
  )
}
```

## Feedback / Questions / Concerns

We want to hear from you! For any inquiries, including Feedback, Questions, or Concerns, reach out on Slack!

[^3]: Only active if REST_API_AUTHORIZATION_ENABLED is true

[^1]: DataHub Cloud only

[^2]: Deprecated feature
