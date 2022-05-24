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

1. **Actors**: The 'who'. Specific users, groups that the policy applies to.
2. **Privileges**: The 'what'. What actions are being permitted by a policy, e.g. "Add Tags".
3. **Resources**: The 'which'. Resources that the policy applies to, e.g. "All Datasets".

#### Actors

We currently support 3 ways to define the set of actors the policy applies to: a) list of users b) list of groups, and
c) owners of the entity. You also have the option to apply the policy to all users.

#### Privileges

Check out the list of
privileges [here](https://github.com/datahub-project/datahub/blob/master/metadata-utils/src/main/java/com/linkedin/metadata/authorization/PoliciesConfig.java)
. Note, the privileges are semantic by nature, and does not tie in 1-to-1 with the aspect model.

All edits on the UI are covered by a privilege, to make sure we have the ability to restrict write access.

We currently support the following:

**Platform-level** privileges for DataHub operators to access & manage the administrative functionality of the system.

| Platform Privileges             | Description                                                                                                                    |
|---------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Manage Policies                 | Allow actor to create and remove access control policies. Be careful - Actors with this privilege are effectively super users. |
| Manage Metadata Ingestion       | Allow actor to create, remove, and update Metadata Ingestion sources.                                                          |
| Manage Secrets                  | Allow actor to create & remove secrets stored inside DataHub.                                                                  |
| Manage Users & Groups           | Allow actor to create, remove, and update users and groups on DataHub.                                                         |
| Manage All Access Tokens        | Allow actor to create, remove, and list access tokens for all users on DataHub.                                                |
| Manage Domains                  | Allow actor to create and remove Asset Domains.                                                                                |
| View Analytics                  | Allow the actor access to the DataHub analytics dashboard.                                                                     |
| Generate Personal Access Tokens | Allow the actor to generate access tokens for personal use with DataHub APIs.                                                  |

**Common metadata privileges** to view & modify any entity within DataHub.

| Common Privileges   | Description                                                                                                                      |
|---------------------|----------------------------------------------------------------------------------------------------------------------------------|
| View Entity Page    | Allow actor to access the entity page for the resource in the UI. If not granted, it will redirect them to an unauthorized page. |
| Edit Tags           | Allow actor to add and remove tags to an asset.                                                                                  |
| Edit Glossary Terms | Allow actor to add and remove glossary terms to an asset.                                                                        |
| Edit Owners         | Allow actor to add and remove owners of an entity.                                                                               |
| Edit Description    | Allow actor to edit the description (documentation) of an entity.                                                                |
| Edit Links          | Allow actor to edit links associated with an entity.                                                                             |
| Edit Status         | Allow actor to edit the status of an entity (soft deleted or not).                                                               |
| Edit Domain         | Allow actor to edit the Domain of an entity.                                                                                     |
| Edit Deprecation    | Allow actor to edit the Deprecation status of an entity.                                                                         |
| Edit Assertions     | Allow actor to add and remove assertions from an entity.                                                                         |
| Edit All            | Allow actor to edit any information about an entity. Super user privileges.                                                      |

**Specific entity-level privileges** that are not generalizable.

| Entity       | Privilege                          | Description                                                                                                                                                                |
|--------------|------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Dataset      | Edit Dataset Column Tags           | Allow actor to edit the column (field) tags associated with a dataset schema.                                                                                              |
| Dataset      | Edit Dataset Column Glossary Terms | Allow actor to edit the column (field) glossary terms associated with a dataset schema.                                                                                    |
| Dataset      | Edit Dataset Column Descriptions   | Allow actor to edit the column (field) descriptions associated with a dataset schema.                                                                                      |
| Dataset      | View Dataset Usage                 | Allow actor to access usage metadata about a dataset both in the UI and in the GraphQL API. This includes example queries, number of queries, etc.                         |
| Dataset      | View Dataset Profile               | Allow actor to access a dataset's profile both in the UI and in the GraphQL API. This   includes snapshot statistics like #rows, #columns, null percentage per field, etc. |
| Tag          | Edit Tag Color                     | Allow actor to change the color of a Tag.                                                                                                                                  |
| Group        | Edit Group Members                 | Allow actor to add and remove members to a group.                                                                                                                          |
| User         | Edit User Profile                  | Allow actor to change the user's profile including display name, bio, title, profile image, etc.                                                                           |
| User + Group | Edit Contact Information           | Allow actor to change the contact information such as email & chat handles.                                                                                                |


#### Resources

Resource filter defines the set of resources that the policy applies to is defined using a list of criteria. Each
criterion defines a field type (like resource_type, resource_urn, domain), a list of field values to compare, and a
condition (like EQUALS). It essentially checks whether the field of a certain resource matches any of the input values.
Note, that if there are no criteria or resource is not set, policy is applied to ALL resources.

For example, the following resource filter will apply the policy to datasets, charts, and dashboards under domain 1.

```json
{
  "resource": {
    "criteria": [
      {
        "field": "resource_type",
        "values": [
          "dataset",
          "chart",
          "dashboard"
        ],
        "condition": "EQUALS"
      },
      {
        "field": "domain",
        "values": [
          "urn:li:domain:domain1"
        ],
        "condition": "EQUALS"
      }
    ]
  }
}
```

Supported fields are as follows

| Field Type    | Description            | Example                 |
|---------------|------------------------|-------------------------|
| resource_type | Type of the resource   | dataset, chart, dataJob |
| resource_urn  | Urn of the resource    | urn:li:dataset:...      |
| domain        | Domain of the resource | urn:li:domain:domainX   |

## Managing Policies

Policies can be managed under the `/policies` page, or accessed via the top navigation bar. The `Policies` tab will only 
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

> Pro-Tip: To login using the `datahub` account, simply navigate to `<your-datahub-domain>/login` and enter `datahub`, `datahub`. Note that the password can be customized for your
deployment by changing the `user.props` file within the `datahub-frontend` module. Notice that JaaS authentication must be enabled. 

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

## Coming Soon

The DataHub team is hard at work trying to improve the Policies feature. We are planning on building out the following:

- Hide edit action buttons on Entity pages to reflect user privileges

Under consideration

- Ability to define Metadata Policies against multiple reosurces scoped to particular "Containers" (e.g. A "schema", "database", or "collection")

## Feedback / Questions / Concerns

We want to hear from you! For any inquiries, including Feedback, Questions, or Concerns, reach out on Slack!
