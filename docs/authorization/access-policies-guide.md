# About DataHub Access Policies

<FeatureAvailability/>

Access Policies define who can do what to which resources. In conjunction with [Roles](./roles.md), Access Policies determine what users are allowed to do on DataHub.

## Policy Types

There are 2 types of Access Policy within DataHub:

1. **Platform** Policies
2. **Metadata** Policies

<p align="center">
  <img width="20%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/policies-select-policy-type.png"/>
</p>

**Platform** Policies determine who has platform-level Privileges on DataHub. These include:

- Managing Users & Groups
- Viewing the DataHub Analytics Page
- Managing Policies themselves

Platform policies can be broken down into 2 parts:

1. **Privileges**: Which privileges should be assigned to the Actors (e.g. "View Analytics")
2. **Actors**: Who the should be granted the privileges (Users, or Groups)

A few Platform Policies in plain English include:

- The Data Platform team should be allowed to manage users & groups, view platform analytics, & manage policies themselves
- John from IT should be able to invite new users

**Metadata** policies determine who can do what to which Metadata Entities. For example:

- Who can edit Dataset Documentation & Links?
- Who can add Owners to a Chart?
- Who can add Tags to a Dashboard?

Metadata policies can be broken down into 3 parts:

1. **Privileges**: The 'what'. What actions are being permitted by a Policy, e.g. "Add Tags".
2. **Resources**: The 'which'. Resources that the Policy applies to, e.g. "All Datasets".
3. **Actors**: The 'who'. Specific users, groups, & roles that the Policy applies to.

A few **Metadata** Policies in plain English include: 

- Dataset Owners should be allowed to edit documentation, but not Tags.
- Jenny, our Data Steward, should be allowed to edit Tags for any Dashboard, but no other metadata.
- James, a Data Analyst, should be allowed to edit the Links for a specific Data Pipeline he is a downstream consumer of.

Each of these can be implemented by constructing DataHub Access Policies.

## Access Policies Setup, Prerequisites, and Permissions

What you need to manage Access Policies on DataHub:

* **Manage Policies** Privilege

This Platform Privilege allows users to create, edit, and remove all Access Policies on DataHub. Therefore, it should only be
given to those users who will be serving as Admins of the platform. The default `Admin` role has this Privilege.


## Using Access Policies

Policies can be created by first navigating to **Settings > Permissions > Policies**.

To begin building a new Policy, click **Create new Policy**.

<p align="center">
  <img width="40%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/manage-permissions.png"/>
</p>

### Creating a Platform Policy

#### Step 1. Provide a Name & Description

In the first step, we select the **Platform** Policy type, and define a name and description for the new Policy. 

Good Policy names describe the high-level purpose of the Policy. For example, a Policy named
"View DataHub Analytics - Data Governance Team" would be a great way to describe a Platform
Policy which grants abilities to view DataHub's Analytics view to anyone on the Data Governance team. 

You can optionally provide a text description to add richer details about the purpose of the Policy.

#### Step 2: Configure Privileges

In the second step, we can simply select the Privileges that this Platform Policy will grant.

<p align="center">
  <img width="40%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/policies-select-platform-privileges.png"/>
</p>

**Platform** Privileges most often provide access to perform administrative functions on the Platform. These include:

| Platform Privileges             | Description                                                                                                                    |
|---------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Manage Policies                 | Allow actor to create and remove access control policies. Be careful - Actors with this Privilege are effectively super users. |
| Manage Metadata Ingestion       | Allow actor to create, remove, and update Metadata Ingestion sources.                                                          |
| Manage Secrets                  | Allow actor to create & remove secrets stored inside DataHub.                                                                  |
| Manage Users & Groups           | Allow actor to create, remove, and update users and groups on DataHub.                                                          |
| Manage All Access Tokens        | Allow actor to create, remove, and list access tokens for all users on DataHub.                                                |
| Create Domains                  | Allow the actor to create new Domains                                                                                          |
| Manage Domains                  | Allow actor to create and remove any Domains.                                                                                |
| View Analytics                  | Allow the actor access to the DataHub analytics dashboard.                                                                      |
| Generate Personal Access Tokens | Allow the actor to generate access tokens for personal use with DataHub APIs.                                                  |
| Manage User Credentials         | Allow the actor to generate invite links for new native DataHub users, and password reset links for existing native users.   |
| Manage Glossaries               | Allow the actor to create, edit, move, and delete Glossary Terms and Term Groups                                               |
| Create Tags                     | Allow the actor to create new Tags                                                                                             |
| Manage Tags                     | Allow the actor to create and remove any Tags                                                                                  |


#### Step 3: Choose Policy Actors

In this step, we can select the actors who should be granted Privileges appearing on this Policy.

To do so, simply search and select the Users or Groups that the Policy should apply to.

<p align="center">
  <img width="40%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/policies-select-users.png"/>
</p>

**Assigning a Policy to a User**

<p align="center">
  <img width="40%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/policies-select-groups.png"/>
</p>

**Assigning a Policy to a Group**


### Creating a Metadata Policy

#### Step 1. Provide a Name & Description

In the first step, we select the **Metadata** Policy, and define a name and description for the new Policy.

Good Policy names describe the high-level purpose of the Policy. For example, a Policy named
"Full Dataset Edit Privileges - Data Platform Engineering" would be a great way to describe a Metadata
Policy which grants all abilities to edit Dataset Metadata to anyone in the "Data Platform" group.

You can optionally provide a text description to add richer detail about the purpose of the Policy.

#### Step 2: Configure Privileges

In the second step, we can simply select the Privileges that this Metadata Policy will grant.
To begin, we should first determine which assets that the Privileges should be granted for (i.e. the *scope*), then
select the appropriate Privileges to grant.

Using the `Resource Type` selector, we can narrow down the *type* of the assets that the Policy applies to. If left blank,
all entity types will be in scope.

For example, if we only want to grant access for `Datasets` on DataHub, we can select
`Datasets`.

<p align="center">
  <img width="40%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/policies-select-resource-type.png"/>
</p>

Next, we can search for specific Entities of the that the Policy should grant privileges on. 
If left blank, all entities of the selected types are in scope.

For example, if we only want to grant access for a specific sample dataset, we can search and
select it directly.

<p align="center">
  <img width="40%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/policies-select-resource-urn.png"/>
</p>

We can also limit the scope of the Policy to assets that live in a specific **Domain**. If left blank,
entities from all Domains will be in scope.

For example, if we only want to grant access for assets part of a "Marketing" Domain, we can search and
select it.

<p align="center">
  <img width="40%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/policies-select-resource-domain.png"/>
</p>

Finally, we will choose the Privileges to grant when the selected entities fall into the defined
scope.

<p align="center">
  <img width="40%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/policies-select-metadata-privileges.png"/>
</p>

**Metadata** Privileges grant access to change specific *entities* (i.e. data assets) on DataHub.

The common Metadata Privileges, which span across entity types, include:

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

**Specific Metadata Privileges** include

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

> **Still have questions about Privileges?** Let us know in [Slack](https://slack.datahubproject.io)!


#### Step 3: Choose Policy Actors

In this step, we can select the actors who should be granted the Privileges on this Policy. Metadata Policies
can target specific Users & Groups, or the *owners* of the Entities that are included in the scope of the Policy.

To do so, simply search and select the Users or Groups that the Policy should apply to.

<p align="center">
  <img width="40%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/policies-select-users.png"/>
</p>

<p align="center">
  <img width="40%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/policies-select-groups.png"/>
</p>

We can also grant the Privileges to the *owners* of Entities (or *Resources*) that are in scope for the Policy. 
This advanced functionality allows of Admins of DataHub to closely control which actions can or cannot be performed by owners.

<p align="center">
  <img width="40%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/policies-select-owners.png"/>
</p>

### Updating an Existing Policy

To update an existing Policy, simply click the **Edit** on the Policy you wish to change.

<p align="center">
  <img width="40%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/edit-policy.png"/>
</p>

Then, make the changes required and click **Save**. When you save a Policy, it may take up to 2 minutes for changes
to be reflected.


### Removing a Policy

To remove a Policy, simply click on the trashcan icon located on the Policies list. This will remove the Policy and
deactivate it so that it no longer applies.

When you delete a Policy, it may take up to 2 minutes for changes to be reflected.


### Deactivating a Policy

In addition to deletion, DataHub also supports "deactivating" a Policy. This is useful if you need to temporarily disable
a particular Policy, but do not want to remove it altogether.

To deactivate a Policy, simply click the **Deactivate** button on the Policy you wish to deactivate. When you change
the state of a Policy, it may take up to 2 minutes for the changes to be reflected.

<p align="center">
  <img width="40%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/deactivate-policy.png"/>
</p>

After deactivating, you can re-enable a Policy by clicking **Activate**.


### Default Policies

Out of the box, DataHub is deployed with a set of pre-baked Policies. This set of policies serves the
following purposes:

1. Assigns immutable super-user privileges for the root `datahub` user account (Immutable)
2. Assigns all Platform Privileges for all Users by default (Editable)

The reason for #1 is to prevent people from accidentally deleting all policies and getting locked out (`datahub` super user account can be a backup)
The reason for #2 is to permit administrators to log in via OIDC or another means outside of the `datahub` root account
when they are bootstrapping with DataHub. This way, those setting up DataHub can start managing Access Policies without friction.
Note that these Privileges *can* and likely *should* be changed inside the **Policies** page before onboarding
your company's users.


## Additional Resources

- [Authorization Overview](./README.md)
- [Roles Overview](./roles.md)
- [Authorization using Groups](./groups.md)


### Videos

- [Introducing DataHub Access Policies](https://youtu.be/19zQCznqhMI?t=282)

### GraphQL

* [listPolicies](../../graphql/queries.md#listPolicies)
* [createPolicy](../../graphql/mutations.md#createPolicy)
* [updatePolicy](../../graphql/mutations.md#updatePolicy)
* [deletePolicy](../../graphql/mutations.md#deletePolicy)

## FAQ and Troubleshooting

**How do Policies relate to Roles?**

Policies are the lowest level primitive for granting Privileges to users on DataHub. 

Roles are built for convenience on top of Policies. Roles grant Privileges to actors indirectly, driven by Policies
behind the scenes. Both can be used in conjunction to grant Privileges to end users. 

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*

### Related Features

- [Roles](./roles.md)