# Authorization using Groups

## Introduction

DataHub provides the ability to use **Groups** to manage policies.

## Why do we need groups for authorization?

### Easily Applying Access Privileges

Groups are useful for managing user privileges in DataHub. If you want a set of Admin users,
or you want to define a set of users that are only able to view metadata assets but not make changes to them, you could
create groups for each of these use cases and apply the appropriate policies at the group-level rather than the
user-level.

### Syncing with Existing Enterprise Groups (via IdP)

If you work with an Identity Provider like Okta or Azure AD, it's likely you already have groups defined there. DataHub
allows you to import the groups you have from OIDC for [Okta](../generated/ingestion/sources/okta.md) and
[Azure AD](../generated/ingestion/sources/azure-ad.md) using the DataHub ingestion framework.

If you routinely ingest groups from these providers, you will also be able to keep groups synced. New groups will
be created in DataHub, stale groups will be deleted, and group membership will be updated!

## Default Groups

DataHub provides some pre-defined groups for you to get started, along with some pre-configured policies. You can find
the full list of policies corresponding to each group at the bottom of this
[file](https://github.com/datahub-project/datahub/blob/master/metadata-service/war/src/main/resources/boot/policies.json).

| Group Name         | Description                                                                             |
|--------------------|-----------------------------------------------------------------------------------------|
| Organization Admin | Top level DataHub administrators. They have ALL privileges.                             |
| Access Admin       | Administrators for managing users, groups, and security-related features.               |
| Metadata Stewards  | Overseers of all metadata assets across DataHub.                                        |
| Metadata Readers   | Users in this group have the ability to read metadata, but cannot change it by default. |


## Custom Groups

DataHub admins can create custom groups by going to the **Settings > Users & Groups > Groups > Create Group**. 
Members can be added to Groups via the Group profile page.

## Feedback / Questions / Concerns

We want to hear from you! For any inquiries, including Feedback, Questions, or Concerns, reach out on Slack!
