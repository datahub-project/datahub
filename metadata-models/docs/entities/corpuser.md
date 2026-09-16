# CorpUser

CorpUser represents an individual user (or account) in the enterprise. These entities serve as the identity layer within DataHub, representing people who interact with data assets, own resources, belong to groups, and have roles and permissions within the organization. CorpUsers can represent LDAP users, Active Directory accounts, SSO identities, or native DataHub users.

## Identity

CorpUsers are uniquely identified by a single piece of information:

- **Username**: A unique identifier for the user within the organization. This is typically sourced from the corporate identity provider (LDAP, Active Directory, etc.) or can be an email address for native DataHub users.

The URN structure for CorpUser is:

```
urn:li:corpuser:<username>
```

### Examples

```
urn:li:corpuser:jdoe
urn:li:corpuser:john.doe@company.com
urn:li:corpuser:jdoe@company.com
```

The username is stored in the `corpUserKey` aspect, which is the identity aspect for this entity. The username field is marked as searchable and enables autocomplete functionality in the DataHub UI.

### Username Conventions

The username can follow various conventions depending on your organization's identity provider:

- **LDAP/Active Directory usernames**: `jdoe`, `john.doe`, `john_doe`
- **Email addresses**: `jdoe@company.com`, `john.doe@company.com`
- **SSO identities**: Depends on your SSO provider's username format
- **Native DataHub users**: Typically email addresses

It's important to maintain consistency in username formats across your DataHub deployment to ensure proper identity resolution and relationship tracking.

## Important Capabilities

### Profile Information

The core profile information about a user is stored in the `corpUserInfo` aspect. This is typically populated automatically by ingestion connectors from identity providers like LDAP, Active Directory, Azure AD, Okta, or other SSO systems.

**Key Fields:**

- **displayName**: The user's name as it should appear in the UI
- **email**: Email address for contacting the user
- **title**: Job title (e.g., "Senior Data Engineer")
- **firstName** and **lastName**: Components of the user's name
- **fullName**: Full name typically formatted as "firstName lastName"
- **managerUrn**: URN reference to the user's direct manager (another CorpUser)
- **departmentId** and **departmentName**: Organizational department information
- **countryCode**: Two-letter country code (e.g., "US", "UK")
- **active**: Whether the user account is active (deprecated in favor of `corpUserStatus`)
- **system**: Whether this is a system/service account rather than a human user

The `managerUrn` field creates a relationship between users, enabling organizational hierarchy visualization in DataHub.

### Editable User Information

The `corpUserEditableInfo` aspect contains information that users can modify through the DataHub UI, allowing users to enrich their profiles beyond what's provided by the identity provider.

**Key Fields:**

- **aboutMe**: A personal description or bio
- **displayName**: User-specified display name (overrides the one from corpUserInfo)
- **title**: User-specified title (overrides the one from corpUserInfo)
- **teams**: List of team names the user belongs to (e.g., ["Data Platform", "Analytics"])
- **skills**: List of skills the user possesses (e.g., ["Python", "SQL", "Machine Learning"])
- **pictureLink**: URL to a profile picture
- **slack**: Slack handle for communication
- **phone**: Contact phone number
- **email**: Contact email (can differ from the system email)
- **platforms**: URNs of data platforms the user commonly works with
- **persona**: URN of the user's DataHub persona (role-based persona like "Data Analyst")

### User Status

The `corpUserStatus` aspect tracks the current status of the user account, replacing the deprecated `active` field in `corpUserInfo`.

**Key Fields:**

- **status**: Current status of the user (e.g., "ACTIVE", "SUSPENDED", "PROVISIONED")
- **lastModified**: Audit stamp with information about who last modified the status and when

This aspect provides more granular control over user account states compared to the simple boolean `active` field.

### Group Membership

Users can be members of groups through two different aspects:

**groupMembership**: Represents membership in CorpGroups that may be managed within DataHub or synchronized from external systems. This creates `IsMemberOfGroup` relationships.

**nativeGroupMembership**: Represents membership in groups that are native to an external identity provider (like Active Directory groups). This creates `IsMemberOfNativeGroup` relationships.

Both aspects store arrays of group URNs, allowing users to belong to multiple groups simultaneously.

### Role Membership

The `roleMembership` aspect associates users with DataHub roles, which define their permissions and access within the platform.

**Key Fields:**

- **roles**: Array of DataHubRole URNs that the user is assigned to

This creates `IsMemberOfRole` relationships and is fundamental to DataHub's role-based access control (RBAC) system.

### Authentication Credentials

The `corpUserCredentials` aspect stores authentication information for native DataHub users (users created directly in DataHub rather than synchronized from an external identity provider).

**Key Fields:**

- **salt**: Salt used for password hashing
- **hashedPassword**: The hashed password
- **passwordResetToken**: Optional token for password reset operations
- **passwordResetTokenExpirationTimeMillis**: When the reset token expires

This aspect is only used for native authentication and is not populated for users authenticated through SSO or LDAP.

### User Settings

The `corpUserSettings` aspect stores user-specific preferences for the DataHub UI and features.

**Key Fields:**

- **appearance**: Settings controlling the look and feel of the DataHub UI
  - `showSimplifiedHomepage`: Whether to show a simplified homepage with only datasets, charts, and dashboards
  - `showThemeV2`: Whether to use the V2 theme
- **views**: Settings for the Views feature
  - `defaultView`: The user's default DataHub view
- **notificationSettings**: Preferences for notifications
- **homePage**: Settings for the home page experience
  - `pageTemplate`: The user's default page template
  - `dismissedAnnouncementUrns`: List of announcements the user has dismissed

### Origin

The `origin` aspect tracks where the user entity originated from, distinguishing between native DataHub users and those synchronized from external systems.

**Key Fields:**

- **type**: Either "NATIVE" or "EXTERNAL"
- **externalType**: Name of the external identity provider (e.g., "AzureAD", "Okta", "LDAP")

This information is useful for understanding the source of truth for user data and managing synchronization processes.

### Slack Integration

The `slackUserInfo` aspect contains detailed information about a user's Slack identity, enabling rich Slack integration features within DataHub.

**Key Fields:**

- **slackInstance**: URN of the Slack workspace
- **id**: Unique Slack member ID
- **name**: Slack username
- **realName**: Real name in Slack
- **displayName**: Display name in Slack
- **email**: Email associated with the Slack account
- **teamId**: Slack team/workspace ID
- **isDeleted**, **isAdmin**, **isOwner**, **isPrimaryOwner**, **isBot**: Account status flags
- **timezone** and **timezoneOffset**: User's timezone information
- **title**: Job title from Slack
- **phone**: Phone number from Slack
- **profilePictureUrl**: URL to Slack profile picture
- **statusText** and **statusEmoji**: Current Slack status

### Tags, Structured Properties, and Forms

Like other DataHub entities, CorpUsers support:

- **globalTags**: Tags attached to the user entity for categorization
- **structuredProperties**: Custom properties defined by your organization's data model
- **forms**: Forms that can be attached to users for collecting structured information
- **status**: Generic status aspect for soft-deletion

These common aspects enable flexible metadata management and integration with DataHub's broader metadata framework.

## Code Examples

### Creating a CorpUser

The simplest way to create a CorpUser is using the high-level Python SDK:

<details>
<summary>Python SDK: Create a basic user</summary>

```python
{{ inline /metadata-ingestion/examples/library/corpuser_create_basic.py show_path_as_comment }}
```

</details>

### Creating a CorpUser with Group Memberships

Users are often members of groups. Here's how to create a user and assign them to groups:

<details>
<summary>Python SDK: Create user with group memberships</summary>

```python
{{ inline /metadata-ingestion/examples/library/corpuser_create_with_groups.py show_path_as_comment }}
```

</details>

### Updating User Profile Information

To update editable profile information for an existing user:

<details>
<summary>Python SDK: Update user profile</summary>

```python
{{ inline /metadata-ingestion/examples/library/corpuser_update_profile.py show_path_as_comment }}
```

</details>

### Adding Tags to a User

Users can be tagged for categorization and discovery:

<details>
<summary>Python SDK: Add tags to a user</summary>

```python
{{ inline /metadata-ingestion/examples/library/corpuser_add_tag.py show_path_as_comment }}
```

</details>

### Querying Users via REST API

You can fetch user information using the REST API:

<details>
<summary>REST API: Get user information</summary>

```bash
# Get a user by URN
curl -X GET "http://localhost:8080/entities/urn%3Ali%3Acorpuser%3Ajdoe" \
  -H "Authorization: Bearer <your-access-token>"

# Get specific aspects of a user
curl -X GET "http://localhost:8080/aspects/urn%3Ali%3Acorpuser%3Ajdoe?aspect=corpUserInfo&aspect=corpUserEditableInfo&aspect=groupMembership" \
  -H "Authorization: Bearer <your-access-token>"
```

</details>

### Searching for Users

You can search for users using the GraphQL API or search API:

<details>
<summary>GraphQL: Search for users</summary>

```graphql
query searchUsers {
  search(input: { type: CORP_USER, query: "john", start: 0, count: 10 }) {
    start
    count
    total
    searchResults {
      entity {
        ... on CorpUser {
          urn
          username
          properties {
            displayName
            email
            title
            fullName
          }
          editableProperties {
            aboutMe
            teams
            skills
            slack
          }
        }
      }
    }
  }
}
```

</details>

## Integration Points

### Relationships with Other Entities

CorpUsers have several important relationships with other DataHub entities:

**Ownership Relationships:**

- CorpUsers can be owners of datasets, dashboards, charts, data flows, and virtually any other entity in DataHub
- The ownership relationship includes the owner type (e.g., DATAOWNER, TECHNICAL_OWNER, BUSINESS_OWNER)

**Group Relationships:**

- Users belong to CorpGroups through `IsMemberOfGroup` relationships
- Groups can also be owners of assets, providing inherited ownership

**Role Relationships:**

- Users are assigned to DataHub roles through `IsMemberOfRole` relationships
- Roles define permissions and access levels within DataHub

**Organizational Hierarchy:**

- The `managerUrn` field in `corpUserInfo` creates `ReportsTo` relationships
- This enables visualization of organizational structure and reporting chains

**Platform Usage:**

- The `platforms` field in `corpUserEditableInfo` creates `IsUserOf` relationships
- This helps identify which platforms users commonly work with

**Persona Assignment:**

- Users can be assigned to DataHub personas through the `persona` field
- This helps categorize users by their role and customize their experience

### Identity Provider Integration

CorpUsers are typically synchronized from external identity providers:

**LDAP/Active Directory:**

- Most organizations use LDAP connectors to automatically synchronize user information
- The username typically corresponds to the LDAP `uid` or `sAMAccountName`
- Profile information is populated from LDAP attributes

**SSO Providers (Okta, Azure AD, etc.):**

- SSO integrations can provision users automatically on first login
- User attributes from the SSO provider populate the `corpUserInfo` aspect
- The `origin` aspect tracks the SSO provider as the source

**Native DataHub Users:**

- Users can be created directly in DataHub for testing or small deployments
- These users have credentials stored in the `corpUserCredentials` aspect
- They are marked with `origin.type = NATIVE`

### Authentication and Authorization

CorpUsers are central to DataHub's security model:

**Authentication:**

- Native users authenticate with username/password
- SSO users authenticate through their identity provider
- API tokens can be associated with users for programmatic access

**Authorization (RBAC):**

- Users are assigned to roles through the `roleMembership` aspect
- Roles define what actions users can perform
- Policies can reference users or groups to grant/restrict access

**Metadata Access:**

- Users can only see metadata they have permission to view
- Ownership and group membership influence what users can edit
- Policies can be user-specific or group-based

## Notable Exceptions

### System Users

CorpUsers can represent both human users and system/service accounts. The `system` field in `corpUserInfo` distinguishes between these:

- **Human Users** (`system: false`): Actual people who interact with DataHub
- **System Accounts** (`system: true`): Service accounts, automated processes, or system-level operations

System users should be marked appropriately to distinguish them in reports, ownership lists, and access reviews.

### Deprecated Active Field

The `active` field in `corpUserInfo` is deprecated. Use the `corpUserStatus` aspect instead, which provides:

- More granular status options beyond just active/inactive
- Audit information about status changes
- Better support for provisioning workflows

When working with users, prefer checking `corpUserStatus.status` over `corpUserInfo.active`.

### Username Immutability

The username (in `corpUserKey`) is immutable once a user is created. If a user's username changes in the source system:

- A new CorpUser entity must be created with the new username
- Ownership and other relationships need to be migrated to the new entity
- The old user can be soft-deleted using the `status` aspect

Plan your username strategy carefully to avoid frequent username changes.

### Display Name Precedence

Display names can appear in multiple aspects with this precedence:

1. `corpUserEditableInfo.displayName` (user-specified, highest priority)
2. `corpUserInfo.displayName` (from identity provider)
3. `corpUserInfo.fullName` (fallback if no display name is set)

The DataHub UI resolves these in order, showing the most specific value available.
