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


**Python SDK: Create a basic user**

```python
# Inlined from /metadata-ingestion/examples/library/corpuser_create_basic.py
# metadata-ingestion/examples/library/corpuser_create_basic.py
import logging
import os

from datahub.api.entities.corpuser.corpuser import CorpUser
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create a basic user with essential information
user = CorpUser(
    id="jdoe",
    display_name="John Doe",
    email="jdoe@company.com",
    title="Senior Data Engineer",
    first_name="John",
    last_name="Doe",
    full_name="John Doe",
    department_name="Data Engineering",
    country_code="US",
)

# Create graph client
gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
datahub_graph = DataHubGraph(DataHubGraphConfig(server=gms_server, token=token))

# Emit the user entity
for event in user.generate_mcp():
    datahub_graph.emit(event)

log.info(f"Created user {user.urn}")

```



### Creating a CorpUser with Group Memberships

Users are often members of groups. Here's how to create a user and assign them to groups:


**Python SDK: Create user with group memberships**

```python
# Inlined from /metadata-ingestion/examples/library/corpuser_create_with_groups.py
# metadata-ingestion/examples/library/corpuser_create_with_groups.py
import logging

from datahub.api.entities.corpuser.corpuser import CorpUser
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create a user with group memberships
user = CorpUser(
    id="jsmith",
    display_name="Jane Smith",
    email="jsmith@company.com",
    title="Data Analyst",
    first_name="Jane",
    last_name="Smith",
    full_name="Jane Smith",
    department_name="Analytics",
    country_code="US",
    groups=["data-engineering", "analytics-team"],
)

# Create graph client
datahub_graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Emit the user entity with group memberships
for event in user.generate_mcp():
    datahub_graph.emit(event)

log.info(f"Created user {user.urn} with group memberships")

```



### Updating User Profile Information

To update editable profile information for an existing user:


**Python SDK: Update user profile**

```python
# Inlined from /metadata-ingestion/examples/library/corpuser_update_profile.py
# metadata-ingestion/examples/library/corpuser_update_profile.py
import logging

from datahub.api.entities.corpuser.corpuser import CorpUser, CorpUserGenerationConfig
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Update a user's editable profile information
user = CorpUser(
    id="jdoe",
    email="jdoe@company.com",
    description="Passionate about data quality and building reliable data pipelines. "
    "10+ years of experience in data engineering.",
    slack="@jdoe",
    phone="+1-555-0123",
    picture_link="https://company.com/photos/jdoe.jpg",
)

# Create graph client
datahub_graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Emit with override_editable=True to update editable fields
for event in user.generate_mcp(
    generation_config=CorpUserGenerationConfig(override_editable=True)
):
    datahub_graph.emit(event)

log.info(f"Updated profile for user {user.urn}")

```



### Adding Tags to a User

Users can be tagged for categorization and discovery:


**Python SDK: Add tags to a user**

```python
# Inlined from /metadata-ingestion/examples/library/corpuser_add_tag.py
# metadata-ingestion/examples/library/corpuser_add_tag.py
import logging

from datahub.emitter.mce_builder import make_tag_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# User to add tag to
user_urn = make_user_urn("jdoe")

# Tag to add
tag_urn = make_tag_urn("DataEngineering")

# Create graph client
datahub_graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Read current tags
current_tags = datahub_graph.get_aspect(
    entity_urn=user_urn, aspect_type=GlobalTagsClass
)

# Initialize tags if they don't exist
if current_tags is None:
    current_tags = GlobalTagsClass(tags=[])

# Check if tag already exists
tag_exists = any(tag.tag == tag_urn for tag in current_tags.tags)

if not tag_exists:
    # Add the new tag
    new_tag = TagAssociationClass(tag=tag_urn)
    current_tags.tags.append(new_tag)

    # Create MCP to update the tags
    mcp = MetadataChangeProposalWrapper(
        entityUrn=user_urn,
        aspect=current_tags,
    )

    # Emit the change
    datahub_graph.emit(mcp)
    log.info(f"Added tag {tag_urn} to user {user_urn}")
else:
    log.info(f"Tag {tag_urn} already exists on user {user_urn}")

```



### Querying Users via REST API

You can fetch user information using the REST API:


**REST API: Get user information**

```bash
# Get a user by URN
curl -X GET "http://localhost:8080/entities/urn%3Ali%3Acorpuser%3Ajdoe" \
  -H "Authorization: Bearer <your-access-token>"

# Get specific aspects of a user
curl -X GET "http://localhost:8080/aspects/urn%3Ali%3Acorpuser%3Ajdoe?aspect=corpUserInfo&aspect=corpUserEditableInfo&aspect=groupMembership" \
  -H "Authorization: Bearer <your-access-token>"
```



### Searching for Users

You can search for users using the GraphQL API or search API:


**GraphQL: Search for users**

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



## Technical Reference Guide

The sections above provide an overview of how to use this entity. The following sections provide detailed technical information about how metadata is stored and represented in DataHub.

**Aspects** are the individual pieces of metadata that can be attached to an entity. Each aspect contains specific information (like ownership, tags, or properties) and is stored as a separate record, allowing for flexible and incremental metadata updates.

**Relationships** show how this entity connects to other entities in the metadata graph. These connections are derived from the fields within each aspect and form the foundation of DataHub's knowledge graph.

### Reading the Field Tables

Each aspect's field table includes an **Annotations** column that provides additional metadata about how fields are used:

- **⚠️ Deprecated**: This field is deprecated and may be removed in a future version. Check the description for the recommended alternative
- **Searchable**: This field is indexed and can be searched in DataHub's search interface
- **Searchable (fieldname)**: When the field name in parentheses is shown, it indicates the field is indexed under a different name in the search index. For example, `dashboardTool` is indexed as `tool`
- **→ RelationshipName**: This field creates a relationship to another entity. The arrow indicates this field contains a reference (URN) to another entity, and the name indicates the type of relationship (e.g., `→ Contains`, `→ OwnedBy`)

Fields with complex types (like `Edge`, `AuditStamp`) link to their definitions in the [Common Types](#common-types) section below.

### Aspects

#### corpUserKey
Key for a CorpUser



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| username | string | ✓ | The name of the AD/LDAP user. | Searchable (ldap) |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "corpUserKey"
  },
  "name": "CorpUserKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "Searchable": {
        "boostScore": 2.0,
        "enableAutocomplete": true,
        "fieldName": "ldap",
        "fieldType": "WORD_GRAM"
      },
      "type": "string",
      "name": "username",
      "doc": "The name of the AD/LDAP user."
    }
  ],
  "doc": "Key for a CorpUser"
}
```





#### corpUserInfo
Linkedin corp user information



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| customProperties | map | ✓ | Custom property bag. | Searchable |
| active | boolean | ✓ | Deprecated! Use CorpUserStatus instead. Whether the corpUser is active, ref: https://iwww.corp.li... | Searchable |
| displayName | string |  | displayName of this user ,  e.g.  Hang Zhang(DataHQ) | Searchable |
| email | string |  | email address of this user | Searchable |
| title | string |  | title of this user | Searchable |
| managerUrn | string |  | direct manager of this user | Searchable (managerLdap), → ReportsTo |
| departmentId | long |  | department id this user belong to |  |
| departmentName | string |  | department name this user belong to |  |
| firstName | string |  | first name of this user |  |
| lastName | string |  | last name of this user |  |
| fullName | string |  | Common name of this user, format is firstName + lastName (split by a whitespace) | Searchable |
| countryCode | string |  | two uppercase letters country code. e.g.  US |  |
| system | boolean |  | Whether the corpUser is a system user. | Searchable |
| isSupportUser | boolean |  | Whether the corpUser is a support user authenticated through the support OIDC flow. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "EntityUrns": [
      "com.linkedin.common.CorpuserUrn"
    ],
    "name": "corpUserInfo"
  },
  "name": "CorpUserInfo",
  "namespace": "com.linkedin.identity",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldType": "TEXT",
          "queryByDefault": true
        }
      },
      "type": {
        "type": "map",
        "values": "string"
      },
      "name": "customProperties",
      "default": {},
      "doc": "Custom property bag."
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN",
        "weightsPerFieldValue": {
          "true": 2.0
        }
      },
      "type": "boolean",
      "name": "active",
      "doc": "Deprecated! Use CorpUserStatus instead. Whether the corpUser is active, ref: https://iwww.corp.linkedin.com/wiki/cf/display/GTSD/Accessing+Active+Directory+via+LDAP+tools"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldNameAliases": [
          "_entityName"
        ],
        "fieldType": "WORD_GRAM",
        "queryByDefault": true,
        "searchLabel": "entityName",
        "searchTier": 1
      },
      "type": [
        "null",
        "string"
      ],
      "name": "displayName",
      "default": null,
      "doc": "displayName of this user ,  e.g.  Hang Zhang(DataHQ)"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD",
        "queryByDefault": true,
        "searchTier": 1
      },
      "type": [
        "null",
        "string"
      ],
      "name": "email",
      "default": null,
      "doc": "email address of this user"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD",
        "queryByDefault": true
      },
      "type": [
        "null",
        "string"
      ],
      "name": "title",
      "default": null,
      "doc": "title of this user"
    },
    {
      "Relationship": {
        "entityTypes": [
          "corpuser"
        ],
        "name": "ReportsTo"
      },
      "Searchable": {
        "fieldName": "managerLdap",
        "fieldType": "URN",
        "queryByDefault": false
      },
      "java": {
        "class": "com.linkedin.common.urn.CorpuserUrn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "managerUrn",
      "default": null,
      "doc": "direct manager of this user"
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "departmentId",
      "default": null,
      "doc": "department id this user belong to"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "departmentName",
      "default": null,
      "doc": "department name this user belong to"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "firstName",
      "default": null,
      "doc": "first name of this user"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "lastName",
      "default": null,
      "doc": "last name of this user"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldType": "WORD_GRAM",
        "queryByDefault": true
      },
      "type": [
        "null",
        "string"
      ],
      "name": "fullName",
      "default": null,
      "doc": "Common name of this user, format is firstName + lastName (split by a whitespace)"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "countryCode",
      "default": null,
      "doc": "two uppercase letters country code. e.g.  US"
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN",
        "queryByDefault": false
      },
      "type": [
        "boolean",
        "null"
      ],
      "name": "system",
      "default": false,
      "doc": "Whether the corpUser is a system user."
    },
    {
      "Searchable": {
        "fieldType": "BOOLEAN",
        "queryByDefault": false
      },
      "type": [
        "null",
        "boolean"
      ],
      "name": "isSupportUser",
      "default": null,
      "doc": "Whether the corpUser is a support user authenticated through the support OIDC flow."
    }
  ],
  "doc": "Linkedin corp user information"
}
```





#### corpUserEditableInfo
Linkedin corp user information that can be edited from UI



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| aboutMe | string |  | About me section of the user |  |
| teams | string[] | ✓ | Teams that the user belongs to e.g. Metadata | Searchable |
| skills | string[] | ✓ | Skills that the user possesses e.g. Machine Learning | Searchable |
| pictureLink | string | ✓ | A URL which points to a picture which user wants to set as a profile photo |  |
| displayName | string |  | DataHub-native display name | Searchable |
| title | string |  | DataHub-native Title, e.g. 'Software Engineer' |  |
| platforms | string[] |  | The platforms that the user commonly works with | → IsUserOf |
| persona | string |  | The user's persona type, based on their role | → IsPersona |
| slack | string |  | Slack handle for the user |  |
| phone | string |  | Phone number to contact the user |  |
| email | string |  | Email address to contact the user |  |
| informationSources | string[] |  | Information sources that have been used to populate this CorpUserEditableInfo. These include plat... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "EntityUrns": [
      "com.linkedin.common.CorpuserUrn"
    ],
    "name": "corpUserEditableInfo"
  },
  "name": "CorpUserEditableInfo",
  "namespace": "com.linkedin.identity",
  "fields": [
    {
      "type": [
        "null",
        "string"
      ],
      "name": "aboutMe",
      "default": null,
      "doc": "About me section of the user"
    },
    {
      "Searchable": {
        "/*": {
          "fieldType": "TEXT"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "teams",
      "default": [],
      "doc": "Teams that the user belongs to e.g. Metadata"
    },
    {
      "Searchable": {
        "/*": {
          "fieldType": "TEXT"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "skills",
      "default": [],
      "doc": "Skills that the user possesses e.g. Machine Learning"
    },
    {
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": "string",
      "name": "pictureLink",
      "default": "assets/platforms/default_avatar.png",
      "doc": "A URL which points to a picture which user wants to set as a profile photo"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "fieldType": "WORD_GRAM",
        "queryByDefault": true
      },
      "type": [
        "null",
        "string"
      ],
      "name": "displayName",
      "default": null,
      "doc": "DataHub-native display name"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "title",
      "default": null,
      "doc": "DataHub-native Title, e.g. 'Software Engineer'"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataPlatform"
          ],
          "name": "IsUserOf"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "platforms",
      "default": null,
      "doc": "The platforms that the user commonly works with"
    },
    {
      "Relationship": {
        "entityTypes": [
          "dataHubPersona"
        ],
        "name": "IsPersona"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "persona",
      "default": null,
      "doc": "The user's persona type, based on their role"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "slack",
      "default": null,
      "doc": "Slack handle for the user"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "phone",
      "default": null,
      "doc": "Phone number to contact the user"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "email",
      "default": null,
      "doc": "Email address to contact the user"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "informationSources",
      "default": null,
      "doc": "Information sources that have been used to populate this CorpUserEditableInfo.\nThese include platform resources, such as Slack members or Looker users.\nThey can also refer to other semantic urns in the future."
    }
  ],
  "doc": "Linkedin corp user information that can be edited from UI"
}
```





#### corpUserStatus
The status of the user, e.g. provisioned, active, suspended, etc.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| status | string | ✓ | Status of the user, e.g. PROVISIONED / ACTIVE / SUSPENDED | Searchable |
| lastModified | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who last modified the status and when. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "corpUserStatus"
  },
  "name": "CorpUserStatus",
  "namespace": "com.linkedin.identity",
  "fields": [
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": "string",
      "name": "status",
      "doc": "Status of the user, e.g. PROVISIONED / ACTIVE / SUSPENDED"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "statusLastModifiedAt",
          "fieldType": "COUNT"
        }
      },
      "type": {
        "type": "record",
        "name": "AuditStamp",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "long",
            "name": "time",
            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": "string",
            "name": "actor",
            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "impersonator",
            "default": null,
            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
      },
      "name": "lastModified",
      "doc": "Audit stamp containing who last modified the status and when."
    }
  ],
  "doc": "The status of the user, e.g. provisioned, active, suspended, etc."
}
```





#### groupMembership
Carries information about the CorpGroups a user is in.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| groups | string[] | ✓ |  | → IsMemberOfGroup |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "groupMembership"
  },
  "name": "GroupMembership",
  "namespace": "com.linkedin.identity",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "corpGroup"
          ],
          "name": "IsMemberOfGroup"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "groups"
    }
  ],
  "doc": "Carries information about the CorpGroups a user is in."
}
```





#### globalTags
Tag aspect used for applying tags to an entity



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| tags | TagAssociation[] | ✓ | Tags associated with a given entity | Searchable, → TaggedWith |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "globalTags"
  },
  "name": "GlobalTags",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Relationship": {
        "/*/tag": {
          "entityTypes": [
            "tag"
          ],
          "name": "TaggedWith"
        }
      },
      "Searchable": {
        "/*/tag": {
          "addToFilters": true,
          "boostScore": 0.5,
          "fieldName": "tags",
          "fieldType": "URN",
          "filterNameOverride": "Tagged With",
          "hasValuesFieldName": "hasTags",
          "queryByDefault": true,
          "searchTier": 2
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TagAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.TagUrn"
              },
              "type": "string",
              "name": "tag",
              "doc": "Urn of the applied tag"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "context",
              "default": null,
              "doc": "Additional context about the association"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "tagAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "tagAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "tagAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Properties of an applied tag. For now, just an Urn. In the future we can extend this with other properties, e.g.\npropagation parameters."
        }
      },
      "name": "tags",
      "doc": "Tags associated with a given entity"
    }
  ],
  "doc": "Tag aspect used for applying tags to an entity"
}
```





#### status
The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.
This aspect is used to represent soft deletes conventionally.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| removed | boolean | ✓ | Whether the entity has been removed (soft-deleted). Kept for backward compatibility. When lifecyc... | Searchable |
| lifecycleStage | string |  | The lifecycle stage of the entity, referencing a lifecycleStageType entity. When null, the entity... | Searchable |
| lifecycleLastUpdated | [AuditStamp](#auditstamp) |  | Attribution for the lifecycle stage transition — who moved the entity into its current stage and ... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "status"
  },
  "name": "Status",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "removed",
      "default": false,
      "doc": "Whether the entity has been removed (soft-deleted).\nKept for backward compatibility. When lifecycleStage is set to a stage\nwith hideInSearch=true, this field is NOT automatically synced \u2014 the\nsearch layer uses lifecycleStage settings directly."
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD",
        "queryByDefault": false
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "lifecycleStage",
      "default": null,
      "doc": "The lifecycle stage of the entity, referencing a lifecycleStageType entity.\nWhen null, the entity is in its default active state (visible in search).\nWhen set, the referenced lifecycle stage's settings determine behavior\n(e.g., hideInSearch=true excludes the entity from default search results).\n\nUsers can override default filtering by explicitly filtering on this field."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "AuditStamp",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "long",
              "name": "time",
              "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "actor",
              "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "impersonator",
              "default": null,
              "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "message",
              "default": null,
              "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
            }
          ],
          "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
        }
      ],
      "name": "lifecycleLastUpdated",
      "default": null,
      "doc": "Attribution for the lifecycle stage transition \u2014 who moved the entity\ninto its current stage and when. Populated automatically by the\nsetLifecycleStage mutation; should be set by any code path that\nwrites the lifecycleStage field."
    }
  ],
  "doc": "The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.\nThis aspect is used to represent soft deletes conventionally."
}
```





#### corpUserCredentials
Corp user credentials



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| salt | string | ✓ | Salt used to hash password |  |
| hashedPassword | string | ✓ | Hashed password generated by concatenating salt and password, then hashing |  |
| passwordResetToken | string |  | Optional token needed to reset a user's password. Can only be set by the admin. |  |
| passwordResetTokenExpirationTimeMillis | long |  | When the password reset token expires. |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "EntityUrns": [
      "com.linkedin.common.CorpuserUrn"
    ],
    "name": "corpUserCredentials"
  },
  "name": "CorpUserCredentials",
  "namespace": "com.linkedin.identity",
  "fields": [
    {
      "type": "string",
      "name": "salt",
      "doc": "Salt used to hash password"
    },
    {
      "type": "string",
      "name": "hashedPassword",
      "doc": "Hashed password generated by concatenating salt and password, then hashing"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "passwordResetToken",
      "default": null,
      "doc": "Optional token needed to reset a user's password. Can only be set by the admin."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "passwordResetTokenExpirationTimeMillis",
      "default": null,
      "doc": "When the password reset token expires."
    }
  ],
  "doc": "Corp user credentials"
}
```





#### nativeGroupMembership
Carries information about the native CorpGroups a user is in.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| nativeGroups | string[] | ✓ |  | → IsMemberOfNativeGroup |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "nativeGroupMembership"
  },
  "name": "NativeGroupMembership",
  "namespace": "com.linkedin.identity",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "corpGroup"
          ],
          "name": "IsMemberOfNativeGroup"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "nativeGroups"
    }
  ],
  "doc": "Carries information about the native CorpGroups a user is in."
}
```





#### corpUserSettings
Settings that a user can customize through the datahub ui



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| appearance | CorpUserAppearanceSettings | ✓ | Settings for a user around the appearance of their DataHub U |  |
| views | CorpUserViewsSettings |  | User preferences for the Views feature. |  |
| notificationSettings | NotificationSettings |  | Notification settings for a user |  |
| homePage | CorpUserHomePageSettings |  | Settings related to the home page for a user |  |
| locale | CorpUserLocaleSettings |  | Locale and language preferences for a user |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "corpUserSettings",
    "schemaVersion": 2
  },
  "name": "CorpUserSettings",
  "namespace": "com.linkedin.identity",
  "fields": [
    {
      "type": {
        "type": "record",
        "name": "CorpUserAppearanceSettings",
        "namespace": "com.linkedin.identity",
        "fields": [
          {
            "type": [
              "null",
              "boolean"
            ],
            "name": "showSimplifiedHomepage",
            "default": null,
            "doc": "Flag whether the user should see a homepage with only datasets, charts and dashboards. Intended for users\nwho have less operational use cases for the datahub tool."
          },
          {
            "type": [
              "null",
              "boolean"
            ],
            "name": "showThemeV2",
            "default": null,
            "doc": "Flag controlling whether the V2 UI for DataHub is shown."
          }
        ],
        "doc": "Settings for a user around the appearance of their DataHub UI"
      },
      "name": "appearance",
      "doc": "Settings for a user around the appearance of their DataHub U"
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "CorpUserViewsSettings",
          "namespace": "com.linkedin.identity",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "defaultView",
              "default": null,
              "doc": "The default View which is selected for the user.\nIf none is chosen, then this value will be left blank."
            }
          ],
          "doc": "Settings related to the 'Views' feature."
        }
      ],
      "name": "views",
      "default": null,
      "doc": "User preferences for the Views feature."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "NotificationSettings",
          "namespace": "com.linkedin.event.notification.settings",
          "fields": [
            {
              "type": {
                "type": "array",
                "items": {
                  "type": "enum",
                  "symbolDocs": {
                    "EMAIL": "Email target type.",
                    "SLACK": "Slack target type.",
                    "TEAMS": "Microsoft Teams target type."
                  },
                  "name": "NotificationSinkType",
                  "namespace": "com.linkedin.event.notification",
                  "symbols": [
                    "SLACK",
                    "EMAIL",
                    "TEAMS"
                  ],
                  "doc": "A type of sink / platform to send a notification to."
                }
              },
              "name": "sinkTypes",
              "doc": "Sink types that notifications are sent to."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "SlackNotificationSettings",
                  "namespace": "com.linkedin.event.notification.settings",
                  "fields": [
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "userHandle",
                      "default": null,
                      "doc": "Optional user handle"
                    },
                    {
                      "type": [
                        "null",
                        {
                          "type": "array",
                          "items": "string"
                        }
                      ],
                      "name": "channels",
                      "default": null,
                      "doc": "Optional list of channels to send notifications to"
                    }
                  ],
                  "doc": "Slack Notification settings for an actor."
                }
              ],
              "name": "slackSettings",
              "default": null,
              "doc": "Slack Notification Settings"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "EmailNotificationSettings",
                  "namespace": "com.linkedin.event.notification.settings",
                  "fields": [
                    {
                      "type": "string",
                      "name": "email",
                      "doc": "Optional user or group email address"
                    }
                  ],
                  "doc": "Email Notification settings for an actor."
                }
              ],
              "name": "emailSettings",
              "default": null,
              "doc": "Email Notification Settings"
            }
          ],
          "doc": "Notification settings for an actor or subscription."
        }
      ],
      "name": "notificationSettings",
      "default": null,
      "doc": "Notification settings for a user"
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "CorpUserHomePageSettings",
          "namespace": "com.linkedin.identity",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "dataHubPageTemplate"
                ],
                "name": "HasPersonalPageTemplate"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "pageTemplate",
              "default": null,
              "doc": "The page template that will be rendered in the UI by default for this user"
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "dismissedAnnouncements",
              "default": null,
              "doc": "The list of announcement urns that have been dismissed by the user"
            }
          ],
          "doc": "Settings related to the home page for a user"
        }
      ],
      "name": "homePage",
      "default": null,
      "doc": "Settings related to the home page for a user"
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "CorpUserLocaleSettings",
          "namespace": "com.linkedin.identity",
          "fields": [
            {
              "type": [
                "null",
                "string"
              ],
              "name": "language",
              "default": null,
              "doc": "BCP 47 language tag representing the user's preferred UI language (e.g. \"en\", \"de\")."
            }
          ],
          "doc": "Settings for a user's locale and language preferences"
        }
      ],
      "name": "locale",
      "default": null,
      "doc": "Locale and language preferences for a user"
    }
  ],
  "doc": "Settings that a user can customize through the datahub ui"
}
```





#### origin
Carries information about where an entity originated from.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| type | OriginType | ✓ | Where an entity originated from. Either NATIVE or EXTERNAL. |  |
| externalType | string |  | Only populated if type is EXTERNAL. The externalType of the entity, such as the name of the ident... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "origin"
  },
  "name": "Origin",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "enum",
        "symbolDocs": {
          "EXTERNAL": "The entity is external to DataHub.",
          "NATIVE": "The entity is native to DataHub."
        },
        "name": "OriginType",
        "namespace": "com.linkedin.common",
        "symbols": [
          "NATIVE",
          "EXTERNAL"
        ],
        "doc": "Enum to define where an entity originated from."
      },
      "name": "type",
      "doc": "Where an entity originated from. Either NATIVE or EXTERNAL."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "externalType",
      "default": null,
      "doc": "Only populated if type is EXTERNAL. The externalType of the entity, such as the name of the identity provider."
    }
  ],
  "doc": "Carries information about where an entity originated from."
}
```





#### roleMembership
Carries information about which roles a user or group is assigned to.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| roles | string[] | ✓ |  | → IsMemberOfRole |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "roleMembership"
  },
  "name": "RoleMembership",
  "namespace": "com.linkedin.identity",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataHubRole"
          ],
          "name": "IsMemberOfRole"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "roles"
    }
  ],
  "doc": "Carries information about which roles a user or group is assigned to."
}
```





#### structuredProperties
Properties about an entity governed by StructuredPropertyDefinition



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| properties | StructuredPropertyValueAssignment[] | ✓ | Custom property bag. |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "structuredProperties"
  },
  "name": "StructuredProperties",
  "namespace": "com.linkedin.structured",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "StructuredPropertyValueAssignment",
          "namespace": "com.linkedin.structured",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "propertyUrn",
              "doc": "The property that is being assigned a value."
            },
            {
              "type": {
                "type": "array",
                "items": [
                  "string",
                  "double"
                ]
              },
              "name": "values",
              "doc": "The value assigned to the property."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "AuditStamp",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "impersonator",
                      "default": null,
                      "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "message",
                      "default": null,
                      "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                    }
                  ],
                  "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                }
              ],
              "name": "created",
              "default": null,
              "doc": "Audit stamp containing who created this relationship edge and when"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "Audit stamp containing who last modified this relationship edge and when"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "structuredPropertyAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "structuredPropertyAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "structuredPropertyAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ]
        }
      },
      "name": "properties",
      "doc": "Custom property bag."
    }
  ],
  "doc": "Properties about an entity governed by StructuredPropertyDefinition"
}
```





#### forms
Forms that are assigned to this entity to be filled out



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| incompleteForms | [FormAssociation](#formassociation)[] | ✓ | All incomplete forms assigned to the entity. | Searchable |
| completedForms | [FormAssociation](#formassociation)[] | ✓ | All complete forms assigned to the entity. | Searchable |
| verifications | FormVerificationAssociation[] | ✓ | Verifications that have been applied to the entity via completed forms. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "forms"
  },
  "name": "Forms",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*/completedPrompts/*/id": {
          "fieldName": "incompleteFormsCompletedPromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/completedPrompts/*/lastModified/time": {
          "fieldName": "incompleteFormsCompletedPromptResponseTimes",
          "fieldType": "DATETIME",
          "queryByDefault": false
        },
        "/*/incompletePrompts/*/id": {
          "fieldName": "incompleteFormsIncompletePromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/urn": {
          "fieldName": "incompleteForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied form"
            },
            {
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "FormPromptAssociation",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "string",
                      "name": "id",
                      "doc": "The id for the prompt. This must be GLOBALLY UNIQUE."
                    },
                    {
                      "type": {
                        "type": "record",
                        "name": "AuditStamp",
                        "namespace": "com.linkedin.common",
                        "fields": [
                          {
                            "type": "long",
                            "name": "time",
                            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                          },
                          {
                            "java": {
                              "class": "com.linkedin.common.urn.Urn"
                            },
                            "type": "string",
                            "name": "actor",
                            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                          },
                          {
                            "java": {
                              "class": "com.linkedin.common.urn.Urn"
                            },
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "impersonator",
                            "default": null,
                            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                          },
                          {
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "message",
                            "default": null,
                            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                          }
                        ],
                        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                      },
                      "name": "lastModified",
                      "doc": "The last time this prompt was touched for the entity (set, unset)"
                    },
                    {
                      "type": [
                        "null",
                        {
                          "type": "record",
                          "name": "FormPromptFieldAssociations",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": {
                                    "type": "record",
                                    "name": "FieldFormPromptAssociation",
                                    "namespace": "com.linkedin.common",
                                    "fields": [
                                      {
                                        "type": "string",
                                        "name": "fieldPath",
                                        "doc": "The field path on a schema field."
                                      },
                                      {
                                        "type": "com.linkedin.common.AuditStamp",
                                        "name": "lastModified",
                                        "doc": "The last time this prompt was touched for the field on the entity (set, unset)"
                                      }
                                    ],
                                    "doc": "Information about the status of a particular prompt for a specific schema field\non an entity."
                                  }
                                }
                              ],
                              "name": "completedFieldPrompts",
                              "default": null,
                              "doc": "A list of field-level prompt associations that are not yet complete for this form."
                            },
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": "com.linkedin.common.FieldFormPromptAssociation"
                                }
                              ],
                              "name": "incompleteFieldPrompts",
                              "default": null,
                              "doc": "A list of field-level prompt associations that are complete for this form."
                            }
                          ],
                          "doc": "Information about the field-level prompt associations on a top-level prompt association."
                        }
                      ],
                      "name": "fieldAssociations",
                      "default": null,
                      "doc": "Optional information about the field-level prompt associations."
                    }
                  ],
                  "doc": "Information about the status of a particular prompt.\nNote that this is where we can add additional information about individual responses:\nactor, timestamp, and the response itself."
                }
              },
              "name": "incompletePrompts",
              "default": [],
              "doc": "A list of prompts that are not yet complete for this form."
            },
            {
              "type": {
                "type": "array",
                "items": "com.linkedin.common.FormPromptAssociation"
              },
              "name": "completedPrompts",
              "default": [],
              "doc": "A list of prompts that have been completed for this form."
            }
          ],
          "doc": "Properties of an applied form."
        }
      },
      "name": "incompleteForms",
      "doc": "All incomplete forms assigned to the entity."
    },
    {
      "Searchable": {
        "/*/completedPrompts/*/id": {
          "fieldName": "completedFormsCompletedPromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/completedPrompts/*/lastModified/time": {
          "fieldName": "completedFormsCompletedPromptResponseTimes",
          "fieldType": "DATETIME",
          "queryByDefault": false
        },
        "/*/incompletePrompts/*/id": {
          "fieldName": "completedFormsIncompletePromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/urn": {
          "fieldName": "completedForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.FormAssociation"
      },
      "name": "completedForms",
      "doc": "All complete forms assigned to the entity."
    },
    {
      "Searchable": {
        "/*/form": {
          "fieldName": "verifiedForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormVerificationAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "form",
              "doc": "The urn of the form that granted this verification."
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "An audit stamp capturing who and when verification was applied for this form."
            }
          ],
          "doc": "An association between a verification and an entity that has been granted\nvia completion of one or more forms of type 'VERIFICATION'."
        }
      },
      "name": "verifications",
      "default": [],
      "doc": "Verifications that have been applied to the entity via completed forms."
    }
  ],
  "doc": "Forms that are assigned to this entity to be filled out"
}
```





#### testResults
Information about a Test Result



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| failing | [TestResult](#testresult)[] | ✓ | Results that are failing | Searchable, → IsFailing |
| passing | [TestResult](#testresult)[] | ✓ | Results that are passing | Searchable, → IsPassing |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "testResults"
  },
  "name": "TestResults",
  "namespace": "com.linkedin.test",
  "fields": [
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsFailing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "failingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasFailingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TestResult",
          "namespace": "com.linkedin.test",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "test",
              "doc": "The urn of the test"
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "FAILURE": " The Test Failed",
                  "SUCCESS": " The Test Succeeded"
                },
                "name": "TestResultType",
                "namespace": "com.linkedin.test",
                "symbols": [
                  "SUCCESS",
                  "FAILURE"
                ]
              },
              "name": "type",
              "doc": "The type of the result"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "testDefinitionMd5",
              "default": null,
              "doc": "The md5 of the test definition that was used to compute this result.\nSee TestInfo.testDefinition.md5 for more information."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "AuditStamp",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "impersonator",
                      "default": null,
                      "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "message",
                      "default": null,
                      "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                    }
                  ],
                  "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                }
              ],
              "name": "lastComputed",
              "default": null,
              "doc": "The audit stamp of when the result was computed, including the actor who computed it."
            }
          ],
          "doc": "Information about a Test Result"
        }
      },
      "name": "failing",
      "doc": "Results that are failing"
    },
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsPassing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "passingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasPassingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.test.TestResult"
      },
      "name": "passing",
      "doc": "Results that are passing"
    }
  ],
  "doc": "Information about a Test Result"
}
```





#### subTypes
Sub Types. Use this aspect to specialize a generic Entity
e.g. Making a Dataset also be a View or also be a LookerExplore



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| typeNames | string[] | ✓ | The names of the specific types. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "subTypes"
  },
  "name": "SubTypes",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldType": "KEYWORD",
          "filterNameOverride": "Sub Type",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "typeNames",
      "doc": "The names of the specific types."
    }
  ],
  "doc": "Sub Types. Use this aspect to specialize a generic Entity\ne.g. Making a Dataset also be a View or also be a LookerExplore"
}
```





#### slackUserInfo
Information about a Slack user.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| slackInstance | string | ✓ | The dataplatform instance that this Slack member belongs to. | → PartOfSlackWorkspace |
| id | string | ✓ | The unique identifier for the Slack member. |  |
| name | string | ✓ | The username of the Slack member. |  |
| realName | string | ✓ | The real name of the Slack member. |  |
| displayName | string | ✓ | The display name of the Slack member. |  |
| email | string |  | The email associated with the Slack member. |  |
| teamId | string | ✓ | The ID associated with the Slack team. |  |
| isDeleted | boolean | ✓ | Whether the member is deleted or not. |  |
| isAdmin | boolean | ✓ | Whether the member is an admin. |  |
| isOwner | boolean | ✓ | Whether the member is an owner. |  |
| isPrimaryOwner | boolean | ✓ | Whether the member is a primary owner. |  |
| isBot | boolean | ✓ | Whether the member is a bot. |  |
| timezone | string |  | The timezone of the Slack member. |  |
| timezoneOffset | int |  | The timezone offset of the Slack member. |  |
| title | string |  | The title of the Slack member. |  |
| phone | string |  | The phone number of the Slack member. |  |
| profilePictureUrl | string |  | The URL of the member's profile picture. |  |
| statusText | string |  | The status text of the Slack member. |  |
| statusEmoji | string |  | The status emoji of the Slack member. |  |
| lastUpdatedSeconds | long |  | The timestamp of when the member was last updated. (in seconds) |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "slackUserInfo"
  },
  "name": "SlackUserInfo",
  "namespace": "com.linkedin.dataplatform.slack",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "dataPlatformInstance"
        ],
        "name": "PartOfSlackWorkspace"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "slackInstance",
      "doc": "The dataplatform instance that this Slack member belongs to."
    },
    {
      "type": "string",
      "name": "id",
      "doc": "The unique identifier for the Slack member."
    },
    {
      "type": "string",
      "name": "name",
      "doc": "The username of the Slack member."
    },
    {
      "type": "string",
      "name": "realName",
      "doc": "The real name of the Slack member."
    },
    {
      "type": "string",
      "name": "displayName",
      "doc": "The display name of the Slack member."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "email",
      "default": null,
      "doc": "The email associated with the Slack member."
    },
    {
      "type": "string",
      "name": "teamId",
      "doc": "The ID associated with the Slack team."
    },
    {
      "type": "boolean",
      "name": "isDeleted",
      "doc": "Whether the member is deleted or not."
    },
    {
      "type": "boolean",
      "name": "isAdmin",
      "doc": "Whether the member is an admin."
    },
    {
      "type": "boolean",
      "name": "isOwner",
      "doc": "Whether the member is an owner."
    },
    {
      "type": "boolean",
      "name": "isPrimaryOwner",
      "doc": "Whether the member is a primary owner."
    },
    {
      "type": "boolean",
      "name": "isBot",
      "doc": "Whether the member is a bot."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "timezone",
      "default": null,
      "doc": "The timezone of the Slack member."
    },
    {
      "type": [
        "null",
        "int"
      ],
      "name": "timezoneOffset",
      "default": null,
      "doc": "The timezone offset of the Slack member."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "title",
      "default": null,
      "doc": "The title of the Slack member."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "phone",
      "default": null,
      "doc": "The phone number of the Slack member."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "profilePictureUrl",
      "default": null,
      "doc": "The URL of the member's profile picture."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "statusText",
      "default": null,
      "doc": "The status text of the Slack member."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "statusEmoji",
      "default": null,
      "doc": "The status emoji of the Slack member."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "lastUpdatedSeconds",
      "default": null,
      "doc": "The timestamp of when the member was last updated. (in seconds)"
    }
  ],
  "doc": "Information about a Slack user."
}
```





### Common Types

These types are used across multiple aspects in this entity.

#### AuditStamp

Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage.

**Fields:**

- `time` (long): When did the resource/association/sub-resource move into the specific lifecyc...
- `actor` (string): The entity (e.g. a member URN) which will be credited for moving the resource...
- `impersonator` (string?): The entity (e.g. a service URN) which performs the change on behalf of the Ac...
- `message` (string?): Additional context around how DataHub was informed of the particular change. ...

#### FormAssociation

Properties of an applied form.

**Fields:**

- `urn` (string): Urn of the applied form
- `incompletePrompts` (FormPromptAssociation[]): A list of prompts that are not yet complete for this form.
- `completedPrompts` (FormPromptAssociation[]): A list of prompts that have been completed for this form.

#### TestResult

Information about a Test Result

**Fields:**

- `test` (string): The urn of the test
- `type` (TestResultType): The type of the result
- `testDefinitionMd5` (string?): The md5 of the test definition that was used to compute this result. See Test...
- `lastComputed` (AuditStamp?): The audit stamp of when the result was computed, including the actor who comp...


### Relationships

#### Self
These are the relationships to itself, stored in this entity's aspects
- ReportsTo (via `corpUserInfo.managerUrn`)
#### Outgoing
These are the relationships stored in this entity's aspects
- IsUserOf

   - DataPlatform via `corpUserEditableInfo.platforms`
- IsPersona

   - DataHubPersona via `corpUserEditableInfo.persona`
- IsMemberOfGroup

   - CorpGroup via `groupMembership.groups`
- TaggedWith

   - Tag via `globalTags.tags`
- IsMemberOfNativeGroup

   - CorpGroup via `nativeGroupMembership.nativeGroups`
- HasPersonalPageTemplate

   - DataHubPageTemplate via `corpUserSettings.homePage.pageTemplate`
- IsMemberOfRole

   - DataHubRole via `roleMembership.roles`
- IsFailing

   - Test via `testResults.failing`
- IsPassing

   - Test via `testResults.passing`
- PartOfSlackWorkspace

   - DataPlatformInstance via `slackUserInfo.slackInstance`
#### Incoming
These are the relationships stored in other entity's aspects
- Has

   - Role via `actors.users.user`
- OwnedBy

   - Dataset via `ownership.owners.owner`
   - DataJob via `ownership.owners.owner`
   - DataFlow via `ownership.owners.owner`
   - DataProcess via `ownership.owners.owner`
   - Chart via `ownership.owners.owner`
   - Dashboard via `ownership.owners.owner`
   - Notebook via `ownership.owners.owner`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
