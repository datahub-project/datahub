---
sidebar_position: 10
title: Corpuser
slug: /generated/metamodel/entities/corpuser
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/corpuser.md
---

# Corpuser

CorpUser represents an identity of a person (or an account) in the enterprise.

## Aspects

### corpUserKey

Key for a CorpUser

<details>
<summary>Schema</summary>

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
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "username",
      "doc": "The name of the AD/LDAP user."
    }
  ],
  "doc": "Key for a CorpUser"
}
```

</details>

### corpUserInfo

Linkedin corp user information

<details>
<summary>Schema</summary>

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
        "fieldType": "TEXT_PARTIAL",
        "queryByDefault": true
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
        "queryByDefault": true
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
        "queryByDefault": true
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
        "fieldType": "TEXT_PARTIAL",
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
    }
  ],
  "doc": "Linkedin corp user information"
}
```

</details>

### corpUserEditableInfo

Linkedin corp user information that can be edited from UI

<details>
<summary>Schema</summary>

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
      "default": "https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/default_avatar.png",
      "doc": "A URL which points to a picture which user wants to set as a profile photo"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "fieldType": "TEXT_PARTIAL",
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
    }
  ],
  "doc": "Linkedin corp user information that can be edited from UI"
}
```

</details>

### corpUserStatus

The status of the user, e.g. provisioned, active, suspended, etc.

<details>
<summary>Schema</summary>

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

</details>

### groupMembership

Carries information about the CorpGroups a user is in.

<details>
<summary>Schema</summary>

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

</details>

### globalTags

Tag aspect used for applying tags to an entity

<details>
<summary>Schema</summary>

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
          "filterNameOverride": "Tag",
          "hasValuesFieldName": "hasTags",
          "queryByDefault": true
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

</details>

### status

The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.
This aspect is used to represent soft deletes conventionally.

<details>
<summary>Schema</summary>

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
      "doc": "Whether the entity has been removed (soft-deleted)."
    }
  ],
  "doc": "The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.\nThis aspect is used to represent soft deletes conventionally."
}
```

</details>

### corpUserCredentials

Corp user credentials

<details>
<summary>Schema</summary>

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

</details>

### nativeGroupMembership

Carries information about the native CorpGroups a user is in.

<details>
<summary>Schema</summary>

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

</details>

### corpUserSettings

Settings that a user can customize through the datahub ui

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "corpUserSettings"
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
    }
  ],
  "doc": "Settings that a user can customize through the datahub ui"
}
```

</details>

### origin

Carries information about where an entity originated from.

<details>
<summary>Schema</summary>

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

</details>

### roleMembership

Carries information about which roles a user is assigned to.

<details>
<summary>Schema</summary>

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
  "doc": "Carries information about which roles a user is assigned to."
}
```

</details>

## Relationships

### Self

These are the relationships to itself, stored in this entity's aspects

- ReportsTo (via `corpUserInfo.managerUrn`)

### Outgoing

These are the relationships stored in this entity's aspects

- IsMemberOfGroup

  - CorpGroup via `groupMembership.groups`

- TaggedWith

  - Tag via `globalTags.tags`

- IsMemberOfNativeGroup

  - CorpGroup via `nativeGroupMembership.nativeGroups`

- IsMemberOfRole

  - DataHubRole via `roleMembership.roles`

### Incoming

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

## [Global Metadata Model](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)
