---
sidebar_position: 9
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
                    "SLACK": "Slack target type."
                  },
                  "name": "NotificationSinkType",
                  "namespace": "com.linkedin.event.notification",
                  "symbols": [
                    "SLACK",
                    "EMAIL"
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
Carries information about which roles a user or group is assigned to.
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
  "doc": "Carries information about which roles a user or group is assigned to."
}
```
</details>

### structuredProperties
Properties about an entity governed by StructuredPropertyDefinition
<details>
<summary>Schema</summary>

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
</details>

### forms
Forms that are assigned to this entity to be filled out
<details>
<summary>Schema</summary>

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
</details>

### testResults
Information about a Test Result
<details>
<summary>Schema</summary>

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
</details>

### subTypes
Sub Types. Use this aspect to specialize a generic Entity
e.g. Making a Dataset also be a View or also be a LookerExplore
<details>
<summary>Schema</summary>

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
</details>

### slackUserInfo
Information about a Slack user.
<details>
<summary>Schema</summary>

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
</details>

## Relationships

### Self
These are the relationships to itself, stored in this entity's aspects
- ReportsTo (via `corpUserInfo.managerUrn`)
### Outgoing
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
## [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
