# AI Agent

An AI Agent is a first-class catalog entity representing an AI agent — a system built on one or more
LLMs that uses tools and skills to accomplish tasks (LangChain agents, Google ADK agents, Snowflake
Cortex agents, DataHub's own "Ask DataHub" assistants, etc.). Cataloging agents lets teams discover
what agents exist, understand what they can do and what they depend on, govern them, and track their
lineage into the data they touch.

Agents come in three flavors, distinguished by the `source.type` of the `aiAgentInfo` aspect:

- **System** (`SYSTEM`): bootstrapped at deployment time (e.g. via YAML) and not editable by end users.
- **Native** (`NATIVE`): created and managed on DataHub by end users through the UI or API.
- **External** (`EXTERNAL`): managed outside DataHub but cataloged here for discovery and governance.

This is an origin/governance axis, not a functional one — a single-agent, multi-agent, or copilot
distinction (if needed) is layered on with the standard `subTypes` aspect.

## Identity

AI Agents are identified by a single field:

- **id**: A unique identifier for the agent, typically a human-readable slug such as `ask-datahub`
  or a generated UUID.

An example URN is `urn:li:aiAgent:ask-datahub`. The identifier is platform-agnostic; the platform, if
any, is carried on the `dataPlatformInstance` aspect rather than in the key.

## Important Capabilities

### Agent Info

The core properties live on the `aiAgentInfo` aspect:

- **name**: Display name, searchable with autocomplete.
- **tagline**: A short (~120 char) one-line summary shown on agent cards, hero subtitles, and picker
  dropdowns. When absent, surfaces fall back to truncating the description.
- **description**: What the agent does **and when to route to it**. The recommended style is
  "X does Y. Use when Z." — for router-style agents (like Ask DataHub) this text is read verbatim when
  deciding whether to delegate, so the "Use when…" tail is load-bearing rather than decorative.
- **instructions**: Custom base instructions injected into the agent's system prompt, beyond the
  standard boilerplate.
- **source**: The origin of the agent definition (see below).
- **created** / **lastModified**: Audit stamps.

### Source and Agent Families

The `source` field (an `AIAgentSource`) records the agent's origin `type` (`SYSTEM` / `NATIVE` /
`EXTERNAL`) and, optionally, a `clonedFrom` reference to the agent this one was cloned from
(the `ClonedFromAgent` relationship). Clones inherit their base's configuration verbatim — one
frontend module, one tool palette, one set of instructions — and differ only in the artifacts they
manage. Grouping a base agent and its clones this way forms an **Agent Family**.

### Dependencies

The `aiAgentDependencies` aspect captures what the agent relies on, as typed relationships to other
first-class entities:

- **skills** → [Agent Skill](./agentSkill.md) entities the agent adopts (`AgentHasSkill`).
- **tools** → [API](./api.md) entities the agent invokes (`AgentUsesTool`). This is a lineage edge: the
  tools are upstream of the agent, contributing to the `repo → service → api → agent` chain.
- **models** → `mlModel` entities (the LLMs) the agent runs on (`AgentUsesModel`).

### Versioning, Governance, and Health

AI Agents support the standard governance aspects — ownership, tags, glossary terms, domains,
structured properties, and institutional memory — as well as native versioning (`versionProperties`,
so successive revisions of an agent group into a version set) and health via the shared incidents
subsystem (`incidentsSummary`). Agents can also participate in lineage through `upstreamLineage`.



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

#### aiAgentInfo
Configuration and metadata for an AI agent.

Agents can be:
1. System (SYSTEM): bootstrapped at deployment time via YAML, not editable
2. Native (NATIVE): created by end users through the UI or API
3. External (EXTERNAL): cataloged on DataHub but managed externally



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string | ✓ | Display name of the agent | Searchable |
| tagline | string |  | Short, user-facing tagline shown on agent cards, hero subtitles, and picker dropdowns. Should be ... | Searchable |
| description | string |  | Description of what this agent does AND when Ask DataHub should route to it. Style: "X does Y. Us... | Searchable |
| instructions | string |  | Custom base instructions for the agent, beyond the standard DataHub boilerplate. Injected into th... |  |
| source | AIAgentSource |  | The source or origin of the agent definition. System agents (source.type = SYSTEM) are bootstrapp... |  |
| created | [AuditStamp](#auditstamp) | ✓ | When this agent was created | Searchable |
| lastModified | [AuditStamp](#auditstamp) | ✓ | When this agent was last modified | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "aiAgentInfo",
    "schemaVersion": 8
  },
  "name": "AIAgentInfo",
  "namespace": "com.linkedin.agent",
  "fields": [
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldNameAliases": [
          "_entityName"
        ],
        "fieldType": "WORD_GRAM"
      },
      "type": "string",
      "name": "name",
      "doc": "Display name of the agent"
    },
    {
      "Searchable": {
        "fieldType": "TEXT",
        "hasValuesFieldName": "hasTagline",
        "queryByDefault": false
      },
      "type": [
        "null",
        "string"
      ],
      "name": "tagline",
      "default": null,
      "doc": "Short, user-facing tagline shown on agent cards, hero subtitles, and\npicker dropdowns. Should be a one-line summary (~120 chars). When\nabsent, surfaces fall back to truncating `description`."
    },
    {
      "Searchable": {
        "fieldType": "TEXT",
        "hasValuesFieldName": "hasDescription"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Description of what this agent does AND when Ask DataHub should\nroute to it. Style: \"X does Y. Use when Z.\" The router reads this\nverbatim when deciding whether to delegate to this agent, so the\n\"Use when...\" tail is load-bearing \u2014 not optional prose. Roughly\n300-1000 chars is the right length."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "instructions",
      "default": null,
      "doc": "Custom base instructions for the agent, beyond the standard DataHub boilerplate.\nInjected into the agent's system prompt."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "AIAgentSource",
          "namespace": "com.linkedin.agent",
          "fields": [
            {
              "Searchable": {
                "addToFilters": true,
                "fieldName": "sourceType",
                "fieldType": "KEYWORD"
              },
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "EXTERNAL": "An agent defined and managed externally, cataloged on DataHub.",
                  "NATIVE": "An agent defined and managed natively on DataHub by end users.",
                  "SYSTEM": "A system-internal agent, bootstrapped at deployment time.\nSystem agents are not editable by end users."
                },
                "name": "AIAgentSourceType",
                "namespace": "com.linkedin.agent",
                "symbols": [
                  "SYSTEM",
                  "NATIVE",
                  "EXTERNAL"
                ],
                "doc": "The source type of an AI agent definition.\n\nFollows the same pattern as AssertionSourceType: NATIVE agents are\ndefined and managed on DataHub, while EXTERNAL agents are cataloged\nbut managed outside DataHub."
              },
              "name": "type",
              "doc": "The source type of the agent"
            },
            {
              "Relationship": {
                "entityTypes": [
                  "aiAgent"
                ],
                "name": "ClonedFromAgent"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "clonedFrom",
                "fieldType": "URN",
                "filterNameOverride": "Cloned From",
                "queryByDefault": false
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "clonedFrom",
              "default": null,
              "doc": "If this agent was cloned from another agent, the URN of the source agent.\nNull on SYSTEM agents and original NATIVE/EXTERNAL agents.\n\nUsed to group clones into an Agent Family \u2014 the base + all its clones share\none frontend module, one tool palette, and one set of instructions. Clones\ninherit configuration verbatim from the base; what differs is their\nstewardship set (the artifacts they manage)."
            }
          ],
          "doc": "The source or origin of an AI agent definition.\nFollows the same pattern as AssertionSource."
        }
      ],
      "name": "source",
      "default": null,
      "doc": "The source or origin of the agent definition.\nSystem agents (source.type = SYSTEM) are bootstrapped at deployment time\nand are not editable by end users."
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "createdAt",
          "fieldType": "DATETIME",
          "queryByDefault": false
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
      "name": "created",
      "doc": "When this agent was created"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "lastModifiedAt",
          "fieldType": "DATETIME",
          "queryByDefault": false
        }
      },
      "type": "com.linkedin.common.AuditStamp",
      "name": "lastModified",
      "doc": "When this agent was last modified"
    }
  ],
  "doc": "Configuration and metadata for an AI agent.\n\nAgents can be:\n1. System (SYSTEM): bootstrapped at deployment time via YAML, not editable\n2. Native (NATIVE): created by end users through the UI or API\n3. External (EXTERNAL): cataloged on DataHub but managed externally"
}
```





#### aiAgentDependencies
Dependencies of an AI agent: the skills it adopts, the tools it
invokes, and the models it runs on. These are governance/discovery
relationships to first-class catalog entities.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| skills | string[] |  | High-level skills this agent adopts. | Searchable, → AgentHasSkill |
| tools | string[] |  | APIs this agent invokes (cataloged API entities). | Searchable, → AgentUsesTool |
| models | string[] |  | Models (LLMs) this agent runs on. | → AgentUsesModel |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "aiAgentDependencies"
  },
  "name": "AIAgentDependencies",
  "namespace": "com.linkedin.agent",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "agentSkill"
          ],
          "name": "AgentHasSkill"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "skills",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "skills",
      "default": null,
      "doc": "High-level skills this agent adopts."
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "api"
          ],
          "isLineage": true,
          "isUpstream": true,
          "name": "AgentUsesTool"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "tools",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "tools",
      "default": null,
      "doc": "APIs this agent invokes (cataloged API entities)."
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "mlModel"
          ],
          "name": "AgentUsesModel"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "models",
      "default": null,
      "doc": "Models (LLMs) this agent runs on."
    }
  ],
  "doc": "Dependencies of an AI agent: the skills it adopts, the tools it\ninvokes, and the models it runs on. These are governance/discovery\nrelationships to first-class catalog entities."
}
```





#### dataPlatformInstance
The specific instance of the data platform that this entity belongs to



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| platform | string | ✓ | Data Platform | Searchable |
| instance | string |  | Instance of the data platform (e.g. db instance) | Searchable (platformInstance) |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataPlatformInstance"
  },
  "name": "DataPlatformInstance",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "URN",
        "filterNameOverride": "Platform"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "platform",
      "doc": "Data Platform"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldName": "platformInstance",
        "fieldType": "URN",
        "filterNameOverride": "Platform Instance"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "instance",
      "default": null,
      "doc": "Instance of the data platform (e.g. db instance)"
    }
  ],
  "doc": "The specific instance of the data platform that this entity belongs to"
}
```





#### displayProperties
Properties related to how the entity is displayed in the Datahub UI



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| colorHex | string |  | The color associated with the entity in Hex. For example #FFFFFF. |  |
| icon | IconProperties |  | The icon associated with the entity |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "displayProperties"
  },
  "name": "DisplayProperties",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": [
        "null",
        "string"
      ],
      "name": "colorHex",
      "default": null,
      "doc": "The color associated with the entity in Hex. For example #FFFFFF."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "IconProperties",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "MATERIAL": "Material UI"
                },
                "name": "IconLibrary",
                "namespace": "com.linkedin.common",
                "symbols": [
                  "MATERIAL"
                ],
                "doc": "Enum of possible icon sources"
              },
              "name": "iconLibrary",
              "doc": "The source of the icon: e.g. Antd, Material, etc"
            },
            {
              "type": "string",
              "name": "name",
              "doc": "The name of the icon"
            },
            {
              "type": "string",
              "name": "style",
              "doc": "Any modifier for the icon, this will be library-specific, e.g. filled/outlined, etc"
            }
          ],
          "doc": "Properties describing an icon associated with an entity"
        }
      ],
      "name": "icon",
      "default": null,
      "doc": "The icon associated with the entity"
    }
  ],
  "doc": "Properties related to how the entity is displayed in the Datahub UI"
}
```





#### ownership
Ownership information of an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| owners | Owner[] | ✓ | List of owners of the entity. |  |
| ownerTypes | map |  | Ownership type to Owners map, populated via mutation hook. | Searchable |
| lastModified | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who last modified the record and when. A value of 0 in the time field indi... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "ownership"
  },
  "name": "Ownership",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Owner",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "corpuser",
                  "corpGroup"
                ],
                "name": "OwnedBy"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "owners",
                "fieldType": "URN",
                "filterNameOverride": "Owned By",
                "hasValuesFieldName": "hasOwners",
                "queryByDefault": false,
                "searchTier": 2
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "owner",
              "doc": "Owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:multiProduct:mp_name\n(Caveat: only corpuser is currently supported in the frontend.)"
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "BUSINESS_OWNER": "A person or group who is responsible for logical, or business related, aspects of the asset.",
                  "CONSUMER": "A person, group, or service that consumes the data\nDeprecated! Use TECHNICAL_OWNER or BUSINESS_OWNER instead.",
                  "CUSTOM": "Set when ownership type is unknown or a when new one is specified as an ownership type entity for which we have no\nenum value for. This is used for backwards compatibility",
                  "DATAOWNER": "A person or group that is owning the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DATA_STEWARD": "A steward, expert, or delegate responsible for the asset.",
                  "DELEGATE": "A person or a group that overseas the operation, e.g. a DBA or SRE.\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DEVELOPER": "A person or group that is in charge of developing the code\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "NONE": "No specific type associated to the owner.",
                  "PRODUCER": "A person, group, or service that produces/generates the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "STAKEHOLDER": "A person or a group that has direct business interest\nDeprecated! Use TECHNICAL_OWNER, BUSINESS_OWNER, or STEWARD instead.",
                  "TECHNICAL_OWNER": "person or group who is responsible for technical aspects of the asset."
                },
                "deprecatedSymbols": {
                  "CONSUMER": true,
                  "DATAOWNER": true,
                  "DELEGATE": true,
                  "DEVELOPER": true,
                  "PRODUCER": true,
                  "STAKEHOLDER": true
                },
                "name": "OwnershipType",
                "namespace": "com.linkedin.common",
                "symbols": [
                  "CUSTOM",
                  "TECHNICAL_OWNER",
                  "BUSINESS_OWNER",
                  "DATA_STEWARD",
                  "NONE",
                  "DEVELOPER",
                  "DATAOWNER",
                  "DELEGATE",
                  "PRODUCER",
                  "CONSUMER",
                  "STAKEHOLDER"
                ],
                "doc": "Asset owner types"
              },
              "name": "type",
              "doc": "The type of the ownership"
            },
            {
              "Relationship": {
                "entityTypes": [
                  "ownershipType"
                ],
                "name": "ownershipType"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "typeUrn",
              "default": null,
              "doc": "The type of the ownership\nUrn of type O"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "OwnershipSource",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": {
                        "type": "enum",
                        "symbolDocs": {
                          "AUDIT": "Auditing system or audit logs",
                          "DATABASE": "Database, e.g. GRANTS table",
                          "FILE_SYSTEM": "File system, e.g. file/directory owner",
                          "ISSUE_TRACKING_SYSTEM": "Issue tracking system, e.g. Jira",
                          "MANUAL": "Manually provided by a user",
                          "OTHER": "Other sources",
                          "SERVICE": "Other ownership-like service, e.g. Nuage, ACL service etc",
                          "SOURCE_CONTROL": "SCM system, e.g. GIT, SVN"
                        },
                        "name": "OwnershipSourceType",
                        "namespace": "com.linkedin.common",
                        "symbols": [
                          "AUDIT",
                          "DATABASE",
                          "FILE_SYSTEM",
                          "ISSUE_TRACKING_SYSTEM",
                          "MANUAL",
                          "SERVICE",
                          "SOURCE_CONTROL",
                          "OTHER"
                        ]
                      },
                      "name": "type",
                      "doc": "The type of the source"
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "url",
                      "default": null,
                      "doc": "A reference URL for the source"
                    }
                  ],
                  "doc": "Source/provider of the ownership information"
                }
              ],
              "name": "source",
              "default": null,
              "doc": "Source information for the ownership"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "ownerAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "ownerAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "ownerAttributionDates",
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
          "doc": "Ownership information"
        }
      },
      "name": "owners",
      "doc": "List of owners of the entity."
    },
    {
      "Searchable": {
        "/$key": {
          "fieldType": "MAP_ARRAY",
          "queryByDefault": false
        }
      },
      "type": [
        {
          "type": "map",
          "values": {
            "type": "array",
            "items": "string"
          }
        },
        "null"
      ],
      "name": "ownerTypes",
      "default": {},
      "doc": "Ownership type to Owners map, populated via mutation hook."
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
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "Audit stamp containing who last modified the record and when. A value of 0 in the time field indicates missing data."
    }
  ],
  "doc": "Ownership information of an entity."
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





#### upstreamLineage
Upstream lineage of a dataset



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| upstreams | Upstream[] | ✓ | List of upstream dataset lineage information |  |
| fineGrainedLineages | FineGrainedLineage[] |  | List of fine-grained lineage information, including field-level lineage | → DownstreamOf |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "upstreamLineage",
    "schemaVersion": 2
  },
  "name": "UpstreamLineage",
  "namespace": "com.linkedin.dataset",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Upstream",
          "namespace": "com.linkedin.dataset",
          "fields": [
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
              "name": "auditStamp",
              "default": {
                "actor": "urn:li:corpuser:unknown",
                "impersonator": null,
                "time": 0,
                "message": null
              },
              "doc": "Audit stamp containing who reported the lineage and when."
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "created",
              "default": null,
              "doc": "Audit stamp containing who created the lineage and when."
            },
            {
              "Relationship": {
                "createdActor": "upstreams/*/created/actor",
                "createdOn": "upstreams/*/created/time",
                "entityTypes": [
                  "dataset"
                ],
                "isLineage": true,
                "name": "DownstreamOf",
                "properties": "upstreams/*/properties",
                "updatedActor": "upstreams/*/auditStamp/actor",
                "updatedOn": "upstreams/*/auditStamp/time",
                "via": "upstreams/*/query"
              },
              "Searchable": {
                "fieldName": "upstreams",
                "fieldType": "URN",
                "hasValuesFieldName": "hasUpstreams",
                "queryByDefault": false
              },
              "java": {
                "class": "com.linkedin.common.urn.DatasetUrn"
              },
              "type": "string",
              "name": "dataset",
              "doc": "The upstream dataset the lineage points to"
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "COPY": "Direct copy without modification",
                  "TRANSFORMED": "Transformed data with modification (format or content change)",
                  "VIEW": "Represents a view defined on the sources e.g. Hive view defined on underlying hive tables or a Hive table pointing to a HDFS dataset or DALI view defined on multiple sources"
                },
                "name": "DatasetLineageType",
                "namespace": "com.linkedin.dataset",
                "symbols": [
                  "COPY",
                  "TRANSFORMED",
                  "VIEW"
                ],
                "doc": "The various types of supported dataset lineage"
              },
              "name": "type",
              "doc": "The type of the lineage"
            },
            {
              "type": [
                "null",
                {
                  "type": "map",
                  "values": "string"
                }
              ],
              "name": "properties",
              "default": null,
              "doc": "A generic properties bag that allows us to store specific information on this graph edge."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "query",
              "default": null,
              "doc": "If the lineage is generated by a query, a reference to the query"
            },
            {
              "type": [
                "null",
                {
                  "type": "enum",
                  "symbolDocs": {
                    "EXACT": "The reference matched an existing entity exactly, including URN casing.",
                    "NORMALIZED": "The reference was case-normalized to match an existing entity whose URN uses\ndifferent casing (i.e. the reference was rewritten to heal a casing mismatch).",
                    "UNRESOLVED": "The reference is on a configured upstream platform but could not be resolved to\na single existing entity \u2014 either no entity matched (under any casing), or the\ncasing was ambiguous (multiple entities share the case-insensitive form). The\nreference was left unchanged; this flags potentially broken lineage."
                  },
                  "name": "LineageMatchType",
                  "namespace": "com.linkedin.dataset",
                  "symbols": [
                    "EXACT",
                    "NORMALIZED",
                    "UNRESOLVED"
                  ],
                  "doc": "How an upstream lineage reference's URN was resolved against the entities stored\nin DataHub. Populated by the lineage URN casing normalization processor for\nreferences on a configured upstream platform; absent when the reference is out of\nscope (platform not configured, feature disabled) or was ingested before the\nfeature was enabled.\n\nThis verdict reflects DataHub's knowledge AT THE TIME THE LINEAGE EDGE WAS\nINGESTED, not the current state of the graph. It is a point-in-time record and is\nnot re-evaluated automatically: e.g. a reference recorded as UNRESOLVED (its target\ndid not exist yet) keeps that value even after the target is later ingested and the\nedge in fact resolves exactly \u2014 the verdict only refreshes when the referencing\nsource is re-ingested."
                }
              ],
              "name": "matchType",
              "default": null,
              "doc": "How this upstream reference's URN was resolved against the entities stored in\nDataHub. Set by the lineage URN casing normalization processor: EXACT when the\nreference already matched an existing entity, NORMALIZED when it was rewritten to\nheal a casing mismatch, UNRESOLVED when it could not be resolved to a single\nexisting entity. Absent when no reconciliation was performed (out of scope).\nReflects DataHub's knowledge at ingestion time and is not re-evaluated later; see\nLineageMatchType."
            }
          ],
          "doc": "Upstream lineage information about a dataset including the source reporting the lineage"
        }
      },
      "name": "upstreams",
      "doc": "List of upstream dataset lineage information"
    },
    {
      "Relationship": {
        "/*/upstreams/*": {
          "entityTypes": [
            "dataset",
            "schemaField"
          ],
          "name": "DownstreamOf"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "FineGrainedLineage",
            "namespace": "com.linkedin.dataset",
            "fields": [
              {
                "type": {
                  "type": "enum",
                  "symbolDocs": {
                    "DATASET": " Indicates that this lineage is originating from upstream dataset(s)",
                    "FIELD_SET": " Indicates that this lineage is originating from upstream field(s)",
                    "NONE": " Indicates that there is no upstream lineage i.e. the downstream field is not a derived field"
                  },
                  "name": "FineGrainedLineageUpstreamType",
                  "namespace": "com.linkedin.dataset",
                  "symbols": [
                    "FIELD_SET",
                    "DATASET",
                    "NONE"
                  ],
                  "doc": "The type of upstream entity in a fine-grained lineage"
                },
                "name": "upstreamType",
                "doc": "The type of upstream entity"
              },
              {
                "Searchable": {
                  "/*": {
                    "fieldName": "fineGrainedUpstreams",
                    "fieldType": "URN",
                    "hasValuesFieldName": "hasFineGrainedUpstreams",
                    "queryByDefault": false
                  }
                },
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": "string"
                  }
                ],
                "name": "upstreams",
                "default": null,
                "doc": "Upstream entities in the lineage"
              },
              {
                "type": {
                  "type": "enum",
                  "symbolDocs": {
                    "FIELD": " Indicates that the lineage is for a single, specific, downstream field",
                    "FIELD_SET": " Indicates that the lineage is for a set of downstream fields"
                  },
                  "name": "FineGrainedLineageDownstreamType",
                  "namespace": "com.linkedin.dataset",
                  "symbols": [
                    "FIELD",
                    "FIELD_SET"
                  ],
                  "doc": "The type of downstream field(s) in a fine-grained lineage"
                },
                "name": "downstreamType",
                "doc": "The type of downstream field(s)"
              },
              {
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": "string"
                  }
                ],
                "name": "downstreams",
                "default": null,
                "doc": "Downstream fields in the lineage"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "transformOperation",
                "default": null,
                "doc": "The transform operation applied to the upstream entities to produce the downstream field(s)"
              },
              {
                "type": "float",
                "name": "confidenceScore",
                "default": 1.0,
                "doc": "The confidence in this lineage between 0 (low confidence) and 1 (high confidence)"
              },
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": [
                  "null",
                  "string"
                ],
                "name": "query",
                "default": null,
                "doc": "The query that was used to generate this lineage.\nPresent only if the lineage was generated from a detected query."
              },
              {
                "type": [
                  "null",
                  "com.linkedin.dataset.LineageMatchType"
                ],
                "name": "matchType",
                "default": null,
                "doc": "Aggregate of how the upstream field references' URNs were resolved against the\nentities stored in DataHub. Set by the lineage URN casing normalization processor:\nNORMALIZED if any field was rewritten to heal a casing mismatch, else UNRESOLVED if\nany could not be resolved, else EXACT. Absent when no reconciliation was performed\n(out of scope). Reflects DataHub's knowledge at ingestion time and is not\nre-evaluated later; see LineageMatchType."
              }
            ],
            "doc": "A fine-grained lineage from upstream fields/datasets to downstream field(s)"
          }
        }
      ],
      "name": "fineGrainedLineages",
      "default": null,
      "doc": " List of fine-grained lineage information, including field-level lineage"
    }
  ],
  "doc": "Upstream lineage of a dataset"
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





#### glossaryTerms
Related business terms information



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| terms | GlossaryTermAssociation[] | ✓ | The related business terms |  |
| auditStamp | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who reported the related business term |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "glossaryTerms"
  },
  "name": "GlossaryTerms",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "GlossaryTermAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "glossaryTerm"
                ],
                "name": "TermedWith"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "glossaryTerms",
                "fieldType": "URN",
                "filterNameOverride": "Glossary Term",
                "hasValuesFieldName": "hasGlossaryTerms",
                "includeSystemModifiedAt": true,
                "systemModifiedAtFieldName": "termsModifiedAt"
              },
              "java": {
                "class": "com.linkedin.common.urn.GlossaryTermUrn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied glossary term"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "actor",
              "default": null,
              "doc": "The user URN which will be credited for adding associating this term to the entity"
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
                  "fieldName": "termAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "termAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "termAttributionDates",
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
          "doc": "Properties of an applied glossary term."
        }
      },
      "name": "terms",
      "doc": "The related business terms"
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
      "name": "auditStamp",
      "doc": "Audit stamp containing who reported the related business term"
    }
  ],
  "doc": "Related business terms information"
}
```





#### semanticContent
Semantic content for enabling vector similarity search.

This aspect stores chunked text and embedding vectors for any entity that supports semantic search. Generation of the data for this aspect should be built in tight collaboration with the embedding generator during semantic search query processing. Chunk determination and generation can happen somewhat independently
The data in this aspect is directly passed along to the semantic search index.

Design notes:
- Supports multiple embedding models (e.g., different providers or versions)
- Supports chunked content for long documents
- Text field is optional to support privacy-sensitive use cases where
  only embeddings (not source text) are shared with DataHub



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| embeddings | map | ✓ | Map of embedding model name to embedding data. Key is the model identifier (e.g., cohere_embed_v3... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "semanticContent"
  },
  "name": "SemanticContent",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "map",
        "values": {
          "type": "record",
          "name": "EmbeddingModelData",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "string",
              "name": "modelVersion",
              "doc": "Full model identifier including provider.\nExamples: bedrock/cohere.embed-english-v3, openai/text-embedding-ada-002"
            },
            {
              "type": "long",
              "name": "generatedAt",
              "doc": "Timestamp when embeddings were generated (milliseconds since epoch)."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "chunkingStrategy",
              "default": null,
              "doc": "Description of the chunking strategy used.\nExamples: sentence_boundary_400t, fixed_512_chars, paragraph"
            },
            {
              "type": "int",
              "name": "totalChunks",
              "doc": "Total number of chunks."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "totalTokens",
              "default": null,
              "doc": "Estimated total token count across all chunks."
            },
            {
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "EmbeddingChunk",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "int",
                      "name": "position",
                      "doc": "Zero-based position/index of this chunk within the document."
                    },
                    {
                      "type": {
                        "type": "array",
                        "items": "float"
                      },
                      "name": "vector",
                      "doc": "The embedding vector for this chunk.\nDimensionality depends on the model (e.g., 1024 for Cohere v3)."
                    },
                    {
                      "type": [
                        "null",
                        "int"
                      ],
                      "name": "characterOffset",
                      "default": null,
                      "doc": "Character offset from start of source text.\nUsed for highlighting and navigation. Optional for privacy."
                    },
                    {
                      "type": [
                        "null",
                        "int"
                      ],
                      "name": "characterLength",
                      "default": null,
                      "doc": "Character length of the chunk in source text.\nOptional for privacy-sensitive use cases."
                    },
                    {
                      "type": [
                        "null",
                        "int"
                      ],
                      "name": "tokenCount",
                      "default": null,
                      "doc": "Estimated token count for this chunk."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "text",
                      "default": null,
                      "doc": "Original text of this chunk.\nOPTIONAL: May be omitted for privacy-sensitive data sources\nwhere only embeddings should be stored, not source text."
                    }
                  ],
                  "doc": "A single chunk of text with its embedding vector.\n\nChunks enable semantic search over long documents by breaking them\ninto smaller, semantically coherent pieces that fit within model\ncontext windows."
                }
              },
              "name": "chunks",
              "doc": "Individual chunks with their embedding vectors."
            }
          ],
          "doc": "Embedding data for a specific model.\nContains metadata about the embedding generation and the chunked vectors."
        }
      },
      "name": "embeddings",
      "doc": "Map of embedding model name to embedding data.\nKey is the model identifier (e.g., cohere_embed_v3, openai_ada_002).\nAllows storing embeddings from multiple models simultaneously."
    }
  ],
  "doc": "Semantic content for enabling vector similarity search.\n\nThis aspect stores chunked text and embedding vectors for any entity that supports semantic search. Generation of the data for this aspect should be built in tight collaboration with the embedding generator during semantic search query processing. Chunk determination and generation can happen somewhat independently\nThe data in this aspect is directly passed along to the semantic search index.\n\nDesign notes:\n- Supports multiple embedding models (e.g., different providers or versions)\n- Supports chunked content for long documents\n- Text field is optional to support privacy-sensitive use cases where\n  only embeddings (not source text) are shared with DataHub"
}
```





#### institutionalMemory
Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| elements | InstitutionalMemoryMetadata[] | ✓ | List of records that represent institutional memory of an entity. Each record consists of a link,... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "institutionalMemory"
  },
  "name": "InstitutionalMemory",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "InstitutionalMemoryMetadata",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.url.Url",
                "coercerClass": "com.linkedin.common.url.UrlCoercer"
              },
              "type": "string",
              "name": "url",
              "doc": "Link to an engineering design document or a wiki page."
            },
            {
              "type": "string",
              "name": "description",
              "doc": "Description of the link."
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
              "name": "createStamp",
              "doc": "Audit stamp associated with creation of this record"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "updateStamp",
              "default": null,
              "doc": "Audit stamp associated with updation of this record"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "InstitutionalMemoryMetadataSettings",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "boolean",
                      "name": "showInAssetPreview",
                      "default": false,
                      "doc": "Show record in asset preview like on entity header and search previews"
                    }
                  ],
                  "doc": "Settings related to a record of InstitutionalMemoryMetadata"
                }
              ],
              "name": "settings",
              "default": null,
              "doc": "Settings for this record"
            }
          ],
          "doc": "Metadata corresponding to a record of institutional memory."
        }
      },
      "name": "elements",
      "doc": "List of records that represent institutional memory of an entity. Each record consists of a link, description, creator and timestamps associated with that record."
    }
  ],
  "doc": "Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity."
}
```





#### domains
Links from an Asset to its Domains



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| domains | string[] | ✓ | The Domains attached to an Asset | Searchable, → AssociatedWith |
| domainAssociations | DomainAssociation[] |  | Additional per-domain association metadata such as attribution and propagation source. A superset... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "domains",
    "schemaVersion": 2
  },
  "name": "Domains",
  "namespace": "com.linkedin.domain",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "domain"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "domains",
          "fieldType": "URN",
          "filterNameOverride": "Domain",
          "hasValuesFieldName": "hasDomain"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "domains",
      "doc": "The Domains attached to an Asset"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "DomainAssociation",
            "namespace": "com.linkedin.domain",
            "fields": [
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "domain",
                "doc": "Urn of the associated domain. Corresponds to an entry in the parallel domains array."
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
                    "fieldName": "domainAttributionActors",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/source": {
                    "fieldName": "domainAttributionSources",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/time": {
                    "fieldName": "domainAttributionDates",
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
                "doc": "Information about who, why, and how this domain was applied.\nsourceDetail may carry flags such as 'propagated'='true' when set via glossary tree propagation."
              }
            ],
            "doc": "Properties of an applied domain association."
          }
        }
      ],
      "name": "domainAssociations",
      "default": null,
      "doc": "Additional per-domain association metadata such as attribution and propagation source.\nA superset of the domains field; entries correspond by domain URN.\nInitial migration handled by the DomainsMigrationMutator;\nthe two fields are kept in sync via the DomainsSyncMutationHook."
    }
  ],
  "doc": "Links from an Asset to its Domains"
}
```





#### incidentsSummary
Summary related incidents on an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| resolvedIncidents | string[] | ✓ | Resolved incidents for an asset Deprecated! Use the richer resolvedIncidentsDetails instead. | ⚠️ Deprecated |
| activeIncidents | string[] | ✓ | Active incidents for an asset Deprecated! Use the richer activeIncidentsDetails instead. | ⚠️ Deprecated |
| resolvedIncidentDetails | [IncidentSummaryDetails](#incidentsummarydetails)[] | ✓ | Summary details about the set of resolved incidents | Searchable, → ResolvedIncidents |
| activeIncidentDetails | [IncidentSummaryDetails](#incidentsummarydetails)[] | ✓ | Summary details about the set of active incidents | Searchable, → ActiveIncidents |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "incidentsSummary"
  },
  "name": "IncidentsSummary",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "resolvedIncidents",
      "default": [],
      "doc": "Resolved incidents for an asset\nDeprecated! Use the richer resolvedIncidentsDetails instead."
    },
    {
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "activeIncidents",
      "default": [],
      "doc": "Active incidents for an asset\nDeprecated! Use the richer activeIncidentsDetails instead."
    },
    {
      "Relationship": {
        "/*/urn": {
          "entityTypes": [
            "incident"
          ],
          "name": "ResolvedIncidents"
        }
      },
      "Searchable": {
        "/*/createdAt": {
          "fieldName": "resolvedIncidentCreatedTimes",
          "fieldType": "DATETIME"
        },
        "/*/priority": {
          "fieldName": "resolvedIncidentPriorities",
          "fieldType": "COUNT"
        },
        "/*/resolvedAt": {
          "fieldName": "resolvedIncidentResolvedTimes",
          "fieldType": "DATETIME"
        },
        "/*/type": {
          "fieldName": "resolvedIncidentTypes",
          "fieldType": "KEYWORD"
        },
        "/*/urn": {
          "fieldName": "resolvedIncidents",
          "fieldType": "URN",
          "hasValuesFieldName": "hasResolvedIncidents",
          "numValuesFieldName": "numResolvedIncidents",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "IncidentSummaryDetails",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "urn",
              "doc": "The urn of the incident"
            },
            {
              "type": "string",
              "name": "type",
              "doc": "The type of an incident"
            },
            {
              "type": "long",
              "name": "createdAt",
              "doc": "The time at which the incident was raised in milliseconds since epoch."
            },
            {
              "type": [
                "null",
                "long"
              ],
              "name": "resolvedAt",
              "default": null,
              "doc": "The time at which the incident was marked as resolved in milliseconds since epoch. Null if the incident is still active."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "priority",
              "default": null,
              "doc": "The priority of the incident"
            }
          ],
          "doc": "Summary statistics about incidents on an entity."
        }
      },
      "name": "resolvedIncidentDetails",
      "default": [],
      "doc": "Summary details about the set of resolved incidents"
    },
    {
      "Relationship": {
        "/*/urn": {
          "entityTypes": [
            "incident"
          ],
          "name": "ActiveIncidents"
        }
      },
      "Searchable": {
        "/*/createdAt": {
          "fieldName": "activeIncidentCreatedTimes",
          "fieldType": "DATETIME"
        },
        "/*/priority": {
          "fieldName": "activeIncidentPriorities",
          "fieldType": "COUNT"
        },
        "/*/type": {
          "fieldName": "activeIncidentTypes",
          "fieldType": "KEYWORD"
        },
        "/*/urn": {
          "addHasValuesToFilters": true,
          "fieldName": "activeIncidents",
          "fieldType": "URN",
          "hasValuesFieldName": "hasActiveIncidents",
          "numValuesFieldName": "numActiveIncidents",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.IncidentSummaryDetails"
      },
      "name": "activeIncidentDetails",
      "default": [],
      "doc": "Summary details about the set of active incidents"
    }
  ],
  "doc": "Summary related incidents on an entity."
}
```





#### versionProperties
Properties about a versioned asset i.e. dataset, ML Model, etc.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| versionSet | string | ✓ | The linked Version Set entity that ties multiple versioned assets together | Searchable, → VersionOf |
| version | [VersionTag](#versiontag) | ✓ | Label for this versioned asset, is unique within a version set | Searchable |
| aliases | [VersionTag](#versiontag)[] | ✓ | Associated aliases for this versioned asset | Searchable |
| comment | string |  | Comment documenting what this version was created for, changes, or represents |  |
| sortId | string | ✓ | Sort identifier that determines where a version lives in the order of the Version Set. What this ... | Searchable (versionSortId) |
| versioningScheme | VersioningScheme | ✓ | What versioning scheme `sortId` belongs to. Defaults to a plain string that is lexicographically ... |  |
| sourceCreatedTimestamp | [AuditStamp](#auditstamp) |  | Timestamp reflecting when this asset version was created in the source system. |  |
| metadataCreatedTimestamp | [AuditStamp](#auditstamp) |  | Timestamp reflecting when the metadata for this version was created in DataHub |  |
| isLatest | boolean |  | Marks whether this version is currently the latest. Set by a side effect and should not be modifi... | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "versionProperties"
  },
  "name": "VersionProperties",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "versionSet"
        ],
        "name": "VersionOf"
      },
      "Searchable": {
        "queryByDefault": false
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "versionSet",
      "doc": "The linked Version Set entity that ties multiple versioned assets together"
    },
    {
      "Searchable": {
        "/versionTag": {
          "fieldName": "version",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "record",
        "name": "VersionTag",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": [
              "null",
              "string"
            ],
            "name": "versionTag",
            "default": null
          },
          {
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
            "name": "metadataAttribution",
            "default": null
          }
        ],
        "doc": "A resource-defined string representing the resource state for the purpose of concurrency control"
      },
      "name": "version",
      "doc": "Label for this versioned asset, is unique within a version set"
    },
    {
      "Searchable": {
        "/*/versionTag": {
          "fieldName": "aliases",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.VersionTag"
      },
      "name": "aliases",
      "default": [],
      "doc": "Associated aliases for this versioned asset"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "comment",
      "default": null,
      "doc": "Comment documenting what this version was created for, changes, or represents"
    },
    {
      "Searchable": {
        "fieldName": "versionSortId",
        "queryByDefault": false
      },
      "type": "string",
      "name": "sortId",
      "doc": "Sort identifier that determines where a version lives in the order of the Version Set.\nWhat this looks like depends on the Version Scheme. For sort ids generated by DataHub we use an 8 character string representation."
    },
    {
      "type": {
        "type": "enum",
        "symbolDocs": {
          "ALPHANUMERIC_GENERATED_BY_DATAHUB": "String managed by DataHub. Currently, an 8 character alphabetical string.",
          "LEXICOGRAPHIC_STRING": "String sorted lexicographically."
        },
        "name": "VersioningScheme",
        "namespace": "com.linkedin.versionset",
        "symbols": [
          "LEXICOGRAPHIC_STRING",
          "ALPHANUMERIC_GENERATED_BY_DATAHUB"
        ]
      },
      "name": "versioningScheme",
      "default": "LEXICOGRAPHIC_STRING",
      "doc": "What versioning scheme `sortId` belongs to.\nDefaults to a plain string that is lexicographically sorted."
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
      "name": "sourceCreatedTimestamp",
      "default": null,
      "doc": "Timestamp reflecting when this asset version was created in the source system."
    },
    {
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "metadataCreatedTimestamp",
      "default": null,
      "doc": "Timestamp reflecting when the metadata for this version was created in DataHub"
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
      "name": "isLatest",
      "default": null,
      "doc": "Marks whether this version is currently the latest. Set by a side effect and should not be modified by API."
    }
  ],
  "doc": "Properties about a versioned asset i.e. dataset, ML Model, etc."
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

#### IncidentSummaryDetails

Summary statistics about incidents on an entity.

**Fields:**

- `urn` (string): The urn of the incident
- `type` (string): The type of an incident
- `createdAt` (long): The time at which the incident was raised in milliseconds since epoch.
- `resolvedAt` (long?): The time at which the incident was marked as resolved in milliseconds since e...
- `priority` (int?): The priority of the incident

#### VersionTag

A resource-defined string representing the resource state for the purpose of concurrency control

**Fields:**

- `versionTag` (string?): 
- `metadataAttribution` (MetadataAttribution?): 


### Relationships

#### Self
These are the relationships to itself, stored in this entity's aspects
- ClonedFromAgent (via `aiAgentInfo.source.clonedFrom`)
#### Outgoing
These are the relationships stored in this entity's aspects
- AgentHasSkill

   - AgentSkill via `aiAgentDependencies.skills`
- AgentUsesTool

   - Api via `aiAgentDependencies.tools`
- AgentUsesModel

   - MlModel via `aiAgentDependencies.models`
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- DownstreamOf

   - Dataset via `upstreamLineage.upstreams.dataset`
   - Dataset via `upstreamLineage.fineGrainedLineages`
   - SchemaField via `upstreamLineage.fineGrainedLineages`
- TaggedWith

   - Tag via `globalTags.tags`
- TermedWith

   - GlossaryTerm via `glossaryTerms.terms.urn`
- AssociatedWith

   - Domain via `domains.domains`
- ResolvedIncidents

   - Incident via `incidentsSummary.resolvedIncidentDetails`
- ActiveIncidents

   - Incident via `incidentsSummary.activeIncidentDetails`
- VersionOf

   - VersionSet via `versionProperties.versionSet`
#### Incoming
These are the relationships stored in other entity's aspects
- IncidentOn

   - Incident via `incidentInfo.entities`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
