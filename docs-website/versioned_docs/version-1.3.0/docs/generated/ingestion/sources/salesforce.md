---
sidebar_position: 62
title: Salesforce
slug: /generated/ingestion/sources/salesforce
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/salesforce.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Salesforce
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ✅ | Only table level profiling is supported via `profiling.enabled` config field. Supported for types - Table. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Domains](../../../domains.md) | ✅ | Supported via the `domain` config field. |
| Extract Tags | ✅ | Enabled by default. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Can be equivalent to Salesforce organization. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Extract table-level lineage for Salesforce objects. Supported for types - Custom Object, Object. |


### Prerequisites

In order to ingest metadata from Salesforce, you will need one of:

- Salesforce username, password, [security token](https://developer.Salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_concepts_security.htm)
- Salesforce username, consumer key and private key for [JSON web token access](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5)
- Salesforce instance url and access token/session id (suitable for one-shot ingestion only, as access token typically expires after 2 hours of inactivity)

The account used to access Salesforce requires the following permissions for this integration to work:

- View Setup and Configuration
- View All Data

## Integration Details

This plugin extracts Salesforce Standard and Custom Objects and their details (fields, record count, etc) from a Salesforce instance.
Python library [simple-salesforce](https://pypi.org/project/simple-salesforce/) is used for authenticating and calling [Salesforce REST API](https://developer.Salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_what_is_rest_api.htm) to retrive details from Salesforce instance.

### REST API Resources used in this integration

- [Versions](https://developer.Salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_versions.htm)
- [Tooling API Query](https://developer.salesforce.com/docs/atlas.en-us.api_tooling.meta/api_tooling/intro_rest_resources.htm) on objects EntityDefinition, EntityParticle, CustomObject, CustomField
- [Record Count](https://developer.Salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_record_count.htm)

### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept  | DataHub Concept                                           | Notes                     |
| --------------- | --------------------------------------------------------- | ------------------------- |
| `Salesforce`    | [Data Platform](../../metamodel/entities/dataPlatform.md) |                           |
| Standard Object | [Dataset](../../metamodel/entities/dataset.md)            | subtype "Standard Object" |
| Custom Object   | [Dataset](../../metamodel/entities/dataset.md)            | subtype "Custom Object"   |

### Caveats

- This connector has only been tested with Salesforce Developer Edition.
- This connector only supports table level profiling (Row and Column counts) as of now. Row counts are approximate as returned by [Salesforce RecordCount REST API](https://developer.Salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_record_count.htm).
- This integration does not support ingesting Salesforce [External Objects](https://developer.Salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_external_objects.htm)

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
pipeline_name: my_salesforce_pipeline
source:
  type: "salesforce"
  config:
    instance_url: "https://mydomain.my.salesforce.com/"
    username: user@company
    password: password_for_user
    security_token: security_token_for_user
    platform_instance: mydomain-dev-ed
    domain:
      sales:
        allow:
          - "Opportunity$"
          - "Lead$"

    object_pattern:
      allow:
        - "Account$"
        - "Opportunity$"
        - "Lead$"

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">access_token</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Access token for instance url <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">api_version</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | If specified, overrides default version used by the Salesforce package. Example value: '59.0' <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">auth</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "USERNAME_PASSWORD", "DIRECT_ACCESS_TOKEN", "JSON_WEB_TOKEN"  |
| <div className="path-line"><span className="path-main">consumer_key</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Consumer key for Salesforce JSON web token access <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">ingest_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest Tags from source. This will override Tags entered from UI <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">instance_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Salesforce instance url. e.g. https://MyDomainName.my.salesforce.com <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">is_sandbox</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Connect to Sandbox instance of your Salesforce <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Password for Salesforce user <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |  <div className="default-line ">Default: <span className="default-value">salesforce</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">private_key</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Private key as a string for Salesforce JSON web token access <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">security_token</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Security token for Salesforce username <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">use_referenced_entities_as_upstreams</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | (Experimental) If enabled, referenced entities will be treated as upstream entities. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Salesforce username <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">object_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">object_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">profile_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">SalesforceProfilingConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether profiling should be done. Supports only table-level profiling at this stage <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">operation_config</span></div> <div className="type-name-line"><span className="type-name">OperationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">lower_freq_profile_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_date_of_month</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_day_of_week</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulIngestionConfig, null</span></div> | Stateful Ingestion Config <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |

</div>


</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "OperationConfig": {
      "additionalProperties": false,
      "properties": {
        "lower_freq_profile_enabled": {
          "default": false,
          "description": "Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling.",
          "title": "Lower Freq Profile Enabled",
          "type": "boolean"
        },
        "profile_day_of_week": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Day Of Week"
        },
        "profile_date_of_month": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Date Of Month"
        }
      },
      "title": "OperationConfig",
      "type": "object"
    },
    "SalesforceAuthType": {
      "enum": [
        "USERNAME_PASSWORD",
        "DIRECT_ACCESS_TOKEN",
        "JSON_WEB_TOKEN"
      ],
      "title": "SalesforceAuthType",
      "type": "string"
    },
    "SalesforceProfilingConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether profiling should be done. Supports only table-level profiling at this stage",
          "title": "Enabled",
          "type": "boolean"
        },
        "operation_config": {
          "$ref": "#/$defs/OperationConfig",
          "description": "Experimental feature. To specify operation configs."
        }
      },
      "title": "SalesforceProfilingConfig",
      "type": "object"
    },
    "StatefulIngestionConfig": {
      "additionalProperties": false,
      "description": "Basic Stateful Ingestion Specific Configuration for any source.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        }
      },
      "title": "StatefulIngestionConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulIngestionConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Stateful Ingestion Config"
    },
    "platform": {
      "default": "salesforce",
      "title": "Platform",
      "type": "string"
    },
    "auth": {
      "$ref": "#/$defs/SalesforceAuthType",
      "default": "USERNAME_PASSWORD"
    },
    "username": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Salesforce username",
      "title": "Username"
    },
    "password": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Password for Salesforce user",
      "title": "Password"
    },
    "consumer_key": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Consumer key for Salesforce JSON web token access",
      "title": "Consumer Key"
    },
    "private_key": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Private key as a string for Salesforce JSON web token access",
      "title": "Private Key"
    },
    "security_token": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Security token for Salesforce username",
      "title": "Security Token"
    },
    "instance_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Salesforce instance url. e.g. https://MyDomainName.my.salesforce.com",
      "title": "Instance Url"
    },
    "is_sandbox": {
      "default": false,
      "description": "Connect to Sandbox instance of your Salesforce",
      "title": "Is Sandbox",
      "type": "boolean"
    },
    "access_token": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Access token for instance url",
      "title": "Access Token"
    },
    "ingest_tags": {
      "default": false,
      "description": "Ingest Tags from source. This will override Tags entered from UI",
      "title": "Ingest Tags",
      "type": "boolean"
    },
    "object_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for Salesforce objects to filter in ingestion."
    },
    "domain": {
      "additionalProperties": {
        "$ref": "#/$defs/AllowDenyPattern"
      },
      "default": {},
      "description": "Regex patterns for tables/schemas to describe domain_key domain key (domain_key can be any string like \"sales\".) There can be multiple domain keys specified.",
      "title": "Domain",
      "type": "object"
    },
    "api_version": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "If specified, overrides default version used by the Salesforce package. Example value: '59.0'",
      "title": "Api Version"
    },
    "profiling": {
      "$ref": "#/$defs/SalesforceProfilingConfig",
      "default": {
        "enabled": false,
        "operation_config": {
          "lower_freq_profile_enabled": false,
          "profile_date_of_month": null,
          "profile_day_of_week": null
        }
      }
    },
    "profile_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for profiles to filter in ingestion, allowed by the `object_pattern`."
    },
    "use_referenced_entities_as_upstreams": {
      "default": false,
      "description": "(Experimental) If enabled, referenced entities will be treated as upstream entities.",
      "title": "Use Referenced Entities As Upstreams",
      "type": "boolean"
    }
  },
  "title": "SalesforceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.salesforce.SalesforceSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/salesforce.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for Salesforce, feel free to ping us on [our Slack](https://datahub.com/slack).
