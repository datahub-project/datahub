---
sidebar_position: 44
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

| Capability                                                                                                 | Status | Notes                                                                        |
| ---------------------------------------------------------------------------------------------------------- | ------ | ---------------------------------------------------------------------------- |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md)                           | ✅     | Only table level profiling is supported via `profiling.enabled` config field |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ❌     | Not supported yet                                                            |
| [Domains](../../../domains.md)                                                                             | ✅     | Supported via the `domain` config field                                      |
| [Platform Instance](../../../platform-instances.md)                                                        | ✅     | Can be equivalent to Salesforce organization                                 |

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

#### Install the Plugin

```shell
pip install 'acryl-datahub[salesforce]'
```

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

| Field                                                                                                                                                                                                                 | Description                                                                                                                                                                                                                                                                                                          |
| :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">access_token</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                      | Access token for instance url                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">auth</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>                                                                | <div className="default-line ">Default: <span className="default-value">USERNAME_PASSWORD</span></div>                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">consumer_key</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                      | Consumer key for Salesforce JSON web token access                                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-main">ingest_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                      | Ingest Tags from source. This will override Tags entered from UI <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                    |
| <div className="path-line"><span className="path-main">instance_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                      | Salesforce instance url. e.g. https://MyDomainName.my.salesforce.com                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-main">is_sandbox</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                       | Connect to Sandbox instance of your Salesforce <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                      |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                          | Password for Salesforce user                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                          | <div className="default-line ">Default: <span className="default-value">salesforce</span></div>                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                 | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">private_key</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                       | Private key as a string for Salesforce JSON web token access                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">security_token</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                    | Security token for Salesforce username                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                          | Salesforce username                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                               | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                                                 |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div>                                         | A class to store allow deny regexes                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>    |                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>     |                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>     | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                          |
| <div className="path-line"><span className="path-main">object_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                          | Regex patterns for Salesforce objects to filter in ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                        |
| <div className="path-line"><span className="path-prefix">object_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>  |                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">object_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>   |                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">object_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>   | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                          |
| <div className="path-line"><span className="path-main">profile_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                         | Regex patterns for profiles to filter in ingestion, allowed by the `object_pattern`. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div> |                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>  |                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>  | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                          |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">SalesforceProfilingConfig</span></div>                                      | <div className="default-line ">Default: <span className="default-value">&#123;&#x27;enabled&#x27;: False&#125;</span></div>                                                                                                                                                                                          |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>           | Whether profiling should be done. Supports only table-level profiling at this stage <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                 |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "SalesforceConfig",
  "description": "Any source that is a primary producer of Dataset metadata should inherit this class",
  "type": "object",
  "properties": {
    "env": {
      "title": "Env",
      "description": "The environment that all assets produced by this connector belong to",
      "default": "PROD",
      "type": "string"
    },
    "platform_instance": {
      "title": "Platform Instance",
      "description": "The instance of the platform that all assets produced by this recipe belong to",
      "type": "string"
    },
    "auth": {
      "default": "USERNAME_PASSWORD",
      "allOf": [
        {
          "$ref": "#/definitions/SalesforceAuthType"
        }
      ]
    },
    "username": {
      "title": "Username",
      "description": "Salesforce username",
      "type": "string"
    },
    "password": {
      "title": "Password",
      "description": "Password for Salesforce user",
      "type": "string"
    },
    "consumer_key": {
      "title": "Consumer Key",
      "description": "Consumer key for Salesforce JSON web token access",
      "type": "string"
    },
    "private_key": {
      "title": "Private Key",
      "description": "Private key as a string for Salesforce JSON web token access",
      "type": "string"
    },
    "security_token": {
      "title": "Security Token",
      "description": "Security token for Salesforce username",
      "type": "string"
    },
    "instance_url": {
      "title": "Instance Url",
      "description": "Salesforce instance url. e.g. https://MyDomainName.my.salesforce.com",
      "type": "string"
    },
    "is_sandbox": {
      "title": "Is Sandbox",
      "description": "Connect to Sandbox instance of your Salesforce",
      "default": false,
      "type": "boolean"
    },
    "access_token": {
      "title": "Access Token",
      "description": "Access token for instance url",
      "type": "string"
    },
    "ingest_tags": {
      "title": "Ingest Tags",
      "description": "Ingest Tags from source. This will override Tags entered from UI",
      "default": false,
      "type": "boolean"
    },
    "object_pattern": {
      "title": "Object Pattern",
      "description": "Regex patterns for Salesforce objects to filter in ingestion.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "domain": {
      "title": "Domain",
      "description": "Regex patterns for tables/schemas to describe domain_key domain key (domain_key can be any string like \"sales\".) There can be multiple domain keys specified.",
      "default": {},
      "type": "object",
      "additionalProperties": {
        "$ref": "#/definitions/AllowDenyPattern"
      }
    },
    "profiling": {
      "title": "Profiling",
      "default": {
        "enabled": false
      },
      "allOf": [
        {
          "$ref": "#/definitions/SalesforceProfilingConfig"
        }
      ]
    },
    "profile_pattern": {
      "title": "Profile Pattern",
      "description": "Regex patterns for profiles to filter in ingestion, allowed by the `object_pattern`.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "platform": {
      "title": "Platform",
      "default": "salesforce",
      "type": "string"
    }
  },
  "additionalProperties": false,
  "definitions": {
    "SalesforceAuthType": {
      "title": "SalesforceAuthType",
      "description": "An enumeration.",
      "enum": [
        "USERNAME_PASSWORD",
        "DIRECT_ACCESS_TOKEN",
        "JSON_WEB_TOKEN"
      ]
    },
    "AllowDenyPattern": {
      "title": "AllowDenyPattern",
      "description": "A class to store allow deny regexes",
      "type": "object",
      "properties": {
        "allow": {
          "title": "Allow",
          "description": "List of regex patterns to include in ingestion",
          "default": [
            ".*"
          ],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "deny": {
          "title": "Deny",
          "description": "List of regex patterns to exclude from ingestion.",
          "default": [],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "ignoreCase": {
          "title": "Ignorecase",
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    },
    "SalesforceProfilingConfig": {
      "title": "SalesforceProfilingConfig",
      "type": "object",
      "properties": {
        "enabled": {
          "title": "Enabled",
          "description": "Whether profiling should be done. Supports only table-level profiling at this stage",
          "default": false,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    }
  }
}
```

</TabItem>
</Tabs>

### Code Coordinates

- Class Name: `datahub.ingestion.source.salesforce.SalesforceSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/salesforce.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Salesforce, feel free to ping us on [our Slack](https://slack.datahubproject.io).
