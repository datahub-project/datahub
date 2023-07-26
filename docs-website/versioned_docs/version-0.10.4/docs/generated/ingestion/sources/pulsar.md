---
sidebar_position: 39
title: Pulsar
slug: /generated/ingestion/sources/pulsar
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/pulsar.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Pulsar

## Integration Details

<!-- Plain-language description of what this integration is meant to do.  -->
<!-- Include details about where metadata is extracted from (ie. logs, source API, manifest, etc.)   -->

The Datahub Pulsar source plugin extracts `topic` and `schema` metadata from an Apache Pulsar instance and ingest the information into Datahub. The plugin uses the [Pulsar admin Rest API interface](https://pulsar.apache.org/admin-rest-api/#) to interact with the Pulsar instance. The following APIs are used in order to:

- [Get the list of existing tenants](https://pulsar.apache.org/admin-rest-api/#tag/tenants)
- [Get the list of namespaces associated with each tenant](https://pulsar.apache.org/admin-rest-api/#tag/namespaces)
- [Get the list of topics associated with each namespace](https://pulsar.apache.org/admin-rest-api/#tag/persistent-topic)
  - persistent topics
  - persistent partitioned topics
  - non-persistent topics
  - non-persistent partitioned topics
- [Get the latest schema associated with each topic](https://pulsar.apache.org/admin-rest-api/#tag/schemas)

The data is extracted on `tenant` and `namespace` basis, topics with corresponding schema (if available) are ingested as [Dataset](docs/generated/metamodel/entities/dataset.md) into Datahub. Some additional values like `schema description`, `schema_version`, `schema_type` and `partitioned` are included as `DatasetProperties`.

### Concept Mapping

<!-- This should be a manual mapping of concepts from the source to the DataHub Metadata Model -->
<!-- Authors should provide as much context as possible about how this mapping was generated, including assumptions made, known shortcuts, & any other caveats -->

This ingestion source maps the following Source System Concepts to DataHub Concepts:

<!-- Remove all unnecessary/irrelevant DataHub Concepts -->

| Source Concept | DataHub Concept                                                    | Notes                                                                     |
| -------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------- |
| `pulsar`       | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md) |                                                                           |
| Pulsar Topic   | [Dataset](docs/generated/metamodel/entities/dataset.md)            | _subType_: `topic`                                                        |
| Pulsar Schema  | [SchemaField](docs/generated/metamodel/entities/schemaField.md)    | Maps to the fields defined within the `Avro` or `JSON` schema definition. |

## Metadata Ingestion Quickstart

For context on getting started with ingestion, check out our [metadata ingestion guide](../../../../metadata-ingestion/README.md).
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)

### Important Capabilities

| Capability                                          | Status | Notes                                   |
| --------------------------------------------------- | ------ | --------------------------------------- |
| [Domains](../../../domains.md)                      | ✅     | Supported via the `domain` config field |
| [Platform Instance](../../../platform-instances.md) | ✅     | Enabled by default                      |

PulsarSource(config: datahub.ingestion.source_config.pulsar.PulsarSourceConfig, ctx: datahub.ingestion.api.common.PipelineContext)

> **_NOTE:_** Always use TLS encryption in a production environment and use variable substitution for sensitive information (e.g. ${CLIENT_ID} and ${CLIENT_SECRET}).

### Prerequisites

In order to ingest metadata from Apache Pulsar, you will need:

- Access to a Pulsar Instance, if authentication is enabled a valid access token.
- Pulsar version >= 2.7.0

> **_NOTE:_** A _superUser_ role is required for listing all existing tenants within a Pulsar instance.

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[pulsar]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: "pulsar"
  config:
    env: "TEST"
    platform_instance: "local"
    ## Pulsar client connection config ##
    web_service_url: "https://localhost:8443"
    verify_ssl: "/opt/certs/ca.cert.pem"
    # Issuer url for auth document, for example "http://localhost:8083/realms/pulsar"
    issuer_url: <issuer_url>
    client_id: ${CLIENT_ID}
    client_secret: ${CLIENT_SECRET}
    # Tenant list to scrape
    tenants:
      - tenant_1
      - tenant_2
    # Topic filter pattern
    topic_patterns:
      allow:
        - ".*sales.*"

sink:
# sink configs
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                              | Description                                                                                                                                                                                                                                                                                                                                                      |
| :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">client_id</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                      | The application's client ID                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">client_secret</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                  | The application's client secret                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">exclude_individual_partitions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                 | Extract each individual partitioned topic. e.g. when turned off a topic with 100 partitions will result in 100 Datasets. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                         |
| <div className="path-line"><span className="path-main">issuer_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                     | The complete URL for a Custom Authorization Server. Mandatory for OAuth based authentication.                                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-main">oid_config</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                                                     | Placeholder for OpenId discovery document                                                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                              | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-main">tenants</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                 |                                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">timeout</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                       | Timout setting, how long to wait for the Pulsar rest api to send data before giving up <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div>                                                                                                                                                              |
| <div className="path-line"><span className="path-main">token</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                          | The access token for the application. Mandatory for token based authentication.                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">verify_ssl</span></div> <div className="type-name-line"><span className="type-name">One of boolean, string</span></div>                                                     | Either a boolean, in which case it controls whether we verify the server's TLS certificate, or a string, in which case it must be a path to a CA bundle to use. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                  |
| <div className="path-line"><span className="path-main">web_service_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                | The web URL for the cluster. <div className="default-line default-line-with-docs">Default: <span className="default-value">http://localhost:8080</span></div>                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                            | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                                                                                             |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div>                                                      | A class to store allow deny regexes                                                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                 |                                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                  |                                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                  | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">namespace_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                   | List of regex patterns for namespaces to include/exclude from ingestion. By default the functions namespace is denied. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#x27;public/functions&#x27;&#93;, &#x27;i...</span></div>       |
| <div className="path-line"><span className="path-prefix">namespace_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>           |                                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-prefix">namespace_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>            |                                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-prefix">namespace_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>            | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">tenant_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                      | List of regex patterns for tenants to include/exclude from ingestion. By default all tenants are allowed. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#x27;pulsar&#x27;&#93;, &#x27;ignoreCase&#x27;...</span></div>               |
| <div className="path-line"><span className="path-prefix">tenant_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>              |                                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-prefix">tenant_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>               |                                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-prefix">tenant_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>               | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">topic_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                       | List of regex patterns for topics to include/exclude from ingestion. By default the Pulsar system topics are denied. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#x27;/\_\_.\*$&#x27;&#93;, &#x27;ignoreCase&#x27;...</span></div> |
| <div className="path-line"><span className="path-prefix">topic_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>               |                                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-prefix">topic_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                |                                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-prefix">topic_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                 | see Stateful Ingestion                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>               | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                     |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "PulsarSourceConfig",
  "description": "Base configuration class for stateful ingestion for source configs to inherit from.",
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
    "stateful_ingestion": {
      "title": "Stateful Ingestion",
      "description": "see Stateful Ingestion",
      "allOf": [
        {
          "$ref": "#/definitions/StatefulStaleMetadataRemovalConfig"
        }
      ]
    },
    "web_service_url": {
      "title": "Web Service Url",
      "description": "The web URL for the cluster.",
      "default": "http://localhost:8080",
      "type": "string"
    },
    "timeout": {
      "title": "Timeout",
      "description": "Timout setting, how long to wait for the Pulsar rest api to send data before giving up",
      "default": 5,
      "type": "integer"
    },
    "issuer_url": {
      "title": "Issuer Url",
      "description": "The complete URL for a Custom Authorization Server. Mandatory for OAuth based authentication.",
      "type": "string"
    },
    "client_id": {
      "title": "Client Id",
      "description": "The application's client ID",
      "type": "string"
    },
    "client_secret": {
      "title": "Client Secret",
      "description": "The application's client secret",
      "type": "string"
    },
    "token": {
      "title": "Token",
      "description": "The access token for the application. Mandatory for token based authentication.",
      "type": "string"
    },
    "verify_ssl": {
      "title": "Verify Ssl",
      "description": "Either a boolean, in which case it controls whether we verify the server's TLS certificate, or a string, in which case it must be a path to a CA bundle to use.",
      "default": true,
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "string"
        }
      ]
    },
    "tenant_patterns": {
      "title": "Tenant Patterns",
      "description": "List of regex patterns for tenants to include/exclude from ingestion. By default all tenants are allowed.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [
          "pulsar"
        ],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "namespace_patterns": {
      "title": "Namespace Patterns",
      "description": "List of regex patterns for namespaces to include/exclude from ingestion. By default the functions namespace is denied.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [
          "public/functions"
        ],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "topic_patterns": {
      "title": "Topic Patterns",
      "description": "List of regex patterns for topics to include/exclude from ingestion. By default the Pulsar system topics are denied.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [
          "/__.*$"
        ],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "exclude_individual_partitions": {
      "title": "Exclude Individual Partitions",
      "description": "Extract each individual partitioned topic. e.g. when turned off a topic with 100 partitions will result in 100 Datasets.",
      "default": true,
      "type": "boolean"
    },
    "tenants": {
      "title": "Tenants",
      "description": "Listing all tenants requires superUser role, alternative you can set a list of tenants you want to scrape using the tenant admin role",
      "default": [],
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "domain": {
      "title": "Domain",
      "description": "Domain patterns",
      "type": "object",
      "additionalProperties": {
        "$ref": "#/definitions/AllowDenyPattern"
      }
    },
    "oid_config": {
      "title": "Oid Config",
      "description": "Placeholder for OpenId discovery document",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "definitions": {
    "DynamicTypedStateProviderConfig": {
      "title": "DynamicTypedStateProviderConfig",
      "type": "object",
      "properties": {
        "type": {
          "title": "Type",
          "description": "The type of the state provider to use. For DataHub use `datahub`",
          "type": "string"
        },
        "config": {
          "title": "Config",
          "description": "The configuration required for initializing the state provider. Default: The datahub_api config if set at pipeline level. Otherwise, the default DatahubClientConfig. See the defaults (https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/graph/client.py#L19)."
        }
      },
      "required": [
        "type"
      ],
      "additionalProperties": false
    },
    "StatefulStaleMetadataRemovalConfig": {
      "title": "StatefulStaleMetadataRemovalConfig",
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "type": "object",
      "properties": {
        "enabled": {
          "title": "Enabled",
          "description": "The type of the ingestion state provider registered with datahub.",
          "default": false,
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "title": "Remove Stale Metadata",
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
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
    }
  }
}
```

</TabItem>
</Tabs>

### Code Coordinates

- Class Name: `datahub.ingestion.source.pulsar.PulsarSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/pulsar.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Pulsar, feel free to ping us on [our Slack](https://slack.datahubproject.io).
