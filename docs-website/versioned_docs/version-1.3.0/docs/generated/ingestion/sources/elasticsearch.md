---
sidebar_position: 21
title: Elasticsearch
slug: /generated/ingestion/sources/elasticsearch
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/elasticsearch.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Elasticsearch
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |


This plugin extracts the following:

- Metadata for indexes
- Column types associated with each index field


### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: "elasticsearch"
  config:
    # Coordinates
    host: 'localhost:9200'

    # Credentials
    username: user # optional
    password: pass # optional

    # SSL support
    use_ssl: False
    verify_certs: False
    ca_certs: "./path/ca.cert"
    client_cert: "./path/client.cert"
    client_key: "./path/client.key"
    ssl_assert_hostname: False
    ssl_assert_fingerprint: "./path/cert.fingerprint"

    # Options
    url_prefix: "" # optional url_prefix
    env: "PROD"
    index_pattern:
      allow: [".*some_index_name_pattern*"]
      deny: [".*skip_index_name_pattern*"]
    ingest_index_templates: False
    index_template_pattern:
      allow: [".*some_index_template_name_pattern*"]

sink:
# sink configs

```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">api_key</span></div> <div className="type-name-line"><span className="type-name">One of object, string, null</span></div> | API Key authentication. Accepts either a list with id and api_key (UTF-8 representation), or a base64 encoded string of id and api_key combined by ':'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">ca_certs</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Path to a certificate authority (CA) certificate. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">client_cert</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Path to the file containing the private key and the certificate, or cert only if using client_key. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">client_key</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Path to the file containing the private key if using separate cert and key files. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">host</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The elastic search host URI. <div className="default-line default-line-with-docs">Default: <span className="default-value">localhost:9200</span></div> |
| <div className="path-line"><span className="path-main">ingest_index_templates</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingests ES index templates if enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The password credential. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">ssl_assert_fingerprint</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Verify the supplied certificate fingerprint if not None. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">ssl_assert_hostname</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use hostname verification if not False. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">url_prefix</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | There are cases where an enterprise would have multiple elastic search clusters. One way for them to manage is to have a single endpoint for all the elastic search clusters and use url_prefix for routing requests to different clusters. <div className="default-line default-line-with-docs">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-main">use_ssl</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to use SSL for the connection or not. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The username credential. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">verify_certs</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to verify SSL certificates. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">collapse_urns</span></div> <div className="type-name-line"><span className="type-name">CollapseUrns</span></div> |   |
| <div className="path-line"><span className="path-prefix">collapse_urns.</span><span className="path-main">urns_suffix_regex</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to remove from the name of the URN. All of the indices before removal of URNs are considered as the same dataset. These are applied in order for each URN. <br />         The main case where you would want to have multiple of these if the name where you are trying to remove suffix from have different formats. <br />         e.g. ending with -YYYY-MM-DD as well as ending -epochtime would require you to have 2 regex patterns to remove the suffixes across all URNs.  |
| <div className="path-line"><span className="path-prefix">collapse_urns.urns_suffix_regex.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">index_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">index_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">index_template_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">index_template_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">ElasticProfiling</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to enable profiling for the elastic search source. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
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
    "CollapseUrns": {
      "additionalProperties": false,
      "properties": {
        "urns_suffix_regex": {
          "description": "List of regex patterns to remove from the name of the URN. All of the indices before removal of URNs are considered as the same dataset. These are applied in order for each URN.\n        The main case where you would want to have multiple of these if the name where you are trying to remove suffix from have different formats.\n        e.g. ending with -YYYY-MM-DD as well as ending -epochtime would require you to have 2 regex patterns to remove the suffixes across all URNs.",
          "items": {
            "type": "string"
          },
          "title": "Urns Suffix Regex",
          "type": "array"
        }
      },
      "title": "CollapseUrns",
      "type": "object"
    },
    "ElasticProfiling": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether to enable profiling for the elastic search source.",
          "title": "Enabled",
          "type": "boolean"
        },
        "operation_config": {
          "$ref": "#/$defs/OperationConfig",
          "description": "Experimental feature. To specify operation configs."
        }
      },
      "title": "ElasticProfiling",
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
    "host": {
      "default": "localhost:9200",
      "description": "The elastic search host URI.",
      "title": "Host",
      "type": "string"
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
      "description": "The username credential.",
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
      "description": "The password credential.",
      "title": "Password"
    },
    "api_key": {
      "anyOf": [
        {},
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "API Key authentication. Accepts either a list with id and api_key (UTF-8 representation), or a base64 encoded string of id and api_key combined by ':'.",
      "title": "Api Key"
    },
    "use_ssl": {
      "default": false,
      "description": "Whether to use SSL for the connection or not.",
      "title": "Use Ssl",
      "type": "boolean"
    },
    "verify_certs": {
      "default": false,
      "description": "Whether to verify SSL certificates.",
      "title": "Verify Certs",
      "type": "boolean"
    },
    "ca_certs": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to a certificate authority (CA) certificate.",
      "title": "Ca Certs"
    },
    "client_cert": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to the file containing the private key and the certificate, or cert only if using client_key.",
      "title": "Client Cert"
    },
    "client_key": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to the file containing the private key if using separate cert and key files.",
      "title": "Client Key"
    },
    "ssl_assert_hostname": {
      "default": false,
      "description": "Use hostname verification if not False.",
      "title": "Ssl Assert Hostname",
      "type": "boolean"
    },
    "ssl_assert_fingerprint": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Verify the supplied certificate fingerprint if not None.",
      "title": "Ssl Assert Fingerprint"
    },
    "url_prefix": {
      "default": "",
      "description": "There are cases where an enterprise would have multiple elastic search clusters. One way for them to manage is to have a single endpoint for all the elastic search clusters and use url_prefix for routing requests to different clusters.",
      "title": "Url Prefix",
      "type": "string"
    },
    "index_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [
          "^_.*",
          "^ilm-history.*"
        ],
        "ignoreCase": true
      },
      "description": "regex patterns for indexes to filter in ingestion."
    },
    "ingest_index_templates": {
      "default": false,
      "description": "Ingests ES index templates if enabled.",
      "title": "Ingest Index Templates",
      "type": "boolean"
    },
    "index_template_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [
          "^_.*"
        ],
        "ignoreCase": true
      },
      "description": "The regex patterns for filtering index templates to ingest."
    },
    "profiling": {
      "$ref": "#/$defs/ElasticProfiling",
      "description": "Configs to ingest data profiles from ElasticSearch."
    },
    "collapse_urns": {
      "$ref": "#/$defs/CollapseUrns",
      "description": "List of regex patterns to remove from the name of the URN. All of the indices before removal of URNs are considered as the same dataset. These are applied in order for each URN.\n        The main case where you would want to have multiple of these if the name where you are trying to remove suffix from have different formats.\n        e.g. ending with -YYYY-MM-DD as well as ending -epochtime would require you to have 2 regex patterns to remove the suffixes across all URNs."
    }
  },
  "title": "ElasticsearchSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.elastic_search.ElasticsearchSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/elastic_search.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for Elasticsearch, feel free to ping us on [our Slack](https://datahub.com/slack).
