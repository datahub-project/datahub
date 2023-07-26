---
sidebar_position: 12
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

| Capability                                          | Status | Notes              |
| --------------------------------------------------- | ------ | ------------------ |
| [Platform Instance](../../../platform-instances.md) | ✅     | Enabled by default |

This plugin extracts the following:

- Metadata for indexes
- Column types associated with each index field

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[elasticsearch]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: "elasticsearch"
  config:
    # Coordinates
    host: "localhost:9200"

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

| Field                                                                                                                                                                                                                           | Description                                                                                                                                                                                                                                                                                                                                             |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| <div className="path-line"><span className="path-main">ca_certs</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                    | Path to a certificate authority (CA) certificate.                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">client_cert</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                 | Path to the file containing the private key and the certificate, or cert only if using client_key.                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">client_key</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                  | Path to the file containing the private key if using separate cert and key files.                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">host</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                        | The elastic search host URI. <div className="default-line default-line-with-docs">Default: <span className="default-value">localhost:9200</span></div>                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">ingest_index_templates</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                     | Ingests ES index templates if enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                    | The password credential.                                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                           | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">ssl_assert_fingerprint</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                      | Verify the supplied certificate fingerprint if not None.                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">ssl_assert_hostname</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                        | Use hostname verification if not False. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">url_prefix</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                  | There are cases where an enterprise would have multiple elastic search clusters. One way for them to manage is to have a single endpoint for all the elastic search clusters and use url_prefix for routing requests to different clusters. <div className="default-line default-line-with-docs">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-main">use_ssl</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                    | Whether to use SSL for the connection or not. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                    | The username credential.                                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">verify_certs</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                               | Whether to verify SSL certificates. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                         | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                                                                                    |
| <div className="path-line"><span className="path-main">collapse_urns</span></div> <div className="type-name-line"><span className="type-name">CollapseUrns</span></div>                                                         |                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">collapse_urns.</span><span className="path-main">urns_suffix_regex</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div> |                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">index_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                     | regex patterns for indexes to filter in ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#x27;^\_.\*&#x27;, &#x27;^ilm-history.\*...</span></div>                                                                   |
| <div className="path-line"><span className="path-prefix">index_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>             |                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">index_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>              |                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">index_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>              | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                             |
| <div className="path-line"><span className="path-main">index_template_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                            | The regex patterns for filtering index templates to ingest. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#x27;^\_.\*&#x27;&#93;, &#x27;ignoreCase&#x27;: ...</span></div>                                                  |
| <div className="path-line"><span className="path-prefix">index_template_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>    |                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">index_template_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>     |                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">index_template_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>     | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                             |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">ElasticProfiling</span></div>                                                         |                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                     | Whether to enable profiling for the elastic search source. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                             |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "ElasticsearchSourceConfig",
  "description": "Any source that connects to a platform should inherit this class",
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
    "host": {
      "title": "Host",
      "description": "The elastic search host URI.",
      "default": "localhost:9200",
      "type": "string"
    },
    "username": {
      "title": "Username",
      "description": "The username credential.",
      "type": "string"
    },
    "password": {
      "title": "Password",
      "description": "The password credential.",
      "type": "string"
    },
    "use_ssl": {
      "title": "Use Ssl",
      "description": "Whether to use SSL for the connection or not.",
      "default": false,
      "type": "boolean"
    },
    "verify_certs": {
      "title": "Verify Certs",
      "description": "Whether to verify SSL certificates.",
      "default": false,
      "type": "boolean"
    },
    "ca_certs": {
      "title": "Ca Certs",
      "description": "Path to a certificate authority (CA) certificate.",
      "type": "string"
    },
    "client_cert": {
      "title": "Client Cert",
      "description": "Path to the file containing the private key and the certificate, or cert only if using client_key.",
      "type": "string"
    },
    "client_key": {
      "title": "Client Key",
      "description": "Path to the file containing the private key if using separate cert and key files.",
      "type": "string"
    },
    "ssl_assert_hostname": {
      "title": "Ssl Assert Hostname",
      "description": "Use hostname verification if not False.",
      "default": false,
      "type": "boolean"
    },
    "ssl_assert_fingerprint": {
      "title": "Ssl Assert Fingerprint",
      "description": "Verify the supplied certificate fingerprint if not None.",
      "type": "string"
    },
    "url_prefix": {
      "title": "Url Prefix",
      "description": "There are cases where an enterprise would have multiple elastic search clusters. One way for them to manage is to have a single endpoint for all the elastic search clusters and use url_prefix for routing requests to different clusters.",
      "default": "",
      "type": "string"
    },
    "index_pattern": {
      "title": "Index Pattern",
      "description": "regex patterns for indexes to filter in ingestion.",
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
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "ingest_index_templates": {
      "title": "Ingest Index Templates",
      "description": "Ingests ES index templates if enabled.",
      "default": false,
      "type": "boolean"
    },
    "index_template_pattern": {
      "title": "Index Template Pattern",
      "description": "The regex patterns for filtering index templates to ingest.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [
          "^_.*"
        ],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "profiling": {
      "$ref": "#/definitions/ElasticProfiling"
    },
    "collapse_urns": {
      "$ref": "#/definitions/CollapseUrns"
    }
  },
  "additionalProperties": false,
  "definitions": {
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
    "ElasticProfiling": {
      "title": "ElasticProfiling",
      "type": "object",
      "properties": {
        "enabled": {
          "title": "Enabled",
          "description": "Whether to enable profiling for the elastic search source.",
          "default": false,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    },
    "CollapseUrns": {
      "title": "CollapseUrns",
      "type": "object",
      "properties": {
        "urns_suffix_regex": {
          "title": "Urns Suffix Regex",
          "description": "List of regex patterns to remove from the name of the URN. All of the indices before removal of URNs are considered as the same dataset. These are applied in order for each URN.\n        The main case where you would want to have multiple of these if the name where you are trying to remove suffix from have different formats.\n        e.g. ending with -YYYY-MM-DD as well as ending -epochtime would require you to have 2 regex patterns to remove the suffixes across all URNs.",
          "type": "array",
          "items": {
            "type": "string"
          }
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

- Class Name: `datahub.ingestion.source.elastic_search.ElasticsearchSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/elastic_search.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Elasticsearch, feel free to ping us on [our Slack](https://slack.datahubproject.io).
