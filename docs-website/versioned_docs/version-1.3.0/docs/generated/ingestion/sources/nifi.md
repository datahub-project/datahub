---
sidebar_position: 47
title: NiFi
slug: /generated/ingestion/sources/nifi
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/nifi.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# NiFi
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Table-Level Lineage | ✅ | Supported. See docs for limitations. |


### Concept Mapping

| Source Concept                    | DataHub Concept                                           | Notes                   |
| --------------------------------- | --------------------------------------------------------- | ----------------------- |
| `"Nifi"`                          | [Data Platform](../../metamodel/entities/dataPlatform.md) |                         |
| Nifi flow                         | [Data Flow](../../metamodel/entities/dataFlow.md)         |                         |
| Nifi Ingress / Egress Processor   | [Data Job](../../metamodel/entities/dataJob.md)           |                         |
| Nifi Remote Port                  | [Data Job](../../metamodel/entities/dataJob.md)           |                         |
| Nifi Port with remote connections | [Dataset](../../metamodel/entities/dataset.md)            |                         |
| Nifi Process Group                | [Container](../../metamodel/entities/container.md)        | Subtype `Process Group` |

### Caveats

- This plugin extracts the lineage information between external datasets and ingress/egress processors by analyzing provenance events. Please check your Nifi configuration to confirm max rentention period of provenance events and make sure that ingestion runs frequent enough to read provenance events before they are disappear.

- Limited ingress/egress processors are supported
  - S3: `ListS3`, `FetchS3Object`, `PutS3Object`
  - SFTP: `ListSFTP`, `FetchSFTP`, `GetSFTP`, `PutSFTP`

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: "nifi"
  config:
    # Coordinates
    site_url: "https://localhost:8443/nifi/"

    # Credentials
    auth: SINGLE_USER
    username: admin
    password: password

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
| <div className="path-line"><span className="path-main">site_url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | URL for Nifi, ending with /nifi/. e.g. https://mynifi.domain/nifi/  |
| <div className="path-line"><span className="path-main">auth</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "NO_AUTH", "SINGLE_USER", "CLIENT_CERT", "KERBEROS", "BASIC_AUTH"  |
| <div className="path-line"><span className="path-main">ca_file</span></div> <div className="type-name-line"><span className="type-name">One of boolean, string, null</span></div> | Path to PEM file containing certs for the root CA(s) for the NiFi.Set to False to disable SSL verification. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">client_cert_file</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Path to PEM file containing the public certificates for the user/client identity, must be set for auth = "CLIENT_CERT" <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">client_key_file</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Path to PEM file containing the client’s secret key <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">client_key_password</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The password to decrypt the client_key_file <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">emit_process_group_as_container</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to emit Nifi process groups as container entities. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits incremental/patch lineage for Nifi processors. When disabled, re-states lineage on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Nifi password, must be set for auth = "SINGLE_USER" <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">provenance_days</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | time window to analyze provenance events for external datasets <div className="default-line default-line-with-docs">Default: <span className="default-value">7</span></div> |
| <div className="path-line"><span className="path-main">site_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Site name to identify this site with, useful when using input and output ports receiving remote connections <div className="default-line default-line-with-docs">Default: <span className="default-value">default</span></div> |
| <div className="path-line"><span className="path-main">site_url_to_site_name</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Nifi username, must be set for auth = "SINGLE_USER" <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">process_group_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">process_group_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
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
    "NifiAuthType": {
      "enum": [
        "NO_AUTH",
        "SINGLE_USER",
        "CLIENT_CERT",
        "KERBEROS",
        "BASIC_AUTH"
      ],
      "title": "NifiAuthType",
      "type": "string"
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
    "site_url": {
      "description": "URL for Nifi, ending with /nifi/. e.g. https://mynifi.domain/nifi/",
      "title": "Site Url",
      "type": "string"
    },
    "auth": {
      "$ref": "#/$defs/NifiAuthType",
      "default": "NO_AUTH",
      "description": "Nifi authentication. must be one of : NO_AUTH, SINGLE_USER, CLIENT_CERT, KERBEROS"
    },
    "provenance_days": {
      "default": 7,
      "description": "time window to analyze provenance events for external datasets",
      "title": "Provenance Days",
      "type": "integer"
    },
    "process_group_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "regex patterns for filtering process groups"
    },
    "site_name": {
      "default": "default",
      "description": "Site name to identify this site with, useful when using input and output ports receiving remote connections",
      "title": "Site Name",
      "type": "string"
    },
    "site_url_to_site_name": {
      "additionalProperties": {
        "type": "string"
      },
      "default": {},
      "description": "Lookup to find site_name for site_url ending with /nifi/, required if using remote process groups in nifi flow",
      "title": "Site Url To Site Name",
      "type": "object"
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
      "description": "Nifi username, must be set for auth = \"SINGLE_USER\"",
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
      "description": "Nifi password, must be set for auth = \"SINGLE_USER\"",
      "title": "Password"
    },
    "client_cert_file": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to PEM file containing the public certificates for the user/client identity, must be set for auth = \"CLIENT_CERT\"",
      "title": "Client Cert File"
    },
    "client_key_file": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to PEM file containing the client\u2019s secret key",
      "title": "Client Key File"
    },
    "client_key_password": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The password to decrypt the client_key_file",
      "title": "Client Key Password"
    },
    "ca_file": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to PEM file containing certs for the root CA(s) for the NiFi.Set to False to disable SSL verification.",
      "title": "Ca File"
    },
    "emit_process_group_as_container": {
      "default": false,
      "description": "Whether to emit Nifi process groups as container entities.",
      "title": "Emit Process Group As Container",
      "type": "boolean"
    },
    "incremental_lineage": {
      "default": true,
      "description": "When enabled, emits incremental/patch lineage for Nifi processors. When disabled, re-states lineage on each run.",
      "title": "Incremental Lineage",
      "type": "boolean"
    }
  },
  "required": [
    "site_url"
  ],
  "title": "NifiSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>

### Authentication

This connector supports following authentication mechanisms

#### Single User Authentication (`auth: SINGLE_USER`)

Connector will pass this `username` and `password` as used on Nifi Login Page over `/access/token` REST endpoint. This mode also works when [Kerberos login identity provider](https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#kerberos_login_identity_provider) is set up for Nifi.

#### Client Certificates Authentication (`auth: CLIENT_CERT`)

Connector will use `client_cert_file`(required) and `client_key_file`(optional), `client_key_password`(optional) for mutual TLS authentication.

#### Kerberos Authentication via SPNEGO (`auth: Kerberos`)

If nifi has been configured to use [Kerberos SPNEGO](https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#kerberos_service), connector will pass user’s Kerberos ticket to nifi over `/access/kerberos` REST endpoint. It is assumed that user's Kerberos ticket is already present on the machine on which ingestion runs. This is usually done by installing krb5-user and then running kinit for user.

```console
sudo apt install krb5-user
kinit user@REALM
```

#### Basic Authentication (`auth: BASIC_AUTH`)

Connector will use [HTTPBasicAuth](https://requests.readthedocs.io/en/latest/user/authentication/#basic-authentication) with `username` and `password`.

#### No Authentication (`auth: NO_AUTH`)

This is useful for testing purposes.

### Access Policies

This connector requires following access policies to be set in Nifi for ingestion user.

#### Global Access Policies

| Policy           | Privilege                                                            | Resource      | Action |
| ---------------- | -------------------------------------------------------------------- | ------------- | ------ |
| view the UI      | Allows users to view the UI                                          | `/flow`       | R      |
| query provenance | Allows users to submit a Provenance Search and request Event Lineage | `/provenance` | R      |

#### Component level Access Policies (required to be set on root process group)

| Policy             | Privilege                                                                                                                             | Resource                                             | Action |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- | ------ |
| view the component | Allows users to view component configuration details                                                                                  | `/<component-type>/<component-UUID>`                 | R      |
| view the data      | Allows users to view metadata and content for this component in flowfile queues in outbound connections and through provenance events | `/data/<component-type>/<component-UUID>`            | R      |
| view provenance    | Allows users to view provenance events generated by this component                                                                    | `/provenance-data/<component-type>/<component-UUID>` | R      |

### Code Coordinates
- Class Name: `datahub.ingestion.source.nifi.NifiSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/nifi.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for NiFi, feel free to ping us on [our Slack](https://datahub.com/slack).
