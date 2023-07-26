---
sidebar_position: 31
title: NiFi
slug: /generated/ingestion/sources/nifi
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/nifi.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# NiFi

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

This plugin extracts the following:

- NiFi flow as `DataFlow` entity
- Ingress, egress processors, remote input and output ports as `DataJob` entity
- Input and output ports receiving remote connections as `Dataset` entity
- Lineage information between external datasets and ingress/egress processors by analyzing provenance events

Current limitations:

- Limited ingress/egress processors are supported
  - S3: `ListS3`, `FetchS3Object`, `PutS3Object`
  - SFTP: `ListSFTP`, `FetchSFTP`, `GetSFTP`, `PutSFTP`

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[nifi]'
```

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

| Field                                                                                                                                                                                                                       | Description                                                                                                                                                                                                                                                                 |
| :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">site_url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                          | URL for Nifi, ending with /nifi/. e.g. https://mynifi.domain/nifi/                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">auth</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>                                                                      | Nifi authentication. must be one of : NO_AUTH, SINGLE_USER, CLIENT_CERT, KERBEROS <div className="default-line default-line-with-docs">Default: <span className="default-value">NO_AUTH</span></div>                                                                        |
| <div className="path-line"><span className="path-main">ca_file</span></div> <div className="type-name-line"><span className="type-name">One of boolean, string</span></div>                                                 | Path to PEM file containing certs for the root CA(s) for the NiFi                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">client_cert_file</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                        | Path to PEM file containing the public certificates for the user/client identity, must be set for auth = "CLIENT_CERT"                                                                                                                                                      |
| <div className="path-line"><span className="path-main">client_key_file</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                         | Path to PEM file containing the client’s secret key                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">client_key_password</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                     | The password to decrypt the client_key_file                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                | Nifi password, must be set for auth = "SINGLE_USER"                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">provenance_days</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                        | time window to analyze provenance events for external datasets <div className="default-line default-line-with-docs">Default: <span className="default-value">7</span></div>                                                                                                 |
| <div className="path-line"><span className="path-main">site_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                               | Site name to identify this site with, useful when using input and output ports receiving remote connections <div className="default-line default-line-with-docs">Default: <span className="default-value">default</span></div>                                              |
| <div className="path-line"><span className="path-main">site_url_to_site_name</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div>                                          |                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                | Nifi username, must be set for auth = "SINGLE_USER"                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                     | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                        |
| <div className="path-line"><span className="path-main">process_group_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                         | regex patterns for filtering process groups <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">process_group_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div> |                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">process_group_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>  |                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">process_group_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>  | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                 |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "NifiSourceConfig",
  "description": "Any source that produces dataset urns in a single environment should inherit this class",
  "type": "object",
  "properties": {
    "env": {
      "title": "Env",
      "description": "The environment that all assets produced by this connector belong to",
      "default": "PROD",
      "type": "string"
    },
    "site_url": {
      "title": "Site Url",
      "description": "URL for Nifi, ending with /nifi/. e.g. https://mynifi.domain/nifi/",
      "type": "string"
    },
    "auth": {
      "description": "Nifi authentication. must be one of : NO_AUTH, SINGLE_USER, CLIENT_CERT, KERBEROS",
      "default": "NO_AUTH",
      "allOf": [
        {
          "$ref": "#/definitions/NifiAuthType"
        }
      ]
    },
    "provenance_days": {
      "title": "Provenance Days",
      "description": "time window to analyze provenance events for external datasets",
      "default": 7,
      "type": "integer"
    },
    "process_group_pattern": {
      "title": "Process Group Pattern",
      "description": "regex patterns for filtering process groups",
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
    "site_name": {
      "title": "Site Name",
      "description": "Site name to identify this site with, useful when using input and output ports receiving remote connections",
      "default": "default",
      "type": "string"
    },
    "site_url_to_site_name": {
      "title": "Site Url To Site Name",
      "description": "Lookup to find site_name for site_url ending with /nifi/, required if using remote process groups in nifi flow",
      "default": {},
      "type": "object",
      "additionalProperties": {
        "type": "string"
      }
    },
    "username": {
      "title": "Username",
      "description": "Nifi username, must be set for auth = \"SINGLE_USER\"",
      "type": "string"
    },
    "password": {
      "title": "Password",
      "description": "Nifi password, must be set for auth = \"SINGLE_USER\"",
      "type": "string"
    },
    "client_cert_file": {
      "title": "Client Cert File",
      "description": "Path to PEM file containing the public certificates for the user/client identity, must be set for auth = \"CLIENT_CERT\"",
      "type": "string"
    },
    "client_key_file": {
      "title": "Client Key File",
      "description": "Path to PEM file containing the client\u2019s secret key",
      "type": "string"
    },
    "client_key_password": {
      "title": "Client Key Password",
      "description": "The password to decrypt the client_key_file",
      "type": "string"
    },
    "ca_file": {
      "title": "Ca File",
      "description": "Path to PEM file containing certs for the root CA(s) for the NiFi",
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "string"
        }
      ]
    }
  },
  "required": [
    "site_url"
  ],
  "additionalProperties": false,
  "definitions": {
    "NifiAuthType": {
      "title": "NifiAuthType",
      "description": "An enumeration.",
      "enum": [
        "NO_AUTH",
        "SINGLE_USER",
        "CLIENT_CERT",
        "KERBEROS",
        "BASIC_AUTH"
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
    }
  }
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

If you've got any questions on configuring ingestion for NiFi, feel free to ping us on [our Slack](https://slack.datahubproject.io).
