# Metadata Change Sync

<!-- Set Support Status -->

![Incubating](https://img.shields.io/badge/support%20status-Incubating-red)

## Overview

This Action is a custom action that can convert the incoming MetadataChangeLogEvent_v1 event to MetadataChangeProposal and emit to another datahub instance, the goal of this action is to keep the data of the two datahub instances in sync. This action is a one-way sync and should be run by the source datahub instance.

### Capabilities

- Convert incoming MetadataChangeLogEvent_v1 to MetadataChangeProposal and use RestEmitter emit to another datahub instance

### Supported Events

- `MetadataChangeLogEvent_v1`

## Action Quickstart

### Prerequisites

No prerequisites. This action comes pre-loaded with `acryl-datahub-actions`.

### Install the Plugin(s)

This action comes with the Actions Framework by default:

`pip install 'acryl-datahub-actions'`

### Configure the Action Config

Use the following config(s) to get started with this Action.

```yml
name: "pipeline-name"
source:
  # source configs
type: "metadata_change_sync"
  config:
    gms_server: ${DEST_DATAHUB_GMS_URL}
```

<details>
  <summary>View All Configuration Options</summary>
  
  | Field | Required | Default | Description |
  | --- | :-: | :-: | --- |
  | `gms_server` | ✅ | null | Destination GMS server endpoint. |
  | `gms_auth_token` | ❌ | null | Destination GMS server auth token if the server has METADATA_SERVICE_AUTH_ENABLED enabled. |
  | `aspects_to_exclude` | ❌ | null | A list of aspects to exclude from the sync. |
  | `extra_headers` | ❌ | null | Extra headers in the emit request with key value format user would like to include. |

</details>

## Troubleshooting

N/A
