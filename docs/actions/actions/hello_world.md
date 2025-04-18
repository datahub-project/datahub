# Hello World

<!-- Set Support Status -->
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


## Overview

This Action is an example action which simply prints all Events it receives as JSON.

### Capabilities

- Printing events that are received by the Action to the console. 

### Supported Events

All event types, including

- `EntityChangeEvent_v1`
- `MetadataChangeLog_v1`


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
action:
  type: "hello_world"
```

<details>
  <summary>View All Configuration Options</summary>
  
  | Field | Required | Default | Description |
  | --- | :-: | :-: | --- |
  | `to_upper` | ❌| `False` | Whether to print events in upper case. |
</details>


## Troubleshooting

N/A