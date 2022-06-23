# Ingestion Executor
<!-- Set Support Status -->
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


## Overview

This Action executes ingestion recipes that are configured via the UI.

### Capabilities

- Executing `datahub ingest` command in a sub-process when an Execution Request command is received from DataHub. (Scheduled or manual ingestion run)
- Resolving secrets within an ingestion recipe from DataHub
- Reporting ingestion execution status to DataHub

### Supported Events

- `MetadataChangeLog_v1`

Specifically, changes to the `dataHubExecutionRequestInput` and `dataHubExecutionRequestSignal` aspects of the `dataHubExecutionRequest` entity are required.


## Action Quickstart 

### Prerequisites

#### DataHub Privileges

This action must be executed as a privileged DataHub user (e.g. using Personal Access Tokens). Specifically, the user must have the `Manage Secrets` Platform Privilege, which allows for retrieval
of decrypted secrets for injection into an ingestion recipe. 

An access token generated from a privileged account must be configured in the `datahub` configuration
block of the YAML configuration, as shown in the example below. 

#### Connecting to Ingestion Sources 

In order for ingestion to run successfully, the process running the Actions must have 
network connectivity to any source systems that are required for ingestion. 

For example, if the ingestion recipe is pulling from an internal DBMS, the actions container
must be able to resolve & connect to that DBMS system for the ingestion command to run successfully.

### Install the Plugin(s)

Run the following commands to install the relevant action plugin(s):

`pip install 'acryl-datahub-actions[executor]'`


### Configure the Action Config

Use the following config(s) to get started with this Action. 

```yml
name: "pipeline-name"
source:
  # source configs
action:
  type: "executor"
# Requires DataHub API configurations to report to DataHub
datahub:
  server: "http://${DATAHUB_GMS_HOST:-localhost}:${DATAHUB_GMS_PORT:-8080}"
  # token: <token> # Must have "Manage Secrets" privilege
```

<details>
  <summary>View All Configuration Options</summary>
  
  | Field | Required | Default | Description |
  | --- | :-: | :-: | --- |
  | `executor_id` | ❌ | `default` | An executor ID assigned to the executor. This can be used to manage multiple distinct executors. |
</details>


## Troubleshooting

### Quitting the Actions Framework

Currently, when you quit the Actions framework, any in-flight ingestion processing will continue to execute as a subprocess on your system. This means that there may be "orphaned" processes which
are never marked as "Succeeded" or "Failed" in the UI, even though they may have completed. 

To address this, simply "Cancel" the ingestion source on the UI once you've restarted the Ingestion Executor action. 
