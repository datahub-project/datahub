import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Microsoft Teams

<FeatureAvailability ossOnly />

| <!-- --> | <!-- --> |
| --- | --- |
| **Status** | ![Incubating](https://img.shields.io/badge/support%20status-incubating-blue) |
| **Version Requirements** | ![Minimum Version Requirements](https://img.shields.io/badge/acryl_datahub_actions-v0.0.9+-green.svg) |

## Overview

This Action integrates DataHub with Microsoft Teams to send notifications to a configured Teams channel in your workspace.

### Capabilities

- Sending notifications of important events to a Teams channel
   - Adding or Removing a tag from an entity (dataset, dashboard etc.)
   - Updating documentation at the entity or field (column) level. 
   - Adding or Removing ownership from an entity (dataset, dashboard, etc.)
   - Creating a Domain
   - and many more.

### User Experience

On startup, the action will produce a welcome message that looks like the one below. 
![](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/integrations/teams/teams_welcome_message.png)


On each event, the action will produce a notification message that looks like the one below.
![](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/integrations/teams/teams_notification_message.png)

Watch the townhall demo to see this in action:
[![Teams Action Demo](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/integrations/teams/teams_demo_image.png)](https://www.youtube.com/watch?v=BlCLhG8lGoY&t=2998s)


### Supported Events

- `EntityChangeEvent_v1`
- Currently, the `MetadataChangeLog_v1` event is **not** processed by the Action.

## Action Quickstart 

### Prerequisites

Ensure that you have configured an incoming webhook in your Teams channel.

Follow the guide [here](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook) to set it up.

Take note of the incoming webhook url as you will need to use that to configure the Team action. 

### Installation Instructions (Deployment specific)

#### Quickstart

If you are running DataHub using the docker quickstart option, there are no additional software installation steps. The `datahub-actions` container comes pre-installed with the Teams action. 

All you need to do is export a few environment variables to activate and configure the integration. See below for the list of environment variables to export.

| Env Variable | Required for Integration | Purpose |
| --- | --- | --- |
| DATAHUB_ACTIONS_TEAMS_ENABLED | ✅ | Set to "true" to enable the Teams action |
| DATAHUB_ACTIONS_TEAMS_WEBHOOK_URL | ✅ | Set to the incoming webhook url that you configured in the [pre-requisites step](#prerequisites) above |
| DATAHUB_ACTIONS_DATAHUB_BASE_URL | ❌ | Defaults to "http://localhost:9002". Set to the location where your DataHub UI is running. On a local quickstart this is usually "http://localhost:9002", so you shouldn't need to modify this |

:::note

You will have to restart the `datahub-actions` docker container after you have exported these environment variables if this is the first time. The simplest way to do it is via the Docker Desktop UI, or by just issuing a `datahub docker quickstart --stop && datahub docker quickstart` command to restart the whole instance.

:::


For example:
```shell
export DATAHUB_ACTIONS_TEAMS_ENABLED=true
export DATAHUB_ACTIONS_TEAMS_WEBHOOK_URL=<teams_webhook_url>

datahub docker quickstart --stop && datahub docker quickstart
```

#### k8s / helm

Similar to the quickstart scenario, there are no specific software installation steps. The `datahub-actions` container comes pre-installed with the Teams action. You just need to export a few environment variables and make them available to the `datahub-actions` container to activate and configure the integration. See below for the list of environment variables to export.

| Env Variable | Required for Integration | Purpose |
| --- | --- | --- |
| DATAHUB_ACTIONS_TEAMS_ENABLED | ✅ | Set to "true" to enable the Teams action |
| DATAHUB_ACTIONS_TEAMS_WEBHOOK_URL | ✅ | Set to the incoming webhook url that you configured in the [pre-requisites step](#prerequisites) above |
| DATAHUB_ACTIONS_DATAHUB_BASE_URL | ✅| Set to the location where your DataHub UI is running. For example, if your DataHub UI is hosted at "https://datahub.my-company.biz", set this to "https://datahub.my-company.biz"|


#### Bare Metal - CLI or Python-based

If you are using the `datahub-actions` library directly from Python, or the `datahub-actions` cli directly, then you need to first install the `teams` action plugin in your Python virtualenv. 

```
pip install "datahub-actions[teams]"
```

Then run the action with a configuration file that you have modified to capture your credentials and configuration.

##### Sample Teams Action Configuration File

```yml
name: datahub_teams_action
enabled: true
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
    topic_routes:
      mcl: ${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:-MetadataChangeLog_Versioned_v1}
      pe: ${PLATFORM_EVENT_TOPIC_NAME:-PlatformEvent_v1}

## 3a. Optional: Filter to run on events (map)
# filter: 
#  event_type: <filtered-event-type>
#  event:
#    # Filter event fields by exact-match
#    <filtered-event-fields>

# 3b. Optional: Custom Transformers to run on events (array)
# transform:
#  - type: <transformer-type>
#    config: 
#      # Transformer-specific configs (map)

action:
  type: teams
  config:
    # Action-specific configs (map)
    base_url: ${DATAHUB_ACTIONS_TEAMS_DATAHUB_BASE_URL:-http://localhost:9002}
    webhook_url: ${DATAHUB_ACTIONS_TEAMS_WEBHOOK_URL}
    suppress_system_activity: ${DATAHUB_ACTIONS_TEAMS_SUPPRESS_SYSTEM_ACTIVITY:-true}

datahub:
  server: "http://${DATAHUB_GMS_HOST:-localhost}:${DATAHUB_GMS_PORT:-8080}"
```

##### Teams Action Configuration Parameters

| Field | Required | Default | Description |
| ---   | ---      | ---  | --- |
| `base_url` | ❌| `False` | Whether to print events in upper case. |
| `webhook_url` | ✅ | Set to the incoming webhook url that you configured in the [pre-requisites step](#prerequisites) above |
| `suppress_system_activity` | ❌ | `True` | Set to `False` if you want to get low level system activity events, e.g. when datasets are ingested, etc. Note: this will currently result in a very spammy Teams notifications experience, so this is not recommended to be changed. |


## Troubleshooting

If things are configured correctly, you should see logs on the `datahub-actions` container that indicate success in enabling and running the Teams action. 

```shell
docker logs datahub-datahub-actions-1

...
[2022-12-04 16:47:44,536] INFO     {datahub_actions.cli.actions:76} - DataHub Actions version: unavailable (installed editable via git)
[2022-12-04 16:47:44,565] WARNING  {datahub_actions.cli.actions:103} - Skipping pipeline datahub_slack_action as it is not enabled
[2022-12-04 16:47:44,581] INFO     {datahub_actions.plugin.action.teams.teams:60} - Teams notification action configured with webhook_url=SecretStr('**********') base_url='http://localhost:9002' suppress_system_activity=True
[2022-12-04 16:47:46,393] INFO     {datahub_actions.cli.actions:119} - Action Pipeline with name 'ingestion_executor' is now running.
[2022-12-04 16:47:46,393] INFO     {datahub_actions.cli.actions:119} - Action Pipeline with name 'datahub_teams_action' is now running.
...
```


If the Teams action was not enabled, you would see messages indicating that. 
e.g. the following logs below show that neither the Teams or Slack action were enabled. 

```shell
docker logs datahub-datahub-actions-1

....
No user action configurations found. Not starting user actions.
[2022-12-04 06:45:27,509] INFO     {datahub_actions.cli.actions:76} - DataHub Actions version: unavailable (installed editable via git)
[2022-12-04 06:45:27,647] WARNING  {datahub_actions.cli.actions:103} - Skipping pipeline datahub_slack_action as it is not enabled
[2022-12-04 06:45:27,649] WARNING  {datahub_actions.cli.actions:103} - Skipping pipeline datahub_teams_action as it is not enabled
[2022-12-04 06:45:27,649] INFO     {datahub_actions.cli.actions:119} - Action Pipeline with name 'ingestion_executor' is now running.
...

```

