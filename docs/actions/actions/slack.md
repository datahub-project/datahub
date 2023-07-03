import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Slack

<FeatureAvailability />


# Slack

| <!-- --> | <!-- --> |
| --- | --- |
| **Status** | ![Incubating](https://img.shields.io/badge/support%20status-incubating-blue) |
| **Version Requirements** | ![Minimum Version Requirements](https://img.shields.io/badge/acryl_datahub_actions-v0.0.9+-green.svg) |


## Overview

This Action integrates DataHub with Slack to send notifications to a configured Slack channel in your workspace.

### Capabilities

- Sending notifications of important events to a Slack channel
   - Adding or Removing a tag from an entity (dataset, dashboard etc.)
   - Updating documentation at the entity or field (column) level. 
   - Adding or Removing ownership from an entity (dataset, dashboard, etc.)
   - Creating a Domain
   - and many more.

### User Experience

On startup, the action will produce a welcome message that looks like the one below. 
![](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/integrations/slack/slack_welcome_message.png)


On each event, the action will produce a notification message that looks like the one below.
![](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/integrations/slack/slack_notification_message.png)

Watch the townhall demo to see this in action:
[![Slack Action Demo](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/integrations/slack/slack_demo_image.png)](https://www.youtube.com/watch?v=BlCLhG8lGoY&t=2998s)

### Supported Events

- `EntityChangeEvent_v1`
- Currently, the `MetadataChangeLog_v1` event is **not** processed by the Action.

## Action Quickstart 

### Prerequisites

Ensure that you have configured the Slack App in your Slack workspace. 

#### Install the DataHub Slack App into your Slack workspace

The following steps should be performed by a Slack Workspace Admin. 
- Navigate to  https://api.slack.com/apps/
- Click Create New App
- Use “From an app manifest” option
- Select your workspace
- Paste this Manifest in YAML. We suggest changing the name and `display_name` to be `DataHub App YOUR_TEAM_NAME` but this is not required. This name will show up in your Slack workspace.
```yml
display_information:
  name: DataHub App
  description: An app to integrate DataHub with Slack
  background_color: "#000000"
features:
  bot_user:
    display_name: DataHub App
    always_online: false
oauth_config:
  scopes:
    bot:
      - channels:history
      - channels:read
      - chat:write
      - commands
      - groups:read
      - im:read
      - mpim:read
      - team:read
      - users:read
      - users:read.email
settings:
  org_deploy_enabled: false
  socket_mode_enabled: false
  token_rotation_enabled: false
```

- Confirm you see the Basic Information Tab

![](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/integrations/slack/slack_basic_info.png)

- Click **Install to Workspace**
- It will show you permissions the Slack App is asking for, what they mean and a default channel in which you want to add the slack app
    - Note that the Slack App will only be able to post in channels that the app has been added to. This is made clear by slack’s Authentication screen also.
- Select the channel you'd like notifications to go to and click **Allow**
- Go to the DataHub App page
    - You can find your workspace's list of apps at https://api.slack.com/apps/

#### Getting Credentials and Configuration

Now that you've created your app and installed it in your workspace, you need a few pieces of information before you can activate your Slack action. 

#### 1. The Signing Secret

On your app's Basic Information page, you will see a App Credentials area. Take note of the Signing Secret information, you will need it later.

![](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/integrations/slack/slack_app_credentials.png)


#### 2. The Bot Token

Navigate to the **OAuth & Permissions** Tab

![](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/integrations/slack/slack_oauth_and_permissions.png)

Here you'll find a “Bot User OAuth Token” which DataHub will need to communicate with your Slack workspace through the bot.

#### 3. The Slack Channel

Finally, you need to figure out which Slack channel you will send notifications to. Perhaps it should be called #datahub-notifications or maybe, #data-notifications or maybe you already have a channel where important notifications about datasets and pipelines are already being routed to. Once you have decided what channel to send notifications to, make sure to add the app to the channel. 

![](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/integrations/slack/slack_channel_add_app.png)

Next, figure out the channel id for this Slack channel. You can find it in the About section for the channel if you scroll to the very bottom of the app.
![](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/integrations/slack/slack_channel_id.png)

Alternately, if you are on the browser, you can figure it out from the URL. e.g. for the troubleshoot channel in OSS DataHub slack

![](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/integrations/slack/slack_channel_url.png)

- Notice `TUMKD5EGJ/C029A3M079U` in the URL
  - Channel ID = `C029A3M079U` from above


In the next steps, we'll show you how to configure the Slack Action based on the credentials and configuration values that you have collected.
 
### Installation Instructions (Deployment specific)

#### Managed DataHub

Head over to the [Configuring Notifications](../../managed-datahub/saas-slack-setup.md#configuring-notifications) section in the Managed DataHub guide to configure Slack notifications for your Managed DataHub instance.


#### Quickstart

If you are running DataHub using the docker quickstart option, there are no additional software installation steps. The `datahub-actions` container comes pre-installed with the Slack action. 

All you need to do is export a few environment variables to activate and configure the integration. See below for the list of environment variables to export.

| Env Variable | Required for Integration | Purpose |
| --- | --- | --- |
| DATAHUB_ACTIONS_SLACK_ENABLED | ✅ | Set to "true" to enable the Slack action | 
| DATAHUB_ACTIONS_SLACK_SIGNING_SECRET | ✅ | Set to the [Slack Signing Secret](#1-the-signing-secret) that you configured in the pre-requisites step above |
| DATAHUB_ACTIONS_SLACK_BOT_TOKEN | ✅ | Set to the [Bot User OAuth Token](#2-the-bot-token) that you configured in the pre-requisites step above |
| DATAHUB_ACTIONS_SLACK_CHANNEL | ✅ | Set to the [Slack Channel ID](#3-the-slack-channel) that you want the action to send messages to |
| DATAHUB_ACTIONS_SLACK_DATAHUB_BASE_URL | ❌ | Defaults to "http://localhost:9002". Set to the location where your DataHub UI is running. On a local quickstart this is usually "http://localhost:9002", so you shouldn't need to modify this |

:::note

You will have to restart the `datahub-actions` docker container after you have exported these environment variables if this is the first time. The simplest way to do it is via the Docker Desktop UI, or by just issuing a `datahub docker quickstart --stop && datahub docker quickstart` command to restart the whole instance.

:::


For example:
```shell
export DATAHUB_ACTIONS_SLACK_ENABLED=true
export DATAHUB_ACTIONS_SLACK_SIGNING_SECRET=<slack-signing-secret>
....
export DATAHUB_ACTIONS_SLACK_CHANNEL=<slack_channel_id>

datahub docker quickstart --stop && datahub docker quickstart
```

#### k8s / helm

Similar to the quickstart scenario, there are no specific software installation steps. The `datahub-actions` container comes pre-installed with the Slack action. You just need to export a few environment variables and make them available to the `datahub-actions` container to activate and configure the integration. See below for the list of environment variables to export.

| Env Variable | Required for Integration | Purpose |
| --- | --- | --- |
| DATAHUB_ACTIONS_SLACK_ENABLED | ✅ | Set to "true" to enable the Slack action | 
| DATAHUB_ACTIONS_SLACK_SIGNING_SECRET | ✅ | Set to the [Slack Signing Secret](#1-the-signing-secret) that you configured in the pre-requisites step above |
| DATAHUB_ACTIONS_SLACK_BOT_TOKEN | ✅ | Set to the [Bot User OAuth Token](#2-the-bot-token) that you configured in the pre-requisites step above |
| DATAHUB_ACTIONS_SLACK_CHANNEL | ✅ | Set to the [Slack Channel ID](#3-the-slack-channel) that you want the action to send messages to |
| DATAHUB_ACTIONS_DATAHUB_BASE_URL | ✅| Set to the location where your DataHub UI is running. For example, if your DataHub UI is hosted at "https://datahub.my-company.biz", set this to "https://datahub.my-company.biz"|


#### Bare Metal - CLI or Python-based

If you are using the `datahub-actions` library directly from Python, or the `datahub-actions` cli directly, then you need to first install the `slack` action plugin in your Python virtualenv. 

```
pip install "datahub-actions[slack]"
```

Then run the action with a configuration file that you have modified to capture your credentials and configuration.

##### Sample Slack Action Configuration File

```yml
name: datahub_slack_action
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
  type: slack
  config:
    # Action-specific configs (map)
    base_url: ${DATAHUB_ACTIONS_SLACK_DATAHUB_BASE_URL:-http://localhost:9002}
    bot_token: ${DATAHUB_ACTIONS_SLACK_BOT_TOKEN}
    signing_secret: ${DATAHUB_ACTIONS_SLACK_SIGNING_SECRET}
    default_channel: ${DATAHUB_ACTIONS_SLACK_CHANNEL}
    suppress_system_activity: ${DATAHUB_ACTIONS_SLACK_SUPPRESS_SYSTEM_ACTIVITY:-true}

datahub:
  server: "http://${DATAHUB_GMS_HOST:-localhost}:${DATAHUB_GMS_PORT:-8080}"
  
```

##### Slack Action Configuration Parameters

| Field | Required | Default | Description |
| ---   | ---      | ---  | --- |
| `base_url` | ❌| `False` | Whether to print events in upper case. |
| `signing_secret` | ✅ |  | Set to the [Slack Signing Secret](#1-the-signing-secret) that you configured in the pre-requisites step above |
| `bot_token` | ✅ |  | Set to the [Bot User OAuth Token](#2-the-bot-token) that you configured in the pre-requisites step above |
| `default_channel` | ✅ |  | Set to the [Slack Channel ID](#3-the-slack-channel) that you want the action to send messages to |
| `suppress_system_activity` | ❌ | `True` | Set to `False` if you want to get low level system activity events, e.g. when datasets are ingested, etc. Note: this will currently result in a very spammy Slack notifications experience, so this is not recommended to be changed. |


## Troubleshooting

If things are configured correctly, you should see logs on the `datahub-actions` container that indicate success in enabling and running the Slack action. 

```shell
docker logs datahub-datahub-actions-1

...
[2022-12-04 07:07:53,804] INFO     {datahub_actions.plugin.action.slack.slack:96} - Slack notification action configured with bot_token=SecretStr('**********') signing_secret=SecretStr('**********') default_channel='C04CZUSSR5X' base_url='http://localhost:9002' suppress_system_activity=True
[2022-12-04 07:07:54,506] WARNING  {datahub_actions.cli.actions:103} - Skipping pipeline datahub_teams_action as it is not enabled
[2022-12-04 07:07:54,506] INFO     {datahub_actions.cli.actions:119} - Action Pipeline with name 'ingestion_executor' is now running.
[2022-12-04 07:07:54,507] INFO     {datahub_actions.cli.actions:119} - Action Pipeline with name 'datahub_slack_action' is now running.
...
```


If the Slack action was not enabled, you would see messages indicating that. 
e.g. the following logs below show that neither the Slack or Teams action were enabled. 

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
