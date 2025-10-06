import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Slack App Features

<FeatureAvailability saasOnly />

The DataHub Slack App brings several of DataHub's key capabilities directly into your Slack experience.
The integration enables your team to:

1. Ask DataHub about your data (by tagging @DataHub)
2. Get notified when Data Assets change
3. Search for & subscribe to data assets using slash commands
4. Manage Data Incidents

The Slack App makes data discovery easier and more accessible, by making DataHub available where you work.
Learn more about [how to set up the Slack app](./saas-slack-setup.md) or [how to troubleshoot issues](./saas-slack-troubleshoot.md).

## Ask DataHub

**Ask DataHub** is DataHub's conversational AI assistant. In Slack, you can mention @DataHub in any channel to ask questions about your metadata.

<p align="center">
<video width="80%" autoPlay muted loop playsInline crossOrigin="anonymous">
  <source src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/chatbot_2.mp4" type="video/mp4" />
  <source src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/chatbot_2.webm" type="video/webm" />
  Your browser does not support the video tag.
</video>
</p>

Key capabilities include:

- Search for data assets using natural language.
- Understand the impact of changes to data assets.
- Dig into specific assets and their glossary terms, owners, and more.
- Write first-drafts of SQL queries to answer specific questions.

#### Enabling **Ask DataHub** in Slack

:::warning Permissions

The current version assumes that all users have read permissions for all assets. A future version will support more granular permissions.

:::

In DataHub Cloud v0.3.13 (July 2025), **Ask DataHub** is in **public beta** and can be enabled in the Settings → AI page. In prior versions, it was in private beta and required a request to your DataHub Cloud representative to get access.

<p align="center">
    <img width="70%" alt="Enabling @DataHub AI in Slack" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/chatbot_config.png" />
</p>

## Get Notified

The DataHub Slack app can send notifications to Slack channels and direct messages.
Notifications [can be configured](../subscription-and-notification.md) in the DataHub UI once the [Slack app is set up](./saas-slack-setup.md#configure-notifications).

<p align="center">
    <img width="70%" alt="Example DataHub notification in Slack." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/notification_1.png" />
</p>

## Slash Commands

The command-based capabilities on the Slack App revolve around search.

### Querying for Assets

You can trigger a search by simply typing `/datahub my favorite table`.

<p align="center">
    <img width="70%" alt="Example of an in-Slack DataHub Cloud search command being performed." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/slack_app_search_1_v2.png" />
</p>

Right within Slack, you'll be presented with results matching your query, and a handful of quick-actions for your convenience.

<p align="center">
    <img width="70%" alt="Example of search results being displayed within Slack." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/slack_app_search_2.png" />
</p>

By selecting **'More Details'** you can preview in-depth information about an asset without leaving Slack.

<p align="center">
    <img width="70%" alt="Example of search results being displayed within Slack." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/slack_app_search_3.png" />
</p>

### Subscribing to be notified about an Asset

You can hit the **'Subscribe'** button on a specific search result to subscribe to it directly from within Slack.

<p align="center">
    <img width="70%" alt="Example of search results being displayed within Slack." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/slack_app_search_2.png" />
</p>

## Manage Data Incidents

One of the most common use cases for the Teams app is incident management.
When an incident is raised, you will get notified about its impact, priority, and more.
You will also be able to directly resolve the incident, or dive deeper into DataHub, directly from Teams.

To learn more about DataHub incident management, check out the [Incidents](https://docs.datahub.com/docs/incidents/incidents/) feature guide.

<p align="center">
    <img width="70%" alt="Example of search results being displayed within Slack." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/slack_incidents_1.png" />
</p>

You can also resolve incidents directly from within Slack. Once resolved, incidents can also be reopened:

<p align="center">
    <img width="70%" alt="Example of search results being displayed within Slack." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/slack_incidents_2.png" />
</p>
