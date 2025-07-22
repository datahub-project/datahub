import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Slack App Features

<FeatureAvailability saasOnly />

## Overview

The DataHub Slack App brings several of DataHub's key capabilities directly into your Slack experience. These include:

1. Receive notifications
2. Searching for Data Assets
3. Subscribing to notifications for Data Assets
4. Managing Data Incidents
5. Chat with the @DataHub bot

_Our goal with the Slack app is to make data discovery easier and more accessible for you._

Learn more about [how to set up the Slack app](./saas-slack-setup.md) or [how to troubleshoot issues](./saas-slack-troubleshoot.md).

## Receive Notifications

The DataHub Slack app can send notifications to Slack channels and direct messages.
Notifications [can be configured](../subscription-and-notification.md) in the DataHub UI once the [Slack app is set up](./saas-slack-setup.md#configure-notifications).

<p align="center">
    <img width="70%" alt="Example DataHub notification in Slack." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/notification_1.png" />
</p>

## Slack App Commands

The command-based capabilities on the Slack App revolve around search.

### Querying for Assets

You can trigger a search by simplying typing `/acryl my favorite table`.

<p align="center">
    <img width="70%" alt="Example of an in-Slack DataHub Cloud search command being performed." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/slack_app_search_1.png" />
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

Some of the most commonly used features within our Slack app are the Incidents management capabilities.
The DataHub UI offers a rich set of [Incident tracking and management](https://docs.datahub.com/docs/incidents/incidents/) features.
When a Slack member or channel receives notifications about an Incident, many of these features are made accessible right within the Slack app.

When an incident is raised, you will recieve rich context about the incident in the Slack message itself. You will also be able to `Mark as Resolved`, update the `Priorty`, set a triage `Stage` and `View Details` - directly from the Slack message.

<p align="center">
    <img width="70%" alt="Example of search results being displayed within Slack." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/slack_incidents_1.png" />
</p>

If you choose to `Mark as Resolved` the message will update in-place, and you will be presented with the ability to `Reopen Incident` should you choose.

<p align="center">
    <img width="70%" alt="Example of search results being displayed within Slack." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/slack_incidents_2.png" />
</p>

## @DataHub Slack Bot

:::info

As of DataHub Cloud v0.3.12, the DataHub Slack bot is in **private beta**. Reach out to your DataHub Cloud representative to get access.

:::

With the DataHub Slack bot, you can mention @DataHub in any channel and ask it questions about your metadata.

Key capabilities include:

- Find relevant data assets.
- Understand the impact of changes to data assets.
- Dig into specific assets and their glossary terms, owners, and more.
- Write first-drafts of SQL queries to answer specific questions.
- Get notified about incidents and updates.

:::warning Permissions

The current version of the DataHub Slack bot assumes that all users have read permissions to all assets. A future version of the Slack bot will support more granular permissions.

:::

<p align="center">
    <img width="60%" alt="Chat experience in Slack with the @DataHub bot." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/chatbot_1.png" />
</p>
