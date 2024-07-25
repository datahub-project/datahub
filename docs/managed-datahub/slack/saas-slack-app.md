import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Slack App Features

<FeatureAvailability saasOnly />

## Overview
The DataHub Slack App brings several of DataHub's key capabilities directly into your Slack experience. These include:
1. Searching for Data Assets
2. Subscribing to notifications for Data Assets
3. Managing Data Incidents

*Our goal with the Slack app is to make data discovery easier and more accessible for you.*

## Slack App Commands
The command-based capabilities on the Slack App revolve around search.

### Querying for Assets
You can trigger a search by simplying typing `/acryl my favorite table`.
<p align="center">
    <img width="70%" alt="Example of an in-Slack Acryl search command being performed." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/slack_app_search_1.png" />
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
The DataHub UI offers a rich set of [Incident tracking and management](https://datahubproject.io/docs/incidents/incidents/) features.
When a Slack member or channel receives notifications about an Incident, many of these features are made accessible right within the Slack app.

When an incident is raised, you will recieve rich context about the incident in the Slack message itself. You will also be able to `Mark as Resolved`, update the `Priorty`, set a triage `Stage` and `View Details` - directly from the Slack message.
<p align="center">
    <img width="70%" alt="Example of search results being displayed within Slack." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/slack_incidents_1.png" />
</p>

If you choose to `Mark as Resolved` the message will update in-place, and you will be presented with the ability to `Reopen Incident` should you choose.
<p align="center">
    <img width="70%" alt="Example of search results being displayed within Slack." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/slack_incidents_2.png" />
</p>


## Coming Soon
We're constantly working on rolling out new features for the Slack app, stay tuned!

