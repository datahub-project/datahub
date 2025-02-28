import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Subscriptions & Notifications

<FeatureAvailability saasOnly />

DataHub's Subscriptions and Notifications feature gives you real-time change alerts on data assets of your choice.
With this feature, you can set up subscriptions to specific changes for an Entity – and DataHub will notify you when those changes happen. Currently, DataHub supports notifications on Slack and Email, with support for Microsoft Teams forthcoming.

Email will work out of box. For installing the DataHub Slack App, see:
👉 [Configure Slack for Notifications](slack/saas-slack-setup.md)

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-user-notifications-in-slack.png"/>
</p>

This feature is especially useful in helping you stay on top of any upstream changes that could impact the assets you or your stakeholders rely on. It eliminates the need for you and your team to manually check for upstream changes, or for upstream stakeholders to identify and notify impacted users.
As a user, you can subscribe to and receive notifications about changes such as deprecations, schema changes, changes in ownership, assertions, or incidents. You’ll always been in the know about potential data quality issues so you can proactively manage your data resources.


## Platform Admin Notifications

Datahub provides three levels of notifications:

- **Platform-level**
- **Group-level** (described in other sections)
- **User-level** (described in other sections)

**Setting Platform-Level Notifications:**
This requires appropriate permissions. Go to `Settings` > `Notifications` (under the `Platform` section, not `My Notifications`).

**Platform-level Notifications:**
Platform-level notifications are applied to all assets within Datahub.
Example: If "An owner is added or removed from a data asset" is ticked, the designated Slack channel or email will receive notifications for any such changes across all assets.

**Our Recommendations:**

Notifying on tag changes for every asset in the platform would be noisy, and so we recommend to use these platform-level notifications only where appropriate. For example, we recommend notifications for ingestion failures routed to a central Slack channel or email. This will help you proactively ensure your Datahub metadata stays fresh.

## Prerequisites

Once you have [configured Slack within your DataHub instance](slack/saas-slack-setup.md), you will be able to subscribe to any Entity in DataHub and begin recieving notifications via DM.

To begin receiving personal notifications, go to Settings > "My Notifications". From here, toggle on Slack Notifications and input your Slack Member ID.

If you want to create and manage group-level Subscriptions for your team, you will need [the following privileges](../../docs/authorization/roles.md#role-privileges):

- Manage Group Notification Settings
- Manage Group Subscriptions

And to manage other user's subscriptions:
- Manage User Subscriptions

## Using DataHub’s Subscriptions and Notifications Feature

The first step is identifying the assets you want to subscribe to. 
DataHub’s [Lineage and Impact Analysis features](../../docs/act-on-metadata/impact-analysis.md#lineage-impact-analysis-setup-prerequisites-and-permissions) can help you identify upstream entities that could impact the assets you use and are responsible for.
You can use the Subscriptions and Notifications feature to sign up for updates for your entire team, or just for yourself.

### Subscribing Your Team/Group to Notifications

The dropdown menu next to the Subscribe button lets you choose who the subscription is for. To create a group subscription, click on Manage Group Subscriptions.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-subscription-dropdown.png"/>
</p>

Next, customize the group’s subscriptions by selecting the types of changes you want the group to be notified about.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-group-subscription-settings.png"/>
</p>

Connect to Slack. Currently, Acryl's Subscriptions and Notifications feature integrates only with Slack. Add your group’s Slack Channel ID to receive notifications on Slack.
(You can find your Channel ID in the About section of your channel on Slack.)

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-group-set-channel-id.png"/>
</p>

### Individually Subscribing to an Entity

Select the **Subscribe Me** option in the Subscriptions dropdown menu.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-subscription-dropdown-zoom.png"/>
</p>

Pick the updates you want to be notified about, and connect your Slack account by using your Slack Member ID.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-user-subscription-settings.png"/>
</p>

:::note
You can find your Slack Member ID in your profile settings.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-user-set-user-id.png"/>
</p>
:::

### Managing Your Subscriptions

You can enable, disable, or manage notifications at any time to ensure that you receive relevant updates.

Simply use the Dropdown menu next to the Subscribe button to unsubscribe from the asset, or to manage/modify your subscription (say, to modify the changes you want to be updated about).

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-subscription-dropdown.png"/>
</p>

You can also view and manage your subscriptions in your DataHub settings page.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-user-subscriptions.png"/>
</p>

You can view and manage the group’s subscriptions on the group’s page on DataHub.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-group-subscriptions.png"/>
</p>

### Subscribing to Assertions
You can always subscribe to _all assertion status changes_ on a table using the steps outlined in the earlier sections. However, in some cases you may want to only be notified about specific assertions on a table. For instance, a table may contain several subsets of information, segmented by a category column - so there may be several different checks for each category. As a consumer, you may only care about the freshness check that runs on one specific category of this larger table.

You can subscribe to individual assertions by clicking the bell button on the assertion itself - either in the list view:
<p align="center">
  <img width="70%" alt="1" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-assertion-sub-1.jpg" />
</p>

Or on the assertion's profile page:
<p align="center">
  <img width="70%" alt="2" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-assertion-sub-2.jpg" />
</p>


Note: if you are subscribed to all assertions at the dataset level, then you will not be able to **Unsubscribe** from an individual assertion.
<p align="center">
  <img width="70%" alt="3" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-assertion-sub-unsub-1.jpg" />
</p>

You must first remove your dataset-level subscription:
<p align="center">
  <img width="70%" alt="4" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-assertion-sub-unsub-2.jpg" />
  <img width="70%" alt="5" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-assertion-sub-unsub-3.jpg" />
</p>


Then select individual assertions you'd like to subscribe to:
<p align="center">
  <img width="70%" alt="7" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-assertion-sub-resub-1.jpg" />
</p>



## FAQ

<details>
<summary>
What changes can I be notified about using this feature?
</summary>
You can subscribe to deprecations, Assertion status changes, Incident status changes, Schema changes, Ownership changes, Glossary Term changes, and Tag changes.
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-user-subscription-settings-zoom.jpg"/>
</p>
</details>

<details>
<summary>
What if I no longer want to receive updates about a data asset?
</summary>
You can unsubscribe from any asset to stop receiving notifications about it. On the asset’s DataHub page, simply use the dropdown menu next to the Subscribe button to unsubscribe from the asset.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/subscription-and-notification/s_n-subscription-dropdown.png"/>
</p>
</details>

<details>
<summary>
What if I want to be notified about different changes?
</summary>
To modify your subscription, use the dropdown menu next to the Subscribe button to modify the changes you want to be notified about.
</details>
<details>
<summary>
I want to configure multiple channels. How many Slack channels or emails can I configure to get notified? 
</summary>
At the platform-level, you can configure one email and one Slack channel.

At the user and group -levels, you can configure one default email and Slack channel as well as overwrite that email/channel when you
go to a specific asset to subscribe to. 

To configure multiple channels, as a prereq, ensure you have the appropriate privileges. And then:
1. Create a datahub group for each channel you want notifications for. 
2. Add yourself as a member to each of the groups.
3. Now, when you visit an asset and go to subscribe, you'll see the option "Manage Group Subscriptions".

</details>

## Reference

- [DataHub Blog - Simplifying Data Monitoring & Management with Subscriptions and Notifications with DataHub Cloud](https://www.acryldata.io/blog/simplifying-data-monitoring-and-management-with-subscriptions-and-notifications-with-acryl-datahub)
- Video Guide - Getting Started with Subscription & Notifications
    <iframe width="560" height="315" src="https://www.loom.com/embed/f02fe71e09494b5e82904c8a47f06ac1?sid=ef041cb7-9c06-4926-8e0c-e948b1dc3af0" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen></iframe>
