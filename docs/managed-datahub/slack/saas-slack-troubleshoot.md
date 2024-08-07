import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Troubleshoot Slack Issues

<FeatureAvailability saasOnly />

This document provides troubleshooting guidance for the Slack integration. For more details on setting up the Slack integration, [click here](./saas-slack-setup.md).

## Prerequisites - Testing the Slack Integration
First and foremost, we recommend using the 'Send a test notification' feature to verify whether the issue is with the integration setup, slack's systems, or DataHub. The modal will provide a rich description of an error if there is one.
You can access this feature either by going to the Notifications page in your settings, or a subscription drawer.
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/setup_7.png"/>
</p>

## Test notification failed with 'Re-Connect DataHub to Slack'
There are several reasons why sending a test notification would fail. The description in the modal should give you insights as to what's broken and what you can do to resolve this issue.
If you're seeing a message that recommends the DataHub admin to re-connect Slack to DataHub, you may want to try the following options:

### Refresh the existing app installation (Recommended)
:::note
Whomever originally installed the Slack app will need to perform this.
If they are unable to do this, you may need to go down the 'Install a new app' path below.
:::
1. Get your App Config tokens by following the first few steps outlined in the [installation guide](https://datahubproject.io/docs/managed-datahub/slack/saas-slack-setup/#step-by-step-guide). If it's showing expired tokens, feel free to delete them and create a new set.
2. Paste them into their respective text inputs, and hit **'Re-connect'**
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/re_connect_1.png"/>
</p>
3. You will be re-directed to a page where you can finalize the app refresh.

### Install a new app
:::note
If you choose to install a new app, your team will have to re-add the new bot into any private channels the old one was previously in.
If you'd like support in getting a list of the private channels that are subscribed to Slack notifications on DataHub, please reach out to your customer success representative.
:::

1. Get your App Config tokens by following the first few steps outlined in the [installation guide](https://datahubproject.io/docs/managed-datahub/slack/saas-slack-setup/#step-by-step-guide). If it's showing expired tokens, feel free to delete them and create a new set.
2. Paste them into their respective text inputs, and hit **'create a new installation'**
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/re_connect_1_1.png"/>
</p>
3. You will be re-directed to a page where you can finalize the app installation.
4. Now to uninstall the old app, visit the **'Manage Apps'** page for your Slack workspace.
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/re_connect_2.png"/>
</p>
5. Find the previously installed DataHub Slack bot in the list of installed apps, and open it.
6. Open the 'App details' page
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/re_connect_3.png"/>
</p>
6. Then, switch to the Configuration tab
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/re_connect_4.png"/>
</p>
6. Finally, scroll to the bottom to find the remove button.
<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/slack/re_connect_5.png"/>
</p>


## Test notification works, but not receiving notifications
There are a few reasons why you may not receive notifications when you'd expect.

### Actors do not receive notifications for their actions
If you've subscribed to an entity, and then performed an action (i.e., raised an incident or added a tag), you will not be notified about your own action.

### There is an issue with DataHub's systems
If sending a test notification works, and you've verified that none of the above cases apply, then you should contact your DataHub Customer Success rep to help troubleshoot and resolve the issue. 

