import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Microsoft Teams App Setup

<FeatureAvailability saasOnly />

## Install the DataHub Microsoft Teams App into your Teams workspace

You can see the permissions required by the DataHub Teams bot [below](#datahub-teams-bot-permissions).

### Video Walkthrough

<div align="center"><iframe width="560" height="315" src="https://www.loom.com/embed/teams-setup-guide?sid=teams-app-setup" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen></iframe></div>

### Step-by-step guide

1. Contact your DataHub Customer Success Rep for access to the Teams Private Beta.

The following steps should be performed by a Teams Administrator and Azure Administrator.

## Azure AD App Registration

**Time Required: 15-20 minutes**
**Who: Azure AD Administrator**

1. **Create Azure AD App Registration**
   - Go to [Azure Portal](https://portal.azure.com)
   - Navigate to **Azure Active Directory** > **App registrations**
   - Click **New registration**
   - Fill in the details:
     - **Name**: `DataHub Teams Bot`
     - **Supported account types**: Accounts in this organizational directory only
     - **Redirect URI**: Leave blank
   - Click **Register**

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/azure_app_registration.png"/>
</p>

2. **Get App Credentials**
   - Copy the **Application (client) ID** ✏️ _Save this - you'll provide to DataHub_
   - Copy the **Directory (tenant) ID** ✏️ _Save this - you'll provide to DataHub_
   - Go to **Certificates & secrets** > **New client secret**
   - Add description: "DataHub Teams Integration"
   - Set expiration (recommend 24 months)
   - **Copy the secret value immediately** ✏️ _Save this - you'll provide to DataHub_

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/azure_credentials.png"/>
</p>

3. **Set API Permissions**
   - Go to **API permissions** > **Add a permission**
   - Select **Microsoft Graph** > **Application permissions**
   - Add these permissions:
     - `User.Read.All`
     - `TeamMember.Read.All`
     - `Channel.ReadBasic.All`
   - Click **Grant admin consent for [Your Organization]**

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/azure_permissions.png"/>
</p>

## Create Azure Bot Service

**Time Required: 10-15 minutes**
**Who: Azure Administrator**

1. **Create Bot Resource**
   - In Azure Portal, search for "Azure Bot"
   - Click **Create** > **Azure Bot**
   - Fill in details:
     - **Bot handle**: `datahub-teams-bot`
     - **Subscription**: Your subscription
     - **Resource group**: Create new or use existing
     - **Pricing tier**: F0 (Free) is sufficient for most use cases

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/azure_bot_creation.png"/>
</p>

2. **Configure Bot Settings**
   - Go to your new Bot resource
   - Navigate to **Configuration**
   - Set **Messaging endpoint**: Your DataHub webhook URL (provided by DataHub team)
   - Set **Microsoft App ID**: Use the App ID from Step 1
   - Save configuration

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/azure_bot_config.png"/>
</p>

3. **Enable Teams Channel**
   - Go to **Channels** in your Bot resource
   - Click **Microsoft Teams** channel
   - Click **Apply** to enable

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/azure_teams_channel.png"/>
</p>

## Install Teams App

**Time Required: 10 minutes**
**Who: Microsoft Teams Administrator**

1. **Upload Custom App**
   - Get the ZIP file from your DataHub team: `datahub-teams-app-{your-org}.zip`
   - Open Microsoft Teams Admin Center: [admin.teams.microsoft.com](https://admin.teams.microsoft.com)
   - Go to **Teams apps** > **Manage apps**
   - Click **Upload new app** > **Upload**
   - Select the ZIP file provided by DataHub team
   - The app will appear in your organization's app catalog

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/teams_upload_app.png"/>
</p>

2. **Configure App Policies**
   - Go to **Teams apps** > **Permission policies**
   - Find the policy applied to your users (usually "Global")
   - Ensure **Custom apps** is set to "Allow all apps" or specifically allow "DataHub"

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/teams_app_policies.png"/>
</p>

3. **Install App for Users/Teams**
   - Go to **Teams apps** > **Manage apps**
   - Find "DataHub" in the list
   - Click **DataHub** > **Publish**
   - Users can now install the app from their Teams app store

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/teams_publish_app.png"/>
</p>

## Share Credentials with DataHub Team

Navigate back to your DataHub [Teams Integration setup page](https://longtailcompanions.acryl.io/settings/integrations/teams), and paste the credentials into their respective boxes, and click **'Connect'**.

**Provide these values:**

- **Azure App ID**: [your-app-id-from-azure-registration]
- **Client Secret**: [your-client-secret-from-azure-registration]
- **Tenant ID**: [your-tenant-id-from-azure-registration]

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/datahub_teams_config.png"/>
</p>

:::note
**Security Note**: These credentials are stored securely and used only for Teams integration with your DataHub instance.
:::

## Test the Integration

Congrats 🎉 Teams is set up! Now try it out:

1. **Install Personal App**
   - Open Microsoft Teams
   - Go to **Apps** > search for "DataHub"
   - Click **Add** to install

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/teams_install_personal.png"/>
</p>

2. **Test Basic Commands**
   ```
   /datahub help
   /datahub search customer data
   ```

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/teams_test_commands.png"/>
</p>

3. **Test URL Unfurling**

   - Paste a DataHub entity URL in Teams
   - Verify it shows entity details automatically

4. **Test @Mentions**
   - @mention the DataHub bot with a question
   - Verify it responds with AI-powered answers

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/teams_test_mentions.png"/>
</p>

Now proceed to the [Subscriptions and Notifications page](https://docs.datahub.com/docs/managed-datahub/subscription-and-notification) to see how you can subscribe to be notified about events on the platform, or visit the [Teams App page](saas-teams-app.md) to see how you can use DataHub's powerful capabilities directly within Teams.

### DataHub Teams bot permissions

The DataHub Teams bot requires certain Microsoft Graph permissions to function. We've listed them below with their explanations:

```
# Required to read user information and resolve Teams user IDs
User.Read.All
# Required to read team membership information
TeamMember.Read.All
# Required to read basic channel information for notifications
Channel.ReadBasic.All
```

### Timeline Summary

| Phase                 | Time Required | Dependencies              |
| --------------------- | ------------- | ------------------------- |
| **Azure AD Setup**    | 30-45 minutes | Azure AD admin access     |
| **Bot Service Setup** | 15-20 minutes | Azure admin access        |
| **Teams App Setup**   | 15-20 minutes | Teams admin access        |
| **Testing**           | 10 minutes    | App installation complete |

**Total Time: 1-2 hours**

## Troubleshooting

**"App won't upload to Teams"**

- Ensure you have Teams admin permissions
- Check ZIP file isn't corrupted
- Verify custom app uploads are enabled in Teams admin center

**"Bot doesn't respond to commands"**

- Verify webhook URL is correct in Azure Bot Service
- Check that credentials were provided correctly to DataHub
- Ensure Bot Framework channel is enabled for Teams

**"Permission errors in Azure"**

- Verify admin consent was granted for API permissions
- Check client secret hasn't expired
- Confirm app ID and tenant ID are correct

For additional support, contact your DataHub team or check the [Teams troubleshooting guide](saas-teams-troubleshoot.md).
