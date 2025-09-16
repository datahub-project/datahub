import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Microsoft Teams App Setup

<FeatureAvailability saasOnly />

## Installing the DataHub Microsoft Teams App

To enable the DataHub Teams app, you will need to install the DataHub Teams bot into your Teams workspace.
DataHub Teams app is currently in **Private Beta** - you will need to manually upload the Teams app package (a ZIP file) to your Teams workspace.

The following steps should be performed by a Teams or Azure Administrator:

### Creating the DataHub App in Teams

1. Download **Teams App zip** file [here](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/setup/datahub-teams.zip)

2. Open [Teams Admin UI](https://admin.teams.microsoft.com/) in your web browser, and in the sidebar click **Teams Apps** > **Manage Apps**
   <p align="center"><img width="80%" alt="Connected to Microsoft Teams." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/setup/step-1.png" />

3. On the Apps page, click **Actions** > **Upload new app**
   <p align="center"><img width="80%" alt="Connected to Microsoft Teams." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/setup/step-2.png" />

4. In the "Upload a custom app" window, click **Upload**, and then in the file browser, select the ZIP file you downloaded in Step #1.
   <p align="center"><img width="80%" alt="Connected to Microsoft Teams." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/setup/step-3.png" />

5. Once the app is uploaded, log out of Teams Admin and User UIs, wait 5 minutes, and then log back into [Teams Web UI](https://teams.microsoft.com/v2/) in a new tab.
   <p align="center"><img width="80%" alt="Connected to Microsoft Teams." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/setup/step-4.png" />

6. On the sidebar in the Teams Web UI, click **Apps** > **Built for your org**
   <p align="center"><img width="80%" alt="Connected to Microsoft Teams." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/setup/step-5.png" />

7. On **Built for your org** page, locate **DataHub** application in the list, and click **Add**.
   <p align="center"><img width="80%" alt="Connected to Microsoft Teams." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/setup/step-6.png" />
   <p align="center"><img width="80%" alt="Connected to Microsoft Teams." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/setup/step-7.png" />

8. Confirm installation by clicking **Add** in the pop-up
   <p align="center"><img width="80%" alt="Connected to Microsoft Teams." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/setup/step-8.png" />

9. In the pop-up, click **Open** to open a chat window with the DataHub Teams bot.
   <p align="center"><img width="80%" alt="Connected to Microsoft Teams." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/setup/step-9.png" />

### Connecting the App to Your DataHub Instance

10. **First, get your team URL:** On the sidebar, click **Chat**, locate your team name in the list, and click "..." next to open a dropdown manu. Then click **Copy Link**
   <p align="center"><img width="80%" alt="Connected to Microsoft Teams." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/setup/step-10.png" />

11. **Then, link in DataHub:** In a separate web browser tab, open your DataHub UI, go to **Settings** > **Integrations** and click **Teams**
   <p align="center"><img width="80%" alt="Connected to Microsoft Teams." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/setup/step-11.png" />

12. Paste the URL obtained is Step #10 into the Teams URL field, and click **Connect to Teams**
   <p align="center"><img width="80%" alt="Connected to Microsoft Teams." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/setup/step-12.png" />

13. When prompted, complete the Teams authentication flow. After completion, you should be redirected back to DataHub.

Your Teams App is now ready to use! Add the DataHub bot to any channel in your Teams workspace, and start asking questions about your data.

## Connecting Your Personal Teams Account

To receive personal notifications via teams (e.g. for asset subscriptions), you'll need to connect your Teams account to your DataHub user profile.
This can be done by navigating to **Settings** > **My Notifications** and enable Teams notifications by clicking the toggle
switch on the right hand side.

<p align="center">
    <img width="80%" alt="Connect to Microsoft Teams." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/connect_user.png" />
</p>

Next, click **Connect to Teams** to be redirected to teams, where you can bind your account.
Upon successfully linking accounts, you'll be redirected to DataHub.

<p align="center">
    <img width="80%" alt="Connected to Microsoft Teams." src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/teams/connected_user.png" />
</p>
