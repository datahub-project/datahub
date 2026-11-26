---
title: Power BI Configuration
---

# Configuring Your Power BI Connector to DataHub

Now that you have created a DataHub-specific Microsoft Entra application with the relevant access to Power BI in [the prior step](setup.md), it's now time to set up a connection via the DataHub UI.

## Configure Secrets

1. Within DataHub, navigate to the **Ingestion** tab in the top, right corner of your screen

<p align="center">
  <img width="75%" alt="Navigate to the &quot;Ingestion Tab&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_ingestion_button.png"/>
</p>

:::note
If you do not see the Ingestion tab, please contact your DataHub admin to grant you the correct permissions
:::

2. Navigate to the **Secrets** tab and click **Create new secret**.

<p align="center">
   <img width="75%" alt="Secrets Tab" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_secrets_tab.png"/>
</p>

3. Create a secret to store the Entra app's Client Secret value

This will securely store your client secret value.

- Enter a name like `POWER_BI_CLIENT_SECRET` - we will use this later to refer to the client secret
- Enter the client secret
- Optionally add a description
- Click **Create**

<p align="center">
   <img width="70%" alt="Entra app client secret" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/powerbi/powerbi-client-secret.png"/>
</p>

## Configure Recipe

1.  Navigate to the **Sources** tab and click **Create Source**

  <p align="center">
    <img width="75%" alt="Click &quot;Create Source&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_click_create_new_source_button.png"/>
  </p>

2.  Choose Power BI

  <p align="center">
    <img width="70%" alt="Select Power BI from the options" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/powerbi/powerbi-source-window.png"/>
  </p>

3.  Enter details into the Power BI Recipe

    Enter a **Source Name** at the top of the **Connection Details** page, then fill in the connection fields.

    You need to set minimum 3 field in the recipe:

    a. **tenant_id:** Use the `Directory (tenant) ID` from your Microsoft Entra application.

    b. **client_id:** Use the `Application (client) ID` from your Microsoft Entra application.

    c. **client_secret:** Use the secret POWER_BI_CLIENT_SECRET with the format "${POWER_BI_CLIENT_SECRET}".

    d. **environment:** (Optional) Specify the Power BI environment to connect to. Use 'commercial' for commercial Power BI (default) or 'government' for Power BI Government Community Cloud (GCC).

Optionally, use the `workspace_id_pattern` field to filter for specific workspaces.

    config:
         ...
         workspace_id_pattern:
            allow:
              - "258829b1-82b1-4bdb-b9fb-6722c718bbd3"

Your recipe should look something like this:

<p align="center">
  <img width="70%" alt="tenant id" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/powerbi/powerbi-recipe-window.png"/>
</p>

## Schedule Execution

Now it's time to schedule a recurring ingestion pipeline to regularly extract metadata from your Power BI instance.

1. In the **Sync Schedule** section, turn on **Run on a schedule**, then decide how regularly you want this ingestion to run-- day, month, year, hour, minute, etc. Select from the dropdown

<p align="center">
    <img width="75%" alt="schedule selector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_set_execution_schedule.png"/>
</p>

2. Ensure you've configured your correct timezone
<p align="center">
    <img width="75%" alt="timezone_selector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_set_execution_timezone.png"/>
</p>

## Save and Run

1. Click **Save and Run** at the top of the page

You will now find your new ingestion source running

<p align="center">
  <img width="75%" alt="ingestion_running" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/powerbi/powerbi-ingestion-running.png"/>
</p>

## Validate Ingestion Runs

1. View the latest status of ingestion runs on the Ingestion page

<p align="center">
  <img width="75%" alt="ingestion succeeded" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/powerbi/powerbi-ingestion-succeeded.png"/>
</p>

2. Open the **Run History** tab to see the full list of historical runs and outcomes; click a run's status to open its **Run Details** page

<p align="center">
  <img width="75%" alt="ingestion_details" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/powerbi/powerbi-ingestion-history.png"/>
</p>

3. In the **Assets** section of the **Run Details** page, click **View all** to see which entities were ingested

<p align="center">
  <img width="75%" alt="ingestion_details_view_all" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/powerbi/powerbi-ingestion-detail.png"/>
</p>

4. Pick an entity from the list to manually validate if it contains the detail you expected

<p align="center">
  <img width="75%" alt="ingestion_details_view_all" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/powerbi/powerbi-ingestion-assets.png"/>
</p>

**Congratulations!** You've successfully set up Power BI as an ingestion source for DataHub!
