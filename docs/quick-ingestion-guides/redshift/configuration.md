---
title: Redshift Configuration
---

# Configuring Your Redshift Connector to DataHub

Now that you have created a DataHub user in Redshift in [the prior step](setup.md), it's time to set up a connection via the DataHub UI.

## Configure Secrets

1. Within DataHub, navigate to the **Ingestion** tab in the top, right corner of your screen

<p align="center">
  <img width="75%" alt="Navigate to the &quot;Ingestion Tab&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/redshift/redshift_ingestion_ingestion_button.png"/>
</p>

:::note
If you do not see the Ingestion tab, please contact your DataHub admin to grant you the correct permissions
:::

2. Navigate to the **Secrets** tab and click **Create new secret**

<p align="center">
   <img width="75%" alt="Secrets Tab" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/redshift/redshift_ingestion_secrets_tab.png"/>
</p>

3. Create a Redshift User's Password secret

This will securely store your Redshift User's password within DataHub

- Click **Create new secret** again
- Enter a name like `REDSHIFT_PASSWORD` - we will use this later to refer to the secret
- Enter your `datahub` redshift user's password
- Optionally add a description
- Click **Create**

<p align="center">
   <img width="75%" alt="Redshift Password Secret" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/redshift/redshift_ingestion_password_secret.png"/>
</p>

## Configure Recipe

4. Navigate to the **Sources** tab and click **Create Source**

<p align="center">
  <img width="75%" alt="Click &quot;Create Source&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/redshift/redshift_ingestion_click_create_new_source_button.png"/>
</p>

5. Select Redshift

<p align="center">
  <img width="75%" alt="Select BigQuery from the options" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/redshift/redshift_ingestion_redshift_button.png"/>
</p>

6. Fill out the Redshift Recipe

Enter a **Source Name** at the top of the **Connection Details** page, then fill in the connection fields.

Populate the Password field by selecting Redshift Password secrets you created in steps 3 and 4.

<p align="center">
  <img width="75%" alt="Fill out the Redshift Recipe" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/redshift/redshift-ingestion-recipe.png"/>
</p>

<!---

7. Click **Test Connection**

This step will ensure you have configured your credentials accurately and confirm you have the required permissions to extract all relevant metadata.

<p align="center">
  <img width="75%" alt="Test BigQuery connection" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery-test-connection.png"/>
</p>

-->

## Schedule Execution

Now it's time to schedule a recurring ingestion pipeline to regularly extract metadata from your Redshift instance.

7. In the **Sync Schedule** section, turn on **Run on a schedule**, then decide how regularly you want this ingestion to run-- day, month, year, hour, minute, etc. Select from the dropdown

<p align="center">
    <img width="75%" alt="schedule selector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/redshift/redshift_ingestion_scheduled_execution.png"/>
</p>

8. Ensure you've configured your correct timezone

<p align="center">
    <img width="75%" alt="timezone_selector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/redshift/redshift_ingestion_timezone_selector.png"/>
</p>

## Save and Run

9. Click **Save and Run** at the top of the page

You will now find your new ingestion source running

<p align="center">
  <img width="75%" alt="ingestion_running" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/redshift/redshift_ingestion_running.png"/>
</p>

## Validate Ingestion Runs

10. View the latest status of ingestion runs on the Ingestion page

<p align="center">
  <img width="75%" alt="ingestion succeeded" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/redshift/redshift_ingestion_succeeded.png"/>
</p>

11. Open the **Run History** tab to see the full list of historical runs and outcomes; click a run's status to open its **Run Details** page

<p align="center">
  <img width="75%" alt="ingestion_details" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/redshift/redshift_ingestion_details.png"/>
</p>

12. In the **Assets** section of the **Run Details** page, click **View all** to see which entities were ingested

<p align="center">
  <img width="75%" alt="ingestion_details_view_all" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/redshift/redshift_ingestion_details_view_all.png"/>
</p>

13. Pick an entity from the list to manually validate if it contains the detail you expected

<p align="center">
  <img width="75%" alt="ingestion_details_view_all" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/redshift/redshift_ingestion_ingested_assets.png"/>
</p>

**Congratulations!** You've successfully set up Redshift as an ingestion source for DataHub!
