---
title: Configuration
---
# Configuring Your Snowflake Connector to DataHub

Now that you have created a DataHub-specific user with the relevant roles in Snowflake in [the prior step](setup.md), it's now time to set up a connection via the DataHub UI.

## Configure Secrets

1. Within DataHub, navigate to the **Ingestion** tab in the top, right corner of your screen

<p align="center">
  <img width="75%" alt="Navigate to the &quot;Ingestion Tab&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_ingestion_button.png"/>
</p>

:::note
If you do not see the Ingestion tab, please contact your DataHub admin to grant you the correct permissions
:::

2. Navigate to the **Secrets** tab and click **Create new secret**

<p align="center">
   <img width="75%" alt="Secrets Tab" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_secrets_tab.png"/>
</p>

3. Add **Password** as secret:
   * Enter a name like `SNOWFLAKE_PASSWORD` - we will use this later to refer to the secret
   * Enter password configured for DataHub user in previous step
   * Optionally add a description
   * Click **Create**

   <img width="70%" alt="Snowflake Password Secret" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_password_secret.png"/>

## Configure Recipe

4. Navigate to the **Sources** tab and click **Create new source**

<p align="center">
  <img width="75%" alt="Click &quot;Create new source&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_click_create_new_source_button.png"/>
</p>

5. Select Snowflake

<p align="center">
  <img width="70%" alt="Select Snowflake from the options" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_snowflake_source.png"/>
</p>

###### PICK UP WORK HERE 
6. Fill out the Snowflake Recipe

Enter the Snowflake Account Identifier as **Account ID** field. Account identifier is the part before `.snowflakecomputing.com` in your snowflake host URL:

   <img width="70%" alt="Account Id Field" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_account_id_field.png"/>

2. Enter previously used Snowflake warehouse as **Warehouse** field :

   <img width="70%" alt="Warehouse Field" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_warehouse_field.png"/>
3. Enter previously created Snowflake DataHub User's name as **Username** field :

   <img width="70%" alt="Username Field" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_username_field.png"/>
4. Add the previously added Password secret to **Password** field:
   * Click on the Password input field
   * Select `SNOWFLAKE_PASSWORD` secret

     <img width="70%" alt="Password field" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_password_secret_field.png"/>
5. Enter previously created Snowflake DataHub role as **Role** field :

   <img width="70%" alt="Role Field" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_role_field.png"/>
6. Click **Next** when you are done.

## Step 3: Schedule Execution

* Decide how regularly you want this ingestion to run-- day, month, year, hour, minute, etc. Select from the dropdown.

  <img width="70%" alt="schedule selector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_schedule_execution_schedule.png"/>

* Ensure you've configured your correct timezone.

  <img width="70%" alt="timezone_selector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_schedule_execution_timezone.png"/>

* Click **Next** when you are done.

## Step 4: Finish Up

* Name your ingestion source.

  <img width="70%" alt="Name your ingestion" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_name_ingestion_source.png"/>

* Click **Save & Run**.
* Now you should see your ingestion job running

  <img width="70%" alt="Ingestion Running" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_source_running.png"/>

## Validating Your Ingestion Run

* Review the **Manage Ingestion** Page to ensure that your ingestion job has executed with status **Succeeded**

  <img width="70%" alt="Ingestion Succeeded" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_ingestion_succeded.png"/>
* Click on the **+** icon on the left of the ingestion to expand the run history
* Click on **Details** to see the ingestion summary page

  <img width="70%" alt="Ingestion Details" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_ingestion_details.png"/>
* Click on **View all** to see all the ingested assets

  <img width="70%" alt="Ingestion Details View All" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_details_view_all.png"/>
* Pick one and validate if it was ingested correctly

  <img width="70%" alt="Ingestion Details View Ingested Assets" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_view_ingested_assets.png"/>
* Congratulations! You've successful set up Snowflake as an ingestion source for DataHub!

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*