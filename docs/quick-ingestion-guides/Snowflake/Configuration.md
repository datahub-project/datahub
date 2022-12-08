---
title: Configuration
---
# Configuring Your Snowflake Connector Within DataHub

## UI Ingestion

* Navigate to the **Ingestion** Tab

  <img width="70%" alt="Navigate to the &quot;Ingestion Tab&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_ingestion_button.png"/>

## Step 1: Configure Secrets

1. Select **Secrets** tab

   <img width="70%" alt="Secrets Tab" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_secrets_create_secret_button.png"/>

2. Add **Password** as secret:
   * Click on **Create new secret** button.
   * Enter a name like `SNOWFLAKE_PASSWORD` to the secret. This name is used to refer to this secret in the future.
   * Enter password configured for DataHub user in previous step.
   * Optionally add a description.

   <img width="70%" alt="Snowflake Password Secret" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_password_secret.png"/>

## Step 2: Configure Recipe

* Click **Create new source**

  <img width="70%" alt="Click &quot;Create new source&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_create_new_source_button.png"/>

* Select Snowflake from the options

  <img width="70%" alt="Select Snowflake from the options" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_snowflake_source.png"/>

* Fill out the Snowflake Recipe.

1. Enter the Snowflake Account Identifier as **Account ID** field. Account identifier is the part before `.snowflakecomputing.com` in your snowflake host URL :

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
