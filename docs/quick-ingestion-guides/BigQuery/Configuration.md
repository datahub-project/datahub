---
title: Configuration
---
# Configuring Your BigQuery Connector to DataHub

## UI Ingestion

* Navigate to the "Ingestion Tab"

  <img width="70%" alt="Navigate to the &quot;Ingestion Tab&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_ingestion_button.png"/>

## Step 1: Configure Secrets
1. Select `Secrets tab`

   <img width="70%" alt="Secrets Tab" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_secrets_tab.png"/>
   
3. Add from Service Account Key's `Private Key` as secret:
   * Click on `Create new secret` button
   * Enter a name like `BIGQUERY_PRIVATE_KEY` to the secret which is used to refer to this secret in the future
   * Copy/Paste the Private Key value from your Service Account Key
   * Optionally add a description
   
   <img width="70%" alt="Private Key Secret" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_private_key_secret.png"/>

5. Add from Service Account Key's `Private Key Id` as secret:
   * Click on `Create new secret` button
   * Enter a name like `BIGQUERY_PRIVATE_KEY_ID` to the secret which is used to refer to this secret in the future
   * Copy/Paste the Private Key Id value from your Service Account Key
   * Optionally add a description
     
   <img width="70%" alt="Private Key Id Secret" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_private_key_id_secret.png"/>

## Step 2: Configure Recipe
* Click "Create new source"

  <img width="70%" alt="Click &quot;Create new source&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_click_create_new_source_button.png"/>

* Select BigQuery from the options

  <img width="70%" alt="Select BigQuery from the options" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_bigquery_button.png"/>

* Using the Project Id/Private key/Private Key Id you noted down during the prerequesites, fill out X field with Y information
1. Enter from Service Account Key's `project_id` to `project_id` field:

   <img width="70%" alt="Project Id Field" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_project_id_field.png"/>
2. Add the previously added `Private Key` secret to `Private Key` field:
   * Click on the `Private Key` input field
   * Select `BIGQUERY_PRIVATE_KEY` secret

     <img width="70%" alt="private key field" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_private_key_field.png"/>
3. Add the previousl added `Private Key Id` secret to `Private Key Id` field:
   * Click on the `Private Key Id` input field
   * Select `BIGQUERY_PRIVATE_KEY_ID` secret

     <img width="70%" alt="private_key_id field" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_private_key_id_field.png"/>
4. Copy/Paste Service Account Key's `client_email` to `Client Email` field:

   <img width="70%" alt="client email field" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_client_email.png"/>
5. Copy/Paste Service Account Key's `client_id` to `Client Id` field:

   <img width="70%" alt="client id field" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_client_id_field.png"/>
6. Click `Next` when you are done

## Step 3: Schedule Execution

* Decide how regularly you want this ingestion to run-- day, month, year, hour, minute, etc. Select from the dropdown

  <img width="70%" alt="schedule selector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_scheduled_execution.png"/>

* Ensure you've configured your correct timezone.

  <img width="70%" alt="timezone_selector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_timezone_selector.png"/>

* Click `Next` when you are done.

## Step 4: Finish Up
* Name your ingestion source.

  <img width="70%" alt="Name your ingestion" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_name_ingestion.png"/>

* Click `Save & Run`.
* Now you should see your ingestion job running

  <img width="70%" alt="ingestion_running" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_running.png"/>

## Validating Your Ingestion Run
* Review the "Manage Ingestion" Page to ensure that your ingestion job has executed with status `succeeded`

  <img width="70%" alt="ingestion succeeded" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_succeeded.png"/>
* Click on the `+` icon on the left of the ingestion to expand the run history
* Click on `Details` to see the ingestion summary page

  <img width="70%" alt="ingestion_details" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_details.png"/>
* Click on `View all` to see all the ingested assets

  <img width="70%" alt="ingestion_details_view_all" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_details_view_all.png"/>
* Pick one and validate if it was ingested correctly

  <img width="70%" alt="ingestion_details_view_all" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_ingested_assets.png"/>
* Congratulations! You've successful set up BigQuery as an ingestion source for DataHub!

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*