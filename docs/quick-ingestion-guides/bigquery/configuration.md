---
title: Configuration
---
# Configuring Your BigQuery Connector to DataHub

Now that you have created a Service Account and Service Account Key in BigQuery in [the prior step](setup.md), it's now time to set up a connection via the DataHub UI.

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


3. Create a Private Key secret

This will securely store your BigQuery Service Account Private Key within DataHub

  * Enter a name like `BIGQUERY_PRIVATE_KEY` - we will use this later to refer to the secret
  * Copy and paste the `private_key` value from your Service Account Key
  * Optionally add a description
  * Click **Create**

<p align="center">   
   <img width="75%" alt="Private Key Secret" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_private_key_secret.png"/>
</p>

4. Create a Private Key ID secret

This will securely store your BigQuery Service Account Private Key ID within DataHub
   
  * Click **Create new secret** again
  * Enter a name like `BIGQUERY_PRIVATE_KEY_ID` - we will use this later to refer to the secret
  * Copy and paste the `private_key_id` value from your Service Account Key
  * Optionally add a description
  * Click **Create**

<p align="center">     
   <img width="75%" alt="Private Key Id Secret" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_private_key_id_secret.png"/>
</p>

## Configure Recipe

5. Navigate to the **Sources** tab and click **Create new source**

<p align="center">
  <img width="75%" alt="Click &quot;Create new source&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_click_create_new_source_button.png"/>
</p>

6. Select BigQuery

<p align="center">
  <img width="75%" alt="Select BigQuery from the options" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_bigquery_button.png"/>
</p>

7. Fill out the BigQuery Recipe

You can find the following details in your Service Account Key file:

* Project ID
* Client Email
* Client ID

Populate the Secret Fields by selecting the Primary Key and Primary Key ID secrets you created in steps 3 and 4. 

<p align="center">
  <img width="75%" alt="Fill out the BigQuery Recipe" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery-ingestion-recipe.png"/>
</p>

8. Click **Test Connection**

This step will ensure you have configured your credentials accurately and confirm you have the required permissions to extract all relevant metadata.

<p align="center">
  <img width="75%" alt="Test BigQuery connection" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery-test-connection.png"/>
</p>

After you have successfully tested your connection, click **Next**.

## Schedule Execution

Now it's time to schedule a recurring ingestion pipeline to regularly extract metadata from your BigQuery instance.

9. Decide how regularly you want this ingestion to run-- day, month, year, hour, minute, etc. Select from the dropdown
<p align="center">
    <img width="75%" alt="schedule selector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_scheduled_execution.png"/>
</p>  

10. Ensure you've configured your correct timezone
<p align="center">
    <img width="75%" alt="timezone_selector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_timezone_selector.png"/>
</p>  

11. Click **Next** when you are done

## Finish Up

12. Name your ingestion source, then click **Save and Run**
<p align="center">
  <img width="75%" alt="Name your ingestion" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_name_ingestion.png"/>
</p>  

You will now find your new ingestion source running

<p align="center">
  <img width="75%" alt="ingestion_running" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_running.png"/>
</p>  

## Validate Ingestion Runs

13. View the latest status of ingestion runs on the Ingestion page

<p align="center">
  <img width="75%" alt="ingestion succeeded" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_succeeded.png"/>
</p>  

14. Click the plus sign to expand the full list of historical runs and outcomes; click **Details** to see the outcomes of a specific run

<p align="center">
  <img width="75%" alt="ingestion_details" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_details.png"/>
</p>

15. From the Ingestion Run Details page, pick **View All** to see which entities were ingested

<p align="center">
  <img width="75%" alt="ingestion_details_view_all" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_details_view_all.png"/>
</p>  

16. Pick an entity from the list to manually validate if it contains the detail you expected  

<p align="center">
  <img width="75%" alt="ingestion_details_view_all" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/bigquery/bigquery_ingestion_ingested_assets.png"/>
</p>  


**Congratulations!** You've successfully set up BigQuery as an ingestion source for DataHub!

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*
