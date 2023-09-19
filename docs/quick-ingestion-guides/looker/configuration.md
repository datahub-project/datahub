---
title: Configuration
---
# Configuring Looker & LookML Connector

Now that you have created a DataHub specific API key with the relevant access in [the prior step](setup.md), it's now time to set up a connection via the DataHub UI.

## Configure Looker Ingestion 

### Configure Secrets

Within DataHub, navigate to the **Ingestion** tab in the top, right corner of your screen

<p align="center">
  <img width="75%" alt="Navigate to the &quot;Ingestion Tab&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_ingestion_button.png"/>
</p>

:::note
If you do not see the Ingestion tab, please contact your DataHub admin to grant you the correct permissions
:::

Navigate to the **Secrets** tab and click **Create new secret**.

<p align="center">
   <img width="75%" alt="Secrets Tab" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_secrets_tab.png"/>
</p>

Create a client id secret:

* Enter a name like `LOOKER_CLIENT_ID` - we will use this later to refer to the `API Key Client ID`
* Enter the `API Key Client ID`
* Optionally add a description
* Click **Create**

<p align="center">
   <img width="70%" alt="API Key Client ID" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-client-id-secret.png"/>
</p>

Then, create a secret to store the API Key Client Secret:

* Enter a name like `LOOKER_CLIENT_SECRET` - we will use this later to refer to the client secret
* Enter the client secret
* Optionally add a description
* Click **Create**

<p align="center">
   <img width="70%" alt="API Key client secret" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-client-secret.png"/>
</p>

### Configure Recipe

Navigate to the **Sources** tab and click **Create new source**.

  <p align="center">
    <img width="75%" alt="Click &quot;Create new source&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_click_create_new_source_button.png"/>
  </p>

Choose `Looker`.

  <p align="center">
    <img width="70%" alt="Select Looker from the options" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-choose-looker.png"/>
  </p>

Enter details into the Looker Recipe. You need to set minimum 3 field in the recipe:

* **Base URL:** This is your looker instance URL. (For example https://abc.cloud.looker.com)
* **Client ID:** Use the secret LOOKER_CLIENT_ID with the format `${LOOKER_CLIENT_ID}`.
* **Client Secret:** Use the secret LOOKER_CLIENT_SECRET with the format `${LOOKER_CLIENT_SECRET}`.


Optionally, use the `dashboard_pattern` and `chart_pattern` fields to filter for specific dashboard and chart.

    config:
         ...
         dashboard_pattern:
            allow:
              - "2"
         chart_pattern:
            allow:
              - "258829b1-82b1-4bdb-b9fb-6722c718bbd3"

Your recipe should look something like this:
<p align="center">
  <img width="70%" alt="Looker Recipe" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-ingestion-source-recipe.png"/>
</p>


After completing the recipe, click **Next**.    

### Schedule Execution

Now it's time to schedule a recurring ingestion pipeline to regularly extract metadata from your Looker instance.

Decide how regularly you want this ingestion to run-- day, month, year, hour, minute, etc. Select from the dropdown.

<p align="center">
    <img width="75%" alt="schedule selector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_set_execution_schedule.png"/>
</p>  

Ensure you've configured your correct timezone.

<p align="center">
    <img width="75%" alt="timezone_selector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_set_execution_timezone.png"/>
</p>  

Finally, click **Next** when you are done.

### Finish Up

Name your ingestion source, then click **Save and Run**.

<p align="center">
  <img width="75%" alt="Name your ingestion" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-ingestion-source-window.png"/>
</p>  

You will now find your new ingestion source running.

<p align="center">
  <img width="75%" alt="ingestion_running" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-ingestion-running.png"/>
</p>  

### Validate Ingestion Runs

View the latest status of ingestion runs on the Ingestion page.

<p align="center">
  <img width="75%" alt="ingestion succeeded" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-ingestion-succeeded.png"/>
</p>  

Click the plus sign to expand the full list of historical runs and outcomes; click **Details** to see the outcomes of a specific run.

<p align="center">
  <img width="75%" alt="ingestion_details" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-ingestion-history.png"/>
</p>

From the Ingestion Run Details page, pick **View All** to see which entities were ingested.

<p align="center">
  <img width="75%" alt="ingestion_details_view_all" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-ingestion-detail.png"/>
</p>  

Pick an entity from the list to manually validate if it contains the detail you expected.

<p align="center">
  <img width="75%" alt="ingestion_details_view_all" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/looker-ingestion-assets.png"/>
</p>  

## Configuring LookML Connector

Now that you have created a DataHub specific API key and Deploy Key with the relevant access in [the prior step](setup.md), it's now time to set up a connection via the DataHub UI.

### Configure Recipe

Navigate to the **Sources** tab and click **Create new source**.

  <p align="center">
    <img width="75%" alt="Click &quot;Create new source&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_click_create_new_source_button.png"/>
  </p>

Choose `LooML`.

  <p align="center">
    <img width="70%" alt="Select Looker from the options" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/lookml-choose-lookml.png"/>
  </p>

Enter details into the Looker Recipe. You need to set minimum 5 field in the recipe for this quick ingestion guide:

* **GitHub Repo:** This is your github repository where LookML model are stored. You can provide full URL (example: https://gitlab.com/gitlab-org/gitlab) or organization/repo in this case connector assume it is GitHub repo
* **GitHub Deploy Key:** Copy the content of `looker_datahub_deploy_key` and paste into this filed.
* **Looker Base URL:** This is your looker instance URL. (For example, https://abc.cloud.looker.com)
* **Looker Client ID:** Use the secret LOOKER_CLIENT_ID with the format `${LOOKER_CLIENT_ID}`.
* **Looker Client Secret:** Use the secret LOOKER_CLIENT_SECRET with the format `${LOOKER_CLIENT_SECRET}`.

Your recipe should look something like this:

<p align="center">
  <img width="70%" alt="LookML Recipe" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/lookml-ingestion-source-recipe.png"/>
</p>


After completing the recipe, click **Next**.    

### Schedule Execution

Now it's time to schedule a recurring ingestion pipeline to regularly extract metadata from your Looker instance.

Decide how regularly you want this ingestion to run-- day, month, year, hour, minute, etc. Select from the dropdown

<p align="center">
    <img width="75%" alt="schedule selector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_set_execution_schedule.png"/>
</p>  

Ensure you've configured your correct timezone.
<p align="center">
    <img width="75%" alt="timezone_selector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_set_execution_timezone.png"/>
</p>  

Click **Next** when you are done.

### Finish Up

Name your ingestion source, then click **Save and Run**.
<p align="center">
  <img width="75%" alt="Name your ingestion" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/lookml-ingestion-source-window.png"/>
</p>  

You will now find your new ingestion source running.

<p align="center">
  <img width="75%" alt="ingestion_running" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/lookml-ingestion-running.png"/>
</p>  

### Validate Ingestion Runs

View the latest status of ingestion runs on the Ingestion page.

<p align="center">
  <img width="75%" alt="ingestion succeeded" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/lookml-ingestion-succeeded.png"/>
</p>  

Click the plus sign to expand the full list of historical runs and outcomes; click **Details** to see the outcomes of a specific run.

<p align="center">
  <img width="75%" alt="ingestion_details" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/lookml-ingestion-history.png"/>
</p>

From the Ingestion Run Details page, pick **View All** to see which entities were ingested.

<p align="center">
  <img width="75%" alt="ingestion_details_view_all" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/lookml-ingestion-detail.png"/>
</p>  

Pick an entity from the list to manually validate if it contains the detail you expected.  

<p align="center">
  <img width="75%" alt="ingestion_details_view_all" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/looker/lookml-ingestion-assets.png"/>
</p>  


**Congratulations!** You've successfully set up Looker & LookML as an ingestion source for DataHub!

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*