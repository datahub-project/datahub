---
title: Configuration
---
# Configuring Your Tableau Connector to DataHub

Now that you have created a DataHub-specific user with the relevant access in Tableau in [the prior step](setup.md), it's now time to set up a connection via the DataHub UI.

## Configure Secrets

1. Within DataHub, navigate to the **Ingestion** tab in the top, right corner of your screen

<p align="center">
  <img width="75%" alt="Navigate to the &quot;Ingestion Tab&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_ingestion_button.png"/>
</p>

:::note
If you do not see the Ingestion tab, please contact your DataHub admin to grant you the correct permissions
:::

2. Navigate to the **Secrets** tab and click **Create new secret**

<p align="center">
   <img width="75%" alt="Secrets Tab" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_secrets_tab.png"/>
</p>

3. Create a `username` secret

  This will securely store your Tableau `username` within DataHub

   * Enter a name like `TABLEAU_USERNAME` - we will use this later to refer in recipe
   * Enter the `username`, setup in the [setup guide](setup.md)
   * Optionally add a description
   * Click **Create**

  <p align="center">
    <img width="70%" alt="Tableau Username Secret" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/tableau/tableau-username-secret.png"/>
  </p>

4. Create a `password` secret

  This will securely store your Tableau `password` within DataHub

   * Enter a name like `TABLEAU_PASSWORD` - we will use this later to refer in recipe
   * Enter the `password` of the user, setup in the [setup guide](setup.md)
   * Optionally add a description
   * Click **Create**

  <p align="center">
    <img width="70%" alt="Tableau Password Secret" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/tableau/tableau-user-password-secret.png"/>
  </p>

## Configure Recipe

5. Navigate to on the **Sources** tab and then **Create new source**

<p align="center">
  <img width="75%" alt="Click &quot;Create new source&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_click_create_new_source_button.png"/>
</p>

6. Select Tableau

<p align="center">
  <img width="70%" alt="Select Tableau from the options" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/tableau/tableau-new-ingestion-source.png"/>
</p>

7.  Fill in the Tableau Recipe form:

    You need to set minimum following fields in the recipe:
    
    a. **Host URL:** URL of your Tableau instance (e.g., https://15az.online.tableau.com/). It is available in browser address bar on Tableau Portal.

    b. **Username:** Use the TABLEAU_USERNAME secret (e.g., "${TABLEAU_USERNAME}").

    c. **Password:** Use the TABLEAU_PASSWORD secret (e.g., "${TABLEAU_PASSWORD}").   

    d. **Site**: Required only if using tableau cloud/ tableau online
    

To filter specific project, use `project_pattern` fields.

    config:
         ...
         project_pattern:
            allow:
              - "SalesProject"

Your recipe should look something like this:
<p align="center">
  <img width="70%" alt="tableau recipe in form format" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/tableau/tableau-recipe.png"/>
</p>

Click **Next** when you're done.

## Schedule Execution

Now it's time to schedule a recurring ingestion pipeline to regularly extract metadata from your Tableau instance.

8. Decide how regularly you want this ingestion to run-- day, month, year, hour, minute, etc. Select from the dropdown

<p align="center">
    <img width="75%" alt="schedule selector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_set_execution_schedule.png"/>
</p>  

9. Ensure you've configured your correct timezone
<p align="center">
    <img width="75%" alt="timezone_selector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_set_execution_timezone.png"/>
</p>  

10. Click **Next** when you are done

## Finish Up

11. Name your ingestion source, then click **Save and Run**
<p align="center">
  <img width="75%" alt="Name your ingestion" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/tableau/tableau-ingestion-save-and-run.png"/>
</p>  

You will now find your new ingestion source running

<p align="center">
  <img width="75%" alt="ingestion_running" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/tableau/tableau-ingestion-running.png"/>
</p>  

## Validate Ingestion Runs

12. View the latest status of ingestion runs on the Ingestion page

<p align="center">
  <img width="75%" alt="ingestion succeeded" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/tableau/tableau-ingestion-succeeded.png"/>
</p>  

13. Click the plus sign to expand the full list of historical runs and outcomes; click **Details** to see the outcomes of a specific run

<p align="center">
  <img width="75%" alt="ingestion_details" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/tableau/tableau-ingestion-history.png"/>
</p>

14. From the Ingestion Run Details page, pick **View All** to see which entities were ingested

<p align="center">
  <img width="75%" alt="ingestion_details_view_all" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/tableau/tableau-ingestion-run-detail.png"/>
</p>  

15. Pick an entity from the list to manually validate if it contains the detail you expected  

<p align="center">
  <img width="75%" alt="ingestion_details_view_all" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/tableau/tableau-ingestion-assets.png"/>
</p>  

**Congratulations!** You've successfully set up Tableau as an ingestion source for DataHub!

