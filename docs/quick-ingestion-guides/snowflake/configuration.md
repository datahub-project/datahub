---
title: Configuration
---
# Configuring Your Snowflake Connector to DataHub

Now that you have created a DataHub-specific user with the relevant roles in Snowflake in [the prior step](setup.md), it's now time to set up a connection via the DataHub UI.

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

3. Create a Password secret

  This will securely store your Snowflake password within DataHub

   * Enter a name like `SNOWFLAKE_PASSWORD` - we will use this later to refer to the secret
   * Enter the password configured for the DataHub user in the previous step
   * Optionally add a description
   * Click **Create**

<p align="center">
   <img width="70%" alt="Snowflake Password Secret" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_password_secret.png"/>
</p>

## Configure Recipe

4. Navigate to the **Sources** tab and click **Create new source**

<p align="center">
  <img width="75%" alt="Click &quot;Create new source&quot;" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_click_create_new_source_button.png"/>
</p>

5. Select Snowflake

<p align="center">
  <img width="70%" alt="Select Snowflake from the options" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_snowflake_source.png"/>
</p>

6. Fill out the Snowflake Recipe

Enter the Snowflake Account Identifier as **Account ID** field. Account identifier is the part before `.snowflakecomputing.com` in your snowflake host URL:

<p align="center">
   <img width="70%" alt="Account Id Field" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_account_id.png"/>
</p>

*Learn more about Snowflake Account Identifiers [here](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#account-identifiers)*

Add the previously added Password secret to **Password** field:
   * Click on the Password input field
   * Select `SNOWFLAKE_PASSWORD` secret

<p align="center">
     <img width="70%" alt="Password field" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_password_secret_field.png"/>
</p>

Populate the relevant fields using the same **Username**, **Role**, and **Warehouse** you created and/or specified in [Snowflake Prerequisites](setup.md).

<p align="center">
   <img width="70%" alt="Warehouse Field" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_warehouse_username_role_fields.png"/>
</p>

7. Click **Test Connection**

This step will ensure you have configured your credentials accurately and confirm you have the required permissions to extract all relevant metadata.

<p align="center">
  <img width="75%" alt="Test Snoflake connection" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_test_connection.png"/>
</p>

After you have successfully tested your connection, click **Next**.

## Schedule Execution

Now it's time to schedule a recurring ingestion pipeline to regularly extract metadata from your Snowflake instance.

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
  <img width="75%" alt="Name your ingestion" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/common/common_ingestion_name_ingestion_source.png"/>
</p>  

You will now find your new ingestion source running

<p align="center">
  <img width="75%" alt="ingestion_running" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_source_running.png"/>
</p>  

## Validate Ingestion Runs

12. View the latest status of ingestion runs on the Ingestion page

<p align="center">
  <img width="75%" alt="ingestion succeeded" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_ingestion_succeded.png"/>
</p>  

13. Click the plus sign to expand the full list of historical runs and outcomes; click **Details** to see the outcomes of a specific run

<p align="center">
  <img width="75%" alt="ingestion_details" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_ingestion_details.png"/>
</p>

14. From the Ingestion Run Details page, pick **View All** to see which entities were ingested

<p align="center">
  <img width="75%" alt="ingestion_details_view_all" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_details_view_all.png"/>
</p>  

15. Pick an entity from the list to manually validate if it contains the detail you expected  

<p align="center">
  <img width="75%" alt="ingestion_details_view_all" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/snowflake/snowflake_ingestion_view_ingested_assets.png"/>
</p>  

**Congratulations!** You've successfully set up Snowflake as an ingestion source for DataHub!

