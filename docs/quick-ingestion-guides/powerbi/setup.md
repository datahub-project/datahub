---
title: Setup
---

# Power BI Ingestion Guide: Setup & Prerequisites

DataHub connects to Power BI using a service principal / Microsoft Entra application.

In order to configure ingestion from Power BI, you'll first have to ensure you have a Microsoft Entra application with permission to access the Power BI resources.

## Power BI Prerequisites

1. **Register a new Microsoft Entra application:** Follow the below steps to register an Entra application:

   a. Login to the Azure portal at https://portal.azure.com

   b. Go to `Microsoft Entra ID`

   c. Navigate to `App registrations`

   d. Click on `+ New registration`

   e. In the `Register an application` window, fill out the `Name` of the application (e.g. `datahub-powerbi-connector-app`) and keep the other defaults as-is, then click `Register`.

      <p align="center">
   <img width="75%" alt="app_registration" src="http://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/powerbi/app-registration.png"/>
      </p>

   g. Once the app is finished registering, the Azure portal will open up the application overview, as shown below. On this screen, note down the `Application (client) ID` and `Directory (tenant) ID`.

      <p align="center">
   <img width="75%" alt="powerbi_app_connector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/powerbi/powerbi-connector-window.png"/>
      </p>

2. **Create a client secret for the Entra application:**

   a. Navigate to `Certificates & secrets` on the Entra application you just created.

   b. Click on `New client secret`

   f. Generate a new secret and note down the secret `Value`

3. **Create a new Microsoft Entra group:** The application you registered will need to be a member of an Entra group in order to be granted Power BI/Fabric permissions. Follow the below steps to create a new Microsoft Entra Group:

   a. Go to `Microsoft Entra ID`

   b. Navigate to `Groups` and click on `New group`

   c. In the `New group` window, leave `Group type` as the default (`Security`) and fill out the `Group name` (e.g. `datahub-powerbi-connector-group`), as shown in the below screenshot:

      <p align="center">
   <img width="75%" alt="powerbi_app_connector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/powerbi/new-group-window.png"/>
      </p>

4. **Add the Entra application as a member of the group:**

   a. Navigate to `All groups` and click on your newly created group.

   b. Navigate to `Members` and click `Add members`.

   c. Add your Microsoft Entra application (e.g. `datahub-powerbi-connector-app`) as a member.

5. **Grant permissions to access Power BI APIs:** You need to add the created Entra group under your Power BI/Fabric tenant settings in order to grant resource access. Follow the below steps to grant privileges to the group and all applications within it:

   a. Login to Power BI: https://app.powerbi.com/

   b. Go to `Settings` -> `Admin portal`

   c. In the `Admin portal`, navigate to `Tenant settings`.

   d. For each of the following options, enable the option and add the previously created security group (e.g. _datahub-powerbi-connector-group_) under `Specific security groups`:

   - `Developer settings > Service principals can call Fabric public APIs` (or `Allow service principals to use Power BI APIs` in older versions of Power BI)
   - `Admin API settings > Service principals can access read-only admin APIs`
   - `Admin API settings > Enhance admin APIs responses with detailed metadata`
   - `Admin API settings > Enhance admin APIs responses with DAX and mashup expressions`

6. **Add the Entra application as a member of your Power BI / Fabric workspaces:** For workspaces which you want to ingest into DataHub, add the Entra application as a member.

   a. Navigate to `Workspaces`

   b. Open the workspace which you want to ingest, as shown in the below screenshot:

      <p align="center">
   <img width="75%" alt="workspace-window-underlined" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/powerbi/workspace-window-undrlined.png"/>
      </p>

   c. Click `Manage Access`

   d. Click `Add people or groups`

   d. Add your Microsoft Entra application (e.g. `datahub-powerbi-connector-app`) as a member. For most cases `Viewer` role is enough, but for profiling the `Contributor` role is required.

## Next Steps

Once you've done all of the above steps, it's time to [move on](configuration.md) to configuring the actual ingestion source within DataHub.
