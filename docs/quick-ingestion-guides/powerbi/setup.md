---
title: Power BI Setup
---

# Power BI Ingestion Guide: Setup & Prerequisites

## Power BI Prerequisites

DataHub connects to Power BI using a service principal / Microsoft Entra application.

In order to configure ingestion from Power BI, you'll first have to ensure you have a Microsoft Entra application with permission to access the Power BI resources.

### Register a new Microsoft Entra application

Follow the below steps to register an Entra application:

1. Login to the Azure portal at https://portal.azure.com

2. Go to `Microsoft Entra ID`

3. Navigate to `App registrations`

4. Click on `+ New registration`

5. In the `Register an application` window, fill out the `Name` of the application (e.g. `datahub-powerbi-connector-app`) and keep the other defaults as-is, then click `Register`.

<p align="center">
<img width="75%" alt="app_registration" src="http://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/powerbi/app-registration.png"/>
</p>

6. Once the app is finished registering, the Azure portal will open up the application overview, as shown below. On this screen, note down the `Application (client) ID` and `Directory (tenant) ID`.

<p align="center">
<img width="75%" alt="powerbi_app_connector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/powerbi/powerbi-connector-window.png"/>
</p>

### Create a client secret for the Entra application

1. Navigate to `Certificates & secrets` on the Entra application you just created.

2. Click on `New client secret`

3. Generate a new secret and note down the secret `Value`

### Create a new Microsoft Entra group

The application you registered will need to be a member of an Entra group in order to be granted Power BI/Fabric permissions. Follow the below steps to create a new Microsoft Entra Group:

1. Go to `Microsoft Entra ID`

2. Navigate to `Groups` and click on `New group`

3. In the `New group` window, leave `Group type` as the default (`Security`) and fill out the `Group name` (e.g. `datahub-powerbi-connector-group`), as shown in the below screenshot:

<p align="center">
<img width="75%" alt="powerbi_app_connector" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/powerbi/new-group-window.png"/>
</p>

### Add the Entra application as a member of the Entra group

1. Navigate to `All groups` and click on your newly created group.

2. Navigate to `Members` and click `Add members`.

3. Add your Microsoft Entra application (e.g. `datahub-powerbi-connector-app`) as a member.

### Grant permissions to access Power BI APIs

You need to add the created Entra group under your Power BI/Fabric tenant settings in order to grant resource access. Follow the below steps to grant privileges to the group and all applications within it:

1. Login to Power BI: https://app.powerbi.com/

2. Go to `Settings` -> `Admin portal`

3. In the `Admin portal`, navigate to `Tenant settings`.

4. For each of the following options, enable the option and add the previously created security group (e.g. _datahub-powerbi-connector-group_) under `Specific security groups`:

   - `Developer settings > Service principals can call Fabric public APIs` (or `Allow service principals to use Power BI APIs` in older versions of Power BI)
   - `Admin API settings > Service principals can access read-only admin APIs`
   - `Admin API settings > Enhance admin APIs responses with detailed metadata`
   - `Admin API settings > Enhance admin APIs responses with DAX and mashup expressions`

### Add the Entra application as a member of your Power BI / Fabric workspaces

For workspaces which you want to ingest into DataHub, add the Entra application as a member.

1. Navigate to `Workspaces`

2. Open the workspace which you want to ingest, as shown in the below screenshot:

<p align="center">
<img width="75%" alt="workspace-window-underlined" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/guides/powerbi/workspace-window-undrlined.png"/>
</p>

3. Click `Manage Access`

4. Click `Add people or groups`

5. Add your Microsoft Entra application (e.g. `datahub-powerbi-connector-app`) as a member. For most cases `Viewer` role is enough, but for profiling the `Contributor` role is required.

## Next Steps

Once you've done all of the above steps, it's time to [move on](configuration.md) to configuring the actual ingestion source within DataHub.
