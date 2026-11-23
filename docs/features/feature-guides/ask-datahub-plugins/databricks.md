import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Databricks Plugin

<FeatureAvailability saasOnly />

> **Note**: Ask DataHub Plugins is currently in **Private Beta**. To enable this feature, please reach out to your DataHub customer support representative.

The **Databricks Plugin** connects Ask DataHub to your Databricks workspace via the [Databricks MCP server](https://docs.databricks.com/aws/en/generative-ai/mcp/managed-mcp), enabling conversational analytics directly from the chat. Like the Snowflake plugin, DataHub acts as the **semantic layer** — finding the right data — while Databricks serves as the **data layer** — executing queries and returning results.

## Why Connect Databricks?

With the Databricks plugin enabled, Ask DataHub can:

- **Conversational analytics** — ask questions in plain language and get answers backed by real data. DataHub finds the right tables using metadata, then Databricks executes the SQL query.
- **Data exploration** — understand dataset structure, preview sample data, and inspect column values across your Unity Catalog.
- **Data debugging** — investigate data issues detected by DataHub assertions by querying the actual data at the source.

Because the plugin uses **OAuth**, each user authenticates with their own Databricks account. Users only see data they are authorized to access.

**Example prompts:**

- _"Query the top 10 customers by lifetime value"_
- _"How many rows were added to the events table today?"_
- _"Show me 5 sample rows from the orders table"_
- _"Check for null values in the email column of the users table"_

## Prerequisites

- A Databricks workspace with **Managed MCP Servers** enabled. A workspace admin turns this on from the workspace **Previews** page (see Databricks-provided MCPs for [AWS](https://docs.databricks.com/aws/en/generative-ai/mcp/managed-mcp), [GCP](https://docs.databricks.com/gcp/en/generative-ai/mcp/managed-mcp), or [Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/agents/mcp-tools/built-in-mcp-services))
- A Databricks account admin to create the OAuth app connection in the account console
- If the workspace uses IP access lists, a workspace admin to allow DataHub's outbound IP addresses ([see Step 4](#step-4-allow-datahub-through-ip-access-lists))
- DataHub Cloud with Ask DataHub Plugins enabled
- Platform admin access in DataHub to configure the plugin

## Admin Setup

The Databricks plugin uses **User OAuth** authentication — each user authenticates with their own Databricks account. Setup involves creating an OAuth app connection in Databricks, then configuring the plugin in DataHub.

### Step 1: Create an OAuth App Connection in Databricks

App connections are created in the Databricks **account console**, not in workspace settings. See Enable or disable partner OAuth applications for [AWS](https://docs.databricks.com/aws/en/integrations/enable-disable-oauth), [GCP](https://docs.databricks.com/gcp/en/integrations/enable-disable-oauth), or [Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/integrations/enable-disable-oauth).

:::caution Use a new app connection
Do not reuse the client ID of the service principal that runs DataHub ingestion. A service principal is not an OAuth app, and signing in with its client ID fails with `OAuth application with client ID ... isn't available in the account`.
:::

1. Log in to the account console (`accounts.cloud.databricks.com`, or `accounts.azuredatabricks.net` on Azure) and click **Settings**
2. On the **App connections** tab, click **Add connection** and fill in the details:

| Field                        | Value                                                    |
| ---------------------------- | -------------------------------------------------------- |
| **Application Name**         | `DataHub` (or any name you'll recognize)                 |
| **Redirect URLs**            | `https://<your-datahub-url>/integrations/oauth/callback` |
| **Access scopes**            | Check **SQL**                                            |
| **Generate a client secret** | Checked                                                  |

:::caution Redirect URL
The **Redirect URL** must match the OAuth Callback URL shown in the DataHub plugin creation form exactly (e.g. `https://your-org.acryl.io/integrations/oauth/callback`). You can copy this URL from the DataHub form when creating the plugin in Step 3.
:::

3. Click **Add**

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/databricks_add_oauth_connection.png"/>
</p>

### Step 2: Collect Credentials

After creating the connection, a dialog will show your **Client ID** and **Client Secret**. Copy both immediately — the client secret won't be shown again. Databricks lists the secret above the client ID, so check which value you are copying.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/databricks_oauth_credentials.png"/>
</p>

### Step 3: Create Plugin in DataHub

1. Navigate to **Settings > AI > Plugins** in DataHub
2. Click **+ Create** and select **Databricks MCP**. If your DataHub version does not list it, select **Custom MCP** and set **Authentication Type** to `User OAuth (Each user authenticates)`
3. Fill in the plugin details:

| Field              | Value                                               |
| ------------------ | --------------------------------------------------- |
| **Name**           | `Databricks`                                        |
| **Description**    | A description for the plugin                        |
| **MCP Server URL** | `https://<your-workspace-hostname>/api/2.0/mcp/sql` |

4. DataHub will automatically discover the OAuth configuration from Databricks, including the authorization URL (`https://<your-workspace-hostname>/oidc/v1/authorize`), token URL (`https://<your-workspace-hostname>/oidc/v1/token`), and available scopes. Provide the following:

| Field              | Value                 |
| ------------------ | --------------------- |
| **Client ID**      | From Step 2           |
| **Client Secret**  | From Step 2           |
| **Default Scopes** | `sql, offline_access` |

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/databricks_plugin_config.png"/>
</p>

:::info Access Scopes
Request only scopes that are granted on the Databricks app connection. Discovery lists every scope Databricks supports, and with **Custom MCP** all of them are selected by default; Databricks then rejects the sign-in with `scopes ... are not assigned to the client`. `sql` is the scope Databricks lists for the Databricks SQL MCP server ([AWS](https://docs.databricks.com/aws/en/agents/mcp-tools/built-in-mcp-services), [Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/agents/mcp-tools/built-in-mcp-services)) and is required for Ask DataHub to execute queries, and `offline_access` lets DataHub refresh the user's token so they stay connected. The **Databricks MCP** template selects these two for you.
:::

5. Optionally add **Instructions for the AI Assistant**, ensure **Enable for Ask DataHub** is on, and click **Create**

### Step 4: Allow DataHub Through IP Access Lists

Skip this step if your workspace does not use IP access lists. When they are enabled, Databricks rejects every request from an address not on an allow list, so users can sign in through the OAuth pop-up (that request comes from their own browser) but every call DataHub then makes to the MCP server returns `403`.

1. Ask your DataHub representative for the outbound IP addresses of your DataHub instance. They depend on where your instance is hosted, so do not copy addresses from another deployment.
2. Check whether IP access lists are enabled on the workspace and which lists exist, for example with `databricks ip-access-lists list` in the Databricks CLI.
3. Add DataHub's addresses to an `ALLOW` list on the workspace, and make sure no `BLOCK` list covers them, since block lists take precedence. Once a workspace has any allow list, every address not on one is blocked, so if you are creating the first allow list, include your users' own networks in it as well.

See the Databricks documentation for your cloud:

| Cloud            | IP access lists for workspaces                                                                                                      | CLI reference                                                                                                            |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| AWS              | [Configure IP access lists](https://docs.databricks.com/aws/en/security/network/front-end/ip-access-list-workspace)                 | [`ip-access-lists`](https://docs.databricks.com/aws/en/dev-tools/cli/reference/ip-access-lists-commands)                 |
| GCP              | [Configure IP access lists](https://docs.databricks.com/gcp/en/security/network/front-end/ip-access-list-workspace)                 | [`ip-access-lists`](https://docs.databricks.com/gcp/en/dev-tools/cli/reference/ip-access-lists-commands)                 |
| Azure Databricks | [Configure IP access lists](https://learn.microsoft.com/en-us/azure/databricks/security/network/front-end/ip-access-list-workspace) | [`ip-access-lists`](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/reference/ip-access-lists-commands) |

## User Setup

Navigate to **Settings > My AI Settings**, find the **Databricks** plugin, and click **Connect**. You'll be redirected to Databricks to authenticate, then back to DataHub. See the [overview](./overview.md#user-setup-enabling-plugins) for more details.

## Troubleshooting

### OAuth Fails

- Verify the **Redirect URL** in Databricks matches the OAuth Callback URL shown in DataHub exactly
- Ensure the Client ID and Client Secret are entered correctly, and that the client ID belongs to an app connection rather than a service principal
- `scopes ... are not assigned to the client`: remove from **Default Scopes** any scope not granted on the app connection
- When editing a saved plugin, re-enter the client secret to enable **Test Connection**

### Sign-in Succeeds but Requests Return 403

The workspace's IP access lists are blocking DataHub. Allow DataHub's outbound IP addresses as described in [Step 4](#step-4-allow-datahub-through-ip-access-lists).

### Query Failures

- Verify the user has access to the tables they're querying in Unity Catalog
- Ensure the `sql` scope was selected when creating the app connection in Databricks
- Check that a SQL warehouse is available and running in the workspace
