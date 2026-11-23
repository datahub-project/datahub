import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Databricks Plugin

<FeatureAvailability saasOnly />

> **Note**: Ask DataHub Plugins is currently in **Private Beta**. To enable this feature, please reach out to your DataHub customer support representative.

The **Databricks Plugin** connects Ask DataHub to your Databricks workspace via the [Databricks MCP server](https://docs.databricks.com/aws/en/generative-ai/mcp/connect-external-services), enabling conversational analytics directly from the chat. Like the Snowflake plugin, DataHub acts as the **semantic layer** — finding the right data — while Databricks serves as the **data layer** — executing queries and returning results.

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

- A Databricks workspace with **Managed MCP Servers** enabled. A workspace admin turns this on from the workspace **Previews** page (see [Databricks managed MCP servers](https://docs.databricks.com/aws/en/generative-ai/mcp/managed-mcp))
- A Databricks account admin to create the OAuth app connection in the account console
- DataHub Cloud with Ask DataHub Plugins enabled
- Platform admin access in DataHub to configure the plugin

## Admin Setup

The Databricks plugin uses **User OAuth** authentication — each user authenticates with their own Databricks account. Setup involves creating an OAuth app connection in Databricks, then configuring the plugin in DataHub.

### Step 1: Create an OAuth App Connection in Databricks

App connections are created in the Databricks **account console**, not in workspace settings. See [Enable or disable partner OAuth applications](https://docs.databricks.com/aws/en/integrations/enable-disable-oauth) in the Databricks documentation.

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
Request only scopes that are granted on the Databricks app connection. Discovery lists every scope Databricks supports, and with **Custom MCP** all of them are selected by default; Databricks then rejects the sign-in with `scopes ... are not assigned to the client`. `sql` is required for Ask DataHub to execute queries, and `offline_access` lets DataHub refresh the user's token so they stay connected. The **Databricks MCP** template selects these two for you.
:::

5. Optionally add **Instructions for the AI Assistant**, ensure **Enable for Ask DataHub** is on, and click **Create**

## User Setup

Navigate to **Settings > My AI Settings**, find the **Databricks** plugin, and click **Connect**. You'll be redirected to Databricks to authenticate, then back to DataHub. See the [overview](./overview.md#user-setup-enabling-plugins) for more details.

## Troubleshooting

### OAuth Fails

- Verify the **Redirect URL** in Databricks matches the OAuth Callback URL shown in DataHub exactly
- Ensure the Client ID and Client Secret are entered correctly, and that the client ID belongs to an app connection rather than a service principal
- `scopes ... are not assigned to the client`: remove from **Default Scopes** any scope not granted on the app connection
- When editing a saved plugin, re-enter the client secret to enable **Test Connection**

### Sign-in Succeeds but Requests Return 403

If your workspace uses [IP access lists](https://docs.databricks.com/aws/en/security/network/front-end/ip-access-list), Databricks blocks DataHub's requests until DataHub's outbound IP addresses are allowed. Ask your DataHub representative for your instance's outbound IPs, then add them to the workspace allow list. To check whether IP access lists are enabled, run `databricks ip-access-lists list` with the Databricks CLI.

### Query Failures

- Verify the user has access to the tables they're querying in Unity Catalog
- Ensure the `sql` scope was selected when creating the app connection in Databricks
- Check that a SQL warehouse is available and running in the workspace
