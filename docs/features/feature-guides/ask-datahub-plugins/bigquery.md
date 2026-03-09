import FeatureAvailability from '@site/src/components/FeatureAvailability';

# BigQuery Plugin

<FeatureAvailability saasOnly />

> **Note**: Ask DataHub Plugins is currently in **Private Beta**. To enable this feature, please reach out to your DataHub customer support representative.

The **BigQuery Plugin** connects Ask DataHub to [Google BigQuery](https://cloud.google.com/bigquery) via the [BigQuery MCP server](https://docs.cloud.google.com/bigquery/docs/use-bigquery-mcp), enabling conversational analytics directly from the chat. Like the Snowflake and Databricks plugins, DataHub acts as the **semantic layer** — finding the right data — while BigQuery serves as the **data layer** — executing queries and returning results.

## Why Connect BigQuery?

With the BigQuery plugin enabled, Ask DataHub can:

- **Conversational analytics** — ask questions in plain language and get answers backed by real data. DataHub finds the right tables using metadata (descriptions, ownership, lineage), then BigQuery executes the query.
- **Data exploration** — understand dataset structure, preview sample data, and inspect column values to evaluate data quality or fitness for a use case.
- **Data debugging** — investigate data issues detected by DataHub assertions by inspecting the actual data: check for nulls, duplicates, unexpected values, or freshness problems at the source.

Because the plugin uses **Google OAuth**, each user authenticates with their own Google account. Users only see data they are authorized to access via IAM — no need to manage separate policies in DataHub.

**Example prompts:**

- _"Query the top 10 customers by revenue this quarter"_
- _"How many null values are in the email column of the customers table?"_
- _"Show me 5 sample rows from the orders table"_
- _"List all datasets in my project"_

## Prerequisites

- A Google Cloud project with the BigQuery API enabled
- The [BigQuery MCP server enabled](https://docs.cloud.google.com/bigquery/docs/use-bigquery-mcp) in the project (see Step 1 below)
- Users must have the [required IAM roles](https://docs.cloud.google.com/bigquery/docs/use-bigquery-mcp#required_roles) on the project (including `roles/bigquery.dataViewer`, `roles/bigquery.jobUser`, and `roles/mcp.toolUser`)
- DataHub Cloud with Ask DataHub Plugins enabled
- Platform admin access in DataHub to configure the plugin

## Admin Setup

The BigQuery plugin uses **User OAuth** authentication — each user authenticates with their own Google account. Setup involves enabling the BigQuery MCP server, creating an OAuth client in Google Cloud, then configuring the plugin in DataHub.

### Step 1: Enable the BigQuery MCP Server

Enable the BigQuery MCP server in your Google Cloud project using the [`gcloud` CLI](https://docs.cloud.google.com/sdk/docs/install):

```bash
gcloud beta services mcp enable bigquery.googleapis.com \
    --project=YOUR_PROJECT_ID
```

:::tip gcloud version
This command requires a recent version of the gcloud CLI beta component. If you see `Invalid choice: 'mcp'`, run `gcloud components update` to update to the latest version.
:::

For more details, see the [Google Cloud documentation](https://docs.cloud.google.com/mcp/enable-disable-mcp-servers).

### Step 2: Create an OAuth Client and Collect Credentials

Create a **Web application** OAuth client in [Google Auth Platform > Clients](https://console.cloud.google.com/auth/clients). When configuring the client, add your DataHub OAuth callback URL as an **Authorized redirect URI**:

```
https://<your-datahub-url>/integrations/oauth/callback
```

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/bigquery_oauth_settings.png"/>
</p>

:::caution Redirect URI
The **Authorized redirect URI** must match the OAuth Callback URL shown in the DataHub plugin creation form exactly (e.g. `https://your-org.acryl.io/integrations/oauth/callback`). You can copy this URL from the DataHub form when creating the plugin in Step 3.
:::

After creating the client, copy the **Client ID** and **Client Secret** immediately — the client secret won't be shown again.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/bigquery_oauth_creds.png"/>
</p>

For detailed instructions, see [Obtain OAuth 2.0 credentials](https://developers.google.com/identity/protocols/oauth2#1.-obtain-oauth-2.0-credentials-from-the-dynamic_data.setvar.console_name.) in the Google documentation.

### Step 3: Create Plugin in DataHub

1. Navigate to **Settings > AI > Plugins** in DataHub
2. Click **+ Create** and select **Custom MCP**
3. Fill in the plugin details:

| Field                   | Value                                  |
| ----------------------- | -------------------------------------- |
| **Name**                | `BigQuery`                             |
| **Description**         | A description for the plugin           |
| **MCP Server URL**      | `https://bigquery.googleapis.com/mcp`  |
| **Authentication Type** | `User OAuth (Each user authenticates)` |

4. DataHub will automatically discover the OAuth configuration from Google. Provide the following:

| Field              | Value                                      |
| ------------------ | ------------------------------------------ |
| **Client ID**      | From Step 2                                |
| **Client Secret**  | From Step 2                                |
| **Default Scopes** | `https://www.googleapis.com/auth/bigquery` |

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/bigquery_plugin_config.png"/>
</p>

:::info OAuth Scopes
The `https://www.googleapis.com/auth/bigquery` scope grants read and write access to BigQuery. For a full list of available scopes, see [BigQuery OAuth scopes](https://developers.google.com/identity/protocols/oauth2/scopes#bigquery).
:::

5. Optionally add **Instructions for the AI Assistant**, ensure **Enable for Ask DataHub** is on, and click **Create**

## User Setup

Navigate to **Settings > My AI Settings**, find the **BigQuery** plugin, and click **Connect**. You'll be redirected to Google to authenticate, then back to DataHub. See the [overview](./overview.md#user-setup-enabling-plugins) for more details.

:::note OAuth Consent Screen
If your OAuth consent screen is set to **Internal**, only users within your Google Workspace organization can authenticate. If set to **External** and not yet verified, users may see a warning — they can proceed by clicking **Advanced > Go to \<app name\>**. For production use, consider [verifying the consent screen](https://support.google.com/cloud/answer/10311615).
:::

## Available Tools

Once connected, Ask DataHub can use the following [BigQuery MCP tools](https://docs.cloud.google.com/bigquery/docs/reference/mcp):

| Tool                         | Description                                    |
| ---------------------------- | ---------------------------------------------- |
| `bigquery__execute_sql`      | Run SQL queries (SELECT statements only)       |
| `bigquery__list_dataset_ids` | List datasets in a Google Cloud project        |
| `bigquery__get_dataset_info` | Get metadata about a dataset                   |
| `bigquery__list_table_ids`   | List tables in a dataset                       |
| `bigquery__get_table_info`   | Get metadata about a table (schema, row count) |

:::note Read-Only Queries
The `execute_sql` tool only supports **SELECT** statements — it cannot modify data. This provides a built-in safety guardrail for production environments.
:::

## Troubleshooting

### OAuth Fails

- Verify the **Authorized redirect URI** in Google Cloud matches the OAuth Callback URL shown in DataHub exactly
- Ensure the Client ID and Client Secret are entered correctly
- Check that the [OAuth consent screen](https://console.cloud.google.com/auth/audience) is configured for your Google Cloud project

### Permission Errors

- Ensure the user has the [required IAM roles](https://docs.cloud.google.com/bigquery/docs/use-bigquery-mcp#required_roles) on the project
- Verify the BigQuery MCP server is enabled in the project

### Query Failures

- Verify the user has access to the datasets and tables they're querying
- Note that `execute_sql` queries are [limited to 3 minutes](https://docs.cloud.google.com/bigquery/docs/use-bigquery-mcp#limitations) by default — longer queries are canceled automatically
- BigQuery MCP does not support querying Google Drive external tables
