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

- A Google Cloud project with the [BigQuery API](https://console.cloud.google.com/flows/enableapi?apiid=bigquery) enabled
- The BigQuery MCP server enabled in the project (see Step 1)
- Users must have the following IAM roles (or equivalent permissions) on the project:
  - `roles/bigquery.dataViewer` — access data
  - `roles/bigquery.jobUser` — run queries
  - `roles/mcp.toolUser` — access MCP tools
- DataHub Cloud with Ask DataHub Plugins enabled
- Platform admin access in DataHub to configure the plugin

## Admin Setup

The BigQuery plugin uses **User OAuth** authentication — each user authenticates with their own Google account. Setup involves enabling the BigQuery MCP server, creating an OAuth client in Google Cloud, then configuring the plugin in DataHub.

### Step 1: Enable the BigQuery MCP Server

Enable the BigQuery MCP server in your Google Cloud project using the `gcloud` CLI:

```bash
gcloud beta services mcp enable bigquery.googleapis.com \
    --project=YOUR_PROJECT_ID
```

If the BigQuery API is not yet enabled, you'll be prompted to enable it as well.

### Step 2: Create an OAuth Client in Google Cloud

1. In Google Cloud Console, navigate to **Google Auth Platform > [Clients](https://console.cloud.google.com/auth/clients)**
2. Click **+ Create Client**
3. Select **Web application** as the application type
4. Set the **Name** to something recognizable (e.g. `DataHub MCP`)
5. Under **Authorized redirect URIs**, add your DataHub OAuth callback URL:

```
https://<your-datahub-url>/integrations/oauth/callback
```

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/bigquery_oauth_settings.png"/>
</p>

:::caution Redirect URI
The **Authorized redirect URI** must match the OAuth Callback URL shown in the DataHub plugin creation form exactly (e.g. `https://your-org.acryl.io/integrations/oauth/callback`). You can copy this URL from the DataHub form when creating the plugin in Step 3.
:::

6. Click **Create**

### Step 3: Collect Credentials

After creating the client, a dialog will show your **Client ID** and **Client Secret**. Copy both immediately — the client secret won't be shown again.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/bigquery_oauth_creds.png"/>
</p>

You can always find the Client ID later under **Google Auth Platform > Clients**, but the secret must be copied at creation time (or a new one generated).

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/bigquery_oauth_success.png"/>
</p>

### Step 4: Create Plugin in DataHub

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
| **Client ID**      | From Step 3                                |
| **Client Secret**  | From Step 3                                |
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
If the Google Cloud OAuth consent screen is set to **Internal**, only users within your Google Workspace organization can authenticate. If set to **External** and not yet verified, users may see a "Google hasn't verified this app" warning — they can proceed by clicking **Advanced > Go to \<app name\>**. For production use, consider [publishing and verifying](https://support.google.com/cloud/answer/10311615) the consent screen.
:::

## Troubleshooting

### OAuth Fails

- Verify the **Authorized redirect URI** in Google Cloud matches the OAuth Callback URL shown in DataHub exactly
- Ensure the Client ID and Client Secret are entered correctly
- Check that the [OAuth consent screen](https://console.cloud.google.com/auth/audience) is configured (Internal or External) for your Google Cloud project

### Permission Errors

- Ensure the user has the `roles/bigquery.dataViewer`, `roles/bigquery.jobUser`, and `roles/mcp.toolUser` IAM roles on the project
- Verify the BigQuery MCP server is enabled in the project (`gcloud beta services mcp list --project=YOUR_PROJECT_ID`)

### Query Failures

- Verify the user has access to the datasets and tables they're querying
- Note that `execute_sql` queries are limited to 3 minutes by default — longer queries will be canceled automatically
- BigQuery MCP does not support querying Google Drive external tables
