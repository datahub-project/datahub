import FeatureAvailability from '@site/src/components/FeatureAvailability';

# dbt Cloud Plugin

<FeatureAvailability saasOnly />

The **dbt Cloud Plugin** connects Ask DataHub to your dbt Cloud environment via the [dbt remote MCP server](https://docs.getdbt.com/docs/dbt-ai/setup-remote-mcp), giving the AI assistant visibility into job runs, model definitions, test results, and — with the right configuration — the ability to execute SQL queries on behalf of users.

## Why Connect dbt Cloud?

With the dbt Cloud plugin enabled, Ask DataHub can:

- **Check job run status** — see whether recent dbt runs succeeded or failed
- **Inspect model definitions** — view the SQL and configuration behind dbt models
- **Review test results** — understand which tests passed or failed and why
- **Execute SQL queries** — run queries against your warehouse through dbt (with additional configuration)

**Example prompts:**

- _"Did the latest dbt run for the revenue model succeed?"_
- _"Show me the SQL definition of the orders model"_
- _"Which dbt tests failed in the last 24 hours?"_
- _"Query the customers table to check for null emails"_

## Prerequisites

- A dbt Cloud account with [AI features enabled](https://docs.getdbt.com/docs/cloud/enable-dbt-copilot) and the remote MCP server active
- A dbt Cloud [personal access token or service token](https://docs.getdbt.com/docs/dbt-cloud-apis/authentication) with **Semantic Layer** and **Developer** permissions
- Your dbt Cloud **production environment ID** (and optionally a **development environment ID** for SQL execution)
- DataHub Cloud with Ask DataHub Plugins enabled
- Platform admin access in DataHub to configure the plugin

## Admin Setup

### Step 1: Enable the dbt Remote MCP Server

Follow the [dbt remote MCP setup guide](https://docs.getdbt.com/docs/dbt-ai/setup-remote-mcp) to enable the remote MCP server for your dbt Cloud project.

### Step 2: Collect Configuration Details

You'll need the following from dbt Cloud:

**MCP Server URL**: Construct the URL using your dbt Cloud host:

```
https://<YOUR_DBT_HOST_URL>/api/ai/v1/mcp/
```

For multi-cell accounts, the host URL is in the format `ACCOUNT_PREFIX.us1.dbt.com`. See [dbt's regions documentation](https://docs.getdbt.com/docs/cloud/about-cloud/access-regions-ip-addresses) for details.

**Production Environment ID**: Navigate to **Orchestration > Environments** in dbt Cloud. Copy the environment ID from the URL.

<!-- TODO: Screenshot of dbt Cloud environments page with ID in URL -->

**API Token**: Generate a [personal access token](https://docs.getdbt.com/docs/dbt-cloud-apis/user-tokens) or [service token](https://docs.getdbt.com/docs/dbt-cloud-apis/service-tokens). The token needs **Semantic Layer** and **Developer** permissions.

### Step 3: Create Plugin in DataHub

1. Navigate to **Settings > AI > Plugins** in DataHub
2. Click **+ Create** and select **dbt Cloud MCP**
3. Fill in the plugin details:

| Field                   | Value                                        |
| ----------------------- | -------------------------------------------- |
| **Name**                | `dbt Cloud`                                  |
| **Description**         | A description for the plugin                 |
| **MCP Server URL**      | `https://<YOUR_DBT_HOST_URL>/api/ai/v1/mcp/` |
| **Authentication Type** | `Shared API Key` or `User API Key`           |
| **Auth Scheme**         | `Token`                                      |

<!-- TODO: Screenshot of dbt Cloud plugin configuration in DataHub -->

:::caution Auth Scheme
Make sure the auth scheme is set to **Token** — this is required for dbt Cloud's API authentication format (`Token <YOUR_TOKEN>`).
:::

4. If using **Shared API Key**: enter the service token that will be used for all users
5. If using **User API Key**: each user will provide their own dbt Cloud API token when connecting

### Step 4: Configure Required Headers

Add the following **custom headers** to scope the plugin to the correct environment:

| Header                      | Required | Value                          |
| --------------------------- | -------- | ------------------------------ |
| `x-dbt-prod-environment-id` | Yes      | Your production environment ID |

### Step 5: Enable SQL Execution (Optional)

To let Ask DataHub execute SQL queries through dbt on a user's behalf, add these additional headers:

| Header                     | Required | Value                                                                                         |
| -------------------------- | -------- | --------------------------------------------------------------------------------------------- |
| `x-dbt-dev-environment-id` | Yes      | Your development environment ID                                                               |
| `x-dbt-user-id`            | Yes      | Your dbt Cloud user ID ([how to find it](https://docs.getdbt.com/faqs/Accounts/find-user-id)) |

<!-- TODO: Screenshot showing custom headers configuration -->

:::caution
If using a **Shared API Key**, the `x-dbt-user-id` header applies to all users — meaning queries will execute under a single identity. Consider using **User API Key** authentication if per-user query execution is important.
:::

Each user also needs **development credentials** configured in dbt Cloud (warehouse connection details under their profile settings) for SQL execution to work.

<!-- TODO: Screenshot of dbt Cloud development credentials setup -->

6. Optionally add **Instructions for the AI Assistant**
7. Ensure **Enable for Ask DataHub** is toggled on
8. Click **Create**

## User Setup

Once the admin has configured the dbt Cloud plugin:

1. Navigate to **Settings > My AI Settings** in DataHub
2. Find the **dbt Cloud** plugin
3. If using **User API Key** auth, click **Connect** and enter your dbt Cloud API token
4. If using **Shared API Key** auth, simply toggle the plugin **on**

## Usage Tips

- Combine dbt Cloud with DataHub lineage for end-to-end debugging — trace a dashboard issue back through DataHub lineage to a failed dbt model
- Ask about specific environments (production vs. staging) to get targeted results
- Use dbt test results alongside DataHub assertions for a complete data quality picture

:::info dbt Copilot Credits
Only the `text_to_sql` tool consumes dbt Copilot credits. Other MCP tools (metadata discovery, semantic layer queries) do not. See [dbt's documentation](https://docs.getdbt.com/docs/dbt-ai/setup-remote-mcp) for details.
:::

## Troubleshooting

### Connection Errors

- Verify your dbt Cloud API token is valid and has **Semantic Layer** and **Developer** permissions
- Ensure the remote MCP server is enabled in your dbt Cloud project
- Check that the auth scheme is set to **Token**

### Wrong Project or Environment

- Verify the `x-dbt-prod-environment-id` custom header is set correctly
- Double-check the environment ID matches what's shown in the dbt Cloud URL

### SQL Queries Not Working

- Ensure [development credentials](https://docs.getdbt.com/docs/cloud/connect-data-platform/connect-your-database) are configured in dbt Cloud for the user
- Verify the `x-dbt-user-id` and `x-dbt-dev-environment-id` headers are set
- Check that the API token has permissions to execute queries
- Note that a personal access token (not a service token) is required for `x-dbt-user-id` functionality
