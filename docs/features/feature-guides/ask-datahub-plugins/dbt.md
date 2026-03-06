import FeatureAvailability from '@site/src/components/FeatureAvailability';

# dbt Cloud Plugin

<FeatureAvailability saasOnly />

The **dbt Cloud Plugin** connects Ask DataHub to your dbt Cloud environment via the [dbt MCP server](https://docs.getdbt.com/docs/dbt-ai/about-mcp), giving the AI assistant visibility into job runs, model definitions, test results, and — with the right configuration — the ability to execute SQL queries on behalf of users.

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

- A dbt Cloud account with the MCP server enabled ([follow dbt's guide](https://docs.getdbt.com/docs/dbt-ai/about-mcp))
- A dbt Cloud [API token](https://docs.getdbt.com/docs/dbt-cloud-apis/authentication) (service token or personal token)
- DataHub Cloud with Ask DataHub Plugins enabled
- Platform admin access in DataHub to configure the plugin

## Admin Setup

### Step 1: Enable the dbt MCP Server

Follow the [dbt Cloud MCP server documentation](https://docs.getdbt.com/docs/dbt-ai/about-mcp) to enable the MCP server for your dbt Cloud project.

### Step 2: Collect Configuration Details

You'll need the following from dbt Cloud:

**Environment ID**: Navigate to **Orchestration > Environments** in dbt Cloud. Copy the environment ID from the URL.

<!-- TODO: Screenshot of dbt Cloud environments page with ID in URL -->

**Base Project URL**: Copy this from your dbt Cloud URL.

**API Token**: Generate a service token or use a personal API token from dbt Cloud.

### Step 3: Create Plugin in DataHub

1. Navigate to **Settings > AI > Plugins** in DataHub
2. Click **+ Create** and select **dbt Cloud MCP**
3. Fill in the plugin details:

| Field                   | Value                              |
| ----------------------- | ---------------------------------- |
| **Name**                | `dbt Cloud`                        |
| **Description**         | A description for the plugin       |
| **MCP Server URL**      | Your dbt Cloud MCP server URL      |
| **Authentication Type** | `Shared API Key` or `User API Key` |
| **Auth Scheme**         | `Token`                            |

<!-- TODO: Screenshot of dbt Cloud plugin configuration in DataHub -->

:::caution Auth Scheme
Make sure the auth scheme is set to **Token** — this is required for dbt Cloud's API authentication.
:::

4. If using **Shared API Key**: enter the service token that will be used for all users
5. If using **User API Key**: each user will provide their own dbt Cloud API token when connecting

### Step 4: Configure Custom Headers

Custom headers are required to scope the plugin to the right environment:

| Header                     | Value                           | Purpose                                 |
| -------------------------- | ------------------------------- | --------------------------------------- |
| `x-dbt-dev-environment-id` | Your environment ID from Step 2 | Targets the correct project environment |

<!-- TODO: Screenshot showing custom headers configuration -->

:::info Enabling Text-to-SQL
To enable Ask DataHub to execute SQL queries through dbt on a user's behalf, add these additional custom headers:

| Header                     | Value                                               | Purpose                                 |
| -------------------------- | --------------------------------------------------- | --------------------------------------- |
| `x-dbt-user-id`            | Your dbt Cloud user ID (found on your profile page) | Identifies the user for query execution |
| `x-dbt-dev-environment-id` | Dev environment ID                                  | Targets the development environment     |

Note: If using a **Shared API Key**, these headers apply to all users — be aware this gives everyone access to execute queries under the same identity.
:::

### Step 5: Set Up Development Credentials

For text-to-SQL to work, each user needs **development credentials** configured in dbt Cloud. Navigate to your dbt Cloud profile and ensure your development credentials (warehouse connection) are set up.

<!-- TODO: Screenshot of dbt Cloud development credentials setup -->

5. Optionally add **Instructions for the AI Assistant**
6. Ensure **Enable for Ask DataHub** is toggled on
7. Click **Create**

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

## Troubleshooting

### Connection Errors

- Verify your dbt Cloud API token is valid and has the required permissions
- Ensure the MCP server is enabled in your dbt Cloud project
- Check that the auth scheme is set to **Token**

### Wrong Project or Environment

- Verify the `x-dbt-dev-environment-id` custom header is set correctly
- Double-check the environment ID matches what's shown in the dbt Cloud URL

### SQL Queries Not Working

- Ensure development credentials are configured in dbt Cloud for the user
- Verify the `x-dbt-user-id` and `x-dbt-dev-environment-id` headers are set
- Check that the API token has permissions to execute queries
