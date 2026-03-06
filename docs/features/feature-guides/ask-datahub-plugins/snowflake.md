import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Snowflake Plugin

<FeatureAvailability saasOnly />

> **Note**: Ask DataHub Plugins is currently in **Private Beta**. To enable this feature, please reach out to your DataHub customer support representative.

The **Snowflake Plugin** connects Ask DataHub to a [Snowflake-managed MCP server](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-mcp), enabling conversational analytics directly from the chat. Users can query tables, explore schemas, and analyze data — all guided by DataHub's metadata catalog.

## Why Connect Snowflake?

With the Snowflake plugin enabled, Ask DataHub can:

- **Execute SQL queries** against your Snowflake warehouse based on natural language questions
- **Explore data** by sampling tables and inspecting results in real time
- **Build on DataHub context** — the AI uses metadata (descriptions, ownership, quality signals) to write better queries against the right tables

**Example prompts:**

- _"Query the top 10 customers by revenue this quarter"_
- _"How many null values are in the email column of the customers table?"_
- _"Compare last month's sales to this month across regions"_

## Prerequisites

- A Snowflake account with `ACCOUNTADMIN` (or equivalent privileges to create MCP servers, security integrations, and grant roles)
- DataHub Cloud with Ask DataHub Plugins enabled
- Platform admin access in DataHub to configure the plugin

## Admin Setup

The Snowflake plugin uses **User OAuth** authentication — each user authenticates with their own Snowflake account. Setup involves creating an MCP server and OAuth security integration in Snowflake, then configuring the plugin in DataHub.

### Step 1: Create an MCP Server in Snowflake

Create an MCP server in Snowflake with the tools you want to expose. The example below creates a server with a SQL execution tool:

```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE YOUR_DATABASE;
USE SCHEMA YOUR_SCHEMA;

CREATE OR REPLACE MCP SERVER YOUR_MCP_SERVER
  FROM SPECIFICATION $$
    tools:
      - name: "execute_sql"
        type: "SYSTEM_EXECUTE_SQL"
        description: "Execute SQL queries against the data warehouse"
        title: "SQL Executor"
  $$;
```

You can also expose Cortex Search, Cortex Analyst, Cortex Agent, and custom UDF/stored procedure tools. See the [Snowflake MCP server documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-mcp) for all available tool types.

### Step 2: Grant Access

Grant usage on the MCP server to the roles that should be able to use it:

```sql
GRANT USAGE ON DATABASE YOUR_DATABASE TO ROLE YOUR_ROLE;
GRANT USAGE ON SCHEMA YOUR_DATABASE.YOUR_SCHEMA TO ROLE YOUR_ROLE;
GRANT USAGE ON MCP SERVER YOUR_DATABASE.YOUR_SCHEMA.YOUR_MCP_SERVER TO ROLE YOUR_ROLE;
```

### Step 3: Create an OAuth Security Integration

Create a security integration to use Snowflake as an OAuth provider. Make sure to set the redirect URI to your DataHub instance's OAuth callback and enable refresh tokens:

```sql
CREATE OR REPLACE SECURITY INTEGRATION YOUR_INTEGRATION_NAME
  TYPE = OAUTH
  ENABLED = TRUE
  OAUTH_CLIENT = CUSTOM
  OAUTH_CLIENT_TYPE = 'CONFIDENTIAL'
  OAUTH_REDIRECT_URI = 'https://<your-datahub-url>/integrations/oauth/callback'
  OAUTH_ISSUE_REFRESH_TOKENS = TRUE
  OAUTH_REFRESH_TOKEN_VALIDITY = 7776000;
```

:::caution Refresh Tokens
Make sure `OAUTH_ISSUE_REFRESH_TOKENS` is set to `TRUE`. Without refresh tokens, users will need to re-authenticate frequently.
:::

### Step 4: Collect Authentication Details

Run the following queries to gather the information you'll need for DataHub:

**OAuth Client Credentials:**

```sql
SELECT SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('YOUR_INTEGRATION_NAME');
```

This returns a JSON object containing `OAUTH_CLIENT_ID` and `OAUTH_CLIENT_SECRET`.

**Authorization and Token Endpoints:**

```sql
DESC SECURITY INTEGRATION "YOUR_INTEGRATION_NAME";
```

Look for `OAUTH_AUTHORIZATION_ENDPOINT` and `OAUTH_TOKEN_ENDPOINT` in the output.

**MCP Server URL:**

The URL follows this format:

```
https://<account-url>/api/v2/databases/<DATABASE>/schemas/<SCHEMA>/mcp-servers/<SERVER_NAME>
```

:::note Account URL Format
Use hyphens (`-`) instead of dots (`.`) in the account locator portion of the URL. For example, if your account locator is `abc12345.us-east-1`, your URL would use `abc12345-us-east-1.snowflakecomputing.com`.
:::

At this point, you should have all five values needed for the DataHub configuration:

| Value                 | Example                                                                                                         |
| --------------------- | --------------------------------------------------------------------------------------------------------------- |
| **MCP Server URL**    | `https://abc12345-us-east-1.snowflakecomputing.com/api/v2/databases/MY_DB/schemas/PUBLIC/mcp-servers/MY_SERVER` |
| **Client ID**         | From `SYSTEM$SHOW_OAUTH_CLIENT_SECRETS` output                                                                  |
| **Client Secret**     | From `SYSTEM$SHOW_OAUTH_CLIENT_SECRETS` output                                                                  |
| **Authorization URL** | From `DESC SECURITY INTEGRATION` (e.g. `https://abc12345.snowflakecomputing.com/oauth/authorize`)               |
| **Token URL**         | From `DESC SECURITY INTEGRATION` (e.g. `https://abc12345.snowflakecomputing.com/oauth/token-request`)           |

### Step 5: Create Plugin in DataHub

1. Navigate to **Settings > AI > Plugins** in DataHub
2. Click **+ Create** and select **Custom MCP**
3. Fill in the plugin details:

| Field                   | Value                                  |
| ----------------------- | -------------------------------------- |
| **Name**                | `Snowflake`                            |
| **Description**         | A description for the plugin           |
| **MCP Server URL**      | The URL from Step 4                    |
| **Authentication Type** | `User OAuth (Each user authenticates)` |

4. Fill in the OAuth provider details:

| Field                 | Value                                    |
| --------------------- | ---------------------------------------- |
| **Provider Name**     | `Snowflake`                              |
| **Client ID**         | From Step 4                              |
| **Client Secret**     | From Step 4                              |
| **Authorization URL** | From Step 4                              |
| **Token URL**         | From Step 4                              |
| **Default Scopes**    | `refresh_token session:role:<ROLE_NAME>` |

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/snowflake_plugin_config.png"/>
</p>

:::info Scopes
The `session:role:<ROLE_NAME>` scope determines which Snowflake role the user assumes when connected. If omitted, the user's default role is used. The user **must** have the specified role granted to them in Snowflake.
:::

5. Optionally add **Instructions for the AI Assistant**, ensure **Enable for Ask DataHub** is on, and click **Create**

## User Setup

Navigate to **Settings > My AI Settings**, find the **Snowflake** plugin, and click **Connect**. You'll be redirected to Snowflake to authenticate, then back to DataHub. See the [overview](./overview.md#user-setup-enabling-plugins) for more details.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/snowflake_user_connect.png"/>
</p>

## Troubleshooting

### Users can't log in / OAuth fails

The OAuth security integration may be blocking the user's role. Ensure the role you want to assume is **not** in the blocked roles list:

```sql
ALTER SECURITY INTEGRATION "YOUR_INTEGRATION_NAME"
  SET BLOCKED_ROLES_LIST = ();
```

### Query failures

- Verify the user's Snowflake role has `SELECT` access to the target tables
- Ensure the user has `USAGE` granted on the MCP server
- Check that the warehouse is running and not suspended

### Token expiration

If users are being asked to re-authenticate frequently, verify that refresh tokens are enabled on the security integration (`OAUTH_ISSUE_REFRESH_TOKENS = TRUE`) and that `OAUTH_REFRESH_TOKEN_VALIDITY` is set to an appropriate duration.
