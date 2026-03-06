import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Snowflake Plugin

<FeatureAvailability saasOnly />

The **Snowflake Plugin** connects Ask DataHub to a [Snowflake MCP server](https://docs.snowflake.com/en/developer-guide/mcp-server), enabling conversational analytics directly from the chat. Users can query tables, explore schemas, and analyze data — all guided by DataHub's metadata catalog.

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

- A Snowflake account with permissions to create MCP servers, security integrations, and grant roles
- DataHub Cloud with Ask DataHub Plugins enabled
- Platform admin access in DataHub to configure the plugin

## Admin Setup

The Snowflake plugin uses **User OAuth** authentication — each user authenticates with their own Snowflake account. Setup involves creating an MCP server and OAuth security integration in Snowflake, then configuring the plugin in DataHub.

### Step 1: Create an MCP Server in Snowflake

Create an MCP server in Snowflake with the tools you want to expose to Ask DataHub.

<!-- TODO: Screenshot of Snowflake MCP server creation -->

See the [Snowflake MCP server documentation](https://docs.snowflake.com/en/developer-guide/mcp-server) for details on creating and configuring MCP servers.

### Step 2: Grant Access

Ensure the appropriate roles have usage on the MCP server:

```sql
GRANT USAGE ON MCP SERVER "your-mcp-server" TO ROLE DATA_ANALYST;
```

### Step 3: Configure OAuth in Snowflake

Create a security integration to use Snowflake as an OAuth provider. Make sure to:

- Set the **OAuth redirect URI** to `https://<your-datahub-url>/integrations/oauth/callback`
- **Enable refresh tokens** (required for persistent sessions)

<!-- TODO: Screenshot of Snowflake security integration creation -->

```sql
CREATE OR REPLACE SECURITY INTEGRATION "your-integration-name"
  TYPE = OAUTH
  ENABLED = TRUE
  OAUTH_CLIENT = CUSTOM
  OAUTH_CLIENT_TYPE = 'CONFIDENTIAL'
  OAUTH_REDIRECT_URI = 'https://<your-datahub-url>/integrations/oauth/callback'
  OAUTH_ISSUE_REFRESH_TOKENS = TRUE
  OAUTH_REFRESH_TOKEN_VALIDITY = 86400;
```

:::caution Refresh Tokens
Make sure `OAUTH_ISSUE_REFRESH_TOKENS` is set to `TRUE`. Without refresh tokens, users will need to re-authenticate frequently.
:::

### Step 4: Collect Authentication Details

Run the following command to retrieve your OAuth client credentials:

```sql
SELECT SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('your-integration-name');
```

This returns a JSON object containing `OAUTH_CLIENT_ID` and `OAUTH_CLIENT_SECRET`.

Next, retrieve the authorization and token endpoints:

```sql
DESC SECURITY INTEGRATION "your-integration-name";
```

Look for the `OAUTH_AUTHORIZATION_ENDPOINT` and `OAUTH_TOKEN_ENDPOINT` values in the output.

<!-- TODO: Screenshot of DESC SECURITY INTEGRATION output -->

You'll also need the MCP server URL, which follows this format:

```
https://<account-url>/api/v2/databases/<database>/schemas/<schema>/mcp-servers/<server-name>
```

You can find your account URL in the Snowflake UI.

<!-- TODO: Screenshot showing where to find account URL -->

### Step 5: Create Plugin in DataHub

1. Navigate to **Settings > AI > Plugins** in DataHub
2. Click **+ Create** and select **Custom MCP**
3. Fill in the plugin details:

| Field                   | Value                                                                             |
| ----------------------- | --------------------------------------------------------------------------------- |
| **Name**                | `Snowflake`                                                                       |
| **Description**         | A description for the plugin                                                      |
| **MCP Server URL**      | `https://<account-url>/api/v2/databases/<db>/schemas/<schema>/mcp-servers/<name>` |
| **Authentication Type** | `User OAuth (Each user authenticates)`                                            |

4. Fill in the OAuth provider details using the values collected in Step 4:

| Field                 | Value                                          |
| --------------------- | ---------------------------------------------- |
| **Provider Name**     | `Snowflake`                                    |
| **Client ID**         | From `SYSTEM$SHOW_OAUTH_CLIENT_SECRETS` output |
| **Client Secret**     | From `SYSTEM$SHOW_OAUTH_CLIENT_SECRETS` output |
| **Authorization URL** | From `DESC SECURITY INTEGRATION` output        |
| **Token URL**         | From `DESC SECURITY INTEGRATION` output        |
| **Default Scopes**    | `refresh_token session:role:<ROLE_NAME>`       |

<!-- TODO: Screenshot of DataHub plugin config for Snowflake -->

:::info Scopes
The `session:role:<ROLE_NAME>` scope determines which Snowflake role the user assumes. If omitted, the user's default role is used. The user **must** have the specified role granted to them in Snowflake.
:::

5. Optionally add **Instructions for the AI Assistant**
6. Ensure **Enable for Ask DataHub** is toggled on
7. Click **Create**

## User Setup

Once the admin has configured the Snowflake plugin:

1. Navigate to **Settings > My AI Settings** in DataHub
2. Find the **Snowflake** plugin and click **Connect**
3. You'll be redirected to Snowflake to authenticate with your account
4. After authentication, you'll be redirected back to DataHub
5. The plugin is now connected and enabled

<!-- TODO: Screenshot of Snowflake OAuth login flow -->

## FAQ

### Users can't log in / OAuth fails

The OAuth security integration may be blocking the user's role. Ensure the role is **not** in the blocked roles list:

```sql
ALTER SECURITY INTEGRATION "your-integration-name"
  SET BLOCKED_ROLES_LIST = ();
```

### Query failures

- Verify the user's Snowflake role has `SELECT` access to the target tables
- Check that the MCP server's warehouse is running and not suspended
- Ensure the user has `USAGE` granted on the MCP server

### Token expiration

If users are being asked to re-authenticate frequently, verify that refresh tokens are enabled on the security integration (`OAUTH_ISSUE_REFRESH_TOKENS = TRUE`).
