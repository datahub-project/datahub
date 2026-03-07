import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Ask DataHub Plugins

<FeatureAvailability saasOnly />

> **Note**: Ask DataHub Plugins is currently in **Private Beta**. To enable this feature, please reach out to your DataHub customer support representative.

**Ask DataHub Plugins** let you connect external tools — like Snowflake, dbt Cloud, GitHub, Glean, Notion, and more — directly into [Ask DataHub](../ask-datahub.md) via the [Model Context Protocol (MCP)](../mcp.md).

With Plugins, can connect any MCP-compatible server that supports [Streamable HTTP transport](https://modelcontextprotocol.io/docs/concepts/transports). Once connected, Ask DataHub can use these tools alongside your metadata catalog to answer richer questions and perform cross-tool workflows.

## Why Use Plugins?

Plugins unlock powerful capabilities that go beyond what metadata alone can provide:

- **Conversational analytics**: Find the right data in DataHub, then query it directly on Snowflake or Databricks — all in a single conversation. DataHub acts as a semantic layer, guiding the AI to the right tables before executing queries.
- **Cross-tool debugging**: Root-cause a data issue by checking dbt run history, querying tables on Snowflake, and reviewing recent code changes on GitHub — without leaving the chat.
- **Governance workflows**: Verify data lineage in DataHub, check transformation logic in dbt, and review access controls — bringing together context from multiple systems.

## Available Plugins

DataHub provides built-in plugin templates for popular tools. For connecting to other MCP Servers — including internal services, custom APIs, or any tool with an MCP server — use the **Custom MCP** option.

| Plugin         | Description                                         | Guide                                         |
| -------------- | --------------------------------------------------- | --------------------------------------------- |
| **Snowflake**  | Query and explore your Snowflake data               | [Setup Guide](./snowflake.md)                 |
| **dbt Cloud**  | Check job runs, model definitions, and test results | [Setup Guide](./dbt.md)                       |
| **GitHub**     | Browse repositories, raise PRs, and manage issues   | [Setup Guide](./github.md)                    |
| **Glean**      | Search your organization's knowledge base           | [Setup Guide](./glean.md)                     |
| **Custom MCP** | Connect any MCP-compatible server                   | [See below](#configuring-a-custom-mcp-plugin) |

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/create_plugin_dialog.png"/>
</p>

## How It Works

Plugins follow a two-step workflow:

1. **Admin configures** the plugin by connecting to an MCP server (via **Settings > AI > Plugins**)
2. **Users enable** plugins they want to use from their personal settings (via **Settings > My AI Settings**) or directly from the chat interface

Once a user enables a plugin, its tools become available during Ask DataHub conversations. The AI assistant will automatically use these tools when relevant to a question.

## Admin Setup: Configuring Plugins

:::info Required Permissions
You must have the **Manage Platform Settings** privilege to configure plugins.
:::

### Creating a Plugin

1. Navigate to **Settings > AI > Plugins**
2. Click **+ Create** in the top-right corner
3. Select a built-in plugin template (Snowflake, dbt Cloud, GitHub) or choose **Custom MCP** for other tools

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/plugins_settings_page.png"/>
</p>

### Configuring a Custom MCP Plugin

Use **Custom MCP** to connect any MCP-compatible server — whether it's a third-party tool like Notion or Glean, an internal service your team has built, or any other API exposed via MCP. The server must support [Streamable HTTP transport](https://modelcontextprotocol.io/docs/concepts/transports).

When creating a Custom MCP plugin, provide:

| Field                                 | Description                                                                     |
| ------------------------------------- | ------------------------------------------------------------------------------- |
| **Name**                              | A display name for the plugin (visible to all users)                            |
| **Description**                       | A brief description of what this plugin provides                                |
| **MCP Server URL**                    | The URL of the remote MCP server                                                |
| **Authentication Type**               | How users authenticate with the MCP server ([see below](#authentication-types)) |
| **Instructions for the AI Assistant** | Custom instructions fed to Ask DataHub when using this plugin (optional)        |

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/create_custom_mcp_plugin.png"/>
</p>

### Authentication Types

Plugins support three authentication mechanisms:

#### None (Public API)

No authentication required. Use this for publicly accessible MCP servers that don't require credentials.

#### Shared API Key (System-wide)

A single API key shared across all users. The admin provides the key during setup, and it is used for every user's requests. Best for tools where a single service account is appropriate.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/auth_shared_api_key.png"/>
</p>

#### User API Key (Each User Provides Their Own)

Each user provides their own API key when enabling the plugin. This ensures per-user access control and audit trails. Users will be prompted to enter their key when they connect to the plugin.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/auth_user_api_key.png"/>
</p>

#### User OAuth (Each User Authenticates)

Each user authenticates via OAuth when enabling the plugin. The admin configures the OAuth provider (client ID, client secret, authorization URL, token URL, scopes), and users go through a standard OAuth login flow to connect.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/auth_user_oauth.png"/>
</p>

### Testing the Connection

After configuring a plugin, click **Test Connection** to verify that the MCP server is reachable and the credentials are valid before saving.

### Custom Instructions

The **Instructions for the AI Assistant** field lets you provide guidance to Ask DataHub on how to use the plugin. For example:

- _"Use this plugin to query Snowflake when users ask about revenue or customer metrics."_
- _"Always check dbt test results before reporting on data quality."_

These instructions are injected into the AI context whenever the plugin is active.

## User Setup: Enabling Plugins

Once an admin has configured a plugin, any user can enable it for their own Ask DataHub experience.

### From Personal Settings

1. Navigate to **Settings > My AI Settings**
2. You'll see a list of all available plugins configured by your admin
3. Toggle a plugin **on** to enable it, or click **Connect** to authenticate (for API Key or OAuth plugins)

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/my_ai_settings.png"/>
</p>

### From the Chat Interface

You can also manage plugins directly from the Ask DataHub chat:

1. Click the **plugins icon** next to the chat input
2. A dropdown shows all available plugins with their connection status
3. Toggle plugins on/off, or click **Manage AI plugins** to configure authentication

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/chat_plugin_selector.png"/>
</p>

For plugins that require authentication, you'll be prompted to provide your credentials:

<p align="center">
  <img width="50%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/connect_api_key_dialog.png"/>
</p>

## Using Plugins in Conversations

Once enabled, plugin tools are seamlessly available during Ask DataHub conversations. Just ask your question and the AI assistant will use the appropriate tools automatically.

**Example workflows:**

- _"What are the most queried tables in Snowflake this month?"_ — DataHub search + Snowflake query
- _"Did the latest dbt run succeed for the revenue model?"_ — dbt Cloud job history
- _"Show me recent changes to the ETL pipeline code"_ — GitHub commit browsing
- _"Find our internal documentation about the data retention policy"_ — Glean knowledge search
