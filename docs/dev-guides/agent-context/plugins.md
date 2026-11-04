# DataHub Cloud Plugins

Give Claude, ChatGPT, or Codex access to your enterprise data context in DataHub with a one-click plugin. The plugin includes:

- **The DataHub MCP server.** You sign in with your DataHub account, so there are no tokens to manage.
- **Skills** for search, lineage, data quality, and SQL.

The plugin can only see and change what your DataHub account can. Source code: [datahub-project/datahub-cloud-ai-plugins](https://github.com/datahub-project/datahub-cloud-ai-plugins).

## Prerequisites

- A DataHub Cloud instance on v1.0.2+
- Claude, ChatGPT, or Codex

## Claude

Works in Claude on the web and desktop, in Cowork, and in Claude Code.

1. Sign in to Claude, go to **Customize → Plugins**, and search for **DataHub Cloud**.
2. Click **Install**. On Team and Enterprise plans, an owner can install it for everyone.
3. The first time you use it, click **Connect**, enter your DataHub domain (e.g. `<tenant>` for `https://<tenant>.acryl.io`), and sign in. In Claude Code, run `/mcp`, select `datahub`, and choose **Authenticate** instead.

Then ask a question, like _"Who owns the orders table?"_ You can also run a skill directly:

```
/datahub-cloud:datahub-search Find tables tagged PII
```

Only need the MCP tools, without skills? Add the [DataHub connector](https://claude.com/connectors/datahub) instead.

Using Claude Code without signing in to a Claude account (for example, with an API key)? The plugin isn't available there. Follow the [manual Claude Code setup](./claude.md#claude-code) instead.

## ChatGPT and Codex

### ChatGPT

1. Open the [DataHub Cloud plugin](https://chatgpt.com/plugins/plugin_asdk_app_6abd46e594a08191a68a2fe9e77a7c88), or search for **DataHub Cloud** in ChatGPT's plugin directory.
2. Click **Add**. On Team and Enterprise plans, an admin may need to turn it on first.
3. Enter your DataHub domain (e.g. `<tenant>` for `https://<tenant>.acryl.io`) and sign in.

### Codex

Install the latest [release](https://github.com/datahub-project/datahub-cloud-ai-plugins/releases) from the source repository:

```bash
git clone --branch v1.0.0 https://github.com/datahub-project/datahub-cloud-ai-plugins.git
codex plugin marketplace add "$PWD/datahub-cloud-ai-plugins"
codex plugin add datahub-cloud@datahub-cloud-ai-plugins
```

The first time Codex uses DataHub, it asks you to sign in.

## Example Questions

- _"Find all Snowflake tables tagged PII in the Finance domain."_
- _"What breaks downstream if we drop the orders table?"_
- _"Is revenue_metrics up to date?"_
- _"Write a query for monthly active users by region."_

## Other Options

- **Need a scoped MCP server or an access token?** The plugin always connects to your default MCP server, so set up the MCP server manually instead. See the [Claude guide](./claude.md), or the [MCP server guide](../../features/feature-guides/mcp.md#configure-your-client) for ChatGPT.
- **Using DataHub Core, or building connectors?** Use [DataHub Skills](./skills.md) instead.
