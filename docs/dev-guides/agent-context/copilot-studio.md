# Microsoft Copilot Studio

Build [Copilot Studio](https://copilotstudio.microsoft.com/) agents that can find trustworthy data, trace lineage, look up ownership, and answer data questions grounded in your enterprise context from DataHub.

## Prerequisites

- A [Microsoft Copilot Studio](https://copilotstudio.microsoft.com/) account
- A DataHub instance: [Cloud](../../features/feature-guides/mcp.md#managed-mcp-server-usage) (OAuth on v1.0.2+, PAT on v0.3.12+) or [self-hosted](../../features/feature-guides/mcp.md#self-hosted-mcp-server-usage) with the MCP server running

## Setup

### 1. Create or Open an Agent

In [Copilot Studio](https://copilotstudio.microsoft.com/), click **+ Create a blank agent** or open an existing one.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_agents_page.png"/>
</p>

### 2. Add an MCP Tool

From your agent's overview, scroll to **Tools** and click **+ Add tool**. Select **Model Context Protocol** under "Create new".

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_add_tool.png"/>
</p>

### 3. Configure the MCP Connection

#### DataHub Cloud — OAuth (Recommended, v1.0.2+)

On DataHub Cloud v1.0.2+, Copilot Studio can connect with OAuth2 and [Dynamic Client Registration (DCR)](https://datatracker.ietf.org/doc/html/rfc7591). Each agent user signs in with their own DataHub account (including SSO such as Okta or Azure AD). No personal access token to mint or paste.

| Field              | Value                                 |
| ------------------ | ------------------------------------- |
| **Server name**    | `DataHub MCP Server`                  |
| **Server URL**     | `https://mcp.datahub.com/mcp`         |
| **Authentication** | **OAuth 2.0** · **Dynamic discovery** |

Click **Create** / **Next**. Copilot Studio discovers DataHub's OAuth metadata and registers itself via DCR. On **Add tool**, choose **Create a new connection**. On first connect, enter your DataHub domain (e.g. `<tenant>` for `https://<tenant>.acryl.io`), complete login (and SSO if your tenant uses it), then **Add to agent**.

Prefer your tenant URL directly? Use `https://<tenant>.acryl.io/integrations/ai/mcp` as the Server URL instead — both endpoints support OAuth2 + DCR.

:::tip Fallback OAuth modes
If **Dynamic discovery** fails, try **Dynamic** and enter the Authorization and Token URLs from the auth server's `/.well-known/oauth-authorization-server` document. Prefer Dynamic discovery when it works — you should not need a Client ID or Client secret.
:::

#### DataHub Cloud — Personal Access Token (v0.3.12+)

For service accounts, unattended agents, or DataHub Cloud versions prior to v1.0.2, use a [personal access token](../../authentication/personal-access-tokens.md):

| Field              | Value                                                 |
| ------------------ | ----------------------------------------------------- |
| **Server name**    | `DataHub MCP Server`                                  |
| **Server URL**     | `https://<tenant>.acryl.io/integrations/ai/mcp`       |
| **Authentication** | API key · Header · `Authorization` · `Bearer <token>` |

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_mcp_config.png"/>
</p>

Include the `Bearer ` prefix in the API key value.

:::note Self-Hosted DataHub
For self-hosted instances, OAuth DCR for the managed MCP path is a DataHub Cloud capability. Expose the [MCP server](../../features/feature-guides/mcp.md#self-hosted-mcp-server-usage) via a publicly accessible URL, use that as the Server URL, and authenticate with a personal access token (API key · Header · `Authorization` · `Bearer <token>`).
:::

### 4. Enable Tools

Click **Next** — Copilot Studio discovers DataHub's tools automatically. Click **Add and configure**, then toggle on the tools you want.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_tools_list.png"/>
</p>

### 5. Test

Click **Test** in the top-right corner and try:

- _"What datasets does the analytics team own?"_
- _"Show me the lineage for the revenue dashboard"_

<p align="center">
  <img width="50%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_test_agent.png"/>
</p>

## Tips

- Use the **Instructions** field to guide behavior — e.g., _"Always search DataHub before answering data questions."_
- **Publish** your agent to Teams, your website, or other channels when ready.
- Prefer OAuth for interactive Copilot agents so each user acts as themselves in DataHub. Keep PATs for service accounts and unattended workflows.

## Troubleshooting

- **Can't connect with OAuth?** Confirm the tenant is on DataHub Cloud v1.0.2+, the Server URL is `https://mcp.datahub.com/mcp` (or your tenant MCP URL), and authentication is **OAuth 2.0** with **Dynamic discovery**. Complete **Create a new connection**, enter your DataHub domain when prompted, and finish the browser login.
- **Can't connect with a PAT?** Verify the DataHub URL, check the token hasn't expired, and confirm auth is set to **API key** (not OAuth). Include the `Bearer ` prefix.
- **Tools not appearing?** Click refresh on the Tools page. Verify the [MCP server](../../features/feature-guides/mcp.md) is running and the connection has the right permissions.
- **Empty results?** Check that your DataHub instance has ingested metadata. Try broader search terms.

**Links:** [Copilot Studio Docs](https://learn.microsoft.com/en-us/microsoft-copilot-studio/mcp-add-existing-server-to-agent) · [Agent Context Kit](./agent-context.md) · [MCP Server Guide](../../features/feature-guides/mcp.md)
