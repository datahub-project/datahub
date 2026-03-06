import FeatureAvailability from '@site/src/components/FeatureAvailability';

# GitHub Plugin

<FeatureAvailability saasOnly />

> **Note**: Ask DataHub Plugins is currently in **Private Beta**. To enable this feature, please reach out to your DataHub customer support representative.

The **GitHub Plugin** connects Ask DataHub to GitHub via the [GitHub MCP server](https://docs.github.com/en/copilot/how-tos/provide-context/use-mcp/use-the-github-mcp-server), enabling the AI assistant to browse repositories, review code changes, manage issues, and more — all from within Ask DataHub.

## Why Connect GitHub?

With the GitHub plugin enabled, Ask DataHub can:

- **Browse repository code** — inspect transformation logic, pipeline definitions, and configuration files
- **Review recent changes** — see commits and pull requests that may have caused data issues
- **Cross-reference with metadata** — correlate code changes with downstream data quality problems surfaced in DataHub
- **Manage issues** — create or review GitHub issues related to data problems

**Example prompts:**

- _"Show me recent changes to the ETL pipeline in the data-platform repo"_
- _"What PRs were merged this week that touch the revenue model?"_
- _"Find the source code for the customer_churn transformation"_
- _"Create a GitHub issue for the broken lineage on the orders table"_

## Prerequisites

- A GitHub account (personal or organization)
- Admin access to create OAuth Apps in GitHub
- DataHub Cloud with Ask DataHub Plugins enabled
- Platform admin access in DataHub to configure the plugin

## Admin Setup

The GitHub plugin uses **User OAuth** authentication — each user authenticates with their own GitHub account. You'll need to create a GitHub OAuth App and configure it in DataHub.

You can create the OAuth App under your **organization** (recommended) or your **personal account**:

- **Organization app**: Immediately available to all org members, not scoped to just one user's repositories
- **Personal app**: Scoped to your personal repositories by default

### Option A: Organization OAuth App (Recommended)

#### Step 1: Create OAuth App in GitHub

1. Open GitHub and navigate to **Organization Settings > Developer Settings > OAuth Apps**
2. Click **New OAuth App** and fill in the application details:

| Field                          | Value                                                      |
| ------------------------------ | ---------------------------------------------------------- |
| **Application name**           | `DataHub AI Plugin` (or any name your team will recognize) |
| **Homepage URL**               | `https://<your-datahub-url>`                               |
| **Authorization callback URL** | `https://<your-datahub-url>/integrations/oauth/callback`   |

3. Click **Register application**

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/github_register_oauth_app.png"/>
</p>

#### Step 2: Collect Credentials

1. Copy the **Client ID** from the app page
2. Click **Generate a new client secret** and copy it immediately (it won't be shown again)

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/github_client_credentials.png"/>
</p>

#### Step 3: Create Plugin in DataHub

1. Navigate to **Settings > AI > Plugins** in DataHub
2. Click **+ Create** and select **Custom MCP**
3. Fill in the plugin details:

| Field                   | Value                                  |
| ----------------------- | -------------------------------------- |
| **Name**                | `GitHub`                               |
| **Description**         | A description for the plugin           |
| **MCP Server URL**      | `https://api.githubcopilot.com/mcp/`   |
| **Authentication Type** | `User OAuth (Each user authenticates)` |

4. The **OAuth Callback URL** shown in DataHub should match what you entered in GitHub (`https://<your-datahub-url>/integrations/oauth/callback`)

5. Fill in the OAuth provider details:

| Field                 | Value                                         |
| --------------------- | --------------------------------------------- |
| **Provider Name**     | `GitHub` (or any label)                       |
| **Client ID**         | The client ID from Step 2                     |
| **Client Secret**     | The client secret from Step 2                 |
| **Authorization URL** | `https://github.com/login/oauth/authorize`    |
| **Token URL**         | `https://github.com/login/oauth/access_token` |
| **Default Scopes**    | `repo, user`                                  |

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/github_plugin_config.png"/>
</p>

6. Optionally add **Instructions for the AI Assistant**, ensure **Enable for Ask DataHub** is on, and click **Create**

### Option B: Personal Account OAuth App

The steps are the same as above, except navigate to **GitHub > Your Profile > Settings > Developer Settings > OAuth Apps** instead of organization settings.

:::tip Side-by-Side Setup
When creating a personal OAuth App, open both GitHub and DataHub side by side. You'll need to copy the OAuth Callback URL from DataHub into GitHub, and the Client ID/Secret from GitHub into DataHub.
:::

### Recommended Scopes

| Scope      | Access                                          |
| ---------- | ----------------------------------------------- |
| `repo`     | Full access to repositories (code, PRs, issues) |
| `read:org` | Read organization data (teams, members)         |
| `user`     | Read user profile information                   |

## User Setup

Navigate to **Settings > My AI Settings**, find the **GitHub** plugin, and click **Connect**. A popup will open asking you to authorize the OAuth App on GitHub. After authorization, you'll be redirected back to DataHub. See the [overview](./overview.md#user-setup-enabling-plugins) for more details.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/plugins/github_user_connect.png"/>
</p>

## Troubleshooting

### OAuth Popup Blocked

If the authorization popup doesn't appear, check your browser's popup blocker settings and allow popups from your DataHub instance URL.

### Authorization Fails

- Verify the **Authorization callback URL** in GitHub matches the OAuth Callback URL shown in DataHub exactly
- Ensure the Client ID and Client Secret are entered correctly in DataHub
- For organization apps, ensure the OAuth App is not restricted by organization policies

### Limited Repository Access

- For organization OAuth Apps, users may need to [grant access to specific organizations](https://docs.github.com/en/apps/oauth-apps/using-oauth-apps/authorizing-oauth-apps) during the authorization flow
- Verify the scopes include `repo` for full repository access
