import FeatureAvailability from '@site/src/components/FeatureAvailability';

# GitHub Plugin

<FeatureAvailability saasOnly />

The **GitHub Plugin** connects Ask DataHub to your GitHub repositories, enabling the AI assistant to browse code, review recent changes, and manage issues — bringing source code context into your data conversations.

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

- A GitHub account with access to the relevant repositories
- A GitHub [personal access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens) or OAuth app configuration
- DataHub Cloud v0.3.x+ with Ask DataHub Plugins enabled
- Admin access to configure the plugin in DataHub

## Admin Setup

1. Navigate to **Settings > AI > Plugins**
2. Click **+ Create** and select **GitHub MCP**
3. Configure the connection:

<!-- TODO: Add GitHub-specific configuration fields table -->

4. Select an **Authentication Type**:
   - **User API Key** — each user provides their own GitHub personal access token
   - **User OAuth** — each user authenticates via GitHub OAuth (recommended for teams)

<!-- TODO: Add screenshot of GitHub plugin configuration -->

5. Optionally add **Instructions for the AI Assistant** (e.g., _"Focus on the data-platform and analytics repos when searching for code."_)
6. Click **Test Connection** to verify, then **Create**

## User Setup

1. Navigate to **Settings > My AI Settings** or use the plugin selector in the chat interface
2. Find the **GitHub** plugin and click **Connect**
3. Authenticate via OAuth or provide your personal access token
4. Toggle the plugin **on**

## Usage Tips

- GitHub plugin works best when combined with DataHub lineage — trace a data issue to a specific dataset, then check recent code changes that may have caused it
- Be specific about repository names and branches in your prompts for better results
- The AI can read file contents, so you can ask it to explain transformation logic in plain English

## Troubleshooting

### Connection Errors

- Verify your GitHub token has the required scopes (`repo`, `read:org` at minimum)
- For OAuth setups, ensure the OAuth app has the correct callback URL configured
- Check that the token has access to the target repositories (especially for private repos)

### Permission Issues

- Ensure the GitHub token or OAuth scopes grant access to the repositories you want to query
- For organization repositories, the token may need SSO authorization
