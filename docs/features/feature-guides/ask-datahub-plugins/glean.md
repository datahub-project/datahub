import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Glean Plugin

<FeatureAvailability saasOnly />

The **Glean Plugin** connects Ask DataHub to your organization's [Glean](https://www.glean.com/) instance, bringing enterprise knowledge search into your data conversations.

## Why Connect Glean?

With the Glean plugin enabled, Ask DataHub can:

- **Search organizational knowledge** — find relevant documentation, wikis, Slack threads, and other content indexed by Glean
- **Supplement metadata with context** — answer questions that require both data catalog knowledge and organizational documentation
- **Bridge the gap** between your data ecosystem and your team's tribal knowledge stored across tools

**Example prompts:**

- _"Find our internal documentation about the data retention policy"_
- _"Search for any runbooks related to the revenue pipeline"_
- _"What does our wiki say about the customer segmentation methodology?"_
- _"Find Slack discussions about the recent data migration"_

## Prerequisites

- A Glean account with access to the MCP server endpoint
- DataHub Cloud with Ask DataHub Plugins enabled
- Platform admin access in DataHub to configure the plugin

## Admin Setup

The Glean plugin is the simplest to set up thanks to DataHub's **OAuth discovery and automatic client registration**. DataHub will automatically discover the OAuth endpoints, available scopes, and register a client with Glean — no manual OAuth configuration required.

### Step 1: Get the Glean MCP Server URL

Obtain the MCP server URL from your Glean instance. This is typically available in your Glean admin settings or API documentation.

### Step 2: Create Plugin in DataHub

1. Navigate to **Settings > AI > Plugins** in DataHub
2. Click **+ Create** and select **Custom MCP**
3. Fill in the plugin details:

| Field                   | Value                                  |
| ----------------------- | -------------------------------------- |
| **Name**                | `Glean`                                |
| **Description**         | A description for the plugin           |
| **MCP Server URL**      | Your Glean MCP server URL              |
| **Authentication Type** | `User OAuth (Each user authenticates)` |

4. DataHub will automatically discover the OAuth configuration from Glean, including:

   - Authorization and token endpoints
   - Available scopes
   - Client registration

5. Review the discovered scopes and select the ones appropriate for your organization
6. Optionally add **Instructions for the AI Assistant** (e.g., _"Use Glean to search for organizational policies and runbooks when the user asks about processes or guidelines."_)
7. Ensure **Enable for Ask DataHub** is toggled on
8. Click **Create**

That's it — no need to manually configure OAuth provider details, client IDs, or secrets.

## User Setup

Once the admin has configured the Glean plugin:

1. Navigate to **Settings > My AI Settings** in DataHub
2. Find the **Glean** plugin and click **Connect**
3. You'll be redirected to Glean to authenticate
4. After authentication, you'll be redirected back to DataHub
5. The plugin is now connected and enabled

## Usage Tips

- Glean plugin complements DataHub's [Context Documents](../context/context-documents.md) — use Context Documents for curated, governable knowledge and Glean for broader organizational search
- The AI will automatically decide when to use Glean vs. DataHub's built-in search based on the question
- Be specific in your prompts about what kind of documentation you're looking for

## Troubleshooting

### OAuth Discovery Fails

- Verify the Glean MCP server URL is correct and accessible
- Ensure your Glean instance supports OAuth discovery (OpenID Connect Discovery)
- Contact your Glean administrator to verify MCP server access

### No Results

- Verify that Glean has indexed the content sources you expect
- Try broader search terms
- Check that the authenticated user has access to the relevant content in Glean
