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

- A Glean account with API access
- A Glean API token
- DataHub Cloud v0.3.x+ with Ask DataHub Plugins enabled
- Admin access to configure the plugin in DataHub

## Admin Setup

1. Navigate to **Settings > AI > Plugins**
2. Click **+ Create** and select **Custom MCP** (or a Glean template if available)
3. Configure the connection:

<!-- TODO: Add Glean-specific configuration fields table -->

4. Select an **Authentication Type**:
   - **Shared API Key** — a single Glean API token shared across all users
   - **User OAuth** — each user authenticates via Glean OAuth

<!-- TODO: Add screenshot of Glean plugin configuration -->

5. Optionally add **Instructions for the AI Assistant** (e.g., _"Use Glean to search for organizational policies and runbooks when the user asks about processes or guidelines."_)
6. Click **Test Connection** to verify, then **Create**

## User Setup

1. Navigate to **Settings > My AI Settings** or use the plugin selector in the chat interface
2. Find the **Glean** plugin and click **Connect**
3. Authenticate via OAuth or verify the shared connection is active
4. Toggle the plugin **on**

## Usage Tips

- Glean plugin complements DataHub's [Context Documents](../context/context-documents.md) — use Context Documents for curated, governable knowledge and Glean for broader organizational search
- The AI will automatically decide when to use Glean vs. DataHub's built-in search based on the question
- Be specific in your prompts about what kind of documentation you're looking for

## Troubleshooting

### Connection Errors

- Verify your Glean API token is valid and has the required permissions
- Ensure the Glean instance URL is correct
- Check that the API token has search access

### No Results

- Verify that Glean has indexed the content sources you expect
- Try broader search terms
- Check that the Glean token's permissions include access to the relevant content
