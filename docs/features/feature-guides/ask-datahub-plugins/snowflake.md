import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Snowflake Plugin

<FeatureAvailability saasOnly />

The **Snowflake Plugin** connects Ask DataHub to your Snowflake warehouse, enabling conversational analytics directly from the chat. Users can query tables, explore schemas, and analyze data — all guided by DataHub's metadata catalog.

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

- A Snowflake account with a user or role that has access to the desired databases/schemas
- DataHub Cloud v0.3.x+ with Ask DataHub Plugins enabled
- Admin access to configure the plugin in DataHub

## Admin Setup

1. Navigate to **Settings > AI > Plugins**
2. Click **+ Create** and select **Snowflake MCP**
3. Configure the connection:

<!-- TODO: Add Snowflake-specific configuration fields table -->

4. Select an **Authentication Type**:
   - **Shared API Key** — a single Snowflake service account used by all users
   - **User API Key** — each user provides their own Snowflake credentials
   - **User OAuth** — each user authenticates via Snowflake OAuth

<!-- TODO: Add screenshot of Snowflake plugin configuration -->

5. Optionally add **Instructions for the AI Assistant** (e.g., _"Default to the ANALYTICS warehouse and PROD database when querying."_)
6. Click **Test Connection** to verify, then **Create**

## User Setup

1. Navigate to **Settings > My AI Settings** or use the plugin selector in the chat interface
2. Find the **Snowflake** plugin and click **Connect**
3. Provide your credentials (if using User API Key or OAuth authentication)
4. Toggle the plugin **on**

## Usage Tips

- Ask DataHub will use your DataHub metadata to identify the right tables before querying Snowflake — so well-documented datasets lead to better results
- You can specify a database, schema, or warehouse in your prompt if needed
- Results are returned inline in the chat for easy review

## Troubleshooting

### Connection Errors

- Verify that the Snowflake account URL is correct
- Ensure the credentials have the necessary permissions (e.g., `USAGE` on warehouse, `SELECT` on tables)
- Check that network policies allow connections from DataHub Cloud

### Query Failures

- Verify the user's role has access to the target database/schema
- Check that the warehouse is running and not suspended
