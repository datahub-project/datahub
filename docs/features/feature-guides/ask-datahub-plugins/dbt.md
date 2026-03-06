import FeatureAvailability from '@site/src/components/FeatureAvailability';

# dbt Cloud Plugin

<FeatureAvailability saasOnly />

The **dbt Cloud Plugin** connects Ask DataHub to your dbt Cloud environment, giving the AI assistant visibility into job runs, model definitions, test results, and transformation logic.

## Why Connect dbt Cloud?

With the dbt Cloud plugin enabled, Ask DataHub can:

- **Check job run status** — see whether recent dbt runs succeeded or failed
- **Inspect model definitions** — view the SQL and configuration behind dbt models
- **Review test results** — understand which tests passed or failed and why
- **Debug data issues** — correlate data quality problems in DataHub with dbt run failures or test results

**Example prompts:**

- _"Did the latest dbt run for the revenue model succeed?"_
- _"Show me the SQL definition of the orders model"_
- _"Which dbt tests failed in the last 24 hours?"_
- _"What changed in the customers model recently?"_

## Prerequisites

- A dbt Cloud account with API access
- A dbt Cloud [API token](https://docs.getdbt.com/docs/dbt-cloud-apis/authentication) (service token or user token)
- DataHub Cloud v0.3.x+ with Ask DataHub Plugins enabled
- Admin access to configure the plugin in DataHub

## Admin Setup

1. Navigate to **Settings > AI > Plugins**
2. Click **+ Create** and select **dbt Cloud MCP**
3. Configure the connection:

<!-- TODO: Add dbt Cloud-specific configuration fields table -->

4. Select an **Authentication Type**:
   - **Shared API Key** — a single dbt Cloud service token shared across all users
   - **User API Key** — each user provides their own dbt Cloud token

<!-- TODO: Add screenshot of dbt Cloud plugin configuration -->

5. Optionally add **Instructions for the AI Assistant** (e.g., _"Focus on the production environment when checking run status."_)
6. Click **Test Connection** to verify, then **Create**

## User Setup

1. Navigate to **Settings > My AI Settings** or use the plugin selector in the chat interface
2. Find the **dbt Cloud** plugin and click **Connect**
3. Provide your API token (if using User API Key authentication)
4. Toggle the plugin **on**

## Usage Tips

- Combine dbt Cloud data with DataHub lineage for end-to-end debugging — e.g., trace a dashboard issue back through DataHub lineage to a failed dbt model
- Ask about specific environments (production, staging) to get targeted results
- Use dbt test results alongside DataHub assertions for a complete data quality picture

## Troubleshooting

### Connection Errors

- Verify your dbt Cloud API token is valid and has the required permissions
- Ensure the dbt Cloud account URL is correct
- Check that the token has access to the target project and environment

### Missing Data

- Verify that dbt jobs have been run recently
- Check that the API token's permissions include access to run history and model metadata
