import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Ask DataHub

<FeatureAvailability saasOnly />

**Ask DataHub** is DataHub's conversational AI assistant that brings intelligent, context-aware help directly to where you work. Using Ask DataHub, you can ask questions about your organization's metadata and get instant, accurate answers about your data landscape.

## What Can Ask DataHub Do?

Ask DataHub empowers your organization to navigate & understand your entire data ecosystem with ease.

Key capabilities include:

### Natural Language Search

Search for data assets using plain English instead of memorizing exact table names or technical identifiers. Ask DataHub understands context and synonyms to help you find what you need.

### Finding Trustworthy Data

Discover high-quality, reliable data assets based on usage patterns, documentation quality, ownership information, and data quality metrics. Ask DataHub helps you identify the most authoritative sources for your analysis.

### Understanding Impact of Changes

Quickly assess how changes to a data asset will ripple through your organization. Ask DataHub can trace lineage and identify all downstream dependencies, helping you make informed decisions before making changes.

### Understanding Social and Organizational Context

Get insights into the human side of your data:

- **Ownership**: Find out who owns and maintains specific assets
- **Expertise**: Identify domain experts and frequent users
- **Collaboration**: Understand which teams work with particular datasets
- **History**: Learn about past usage patterns and organizational knowledge

### Assessing Data Quality

Quickly understand the health and reliability of your data assets:

- View assertion results and data quality scores
- Understand freshness and validation status
- Identify potential issues before using data
- Get context on historical quality trends

### Writing SQL Queries

Generate first-draft SQL queries to answer specific analytical questions, accelerating your data exploration and analysis workflows.

## Where You Can Use Ask DataHub

### Slack

Ask DataHub is available in Slack by mentioning @DataHub in any channel. This brings DataHub's intelligence directly into your team conversations.

Learn more: [Ask DataHub in Slack](../../managed-datahub/slack/saas-slack-app.md#ask-datahub-in-slack)

### Microsoft Teams

Similarly, you can use Ask DataHub in Microsoft Teams by mentioning @DataHub in channels and chats.

Learn more: [Ask DataHub in Teams](../../managed-datahub/teams/saas-teams-app.md#ask-datahub-in-slack)

### DataHub UI

Ask DataHub is coming soon to the DataHub UI, providing seamless access to AI assistance while browsing your data catalog.

<small>_Ask DataHub is currently available in the DataHub UI as part of a private beta. If you're interested in enabling this feature, please reach out to your DataHub representative._</small>

## Customize Ask DataHub

You can customize how Ask DataHub responds to queries by configuring custom instructions. These are injected into AI context to tailor the AI assistant's behavior to match your organization's specific needs, terminology, and guidelines.

### Configuring Custom Instructions

To configure custom instructions:

1. Navigate to **Settings > AI** in your DataHub instance
2. Locate the **Ask DataHub** section
3. Enter your custom instructions in the **Instructions** field

That's it!

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/add_custom_prompts_chat.png"/>
</p>

Custom base prompts can be used to:

- Define response style and tone
- Define organization specific terminology or terms
- Guide the model on how to navigate Glossary Terms, Tags, Domains, and properties.
- Guide the assistant toward specific of recommendations (e.g. helping differentiate production over staging assets)

### Default Organization View

By default, Ask DataHub automatically applies your organization's default view (if one is set). This means the assistant will prioritize asset discovery within the scope of your default view filters.

This ensures that Ask DataHub's responses are always relevant to your team's specific data landscape, without requiring additional filtering in your queries.

:::info Note
After updating custom prompts or organization view settings, it may take up to 5 minutes for changes to take effect.
:::

## How It Works

Ask DataHub leverages your complete metadata graph to provide intelligent, context-aware responses. The AI assistant considers:

- Asset names, descriptions, and documentation
- Lineage relationships (upstream and downstream)
- Ownership and domain information
- Usage patterns and popularity metrics
- Data quality and assertion results
- Tags, glossary terms, and classifications
- Schema information and sample values

## Getting Started

To start using Ask DataHub:

1. **In Slack or Teams**: Simply mention @DataHub followed by your question in any channel or chat
2. **Enable the feature**: As of DataHub Cloud v0.3.13, Ask DataHub is in public beta and can be enabled in Settings → AI
3. **Start asking questions**: Try queries like:
   - "What tables contain customer email data?"
   - "Who owns the sales_facts table?"
   - "What would break if I change the orders table schema?"
   - "Find me the most reliable revenue datasets"
   - "Show me tables with data quality issues"

## Examples

Here are some example queries to help you get started:

- **Discovery**: "Show me all tables related to customer churn"
- **Impact Analysis**: "What dashboards depend on the user_events table?"
- **Ownership**: "Who should I talk to about the marketing schema?"
- **Quality**: "Are there any data quality issues with the revenue table?"
- **Context**: "Tell me about the customer_360 dataset"
- **SQL Generation**: "Write a query to find the top 10 customers by revenue"

Start exploring your data ecosystem with Ask DataHub today!
