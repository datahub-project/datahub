import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Ask DataHub

<FeatureAvailability saasOnly />

**Ask DataHub** is DataHub's conversational AI assistant that brings intelligent, context-aware help directly to where you work. Using Ask DataHub, you can ask questions about your organization's data and get instant, accurate answers grounded in both your **metadata graph** and your **organizational knowledge**—like runbooks, policies, and FAQs stored in Context Graph.

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/ai/ask_datahub_chat.png"/>
</p>

## What Can Ask DataHub Do?

Ask DataHub empowers your organization to navigate and understand your entire data ecosystem with ease—combining technical metadata with your team's tribal knowledge for smarter, more reliable answers.

Key capabilities include:

### Natural Language Search

Search for data assets using plain English instead of memorizing exact table names or technical identifiers. Ask DataHub understands context and synonyms to help you find what you need.

### Find Trustworthy Data

Discover high-quality, reliable data assets based on usage patterns, documentation quality, ownership information, and data quality metrics. Ask DataHub helps you identify the most authoritative sources for your analysis.

### Impact Analysis

Quickly assess how changes to a data asset will ripple through your organization. Ask DataHub can trace lineage and identify all downstream dependencies, helping you make informed decisions before making changes.

### Understand Social and Organizational Context

Get insights into the human side of your data:

- **Ownership**: Find out who owns and maintains specific assets
- **Expertise**: Identify domain experts and frequent users
- **Collaboration**: Understand which teams work with particular datasets
- **History**: Learn about past usage patterns and organizational knowledge

### Answer Questions With Curated Knowledge

Ask DataHub can reference your organization's **Context Documents**, **Glossary Terms**, **Domains**, and more to provide consistent, reliable answers grounded in your team's shared knowledge:

- **Best practices**: Follow documented runbooks and quality standards
- **Runbooks, guides, & FAQs**: Learn the right process for requesting data access
- **Policies**: Understand compliance requirements and data handling guidelines

:::tip Improve Ask DataHub Responses
Document your most-asked questions, policies, and definitions in [Context Documents](context/context-documents.md) to help Ask DataHub provide more accurate, consistent answers across your organization.
:::

### Assess Data Quality

Quickly understand the health and reliability of your data assets:

- View assertion results and data quality scores
- Understand freshness and validation status
- Identify potential issues before using data
- Get context on historical quality trends

### Write SQL Queries

Generate first-draft SQL queries to answer specific analytical questions, accelerating your data exploration and analysis workflows.

## Where You Can Use Ask DataHub

## Getting Started

### DataHub UI

Ask DataHub is currently available **Public Beta** within DataHub in multiple places:

- **Chat** tab in the left-hand navigation bar, where you can start new conversations with DataHub
- **Data Sources** configuration flow, where DataHub can help you troubleshoot ingestion sources
- **Asset Profile Sidebar** where you can start a chat about a specific DataHub asset.
- **Chrome Extension** sidebar under the **Ask Tab**.

### Slack

Ask DataHub is available in Slack by mentioning @DataHub in any channel. This brings DataHub's intelligence directly into your team conversations.

Learn more: [Ask DataHub in Slack](../../managed-datahub/slack/saas-slack-app.md#ask-datahub-in-slack)

### Microsoft Teams

Similarly, you can use Ask DataHub in Microsoft Teams by mentioning @DataHub in channels and chats.

Learn more: [Ask DataHub in Teams](../../managed-datahub/teams/saas-teams-app.md#ask-datahub-in-slack)

## Customize Ask DataHub

As of v0.3.15, you can customize how Ask DataHub responds to queries by configuring custom instructions. These are injected into AI context to tailor the AI assistant's behavior to match your organization's specific needs, terminology, and guidelines.

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
- Guide the model on how to navigate Glossary Terms, Tags, Domains, and properties
- Guide the assistant toward specific recommendations (e.g. helping differentiate production over staging assets)

:::tip Context Documents + Custom Instructions
For organization-specific definitions, policies, and procedures, consider documenting them in [Context Documents](context/context-documents.md) instead of (or in addition to) custom instructions. Context Documents are governable, versionable, and can be linked to specific assets—making them easier to maintain as your organization evolves.
:::

### Default Organization View

By default, Ask DataHub applies your organization's default **Search View** (if one is set). This means the assistant will prioritize finding assets from a narrower set of assets, enabling it to separate the signal from the noise by default.

This ensures that Ask DataHub's responses are always relevant to your team's specific data landscape, without requiring additional filtering in your queries.

:::info Note
After updating custom prompts or organization view settings, it may take up to 5 minutes for changes to take effect.
:::

## How It Works

Ask DataHub leverages your complete metadata graph **and organizational knowledge** to provide intelligent, context-aware responses. The AI assistant considers:

- Asset names, descriptions, and documentation
- Lineage relationships (upstream and downstream)
- Ownership and domain information
- Usage patterns and popularity metrics
- Data quality and assertion results
- Tags, glossary terms, and classifications
- Schema information and sample values
- Context Documents (published runbooks, policies, FAQs, and definitions)

By combining structured metadata with unstructured organizational knowledge, Ask DataHub can answer both technical questions ("What's in this table?") and contextual questions ("How should I use this table?") with consistent, reliable answers grounded in your team's shared understanding.
