import FeatureAvailability from '@site/src/components/FeatureAvailability';

# AI Glossary Term Suggestions

<FeatureAvailability saasOnly />

:::info

This feature is currently in closed beta. Reach out to your Acryl representative to get access.

:::

The AI Glossary Term Suggestion automation uses LLMs to suggest [Glossary Terms](../glossary/business-glossary.md) for tables and columns in your data.

This is useful for improving coverage of glossary terms across your organization, which is important for compliance and governance efforts.

This automation can:

- Automatically suggests glossary terms for tables and columns.
- Goes beyond a predefined set of terms and works with your business glossary.
- Generates [proposals](../managed-datahub/approval-workflows.md) for owners to review, or can automatically add terms to tables/columns.
- Automatically adjusts to human-provided feedback and curation (coming soon).

## Prerequisites

- A business glossary with terms defined. Additional metadata, like documentation and existing term assignments, will improve the accuracy of our suggestions.

## Configuring

1. **Navigate to Automations**: Click on 'Govern' > 'Automations' in the navigation bar.

  <p align="center">
    <img width="30%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automations-nav-link.png"/>
  </p>

2. **Create the Automation**: Click on 'Create' and select 'AI Glossary Term Suggestions'.

  <p align="center">
    <img width="40%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/ai-term-suggestion/automation-type.png"/>
  </p>

3. **Configure the Automation**: Fill in the required fields to configure the automation.
   The main fields to configure are (1) what terms to use for suggestions and (2) what entities to generate suggestions for.

  <p align="center">
    <img width="50%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/ai-term-suggestion/automation-config.png"/>
  </p>

4. Once it's enabled, that's it! You'll start to see terms show up in the UI, either on assets or in the proposals page.

  <p align="center">
    <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/ai-term-suggestion/term-proposals.png"/>
  </p>

## How it works

The automation will scan through all the datasets matched by the configured filters. For each one, it will generate suggestions.
If new entities are added that match the configured filters, those will also be classified within 24 hours.

We take into account the following metadata when generating suggestions:

- Dataset name and description
- Column name, type, description, and sample values
- Glossary term name, documentation, and hierarchy
- Feedback loop: existing assignments and accepted/rejected proposals (coming soon)

Data privacy: Your metadata is not sent to any third-party LLMs. We use AWS Bedrock internally, which means all metadata remains within the Acryl AWS account. We do not fine-tune on customer data.

## Limitations

- A single configured automation can classify at most 10k entities.
- We cannot do partial reclassification. If you add a new column to an existing table, we won't regenerate suggestions for that table.
