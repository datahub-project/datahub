import FeatureAvailability from '@site/src/components/FeatureAvailability';

# AI Glossary Term Suggestions

<FeatureAvailability saasOnly />

The AI Glossary Term Suggestions automation uses LLMs to suggest Glossary Terms for tables and columns in your data.

This is useful for improving coverage of glossary terms across your organization, which is important for compliance and governance efforts.

## Capabilities

- Automatically suggests glossary terms for tables and columns.
- Go beyond a predefined set of terms and works with your business glossary.
- Can generate proposals for owners to review, or can automatically add terms to tables/columns.
- Automatically adjusts to human-provided feedback and curation (coming soon).

## Prerequisites

- A business glossary with terms defined. Additional metadata, like documentation and existing term assignments, will improve the accuracy of our suggestions.

## Limitations

- A single configured automation can classify at most 10k entities.

## Enabling AI Glossary Term Suggestions

1. **Navigate to Automations**: Click on 'Govern' > 'Automations' in the navigation bar.

  <p align="center">
    <img width="20%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automations-nav-link.png"/>
  </p>

2. **Create the Automation**: Click on 'Create' and select 'AI Glossary Term Suggestions'.

  <p align="center">
    <img width="20%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/ai-term-suggestion/automation-type.png"/>
  </p>

3. **Configure the Automation**: Fill in the required fields to configure the automation.
   The main fields to configure are (1) what terms to use for suggestions and (2) what entities to generate suggestions for.

  <p align="center">
    <img width="20%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/ai-term-suggestion/automation-config.png"/>
  </p>

4. Once it's enabled, that's it! You'll start to see terms show up in the UI, either on assets or in the proposals page.

## How it works

The automation will

TODO

- details on what metadata is taken into account
