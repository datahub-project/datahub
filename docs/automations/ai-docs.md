import FeatureAvailability from '@site/src/components/FeatureAvailability';

# AI Documentation

<FeatureAvailability saasOnly />

With AI-powered documentation, you can automatically generate documentation for tables and columns.

<p align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/_7DieZeZspY?si=Q5FkCA0gZPEFMj0Y" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
</p>

## Prerequisites

As of DataHub Cloud v0.3.12, AI documentation is in public beta. Admins (or users with the "Manage Platform Settings" privilege) can enable it from settings.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/ai-docs/ai-docs-toggle.png"/>
</p>

## Usage

Ensure you have permissions to edit the dataset description. No other configuration is required - just hit "Generate" on any table or column in the UI.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/ai-docs/ai-docs-generation.gif"/>
</p>

## How it works

Generating good documentation requires a holistic understanding of the data. Information we take into account includes, but is not limited to:

- Dataset name and any existing documentation
- Column name, type, description, and sample values
- Lineage relationships to upstream and downstream assets
- Metadata about other related assets

Data privacy: Your metadata is not sent to any third-party LLMs. We use AWS Bedrock internally, which means all metadata remains within the DataHub Cloud AWS account. We do not fine-tune on customer data.

## Limitations

- AI documentation is not available for tables with more than 1000 columns (prior to v0.3.12, this was 100 columns).
- This feature is powered by LLMs, which can produce inaccurate results. While we've taken steps to reduce the likelihood of hallucinations, they may still occur.
