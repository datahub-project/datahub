import FeatureAvailability from '@site/src/components/FeatureAvailability';

# AI Documentation

<FeatureAvailability saasOnly />

:::info

This feature is currently in closed beta. Reach out to your Acryl representative to get access.

:::

With AI-powered documentation, you can automatically generate documentation for tables and columns.

<p align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/_7DieZeZspY?si=Q5FkCA0gZPEFMj0Y" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
</p>

## Configuring

No configuration is required - just hit "Generate" on any table or column in the UI.

## How it works

Generating good documentation requires a holistic understanding of the data. Information we take into account includes, but is not limited to:

- Dataset name and any existing documentation
- Column name, type, description, and sample values
- Lineage relationships to upstream and downstream assets
- Metadata about other related assets

Data privacy: Your metadata is not sent to any third-party LLMs. We use AWS Bedrock internally, which means all metadata remains within the Acryl AWS account. We do not fine-tune on customer data.

## Limitations

- This feature is powered by an LLM, which can produce inaccurate results. While we've taken steps to reduce the likelihood of hallucinations, they can still occur.
