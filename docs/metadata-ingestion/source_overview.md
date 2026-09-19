
# Sources

Sources are **the data systems that we are extracting metadata from.**

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/sources-sinks.png"/>
</p>

In general, the source will be defined at the top of the [recipe](./recipe_overview.md) like below.

```yaml
#my_recipe.yml
source:
  type: <source_name>
  config:
    option_1: <value>
    ...
```

## Types of Source

The `Sources` tab on the left in the sidebar shows you all the sources that are available for you to ingest metadata from. For example, we have sources for [BigQuery](/docs/generated/ingestion/sources/bigquery), [Looker](/docs/generated/ingestion/sources/looker), [Tableau](/docs/generated/ingestion/sources/tableau) and many others.

:::tip Find an Integration Source
See the full **[list of integrations](/integrations)** and filter on their features.
:::

## Metadata Ingestion Source Status

We apply a Support Status to each Metadata Source to help you understand the integration reliability at a glance.

![GA](https://img.shields.io/badge/support%20status-GA-brightgreen): GA (Generally Available) Sources are maintained by the DataHub Ingestion team and are widely adopted in production. We expect the integration to be stable with few user-facing issues.

![Beta](https://img.shields.io/badge/support%20status-Beta-blue): Beta Sources are maintained by the DataHub Ingestion team but have limited production adoption so far. They are ready to use, but have not been exercised against a wide variety of edge-cases; we eagerly solicit feedback to strengthen them.

![Alpha](https://img.shields.io/badge/support%20status-Alpha-lightgrey): Alpha Sources are early-stage integrations with limited production adoption, and are typically maintained by the community, the field team, or a team outside of Ingestion. They are available for experimentation and may change without notice.
