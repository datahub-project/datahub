# Sinks

Sinks are **destinations for metadata**.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/sources-sinks.png"/>
</p>

In general, the sink will be defined in the [recipe](./recipe_overview.md) after the [source](./source-docs-template.md) like below.

```yaml
source: ...

sink:
  type: <sink_type>
  config: ...
```

## Types of Sinks

When configuring ingestion for DataHub, you're likely to be sending the metadata to DataHub over either one of the following.

- [REST (datahub-rest)](sink_docs/datahub.md#datahub-rest)
- [Kafka (datahub-kafka)](sink_docs/datahub.md#datahub-kafka)

For debugging purposes or troubleshooting, the following sinks can be useful:

- [Metadata File](sink_docs/metadata-file.md)
- [Console](sink_docs/console.md)

## Default Sink

Since `acryl-datahub` version `>=0.8.33.2`, the default sink is assumed to be a `datahub-rest` endpoint.
