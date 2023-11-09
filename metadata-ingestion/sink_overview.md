# Sinks

Sinks are **destinations for metadata**.
In general, the sink will be defined in the recipe after the _source_ like below.

```yaml
source: ...

sink:
  type: <sink_type>
  config: ...
```

When configuring ingestion for DataHub, you're likely to be sending the metadata to DataHub over either one of the following.

- [REST (datahub-rest)](sink_docs/datahub.md#datahub-rest)
- [Kafka (datahub-kafka)](sink_docs/datahub.md#datahub-kafka)
- [File](sink_docs/file.md)
- Since `acryl-datahub` version `>=0.8.33.2`, the default sink is assumed to be a `datahub-rest` endpoint.

- Hosted at "http://localhost:8080" or the environment variable `${DATAHUB_GMS_URL}` if present
- With an empty auth token or the environment variable `${DATAHUB_GMS_TOKEN}` if present.

If you want to override the default endpoints, you can provide the environment variables as part of the command like below:

```shell
DATAHUB_GMS_URL="https://my-datahub-server:8080" DATAHUB_GMS_TOKEN="my-datahub-token" datahub ingest -c recipe.dhub.yaml
```
