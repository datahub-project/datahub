### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Probe support

`datahub recipe probe` checks a recipe against the cluster without running ingestion. For
Elasticsearch and OpenSearch it offers `indices` and `index_templates`:

```shell
datahub recipe probe methods --recipe elasticsearch_recipe.yml
datahub recipe probe run indices --recipe elasticsearch_recipe.yml --report-to indices.json
datahub recipe probe filter --recipe elasticsearch_recipe.yml --from-run indices.json
```

The probe connects with the client ingestion builds (credentials, API key, TLS and `url_prefix`)
and reads index metadata only, never documents. `indices` lists what `GET _alias` returns, as
ingestion does. Where the server returns a data stream's backing indices there, they are listed
under their own names, which is what `index_pattern` is matched against, with `data_stream` naming
the dataset ingestion emits them as. Elasticsearch 8 leaves hidden indices, backing indices among
them, out of that listing, so neither ingestion nor the probe sees its data streams.
Each record carries `mapped_fields`, the number of schema fields ingestion would emit: an index or
template with none is not emitted, and `probe filter --from-run` reports it as `no_mapped_fields`.
Bare names given to `probe filter` are judged on the pattern alone, with a warning.
`index_templates` lists legacy and composable templates; they are judged excluded unless
`ingest_index_templates` is set.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
