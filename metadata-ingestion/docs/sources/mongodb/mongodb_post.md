### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Emitting as the DocumentDB platform

By default the connector emits all ingested entities under the `mongodb` data platform, regardless of whether the underlying source is MongoDB or AWS DocumentDB. To surface DocumentDB clusters as their own platform, set `platform: documentdb`. This requires `hostingEnvironment` to be `AWS_DOCUMENTDB`; any other hosting environment is rejected at config validation time.

Switching an existing recipe to `platform: documentdb` will generate new `documentdb` dataset and container URNs; the previously emitted `mongodb` URNs will need to be cleaned up via stateful ingestion (if enabled) or manually soft-deleted.

#### Probe support

`datahub recipe probe` checks a recipe against the server without running ingestion. For MongoDB it
offers `databases` and `collections` (one database's collections and views):

```shell
datahub recipe probe methods --recipe mongodb_recipe.yml
datahub recipe probe run databases --recipe mongodb_recipe.yml
datahub recipe probe run collections --recipe mongodb_recipe.yml --database my_db
```

The probe connects with the client ingestion builds (`connect_uri`, credentials, `authMechanism` and
`options`, TLS included) and lists names only: it never reads documents or samples. It lists what
ingestion skips too, so `probe filter` can explain it: the system databases `admin`, `config` and
`local` are always excluded, `system.*` collections are excluded while `excludeSystemCollections` is
set, and `collection_pattern` is matched against `database.collection`, so pass the database as
`--parent` when judging collection names.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
