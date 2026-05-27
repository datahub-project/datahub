### Capabilities

:::note
Stateful Ingestion (deletion detection) requires a `platform_instance` to be set on this source.
:::

Use the **Important Capabilities** table above the page as the source of truth for which capabilities are supported and which require additional configuration.

| Capability | Notes |
| --- | --- |
| `DESCRIPTIONS` | Enabled by default. Stream and delivery-stream metadata is surfaced via `datasetProperties` / `dataJobInfo`. |
| `CONTAINERS` | One regional Container per recipe — parent of all streams and Firehose delivery streams. |
| `LINEAGE_COARSE` | Firehose delivery streams emit `dataJobInputOutput` edges from the source Kinesis stream to the destination platform (custom-upstream, coarse-grained lineage). |
| `TAGS` | AWS resource tags on streams and delivery streams are emitted as `globalTags`. Gated by `extract_tags`. |
| `OWNERSHIP` | The value of a configurable AWS resource-tag key (default `owner_tag_key: owner`) becomes a DataHub owner. Gated by `extract_owners`. |
| `SCHEMA_METADATA` | Opt-in. With `glue_schema_registry.enabled: true`, Avro / JSON / Protobuf schemas are fetched from AWS Glue Schema Registry. Requires the `GlueSchemaRegistryRead` IAM statement in the prerequisites. |
| `DELETION_DETECTION` | Via stateful ingestion. Entities removed from AWS since the last successful run are soft-deleted in DataHub. Requires `platform_instance`. |

#### Firehose lineage — supported destinations

The following destination types produce a `dataJobInputOutput.outputDatasets` edge:

| AWS destination | DataHub platform | URN format |
| --- | --- | --- |
| Amazon S3 / Extended S3 | `s3` | `urn:li:dataset:(urn:li:dataPlatform:s3,<bucket>[/<prefix>],...)` |
| Amazon Redshift | `redshift` | `urn:li:dataset:(urn:li:dataPlatform:redshift,<db>.<schema>.<table>,...)` |
| Amazon OpenSearch / Elasticsearch | `elasticsearch` | `urn:li:dataset:(urn:li:dataPlatform:elasticsearch,<index>,...)` |
| Snowflake | `snowflake` | `urn:li:dataset:(urn:li:dataPlatform:snowflake,<db>.<schema>.<table>,...)` |
| Apache Iceberg | `iceberg` | `urn:li:dataset:(urn:li:dataPlatform:iceberg,<namespace>.<table>,...)` |
| MongoDB | `mongodb` | `urn:li:dataset:(urn:li:dataPlatform:mongodb,<database>.<collection>,...)` |

URN names are lowercased by default (matching the Snowflake source's `convert_urns_to_lowercase=True` default). Override per destination via `destination_platform_map.<platform>.convert_urns_to_lowercase: false` — see [Cross-platform lineage](#cross-platform-lineage-with-destination_platform_map) below.

Unsupported Firehose destinations (HTTP, Datadog, Splunk, New Relic, Coralogix, LogicMonitor, Dynatrace, Honeycomb, Sumo Logic, etc.) do **not** produce a lineage edge — the connector logs a warning (_"Unsupported Firehose destination"_) and records the destination configuration as a custom property on the DataJob. The DataJob itself is still emitted.

#### Cross-platform lineage with `destination_platform_map`

Firehose destinations live on other platforms (S3, Redshift, Snowflake, etc.), so Kinesis-emitted lineage URNs must match the URN convention of those platforms' own DataHub sources. The `destination_platform_map` lets you override per-destination URN parameters:

```yaml
destination_platform_map:
  snowflake:
    platform_instance: "prod-snowflake-east"
    env: "PROD"
    # Set to false if your Snowflake source recipe also has
    # `convert_urns_to_lowercase: false` (preserves UPPER_CASE identifiers):
    convert_urns_to_lowercase: false
  redshift:
    platform_instance: "analytics-cluster"
    env: "PROD"
  iceberg:
    # Iceberg catalogs are case-sensitive — disable lowercasing
    # to preserve the table's original case:
    convert_urns_to_lowercase: false
```

Three knobs are available per destination platform:

- **`platform_instance`** — the same string the destination platform's own source recipe used. Without this, Firehose lineage edges resolve to dead URNs in the DataHub UI.
- **`env`** — `PROD` / `DEV` / etc. Inherits from this source's `env` when unset.
- **`convert_urns_to_lowercase`** — default `true`. Set to `false` for case-sensitive destinations (Iceberg, MongoDB) or for Snowflake / Redshift sources ingested with their own `convert_urns_to_lowercase=false`.

#### Glue table lineage (Firehose format conversion)

When a Firehose delivery stream has **Parquet/ORC format conversion** enabled, its `SchemaConfiguration` references a Glue table that defines the output schema. The connector surfaces this as an additional upstream input on the Firehose DataJob:

- The source Kinesis stream remains an input (existing behavior).
- The destination S3 path remains an output (existing behavior).
- The Glue table is added as a second input — its schema governs what gets written to the S3 path.

For the Glue table URN to be emitted, `SchemaConfiguration` must include `DatabaseName` and `TableName`. `CatalogId` is **not** required — AWS omits it from `DescribeDeliveryStream` responses when it equals the caller's account (per AWS docs, it's an input-side default). SchemaConfigurations present but missing `DatabaseName` / `TableName` are recorded in the source report's `firehose_glue_schema_skipped` field for diagnosis.

If your Glue catalog was ingested under a non-default `platform_instance`, set the override:

```yaml
destination_platform_map:
  glue:
    platform_instance: "central-catalog"
    env: "PROD"
```

This whole behavior is gated by the `include_table_lineage` flag — when that's off, no Glue lineage is emitted.

#### Filtering streams and delivery streams

`stream_pattern` and `delivery_stream_pattern` use the standard DataHub `AllowDenyPattern` shape. A common deny rule excludes internal / audit / debug streams:

```yaml
stream_pattern:
  deny:
    - "^_.*"
    - ".*-debug$"
```

#### Tags-as-ownership

The connector reads owner information from a single AWS resource-tag key (default `owner`). Set `owner_tag_key` to whatever convention your organization uses (e.g. `team`, `data_owner`). The tag value is used verbatim as the owner identifier and emitted as a `urn:li:corpuser:<value>` owner — DataHub's identity layer handles group-membership mapping at the platform level if the value names a group rather than an individual user.

#### Glue Schema Registry

GSR is **opt-in** because it requires extra IAM permissions (`glue:Get*` / `glue:List*`). To enable it:

```yaml
glue_schema_registry:
  enabled: true
  registry_name: "default-registry"
  # Recommended: declare known stream -> schema associations explicitly.
  stream_schema_map:
    events: "events-v2"
    clicks: "click-events-schema"
  # Optional heuristic — see explanation below:
  use_naming_convention: false
```

Schema resolution order for each stream:

1. If the stream name is a key in `stream_schema_map`, use the mapped schema name.
2. Otherwise, if `use_naming_convention: true`, look up a schema whose name equals the stream name in the configured `registry_name`.
3. Otherwise, emit the stream without `schemaMetadata`.

#### Why is `use_naming_convention` off by default?

Unlike Kafka + Confluent Schema Registry — which defines a standardized `TopicNameStrategy` (`<topic>-key/-value` subject naming) — **AWS does not define any relationship between a Kinesis Data Stream and a Glue schema**. Schemas are chosen per-record by producers (the `GlueSchemaRegistrySerializer` embeds the schema-id in each record), and the stream itself has no schema binding. Multiple producers can write to one stream using different schemas, and one schema can be reused across multiple streams.

Some organizations adopt "schema name == stream name" as an internal convention, but it's not AWS best practice. If your organization follows that convention, set `use_naming_convention: true`. Otherwise, declare your known associations in `stream_schema_map` — that's the most predictable mode.

(For **Firehose** delivery streams with Parquet/ORC format conversion, AWS _does_ define a relationship via `SchemaConfiguration` — see [Glue table lineage](#glue-table-lineage-firehose-format-conversion) above. That extraction is on by default and unaffected by this flag.)

### Limitations

1. **`destination_platform_map` is required for any destination platform ingested with a non-default `platform_instance`.** Without it, Firehose lineage edges reference URNs that are syntactically valid but resolve to nothing in DataHub — the lineage looks correct in the emitted JSON but the target is a dead link in the UI. Always populate `destination_platform_map` for destinations whose own source recipe set a `platform_instance`. Example:

   ```yaml
   destination_platform_map:
     snowflake:
       platform_instance: "prod-snowflake-east"
     redshift:
       platform_instance: "analytics-cluster"
   ```

2. **Glue Schema Registry cross-schema references aren't resolved.** Schemas with `$ref` (JSON Schema) or named imports (Avro / Protobuf) emit the top-level schema only — nested references aren't followed. Streams whose schemas depend on cross-schema imports will be missing some fields.

3. **One region per recipe.** The connector ingests a single AWS region per run. For multi-region accounts, run one recipe per region with a distinct `platform_instance`.

4. **One Glue Schema Registry per recipe.** Only the registry named in `glue_schema_registry.registry_name` is queried. If your schemas span multiple registries, run multiple recipes.

5. **No `USAGE_STATS` capability.** Read/write throughput per stream is available in CloudWatch but isn't read by this connector.

6. **Unsupported Firehose destinations.** Splunk, HTTP, Datadog, New Relic, Coralogix, LogicMonitor, Dynatrace, Honeycomb, and Sumo Logic destinations emit a DataJob without an output lineage edge and surface a warning in the report. The six supported destinations are listed in the [Firehose lineage table](#firehose-lineage--supported-destinations) above.

7. **No schema inference from record sampling.** The connector relies on AWS Glue Schema Registry for schemas — streams without a registered schema are emitted without `schemaMetadata`.

8. **No Lambda consumer discovery.** The connector doesn't enumerate Lambda functions consuming a stream, so `Stream → Lambda` lineage is not emitted.

9. **No Kinesis Data Analytics (KDA / managed Flink) support.** KDA applications are not ingested by this connector.

### Troubleshooting

**Lineage edges show but the target dataset isn't found in DataHub.**
Your `destination_platform_map` doesn't match the destination platform's own ingestion settings. Check that `platform_instance` and `env` in `destination_platform_map.<platform>` exactly match the values that platform's source recipe used. See Limitation #1.

**Snowflake or Redshift lineage doesn't resolve, and the destination identifiers are mixed-case.**
The connector lowercases destination URN names by default. If your Snowflake / Redshift source recipe set `convert_urns_to_lowercase: false`, mirror that on the Kinesis side:

```yaml
destination_platform_map:
  snowflake:
    convert_urns_to_lowercase: false
```

**Iceberg or MongoDB lineage doesn't resolve.**
Both platforms have case-sensitive identifiers. Disable lowercasing for them as above (`convert_urns_to_lowercase: false`).

**Ingestion succeeded but search doesn't show the entities.**
Your DataHub backend may have entered a read-only state due to disk pressure on the search index. From the host running DataHub:

```shell
docker exec <opensearch-or-elasticsearch-container> \
  curl -s localhost:9200/_cluster/allocation/explain
```

If the cluster reports `disk.watermark.low` exceeded, free disk space and re-index.

**`AccessDeniedException` on `kinesis:ListStreams`.**
The IAM identity used by the recipe is missing the `KinesisDataStreamsRead` permissions in the [AWS IAM Permissions](#aws-iam-permissions) section. A first-page denial is logged as a warning and skips the KDS section (the user may intentionally have Firehose-only IAM); a mid-pagination failure escalates to `report.failure` to prevent stateful soft-deletion of un-listed streams.

**Firehose section is silently empty even though delivery streams exist.**
The IAM identity is missing `firehose:ListDeliveryStreams` and/or `firehose:DescribeDeliveryStream` — Kinesis Data Streams permissions don't cover Firehose. A missing Firehose permission is logged as a warning (_"Permission denied for Firehose"_) rather than a failure; check the ingestion run report.

**Schema not found for a stream.**
`glue:GetSchemaVersion` returned `EntityNotFoundException` for the expected schema name. Either add an explicit entry in `glue_schema_registry.stream_schema_map`, rename the GSR schema to match the stream name (and enable `use_naming_convention: true`), or accept that the stream will be emitted without `schemaMetadata`.
