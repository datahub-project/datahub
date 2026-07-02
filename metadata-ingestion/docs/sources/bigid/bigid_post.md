### Capabilities

- **Business glossary sync** — BigID business glossary items become GlossaryTerms under a `BigID` root GlossaryNode, with domains and ownership optionally attached.
- **Column classification** — classification findings are emitted as GlossaryTerms on schema fields, carrying `MetadataAttribution` that records the classifier, confidence level, and finding counts. Classifiers not linked to a business glossary item are auto-generated under a `BigID > Classifier` node (controlled by `sync_unlinked_classifiers`).
- **IDSoR correlation** — Identity Source of Record findings are resolved via a three-path strategy: reuse a linked business glossary term, auto-generate a term under a `BigID > IDSoR` node, or synthesize one from the raw attribute name.
- **Tags and risk score** — OBJECT-scoped BigID tags become DataHub Tags; risk scores are written to the `bigid.riskScore` structured property.
- **Non-destructive enrichment** — tags, glossary terms, schema-field annotations, and the risk score are applied to existing datasets via PATCH (merge) semantics, so BigID metadata is added alongside — never overwriting — tags and terms that stewards curate in the DataHub UI.
- **No placeholder datasets** — in pure-enrichment mode (`create_datasets: false`) dataset aspects are emitted as non-primary, so BigID never materializes (or later soft-deletes) a dataset that a native connector has not already created. Enable `create_datasets` to have BigID own and create datasets it scans.
- **Profiling** — column-level statistics from BigID `columnProfile` data are emitted as Dataset Profiles.
- **Stateful ingestion** — enables automatic removal of entities emitted by this source when they disappear from BigID.

### Limitations

#### Enrichment adds are not retracted

Because enrichment is applied additively (PATCH), removing a classification, tag, or glossary link **in BigID** does not remove the previously-added term or tag from an existing DataHub dataset on the next run — the PATCH only adds. Stateful ingestion removes entities this connector _owns_ (e.g. glossary terms/nodes it created), but it does not retract annotations merged onto datasets owned by a native connector. Remove such annotations in the DataHub UI if needed.

#### In-memory catalog buffer

The connector buffers the full BigID catalog in memory to collect unique tag pairs and process each object once. Objects are roughly 2 KB each, so a typical deployment (~2,800 objects ≈ 5.6 MB) is trivial. Very large enterprise deployments are the ceiling: 100k–500k catalog objects buffer roughly 200 MB–1 GB at peak. Use `connection_pattern` to scope ingestion if memory is a concern.

#### dataPlatformInstance is only emitted when configured

To avoid overwriting the platform instance already set by a native connector (e.g. Snowflake, BigQuery), the `dataPlatformInstance` aspect is emitted only when a `platform_instance` is explicitly configured for the connection.

### Troubleshooting

#### Datasets are not being enriched

The connector matches BigID objects to existing DataHub dataset URNs. If enrichment does not appear, verify that the resolved platform, `env`, and `platform_instance` produce a URN that matches the one created by your native connector. Use `datasource_platform_mapping` to align them.

#### No enrichment applied at all

If both the business glossary and classification map fail to load, the connector reports a failure and emits nothing. Check BigID API connectivity and that the token has read access to the catalog, classifications, and glossary APIs.

#### Unknown connection type

When a BigID connection `type` has no built-in platform mapping, the raw type is used as the platform in URNs and a warning is reported. Add an entry to `datasource_platform_mapping` to map it to the correct DataHub platform.
