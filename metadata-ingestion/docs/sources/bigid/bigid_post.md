### Capabilities

- **Business glossary sync** — BigID business glossary items become GlossaryTerms under a `BigID` root GlossaryNode, with domains and ownership optionally attached.
- **Column classification** — classification findings are emitted as GlossaryTerms on schema fields, carrying `MetadataAttribution` that records the classifier, confidence level, and finding counts.
- **IDSoR correlation** — Identity Source of Record findings are resolved via a three-path strategy: reuse a linked business glossary term, auto-generate a term under a `BigID > IDSoR` node, or synthesize one from the raw attribute name.
- **Tags and risk score** — OBJECT-scoped BigID tags become DataHub Tags; risk scores are written to the `bigid.riskScore` structured property using patch semantics so the value is not overwritten by other aspects.
- **Profiling** — column-level statistics from BigID `columnProfile` data are emitted as Dataset Profiles.
- **Stateful ingestion** — enables automatic removal of entities emitted by this source when they disappear from BigID.

### Limitations

#### Enrichment aspects use UPSERT semantics

GlobalTags, EditableSchemaMetadata, and GlossaryTerms are emitted with UPSERT change type, which replaces the full aspect on each run. Tags or terms added manually in the DataHub UI on these aspects will be overwritten on the next ingestion. Run this connector against datasets whose tags/terms are managed by BigID, and treat DataHub as the mirror rather than the system of record for those aspects. (Risk score is the exception — it is applied via a patch and preserves other structured properties.)

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
