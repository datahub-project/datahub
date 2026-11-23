### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

**Top-level dbt metrics are not available from dbt Cloud.** The Discovery API this source reads does not expose the `metrics:` block - metric definitions live in dbt's separate Semantic Layer GraphQL API, which needs its own endpoint and a token with the "Semantic Layer Only" permission set. With `emit_semantic_model_entities` on, dbt Cloud therefore emits metrics only for measures with `create_metric: true`, which the `semanticModels` query does return. Use the dbt Core source (reading `manifest.json`) if you need the `metrics:` block. The ingestion report states this on every affected run.

**Auto-discovery keeps the metadata of the last run that built a node.** A node whose only builder runs rarely keeps that run's metadata until it next builds, even when other jobs have since seen newer code for it without building it. A node that no discovered job built keeps the copy from the most recently started run and is listed in the ingestion report under the warning "dbt Cloud node not built by any discovered job". Source freshness results come from one job only, the one with the most recent freshness check; dbt test results are kept from every job.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

**Auto-discovery with several jobs.** The ingestion report shows how the jobs' results were combined: `auto_discovery_node_copies_seen`, `auto_discovery_nodes_merged`, `auto_discovery_padding_rows_dropped`, `auto_discovery_nodes_not_built_by_any_job` (with the list of nodes), `auto_discovery_test_results_merged` and `auto_discovery_exposures_deduplicated`. With `only_emit_latest_test_result` (the default), a failing result from a job whose run of a test is older than another job's is reported under "Superseded dbt test failures not emitted" instead of being emitted. If the combined metadata looks wrong, set `auto_discovery.node_merge_strategy: last_job_wins` to get the previous behaviour while you investigate, and check the `job_type_pattern` and `job_id_pattern` filters: the report lists every skipped job with its reason.
