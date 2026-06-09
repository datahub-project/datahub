### Capabilities

#### Cross-platform lineage

QuickSight Datasets are stitched to their upstream warehouse/database tables so lineage spans from a dashboard down to the source table. The connector resolves the upstream platform from the data source's connection parameters (Athena, Redshift, Snowflake, S3, RDS variants, and more).

For the upstream Dataset URNs to line up with the platform's own ingested tables, the env, `platform_instance`, and URN casing must match the upstream connector's recipe. Configure these per data source via `external_data_sources`, keyed by the QuickSight `DataSourceId` (UUID, preferred — it survives renames) with the display name accepted as a fallback.

##### Column-level lineage

`CustomSql` datasets carry a SQL definition that is parsed with sqlglot to derive column-level lineage. Unqualified table references are resolved using the `default_database` / `default_schema` configured for that data source in `external_data_sources`. Column-level lineage requires both `extract_lineage` and `extract_column_lineage` to be enabled.

#### Ownership, tags, users and groups

With `extract_ownership` enabled, owners are derived from each asset's QuickSight resource permissions. IAM/SSO-federated principals are normalized to the role-session name (typically the user's email) so the resulting `CorpUser` URN matches DataHub's `urn:li:corpuser:<email>` convention; set `strip_user_email_domain` to use the bare username instead. `extract_tags` maps AWS resource tags to DataHub tags (filterable via `tag_pattern`). `extract_users_and_groups` (opt-in) emits `CorpUser` / `CorpGroup` entities and their memberships.

#### Stateful ingestion

Enable `stateful_ingestion.enabled` to automatically soft-delete entities that disappear from QuickSight between runs (stale entity removal).

### Limitations

#### Regional scope

QuickSight is a regional service, so a single ingestion run only sees assets in one `aws_region`. Run one recipe per region for multi-region deployments.

#### Folder hierarchy requires Enterprise edition

Folders are a QuickSight Enterprise-edition feature. On Standard-edition accounts no folders exist, so assets are emitted directly under the platform / `platform_instance`.

#### Definition payloads

Chart (visual) entities are only emitted when `extract_dashboard_definitions` is enabled. Definition payloads are large; disable `extract_dashboard_definitions` / `extract_analysis_definitions` to reduce API cost at the expense of visual-level detail.

### Troubleshooting

#### AccessDeniedException despite a correct IAM policy

QuickSight enforces three independent permission layers (AWS IAM policy, the QuickSight user role, and per-resource share permissions — see Prerequisites). Its `AccessDeniedException` messages **always** point at IAM ("no identity-based policy allows...") even when the real cause is the user's role being `READER` instead of `AUTHOR`, or the asset simply not being shared with the service user. When you hit this error, verify all three layers, not just the IAM policy.

#### Throttling / TPS errors

QuickSight applies per-API transactions-per-second limits. The connector uses adaptive retry mode, but very large accounts may still see throttling — re-run the ingestion, optionally narrowing scope with the `*_pattern` filters.

#### Unresolved upstream lineage

If dashboard-to-table lineage is missing, the upstream Dataset URN produced by QuickSight likely does not match the URN the upstream connector emits. Confirm the env, `platform_instance`, and `convert_urns_to_lowercase` in `external_data_sources` match the upstream recipe exactly.
