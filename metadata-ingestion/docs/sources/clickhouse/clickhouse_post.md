### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Query Log Extraction

Enable query-log based metadata extraction to augment definition-based lineage:

- `include_query_log_lineage`: derive lineage from INSERT/CREATE queries in `system.query_log`
- `include_usage_statistics`: derive usage statistics from SELECT query activity

This complements view/materialized-view lineage and improves operational usage visibility.

#### Connection Tagging

DataHub tags its connections with a `datahub` client identity so its own reads are attributable in `system.query_log`. On HTTP drivers this is the `User-Agent` header (`http_user_agent = 'datahub'`); on the native/asynch drivers it is `client_name`, which the driver records as `ClickHouse datahub`. To override it, set your own value via `uri_opts` (`header__User-Agent` for HTTP, `client_name` for native).

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
