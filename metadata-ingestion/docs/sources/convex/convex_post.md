### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Profiling Details

When `include_row_counts` is enabled, each table is counted by paging through a snapshot of it. `max_count_pages` caps how many pages are read per table; if a table is larger than the cap, the row count is reported as a lower bound (`1234+` in the dataset's custom properties) and the table is listed under `row_counts_capped` in the ingestion report.

### Limitations

- Convex does not expose relationships between tables, so no lineage is emitted. Document references are visible as `Id(<table>)` field descriptions.
- Schemas come from the JSON Schema that Convex derives from stored documents. A table with no documents yields no fields.
- Only the tables of the deployment's default component are ingested.
- Stateful ingestion is not supported, so tables deleted in Convex are not soft-deleted in DataHub.

### Troubleshooting

If ingestion fails at the deployment level, confirm that the deploy key belongs to the deployment named in the same entry and that the URL is the `.convex.cloud` deployment URL rather than the dashboard URL. Row-count failures are reported per table and do not stop the run; set `include_row_counts: false` to skip counting entirely on large deployments.
