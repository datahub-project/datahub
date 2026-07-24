### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Feature naming

`MLFeature` names mirror Chronon's own backfill output columns, following the `{input_column}_{operation}_{window}` convention (for example `purchase_amount_sum_3d`). Bucketed aggregations append `_by_{bucket}`, and derivations are applied on top of aggregation outputs. Feature data types are inferred from the aggregation operation; derived columns fall back to `UNKNOWN`.

#### Tags and ownership

Tag extraction (`enable_tag_extraction`) reads tag bags stored in each object's `MetaData.customJson` (`groupby_tags`, `join_tags`, and per-column `column_tags`). Owner extraction (`enable_owner_extraction`) requires `owner_mappings` to map each Chronon team to a DataHub owner URN.

### Limitations

- The connector reads the **compiled** repository, not the Python config source. Run ingestion after `compile.py` so metadata reflects the latest changes.
- `JoinSource` GroupBys (whose source is another Join's output) are reported and skipped, because resolving them requires the parent join's compiled output.
- Primary key data types are not carried in the compiled config and are emitted as `UNKNOWN`.

### Troubleshooting

If ingestion fails, first confirm that `path` points at a compiled output directory containing `group_bys/`. Review the ingestion report for unmapped source namespaces (extend `source_platform_map`), unparseable files, and StagingQuery SQL parse failures.
