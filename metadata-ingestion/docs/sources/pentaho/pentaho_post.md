### Capabilities

Each transformation and job becomes a DataJob carrying:

- `DataJobInfo` with the name from the file, falling back to the filename, a type of `TRANSFORMATION` or `JOB`, and the embedded description.
- Custom properties for the source file path, counts of inputs, outputs, and steps processed, and for jobs, each referenced transformation or job.
- An owner from `default_owner`, and the `pentaho` tag.
- `DataJobInputOutput` lineage when at least one `TableInput` or `TableOutput` step resolves to a dataset.

### Limitations

- Only `TableInput` and `TableOutput` steps contribute lineage. Transformations built from other step types yield a DataJob with no lineage.
- Lineage is table-level only. Column-level lineage is not emitted.
- Pentaho variables are not resolved. A table name or SQL statement containing `${...}` passes verbatim into the dataset URN, so the edge will not match the real dataset.
- Jobs record the transformations and jobs they invoke as custom properties, not as lineage edges.
- Setting `platform_mappings` replaces the built-in mapping instead of extending it. Unmatched connection types resolve to the `unknown` platform.
- Files larger than `file_size_limit_mb` are skipped, as are files that are neither `.ktr` nor `.kjb`.

### Troubleshooting

- No lineage on a transformation: confirm it uses `TableInput` or `TableOutput` steps, and that table names contain no unresolved `${...}` variables.
- Datasets under the `unknown` platform: the connection type is missing from `platform_mappings`. Add an entry for it.
- A file is missing entirely: check the logs. Parse failures and oversized files are reported as warnings and failures, not fatal errors.
