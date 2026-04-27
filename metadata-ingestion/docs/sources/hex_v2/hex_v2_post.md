### Capabilities

#### Lineage

`hex-v2` extracts upstream lineage by exporting each project's YAML and parsing the SQL source from every SQL cell that references an external data connection. No warehouse ingestion or `use_queries_v2` setup is required.

The connector maps each Hex data connection to a DataHub platform using the connection's type (e.g., `snowflake`, `bigquery`, `redshift`). SQL is parsed with [sqlglot](https://github.com/tobymao/sqlglot) to extract the upstream table references, which are then resolved to DataHub dataset URNs.

SQL cells that operate on in-memory dataframes from other cells (i.e., cells with no `dataConnectionId`) are automatically skipped — they do not represent external data reads.

If a connection type is not in the built-in mapping, the connector falls back to the `sql_parsing_platform_default` dialect (default: `snowflake`). Use `connection_platform_map` to override specific connection IDs.

#### Run History

When `include_run_history` is enabled (default), the connector fetches the most recent run for each project and emits it as a DataHub `Operation` aspect. This records the last scheduled execution time and status.

#### Context Documents

When `include_context_documents` is enabled (default), the connector emits one DataHub `Document` entity per project. Each document contains:

- Project metadata (workspace, owner, status, categories, collections)
- All notebook section names
- All SQL cell sources with their connection and label
- All EXPLORE visualisation references
- All MARKDOWN cell documentation from the notebook

Documents are hidden from global search (`show_in_global_context=False`) and linked to the project Dashboard as a related asset. They are designed to be retrieved by AI agents that have the project in scope, not surfaced in catalog browse.

### Limitations

#### Hex CLI Dependency

The connector pins to Hex CLI **v1.2.2** and downloads it automatically to `~/.datahub/tools/hex/` if not found on `PATH`. Set `auto_install_hex_cli: false` and provide `hex_cli_path` to manage the binary yourself. Platforms supported for auto-download: macOS (arm64, x86\_64) and Linux (arm64, x86\_64). Windows is not supported.

#### Page Size

The Hex CLI API rejects project list requests with more than 30 items per page. The connector handles this automatically via pagination, but large workspaces will require more API calls than a single-page request.

#### Column-Level Lineage

Only table-level lineage is extracted. Column-level lineage is not supported.

#### SQL Parsing Coverage

SQL parsing is best-effort. Complex queries involving dynamic SQL, unsupported dialect constructs, or deeply nested CTEs may partially parse or produce no upstream tables for that cell. The connector warns per cell and continues — a parse failure on one cell does not block lineage for the rest of the project.

#### Unknown Connection Types

If a Hex data connection uses a platform type not in the built-in mapping (e.g., a custom or newly supported database), the connector falls back to `sql_parsing_platform_default` and logs a warning. Use `connection_platform_map` to explicitly map connection IDs to DataHub platform names.

#### Projects-to-Components Relationship

The Hex API does not expose the many-to-many relationship between Projects and Components. Components are ingested independently.

#### Chart Entities

EXPLORE (visualisation) cells are not emitted as DataHub Chart entities. Their metadata is captured in the project's context document. Chart entity extraction is a planned optional feature.

### Troubleshooting

**`hex` binary not found / auto-download fails**

By default the connector auto-downloads Hex CLI v1.2.2 to `~/.datahub/tools/hex/`. If your environment blocks outbound GitHub downloads, set `auto_install_hex_cli: false` and install the binary manually from the [Hex CLI releases page](https://github.com/hex-inc/hex-cli/releases/tag/v1.2.2). Then set `hex_cli_path` to the binary location.

**Export returns no file / silent failure**

Do not pass `--version draft` to the export command — the `--version` flag conflicts with the CLI's global version flag. The connector handles this correctly internally.

**`connection list` returns Forbidden**

Collections and connection details require standard user access. If the token lacks sufficient permissions, the connector falls back to tag-less collection handling and the `sql_parsing_platform_default` dialect. Upgrade to a Workspace Token with read access.

**Missing lineage for some projects**

Projects whose SQL cells only transform in-memory dataframes (no external connection) produce no lineage. This is expected. Check the `projects_without_sql_cells` metric in the ingestion report.

**Unknown connection IDs in report**

Some connection IDs appear in project YAML cells but are not returned by `connection list`. This happens when a connection was deleted or is owned by a different workspace. Use `connection_platform_map` to manually resolve them.
