### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Example Queries File

```json
{"query": "SELECT x FROM my_table", "timestamp": 1689232738.051, "user": "user_a", "downstream_tables": [], "upstream_tables": ["my_database.my_schema.my_table"]}
{"query": "INSERT INTO my_table VALUES (1, 'a')", "timestamp": 1689232737.669, "user": "user_b", "downstream_tables": ["my_database.my_schema.my_table"], "upstream_tables": []}
```

**Note:** This uses newline-delimited JSON format (NDJSON), where each line is a separate JSON object.

#### Query File Format

The source expects newline-delimited JSON (NDJSON), with one query object per line. Common fields:

- `query` (required): SQL text to parse
- `timestamp` (optional): query execution timestamp
- `user` (optional): actor used for usage attribution
- `operation_type` (optional): fallback operation classification
- `session_id` (optional): session key used to resolve temporary tables across related queries
- `downstream_tables` / `upstream_tables` (optional): fallback lineage hints if parsing fails

#### Incremental Lineage

When stateful ingestion is enabled, lineage extraction can progress incrementally over new query records while preserving historical checkpoints.

#### Temporary Table Support

`session_id` enables correlation of temporary-table creation and downstream usage across query sequences, improving lineage continuity.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
