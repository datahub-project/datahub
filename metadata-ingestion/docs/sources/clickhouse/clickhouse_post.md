### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Query Log Extraction

Enable query-log based metadata extraction to augment definition-based lineage:

- `include_query_log_lineage`: derive lineage from INSERT/CREATE queries in `system.query_log`
- `include_usage_statistics`: derive usage statistics from SELECT query activity

This complements view/materialized-view lineage and improves operational usage visibility.

Usage from `SELECT` queries is read from the `tables` and `columns` that ClickHouse itself
resolved, rather than from parsing the SQL. Two consequences:

- Column counts cover every column the `SELECT` touched, including ones used only in `WHERE`,
  `JOIN` or `GROUP BY`, and the expansion of `SELECT *`. ClickHouse names some reads after a
  subcolumn rather than a column — a `Map` key access is reported as `m.key_k`, an array length as
  `arr.size0`. Those appear in the field counts as reported, so they will not match a field in the
  table's schema, and repeated reads of different keys are all attributed to whichever key was seen
  first.
- A read through a view is counted against the view **and** its underlying tables, because
  ClickHouse reports both.

Usage attributed to `INSERT`/`CREATE` queries still comes from SQL parsing, so it covers only
columns that contribute to the written output — a column read solely in an `INSERT ... SELECT`'s
`WHERE` clause is not counted.

#### Config-File Dictionaries

Dictionaries defined in server config files (XML or YAML) are not listed in `system.tables`. Set
`include_config_file_dictionaries: true` to read them from `system.dictionaries`; it is off by
default because it needs the extra grants listed in the prerequisites, and a failed read fails the
run. A dictionary that declares `<database>` is ingested as `<database>.<name>` under that database's container, and
`database_pattern` applies to that database. Otherwise it belongs to the server and is ingested
under its full name without a container. A global name, even a dotted one such as `db.my_dict`, is
filtered only by `table_pattern`. When `database` is set, only dictionaries declared in that
database are ingested. With `include_table_lineage`, a dictionary whose source is an existing
ClickHouse table gets that table as its upstream. ClickHouse reports the source only after the
dictionary is loaded, and loads dictionaries lazily by default, so a dictionary that has not been
used since the server started has no upstream. A config-file dictionary with the same name as a
table or DDL dictionary (for example `<database>db</database>` and `<name>x</name>` next to table
`db.x`) is skipped with a warning, so `dictGet` lineage to it points at the table.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
