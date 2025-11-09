# Apache Doris and MySQL Compatibility

## Overview

The Apache Doris connector in DataHub extends the MySQL connector, leveraging Doris's MySQL protocol compatibility. However, **Doris is NOT 100% compatible with MySQL**, and this document outlines the key differences and how the connector handles them.

## What the Connector Queries

The MySQL/Doris connector queries these system tables:

1. **`information_schema.tables`** - For table metadata and profiling (data_length column) ✅ Works
2. **`information_schema.ROUTINES`** - For stored procedures ❌ **ALWAYS EMPTY in Doris** ([source](https://doris.apache.org/docs/3.x/admin-manual/system-tables/information_schema/routines))
3. **SQLAlchemy's `inspector.get_schema_names()`** - Uses MySQL dialect queries under the hood ✅ Works

## Compatibility Status

### ✅ What Works Well

- **MySQL Protocol**: Doris fully supports MySQL client protocol on port 9030
- **`information_schema.tables`**: Available and compatible for basic metadata
- **Basic Queries**: Standard SELECT, WHERE, JOIN operations
- **Data Extraction**: Tables, views, columns, and basic profiling
- **Authentication**: Standard username/password authentication

### ⚠️ Differences to Be Aware Of

#### 1. **Data Types**

Doris has unique data types not present in MySQL:

- `HyperLogLog` - For approximate cardinality
- `Bitmap` - For bitmap indexing
- `Array` - Native array support
- `JSONB` - Binary JSON (vs MySQL's JSON)

These will appear in DataHub but may not have exact MySQL equivalents.

#### 2. **Stored Procedures**

- **Issue**: `information_schema.ROUTINES` is **always empty** in Doris ([documented behavior](https://doris.apache.org/docs/3.x/admin-manual/system-tables/information_schema/routines))
- **Solution**: The connector disables `include_stored_procedures` by default and returns empty results even if enabled
- **Quote from Doris docs**: _"This table is solely for the purpose of maintaining compatibility with MySQL behavior. It is always empty."_
- **Impact**: No stored procedures will ever be ingested from Doris

#### 3. **System Tables**

- Doris uses **virtual read-only** system tables
- Some columns in `information_schema` may have different values or be NULL
- Certain MySQL system tables may not exist

#### 4. **Port Number**

- MySQL default: **3306**
- Doris default: **9030** (query port)
- Make sure to specify the correct port!

## Connector Implementation Details

### DorisConfig Class

```python
class DorisConfig(MySQLConfig):
    include_stored_procedures: bool = Field(
        default=False,  # Disabled by default for Doris
        description="Include ingest of stored procedures. Note: Doris has limited stored procedure support compared to MySQL.",
    )
```

### Error Handling

The connector overrides `get_procedures_for_schema()` to:

1. Return empty list if stored procedures are disabled (default)
2. Try MySQL approach if explicitly enabled
3. Fail gracefully with a warning if `information_schema.ROUTINES` query fails

### Example Configuration

```yaml
source:
  type: doris
  config:
    host_port: doris-server:9030 # Note: 9030, not 3306
    database: my_database
    username: root
    password: ${DORIS_PASSWORD}

    # Stored procedures disabled by default
    # include_stored_procedures: false

    include_tables: true
    include_views: true
    profiling:
      enabled: true
```

## Testing Recommendations

When using the Doris connector, we recommend:

1. **Start Simple**: Test with basic table/view extraction first
2. **Verify Data Types**: Check that Doris-specific types (HyperLogLog, Bitmap, Array) are handled appropriately
3. **Skip Stored Procedures**: Unless you specifically need them and have verified they work
4. **Check Profiling**: Verify that `information_schema.tables` queries return expected data

## Known Limitations

1. **Stored Procedures**: Limited support, disabled by default
2. **Type Mapping**: Some Doris-specific types may not have perfect DataHub representations
3. **System Tables**: Not all MySQL system tables are available in Doris
4. **Version Differences**: Compatibility may vary between Doris versions

## Future Enhancements

Potential improvements for better Doris support:

- [ ] Custom type mapping for HyperLogLog, Bitmap, Array
- [ ] Doris-specific metadata (e.g., tablet information, materialized views)
- [ ] Better handling of Doris's distributed architecture metadata
- [ ] Support for Doris-specific features (e.g., rollup tables)

## References

- [Apache Doris MySQL Compatibility Docs](https://doris.apache.org/docs/2.0/query/query-data/mysql-compatibility/)
- [Doris System Tables](https://doris.apache.org/docs/dev/admin-manual/system-tables/overview/)
- [DataHub MySQL Connector](https://datahubproject.io/docs/generated/ingestion/sources/mysql/)
