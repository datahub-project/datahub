### Prerequisites

#### Doris Version

Doris 3.0.x is supported. Doris 2.0+ may work but is untested.

Check your version:

```sql
SELECT version();
```

#### Network Access

- **Doris FE port**: `9030`
- Ensure network connectivity from DataHub to Doris FE

#### User Permissions

Minimal required permissions for the DataHub user:

```sql
-- Create user
CREATE USER 'datahub'@'%' IDENTIFIED BY 'your_password';

-- Grant minimal permissions
GRANT SELECT_PRIV ON *.* TO 'datahub'@'%';
GRANT SHOW_VIEW_PRIV ON *.* TO 'datahub'@'%';
```

### Doris-Specific Features

This connector preserves Doris-specific data types:

| Doris Type         | MySQL Fallback | Description                                |
| ------------------ | -------------- | ------------------------------------------ |
| **HLL**            | BLOB           | HyperLogLog for approximate COUNT DISTINCT |
| **BITMAP**         | BLOB           | Bitmap for efficient set operations        |
| **QUANTILE_STATE** | BLOB           | For percentile calculations                |
| **ARRAY**          | TEXT           | Array data types                           |
| **JSONB**          | JSON           | Binary JSON storage                        |

### Known Limitations

#### Stored Procedures

Doris's `information_schema.ROUTINES` is always empty. Stored procedure ingestion is disabled by default.

#### Profiling

Doris-specific types (HLL, BITMAP, QUANTILE_STATE, ARRAY, JSONB) are automatically excluded from field-level profiling because they don't support `COUNT DISTINCT` operations. Table-level statistics are still collected.

### Migration from MySQL Connector

If previously using the MySQL connector:

- Change `type: mysql` → `type: doris`
- Change port: `3306` → `9030`
- Dataset URNs will change from `platform:mysql` to `platform:doris`, creating new entities in DataHub
- Enable stateful ingestion to automatically clean up old MySQL entities (see configuration for threshold settings)
