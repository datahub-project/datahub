### Prerequisites

Apache Doris connector requires:

- **Doris version**: 2.0.x or higher recommended (see [version compatibility](#version-compatibility) below)
- **Python driver**: `pymysql` (automatically installed with `acryl-datahub[doris]`)
- **SQLAlchemy**: >= 1.4.39 (enforced by DataHub)

### Connection Details

Doris uses MySQL's wire protocol but requires the **FE (Frontend) query port**:

- **Default Doris port**: `9030` (not MySQL's `3306`)
- **Protocol**: MySQL-compatible via `pymysql`
- **Authentication**: Standard username/password

### Doris-Specific Features

This connector preserves Doris-specific data types that would otherwise be lost when using the MySQL connector:

| Doris Type         | MySQL Fallback | Description                                |
| ------------------ | -------------- | ------------------------------------------ |
| **HLL**            | BLOB           | HyperLogLog for approximate COUNT DISTINCT |
| **BITMAP**         | BLOB           | Bitmap for efficient set operations        |
| **QUANTILE_STATE** | BLOB           | For percentile calculations (Doris 2.0+)   |
| **ARRAY**          | TEXT           | Array data types                           |
| **JSONB**          | JSON           | Binary JSON storage                        |

### Version Compatibility

#### Supported Doris Versions

| Doris Version | Status             | Notes                        |
| ------------- | ------------------ | ---------------------------- |
| 3.0.x         | ✅ Fully Supported | Tested with 3.0.8            |
| 2.1.x         | ✅ Fully Supported | All Doris-specific types     |
| 2.0.x         | ✅ Fully Supported | All Doris-specific types     |
| 1.2.x         | ⚠️ Partial Support | QUANTILE_STATE not available |
| < 1.2         | ❌ Not Recommended | Limited type support         |

To check your Doris version:

```sql
SELECT version();
```

### Known Limitations

#### Stored Procedures

Doris's `information_schema.ROUTINES` is always empty, so stored procedure ingestion is disabled by default. This is a Doris limitation with no workaround.

#### Profiling Limitations

Doris-specific types are automatically excluded from field-level profiling because they don't support `COUNT DISTINCT` operations:

- HLL, BITMAP, QUANTILE_STATE: Approximate/aggregate types
- ARRAY, JSONB: Complex types

Table-level statistics (row count, size) are still collected for all tables.

### Migration from MySQL Connector

If you were previously ingesting Doris using the MySQL connector:

**What changes:**

- `type: mysql` → `type: doris`
- `host_port: localhost:3306` → `host_port: localhost:9030`
- Types preserved: MySQL types (INT, BLOB) → Doris types (INTEGER, HLL)

**What stays the same:**

- Connection config (username, password, SSL)
- Schema/table filtering patterns
- Profiling configuration

**Important:** Dataset URNs will change from `platform:mysql` to `platform:doris`, creating new entities in DataHub. Plan for cleanup of old MySQL entities if needed.
