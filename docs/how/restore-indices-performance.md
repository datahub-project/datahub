# Improving RestoreIndices Performance

If you're experiencing slow RestoreIndices performance on large datasets (>10M aspects), you may need to add a database index.

---

## Symptoms

- RestoreIndices job takes many hours (>10 hours)
- Database CPU usage is high during RestoreIndices
- Slow queries on `metadata_aspect_v2` table
- Logs show busy connections with queries like:
  ```
  stmt[select count(*) from metadata_aspect_v2 t0 where t0.version = ?]
  busySeconds[180]
  ```

---

## Root Cause

The RestoreIndices process runs this query for each batch:

```sql
SELECT COUNT(*) FROM metadata_aspect_v2 WHERE version = ?
```

Without an index on the `version` column, this requires a full table scan, making it extremely slow on large datasets.

**Evidence from production:**

- Query time: 180+ seconds (3 minutes) per batch
- On 80M+ aspects: 21+ hours total time
- High database CPU usage

---

## Solution

Add an index on the `version` column to the `metadata_aspect_v2` table.

### For New Installations

**PostgreSQL:**

If you're using the Docker setup, the index is automatically created from the schema file (`docker/postgres/init.sql`).

**MySQL:**

If you're using MySQL, the schema is managed by Ebean. For now, you'll need to add the index manually (see below). Future versions may include automatic index creation via Ebean migrations.

### For Existing Installations

If you already have DataHub running and want to add the index:

#### PostgreSQL

```sql
-- Use CONCURRENTLY to avoid locking the table during index creation
CREATE INDEX CONCURRENTLY idx_version ON metadata_aspect_v2(version);
```

**Note:** `CONCURRENTLY` allows the index to be created without blocking reads/writes to the table. This is important for production systems.

#### MySQL

```sql
CREATE INDEX idx_version ON metadata_aspect_v2(version);
```

**Note:** MySQL index creation is online by default in MySQL 5.6+, so it won't block reads/writes.

---

## Verification

Check if the index exists:

### PostgreSQL

```sql
-- Connect to database
psql -U postgres -d datahub

-- Check indexes
\d metadata_aspect_v2

-- Should show:
-- Indexes:
--     "pk_metadata_aspect_v2" PRIMARY KEY, btree (urn, aspect, version)
--     "timeIndex" btree (createdon)
--     "idx_version" btree (version)  ← This should exist
```

### MySQL

```sql
-- Connect to database
mysql -u datahub -p datahub

-- Check indexes
SHOW INDEX FROM metadata_aspect_v2;

-- Should show an index named 'idx_version' on column 'version'
```

---

## Expected Improvement

**Before adding the index:**

- RestoreIndices on 80M+ aspects: 21+ hours
- Query time: 180+ seconds per batch
- High database CPU usage (80-100%)

**After adding the index:**

- RestoreIndices on 80M+ aspects: 2-4 hours (5-10x faster!)
- Query time: < 1 second per batch
- Normal database CPU usage (10-20%)

---

## When to Apply

- If you have >10M aspects in `metadata_aspect_v2`
- If RestoreIndices is taking >10 hours
- Before running a full reindex
- If you see "busy connection" warnings in logs with `version = ?` queries

---

## Troubleshooting

### Index Creation is Slow

If index creation is taking a long time:

**PostgreSQL:**

```sql
-- Check index creation progress
SELECT * FROM pg_stat_progress_create_index;
```

**MySQL:**

```sql
-- Check running queries
SHOW PROCESSLIST;
```

**Tip:** Index creation time depends on table size. For 80M+ rows, expect 10-30 minutes.

### Index Already Exists

If you get an error that the index already exists:

**PostgreSQL:**

```sql
-- Use IF NOT EXISTS
CREATE INDEX IF NOT EXISTS idx_version ON metadata_aspect_v2(version);
```

**MySQL:**

```sql
-- Check if index exists first
SHOW INDEX FROM metadata_aspect_v2 WHERE Key_name = 'idx_version';

-- If it doesn't exist, create it
CREATE INDEX idx_version ON metadata_aspect_v2(version);
```

---

## Related Issues

- [#11276](https://github.com/datahub-project/datahub/issues/11276) - RestoreIndices process performance is poor on large sets of data

---

## Summary

**Problem:** RestoreIndices slow on large datasets due to missing index

**Solution:** Add index on `version` column

**Impact:** 5-10x faster RestoreIndices (21 hours → 2-4 hours)

**For new installations:** Index is automatically created (PostgreSQL)

**For existing installations:** Run manual migration SQL (see above)
