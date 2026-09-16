# HMS 3.x Catalog Testing

Test environment for the `hive-metastore` connector with HMS 3.x multi-catalog support.

## Quick Start

```bash
# Start HMS 3.x (from metadata-ingestion directory)
cd tests/integration/hive-metastore
docker compose -f docker-compose.hms3.yml up -d

# Wait for HMS to be ready (~60-90s), then setup test data
cd ../../..
source venv/bin/activate
python tests/integration/hive-metastore/hms3/setup-catalogs.py

# Run tests (HMS3_EXTERNAL skips docker-compose, HMS3_SKIP_SETUP skips setup script)
HMS3_EXTERNAL=1 HMS3_SKIP_SETUP=1 pytest tests/integration/hive-metastore/test_hive_metastore_catalog.py -v

# Cleanup when done
docker compose -f tests/integration/hive-metastore/docker-compose.hms3.yml down -v
```

## Test Data

| Catalog         | Database | Tables                   |
| --------------- | -------- | ------------------------ |
| hive            | test_db  | users, events            |
| spark_catalog   | test_db  | users (different schema) |
| iceberg_catalog | test_db  | transactions             |

The `users` table exists in both `hive` and `spark_catalog` with **different schemas** to test namespace isolation.

## Config Options

```yaml
source:
  type: hive-metastore
  config:
    host_port: localhost:9084
    catalog_name: spark_catalog # optional, defaults to 'hive'
    include_catalog_name_in_ids: true # include catalog in URN
```

**URN examples:**

- Without catalog in IDs: `urn:li:dataset:(urn:li:dataPlatform:hive,test_db.users,PROD)`
- With catalog in IDs: `urn:li:dataset:(urn:li:dataPlatform:hive,spark_catalog.test_db.users,PROD)`

## Troubleshooting

| Issue                 | Solution                         |
| --------------------- | -------------------------------- |
| Connection refused    | HMS takes ~60-90s to start       |
| pymetastore not found | `pip install pymetastore thrift` |
