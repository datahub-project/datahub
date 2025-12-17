# Kerberos Hive Metastore Test Environment

Test infrastructure for the `hive-metastore` connector with Kerberos/SASL authentication.

> **Important**: Kerberos tests must run FROM INSIDE the Docker container. The KDC, keytabs, and kinit are only available there.

## Quick Start

### 1. Build DataHub Wheel

```bash
cd metadata-ingestion
source venv/bin/activate
pip wheel . -w /tmp/datahub_wheels --no-deps
```

### 2. Start Kerberized Environment

```bash
cd tests/integration/hive-metastore/kerberos

# Clean start
docker compose -f docker-compose.kerberos.yml down -v
docker compose -f docker-compose.kerberos.yml up -d

# Wait ~2 min for setup
sleep 120

# Verify: look for "✅ Test data setup complete!"
docker logs kerberos-client 2>&1 | tail -30
```

### 3. Install DataHub in Container

```bash
# Copy wheel (rename to remove epoch marker)
WHEEL=$(ls /tmp/datahub_wheels/*.whl | head -1)
docker cp "$WHEEL" kerberos-client:/tmp/datahub.whl

# Install local wheel + hive-metastore extras (includes kerberos for GSSAPI)
docker exec kerberos-client bash -c "
  mv /tmp/datahub.whl /tmp/acryl_datahub-0.0.0.dev0-py3-none-any.whl
  pip3 install '/tmp/acryl_datahub-0.0.0.dev0-py3-none-any.whl[hive-metastore]'
"

# Verify
docker exec kerberos-client python3 -c "
from datahub.ingestion.source.sql.hive.hive_metastore_source import HiveMetastoreSource
from datahub.ingestion.source.sql.hive.hive_thrift_fetcher import ThriftDataFetcher
print('✓ Import successful')
"
```

### 4. Run Ingestion Test

```bash
# Create recipe
docker exec kerberos-client bash -c "
cat > /tmp/recipe.yaml << 'EOF'
source:
  type: hive-metastore
  config:
    connection_type: thrift
    host_port: hive-metastore:9083
    use_kerberos: true
    kerberos_service_name: hive
    database_pattern:
      allow:
        - ^db1$
        - ^db2$

sink:
  type: file
  config:
    filename: /tmp/output.json
EOF
"

# Run ingestion
docker exec kerberos-client datahub ingest -c /tmp/recipe.yaml

# Verify output
docker exec kerberos-client bash -c "
  echo '=== Datasets ==='
  grep -o 'urn:li:dataset[^\"]*' /tmp/output.json | sort -u | head -10
"
```

Expected output:

- **9 tables scanned** (8 from db1 + 1 from db2)
- **62 events produced** (~6-7 aspects per table + containers)
- No failures

## Validate Kerberos Connection (Quick Test)

```bash
docker exec kerberos-client /test-connection.sh
```

Expected:

```
✓ Authenticated as: testuser@TEST.LOCAL
✓ Connected! Found 5 database(s)
✓ All tests passed!
```

## Architecture

| Service         | Port | Purpose                                 |
| --------------- | ---- | --------------------------------------- |
| KDC             | 88   | MIT Kerberos (TEST.LOCAL realm)         |
| HMS             | 9083 | Kerberized Hive Metastore (SASL/GSSAPI) |
| kerberos-client | -    | Auto: kinit + load test data via Thrift |

## Recipe Format

The connector uses `hive-metastore` with `connection_type: thrift`:

```yaml
source:
  type: hive-metastore
  config:
    connection_type: thrift # Use Thrift API (not SQL)
    host_port: hive-metastore:9083
    use_kerberos: true
    kerberos_service_name: hive
    # Optional: kerberos_hostname_override for load balancer scenarios
```

## Files

| File                     | Purpose                        |
| ------------------------ | ------------------------------ |
| `Dockerfile.kdc`         | KDC with keytab generation     |
| `krb5.conf`              | Kerberos client config         |
| `hive-site-kerberos.xml` | HMS Kerberos settings          |
| `setup-test-data.py`     | Creates test tables via Thrift |
| `test-connection.sh`     | Validates Kerberos auth        |

## Cleanup

```bash
cd tests/integration/hive-metastore/kerberos
docker compose -f docker-compose.kerberos.yml down -v
```

## Dependencies

The `hive-metastore` extra includes all required dependencies for Kerberos/GSSAPI authentication:

- `acryl-pyhive[hive-pure-sasl]` - Thrift transport with SASL support
- `pymetastore>=0.4.2` - HMS Thrift client
- `kerberos>=1.3.0` - Python GSSAPI bindings (used by pure-sasl)
- `tenacity>=8.0.1` - Retry logic

System requirements (already in container):

- `krb5-user` - Kerberos client tools (kinit, klist)
- `libkrb5-dev` - Kerberos development headers

## Troubleshooting

| Issue                   | Solution                                                                             |
| ----------------------- | ------------------------------------------------------------------------------------ |
| Kerberos ticket expired | `docker exec kerberos-client kinit -kt /keytabs/testuser.keytab testuser@TEST.LOCAL` |
| Connection refused      | Wait for HMS: `docker compose logs hive-metastore`                                   |
| ModuleNotFoundError     | Reinstall wheel (Step 3)                                                             |
| GSSAPI import error     | Ensure `kerberos` package is installed: `pip install kerberos`                       |
