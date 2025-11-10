# Apache Doris Integration Tests

## Overview

These integration tests verify the Apache Doris connector functionality using the official Apache Doris Docker image (`apache/doris:latest`). The container runs a single-node Doris cluster with both Frontend (FE) and Backend (BE) components.

## Running the Tests

```bash
# Run all Doris integration tests
pytest tests/integration/doris/

# Run specific test
pytest tests/integration/doris/test_doris.py::test_doris_ingest -v

# Run with docker compose logs
pytest tests/integration/doris/test_doris.py -v -s
```

## Test Structure

- `docker-compose.yml` - Real Apache Doris cluster with FE and BE:
  - **Frontend (FE)**: `apache/doris:fe-3.0.8` at 172.28.88.2
    - Port 58030: FE web UI
    - Port 59030: MySQL protocol port (query port)
    - Port 59010: FE edit log port
  - **Backend (BE)**: `apache/doris:be-3.0.8` at 172.28.88.3
    - Port 58040: BE web UI
    - Port 59050: BE heartbeat port
  - Custom bridge network (172.28.88.0/24) with static IPs for proper FE-BE communication
- `setup/setup.sql` - Initial schema and data setup for dorisdb
- `test_doris.py` - Integration test cases mirroring MySQL test structure
- `doris_to_file.yml` - Test ingestion configuration
- `doris_mces_golden.json` - Expected output (golden file)

## Regenerating Golden Files

If you make changes that affect the output, regenerate the golden file:

```bash
# Run the test to generate new output
pytest tests/integration/doris/test_doris.py::test_doris_ingest

# The output will be in the temp directory, copy it to golden file
cp /tmp/pytest-*/doris_mces.json tests/integration/doris/doris_mces_golden.json
```

## Notes

- Uses official Apache Doris Docker images with real FE and BE components
- Custom network with static IPs required for macOS Docker environment
- FE healthcheck waits for MySQL protocol (port 9030) to be ready
- BE healthcheck waits for web UI (port 8040) to be ready
- Setup script runs after FE is ready to create test database and schema
- BE registration with FE takes ~30 seconds after FE is healthy
- Test structure mirrors `tests/integration/mysql/` for consistency
- Setup script is automatically run via Doris's MySQL protocol after containers start
- Doris-specific types (HLL, BITMAP, ARRAY, JSONB) can be tested with real Doris instance

## Manual Setup for Development

To manually set up the Doris cluster for testing:

```bash
# Start the Doris cluster (FE and BE)
cd tests/integration/doris
docker-compose up -d

# Wait for containers to be healthy (FE takes ~60-90s, BE another ~30s)
docker ps --filter name=testdoris

# Check FE logs to see startup progress
docker logs testdoris-fe

# Verify FE is ready
docker exec testdoris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1; SHOW FRONTENDS;"

# Verify BE has registered with FE (wait ~30s after FE is ready)
docker exec testdoris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW BACKENDS;"

# Run the setup script to create test database
docker exec -i testdoris-fe mysql -h 127.0.0.1 -P 9030 -u root < setup/setup.sql

# Connect to Doris via exposed port
mysql -h 127.0.0.1 -P 59030 -u root

# Access FE web UI
open http://localhost:58030

# Access BE web UI
open http://localhost:58040

# Stop the cluster
docker-compose down
```
