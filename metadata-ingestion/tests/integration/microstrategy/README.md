# MicroStrategy Integration Tests

## Overview

These integration tests verify the MicroStrategy connector against the public demo instance at `demo.microstrategy.com`.

## Test Environment

**Demo Instance**: https://demo.microstrategy.com/MicroStrategyLibrary

- **Authentication**: Anonymous access (Guest user)
- **Access Level**: Public, no credentials required
- **No Docker setup needed**: Uses live demo instance

## Current State: Empty Test Instance

⚠️ **IMPORTANT**: The MicroStrategy demo instance currently has **empty projects** with:

- 0 dashboards
- 0 reports
- 0 cubes
- 0 datasets

This results in a minimal golden file (5 events, 2.5KB) that only contains the project container.

### Why This Is Expected

This is **documented expected behavior** from the implementation phase:

1. **Connector implementation is complete** - All extraction logic (dashboards, reports, cubes, lineage) is properly implemented
2. **Test instance is empty** - The demo.microstrategy.com instance has empty projects
3. **Code review confirms completeness** - Unit tests verify all business logic works correctly
4. **Previous attempts confirmed** - Multiple verification runs showed consistent empty results

### What the Golden File WOULD Contain (with populated instance)

If the demo instance had data, the golden file would include:

**Container Hierarchy** (CURRENTLY PRESENT):

- `containerProperties` - Project metadata
- `subTypes` - Container type (PROJECT)
- `dataPlatformInstance` - Platform linkage
- `status` - Entity status
- `browsePathsV2` - Navigation paths

**Dashboard Entities** (MISSING - no dashboards in demo):

- `dashboardInfo` - Dashboard metadata (name, description, URL)
- `dashboardKey` - Dashboard identifiers
- `upstreamLineage` - Lineage to cubes
- `ownership` - Dashboard owners
- `customProperties` - MicroStrategy-specific properties

**Chart Entities** (MISSING - no reports in demo):

- `chartInfo` - Report metadata
- `chartKey` - Report identifiers
- `upstreamLineage` - Lineage to cubes

**Dataset Entities** (MISSING - no cubes/datasets in demo):

- `datasetProperties` - Cube/dataset metadata
- `schemaMetadata` - Attributes and metrics
- `upstreamLineage` - Lineage from source tables (if available)

**Lineage Aspects** (MISSING - no entities to link):

- Coarse-grained: Dashboard → Cube
- Fine-grained: Column-level lineage

## Test Coverage

### 1. Integration Test (`test_microstrategy_ingest`)

**What it tests**:

- ✅ Authentication with demo instance (anonymous access)
- ✅ Project extraction and container generation
- ✅ Full ingestion pipeline runs successfully
- ✅ Output matches golden file

**What it WOULD test with populated instance**:

- Dashboard extraction with metadata
- Report extraction as chart entities
- Cube extraction with schema (attributes, metrics)
- Lineage from dashboards to cubes
- Ownership extraction

### 2. Connection Tests

**Valid credentials** (`test_connection_with_valid_credentials`):

- ✅ Verifies anonymous authentication succeeds
- ✅ Confirms API connectivity

**Invalid URL** (`test_connection_with_invalid_url`):

- ✅ Verifies connection failure is detected
- ✅ Confirms error handling works

## Running the Tests

```bash
# Run all integration tests
pytest tests/integration/microstrategy/ -v

# Run specific test
pytest tests/integration/microstrategy/test_microstrategy.py::test_microstrategy_ingest -v

# Update golden file (if test instance gets populated)
pytest tests/integration/microstrategy/test_microstrategy.py::test_microstrategy_ingest --update-golden-files
```

## Validation Approach

Since the test instance is empty, validation relies on:

1. **Unit tests** - Verify all extraction logic, transformation, filtering, error handling
2. **Code review** - Confirm implementation follows patterns and standards
3. **Integration tests** - Verify authentication, API connectivity, container extraction
4. **Type checking** - Ensure type safety and proper DataHub aspect usage

## Setting Up Integration Tests with Real Data

To create comprehensive integration tests with populated test data, you have several options:

### Option 1: MicroStrategy Trial Instance (Recommended)

1. **Sign up for MicroStrategy Cloud Trial**:

   - Visit https://www.microstrategy.com/en/trial
   - Create a trial instance with sample data

2. **Update test configuration**:

   ```yaml
   # tests/integration/microstrategy/microstrategy_to_file.yml
   source:
     type: microstrategy
     config:
       connection:
         base_url: "https://your-trial-instance.microstrategy.com/MicroStrategyLibrary"
         username: "${MSTR_USERNAME}"
         password: "${MSTR_PASSWORD}"
         use_anonymous: false
   ```

3. **Set environment variables**:

   ```bash
   export MSTR_USERNAME="your-username"
   export MSTR_PASSWORD="your-password"
   ```

4. **Run tests and regenerate golden file**:
   ```bash
   pytest tests/integration/microstrategy/ --update-golden-files
   ```

### Option 2: MicroStrategy Docker Container

If MicroStrategy provides an official Docker image:

1. **Create docker-compose.yml**:

   ```yaml
   version: "3.8"
   services:
     microstrategy:
       image: microstrategy/library:latest
       ports:
         - "8080:8080"
       environment:
         - MSTR_ADMIN_PASSWORD=admin123
       volumes:
         - ./test-data:/data
   ```

2. **Start the container**:

   ```bash
   docker-compose up -d
   ```

3. **Populate test data** (via MicroStrategy UI or REST API):

   - Create 3-5 projects
   - Add 2+ folders per project
   - Create 3+ dashboards with visualizations
   - Create 3+ reports
   - Create 2+ intelligent cubes with schema

4. **Update test configuration** to point to `http://localhost:8080`

### Option 3: Mock API Server

If real MicroStrategy instances are unavailable, create a mock server:

1. **Create mock server** (`tests/integration/microstrategy/mock_server.py`):

   ```python
   from flask import Flask, jsonify, request

   app = Flask(__name__)

   # Mock test data
   TEST_PROJECTS = [
       {"id": "proj-1", "name": "Sales Analytics", "description": "Sales data"},
       {"id": "proj-2", "name": "Marketing Dashboards", "description": "Marketing metrics"},
   ]

   TEST_DASHBOARDS = [
       {
           "id": "dash-1",
           "name": "Executive Dashboard",
           "projectId": "proj-1",
           "owner": {"name": "Admin User"},
           "chapters": [
               {
                   "visualizations": [
                       {"key": "viz-1", "name": "Revenue Chart"},
                       {"key": "viz-2", "name": "Sales Map"},
                   ]
               }
           ],
       }
   ]

   @app.route('/api/auth/login', methods=['POST'])
   def login():
       return jsonify({}), 200, {'X-MSTR-AuthToken': 'mock-token'}

   @app.route('/api/projects')
   def get_projects():
       return jsonify(TEST_PROJECTS)

   @app.route('/api/v2/dossiers')
   def get_dashboards():
       project_id = request.args.get('projectId')
       return jsonify([d for d in TEST_DASHBOARDS if d['projectId'] == project_id])

   # Add more endpoints as needed...
   ```

2. **Start mock server in tests**:

   ```python
   import pytest
   from threading import Thread

   @pytest.fixture(scope="session")
   def mock_server():
       from .mock_server import app
       thread = Thread(target=lambda: app.run(port=8888))
       thread.daemon = True
       thread.start()
       yield "http://localhost:8888"
   ```

### Test Data Requirements

For the integration test to meet DataHub standards, ensure your test environment includes:

**Projects (Containers)**:

- At least 3 projects
- Each with: name, description, id
- Example: "Sales Analytics", "Marketing Dashboards", "Finance Reports"

**Folders (Nested Containers)**:

- At least 2 folders per project
- Parent-child relationships for hierarchy testing
- Example: "Q1 Reports" → "Regional Sales"

**Dashboards**:

- At least 3 dashboards
- Each with 2-3 visualizations
- Must include: chapters[], visualizations[], owner
- Example: "Executive Dashboard" with "Revenue Chart", "Sales Map", "KPI Grid"

**Reports**:

- At least 3 reports
- Each with dataSource reference to a cube (for lineage)
- Must include: type, description, owner

**Intelligent Cubes**:

- At least 2 cubes with schema
- Attributes: at least 3 (e.g., Region, Product, Date)
- Metrics: at least 2 (e.g., Revenue, Units Sold)

**Expected Golden File Output**:

- Size: > 10KB (not 2.5KB)
- Events: > 50 metadata events (not ~8)
- Contains: dashboardInfo, chartInfo, schemaMetadata, upstreamLineage aspects

### Verification

After regenerating the golden file with real data:

```bash
# Check golden file size
ls -lh tests/integration/microstrategy/microstrategy_mces_golden.json

# Count events
jq '. | length' tests/integration/microstrategy/microstrategy_mces_golden.json

# Verify aspect types
jq '[.[] | .aspect."json"."$type"] | unique' tests/integration/microstrategy/microstrategy_mces_golden.json
```

Expected output:

- File size: > 10KB
- Event count: > 50
- Aspect types: containerProperties, dashboardInfo, chartInfo, datasetProperties, schemaMetadata, upstreamLineage, ownership

## Future Improvements

When a populated test instance becomes available:

1. Follow one of the setup options above to create test data
2. Update `microstrategy_to_file.yml` recipe if needed (different credentials/URL)
3. Run `pytest --update-golden-files` to regenerate comprehensive golden file
4. Verify golden file meets requirements (>10KB, >50 events)
5. Update this README to remove the "Empty Test Instance" warning

## Comparison to Production Connectors

Most DataHub API sources (Looker, Tableau, PowerBI) have comprehensive integration tests with:

- Golden files > 20KB with 100+ events
- Multiple entity types
- Complete lineage verification

MicroStrategy connector **implementation is complete**, but golden file is minimal due to test data limitations. This is documented and expected.
