# Ingestion Recording and Replay

Debug ingestion issues by capturing all external I/O (HTTP requests and database queries) during ingestion runs, then replaying them locally in an air-gapped environment with full debugger support.

## Overview

The recording system captures:

- **HTTP Traffic**: All requests to external APIs (Looker, PowerBI, Snowflake REST, etc.) and DataHub GMS
- **Database Queries**: SQL queries and results from native database connectors (Snowflake, Redshift, BigQuery, Databricks, etc.)

Recordings are stored in encrypted, compressed archives that can be replayed offline to reproduce issues exactly as they occurred in production.

### Comparing Recording and Replay Output

The recorded and replayed MCPs are **semantically identical** - they contain the same source data. However, certain metadata fields will differ because they reflect _when_ MCPs are emitted, not the source data itself:

- `systemMetadata.lastObserved` - timestamp of MCP emission
- `systemMetadata.runId` - unique run identifier
- `auditStamp.time` - audit timestamp

Use `datahub check metadata-diff` to compare recordings semantically:

```bash
# Compare MCPs ignoring system metadata
datahub check metadata-diff \
    --ignore-path "root['*']['systemMetadata']['lastObserved']" \
    --ignore-path "root['*']['systemMetadata']['runId']" \
    recording_output.json replay_output.json
```

A successful replay will show **PERFECT SEMANTIC MATCH** when ignoring these fields.

## Installation

Install the optional `debug-recording` plugin:

```bash
pip install 'acryl-datahub[debug-recording]'

# Or with your source connectors
pip install 'acryl-datahub[looker,debug-recording]'
```

**Dependencies:**

- `vcrpy>=7.0.0` - HTTP recording/replay
- `pyzipper>=0.3.6` - AES-256 encrypted archives
- `urllib3>=2.0.0` - HTTP client compatibility

## Quick Start

### Recording an Ingestion Run

```bash
# Record with password protection
datahub ingest run -c recipe.yaml --record --record-password mysecret

# Record without S3 upload (for local testing)
datahub ingest run -c recipe.yaml --record --record-password mysecret --no-s3-upload
```

The recording creates an encrypted ZIP archive containing:

- HTTP cassette with all request/response pairs
- Database query recordings (if applicable)
- Redacted recipe (secrets replaced with safe markers)
- Manifest with metadata and checksums

### Replaying a Recording

```bash
# Replay in air-gapped mode (default) - no network required
datahub ingest replay recording.zip --password mysecret

# Replay with live sink - replay source data, emit to real DataHub
datahub ingest replay recording.zip --password mysecret \
    --live-sink --server http://localhost:8080
```

### Inspecting Recordings

```bash
# View archive metadata
datahub recording info recording.zip --password mysecret

# Extract archive contents
datahub recording extract recording.zip --password mysecret --output-dir ./extracted

# List available recordings
datahub recording list
```

## Configuration

### Recipe Configuration

```yaml
source:
  type: looker
  config:
    # ... source config ...

# Optional recording configuration
recording:
  enabled: true
  password: ${DATAHUB_RECORDING_PASSWORD} # Or use --record-password CLI flag
  s3_upload:
    enabled: true # Upload to S3 after recording
    # S3 bucket is auto-configured from DataHub server settings
```

### Environment Variables

| Variable                     | Description                                      |
| ---------------------------- | ------------------------------------------------ |
| `DATAHUB_RECORDING_PASSWORD` | Default password for recording encryption        |
| `ADMIN_PASSWORD`             | Fallback password (used in managed environments) |

### CLI Options

**Recording:**

```bash
datahub ingest run -c recipe.yaml \
    --record                    # Enable recording
    --record-password <pwd>     # Encryption password
    --no-s3-upload              # Disable S3 upload
```

**Replay:**

```bash
datahub ingest replay <archive> \
    --password <pwd>            # Decryption password
    --live-sink                 # Enable real GMS sink
    --server <url>              # GMS server for live sink
    --token <token>             # Auth token for live sink
```

## Archive Format

```
recording-{run_id}.zip (AES-256 encrypted, LZMA compressed)
├── manifest.json           # Metadata, versions, checksums
├── recipe.yaml             # Recipe with redacted secrets
├── http/
│   └── cassette.yaml       # VCR HTTP recordings (YAML for binary data support)
└── db/
    └── queries.jsonl       # Database query recordings
```

### Manifest Contents

```json
{
  "format_version": "1.0.0",
  "run_id": "looker-2024-12-03-10_30_00-abc123",
  "source_type": "looker",
  "sink_type": "datahub-rest",
  "datahub_version": "0.14.0",
  "created_at": "2024-12-03T10:35:00Z",
  "recording_start_time": "2024-12-03T10:30:00Z",
  "files": ["http/cassette.yaml", "db/queries.jsonl"],
  "checksums": { "http/cassette.yaml": "sha256:..." },
  "has_exception": false,
  "exception_info": null
}
```

- `recording_start_time`: When recording began (informational)
- `has_exception`: Whether the recording captured an exception
- `exception_info`: Stack trace and details if an exception occurred

## Best Practices

### 1. Use Consistent Passwords

Store the recording password in a secure location (secrets manager, environment variable) and use the same password across your team:

```bash
export DATAHUB_RECORDING_PASSWORD=$(vault read -field=password secret/datahub/recording)
datahub ingest run -c recipe.yaml --record
```

### 2. Record in Production-Like Environments

For best debugging results, record in an environment that matches production:

- Same credentials and permissions
- Same network access
- Same data volume (or representative sample)

### 3. Use Descriptive Run IDs

The archive filename includes the run_id. Use meaningful recipe names for easy identification:

```yaml
# Recipe: snowflake-prod-daily.yaml
# Archive: snowflake-prod-daily-2024-12-03-10_30_00-abc123.zip
```

### 4. Test Replay Immediately

After recording, test the replay to ensure the recording is complete:

```bash
# Record (save MCP output for comparison)
datahub ingest run -c recipe.yaml --record --record-password test --no-s3-upload \
    | tee recording_output.json

# Immediately test replay (save output)
datahub ingest replay /tmp/recording.zip --password test \
    | tee replay_output.json

# Verify semantic equivalence
datahub check metadata-diff \
    --ignore-path "root['*']['systemMetadata']['lastObserved']" \
    --ignore-path "root['*']['systemMetadata']['runId']" \
    recording_output.json replay_output.json
```

### 5. Include Exception Context

If recording captures an exception, the archive includes exception details:

```bash
datahub recording info recording.zip --password mysecret
# Output includes: has_exception: true, exception_info: {...}
```

### 6. Secure Archive Handling

- Never commit recordings to source control
- Use strong passwords (16+ characters)
- Delete recordings after debugging is complete
- Use S3 lifecycle policies for automatic cleanup

### 7. Minimize Recording Scope

For faster recordings and smaller archives, limit the scope:

```yaml
source:
  type: looker
  config:
    dashboard_pattern:
      allow:
        - "^specific-dashboard-id$"
```

## Limitations

### 1. Thread-Safe Recording Impact

To capture all HTTP requests reliably, recording serializes HTTP calls. This has performance implications:

| Scenario           | Without Recording | With Recording |
| ------------------ | ----------------- | -------------- |
| Parallel API calls | ~10s              | ~90s           |
| Single-threaded    | ~90s              | ~90s           |

**Mitigation:** Recording is intended for debugging, not production. Use `--no-s3-upload` for faster local testing.

### 2. Timestamps Differ Between Runs

MCP metadata timestamps will always differ between recording and replay:

- `systemMetadata.lastObserved` - set when MCP is emitted
- `systemMetadata.runId` - unique per run
- `auditStamp.time` - set during processing

**Mitigation:** The actual source data is identical. Use `datahub check metadata-diff` with `--ignore-path` to verify semantic equivalence (see "Comparing Recording and Replay Output" above).

### 3. Non-Deterministic Source Behavior

Some sources have non-deterministic behavior:

- Random sampling or ordering of results
- Rate limiting/retry timing variations
- Parallel processing order

**Mitigation:** The replay serves recorded API responses, so data is identical. The system includes custom VCR matchers that handle non-deterministic request ordering (e.g., Looker usage queries with varying filter orders).

### 4. Database Connection Mocking

Database replay mocks the connection entirely - authentication is bypassed. This means:

- Connection pooling behavior may differ
- Transaction semantics are simplified
- Cursor state is simulated

**Mitigation:** For complex database debugging, use database-specific profiling tools alongside recording.

### 5. Large Recordings

Recordings can be large for high-volume sources:

- Looker with 1000+ dashboards: ~50MB
- PowerBI with many workspaces: ~100MB
- Snowflake with full schema extraction: ~200MB

**Mitigation:**

- Use patterns to limit scope
- Enable LZMA compression (default)
- Use S3 for storage instead of local disk

### 6. Secret Handling

Secrets are redacted in the stored recipe using `__REPLAY_DUMMY__` markers. During replay:

- Pydantic validation receives valid dummy values
- Actual API/DB calls use recorded responses (no real auth needed)
- Some sources may have validation that fails with dummy values

**Mitigation:** The replay system auto-injects valid dummy values that pass common validators.

### 7. HTTP-Only for Some Sources

Sources using non-HTTP protocols cannot be fully recorded:

- Direct TCP/binary database protocols (partially supported via db_proxy)
- gRPC (not currently supported)
- WebSocket (not currently supported)

**Mitigation:** Most sources use HTTP REST APIs which are fully supported.

### 8. Vendored HTTP Libraries (Snowflake, Databricks)

Some database connectors bundle (vendor) their own copies of HTTP libraries:

- **Snowflake**: Uses `snowflake.connector.vendored.urllib3` and `vendored.requests`
- **Databricks**: Uses internal Thrift HTTP client

VCR can only intercept the standard `urllib3`/`requests` libraries, not vendored copies. This means:

- HTTP requests through vendored libraries are NOT recorded
- Connection attempts may fail during recording due to VCR interference
- Affects: Snowflake source, Fivetran with Snowflake/Databricks destinations

**Workarounds:**

1. Use sources that don't use vendored HTTP (e.g., Looker, PowerBI, dbt Cloud)
2. Debug database issues using the db_proxy recordings (SQL queries are still captured)
3. Test without `--record` flag to verify credentials work, then debug other issues

### 9. Stateful Ingestion

Stateful ingestion checkpoints may behave differently during replay:

- Recorded state may reference timestamps that don't match replay time
- State backend calls are mocked

**Mitigation:** For stateful debugging, record a fresh run without existing state.

### 10. Memory Usage

Large recordings are loaded into memory during replay:

- HTTP cassette is fully loaded
- DB queries are streamed from JSONL

**Mitigation:** For very large recordings, extract and inspect specific parts:

```bash
datahub recording extract recording.zip --password mysecret --output-dir ./extracted
# Manually inspect http/cassette.yaml
```

## Supported Sources

### Fully Supported (HTTP-based)

| Source    | HTTP Recording | Notes                            |
| --------- | -------------- | -------------------------------- |
| Looker    | ✅             | Full support including SDK calls |
| PowerBI   | ✅             | Full support                     |
| Tableau   | ✅             | Full support                     |
| Superset  | ✅             | Full support                     |
| Mode      | ✅             | Full support                     |
| Sigma     | ✅             | Full support                     |
| dbt Cloud | ✅             | Full support                     |
| Fivetran  | ✅             | Full support                     |

### Database Sources

| Source     | HTTP Recording | DB Recording | Notes                                           |
| ---------- | -------------- | ------------ | ----------------------------------------------- |
| Snowflake  | ⚠️ Limited     | ✅           | Uses vendored urllib3 - HTTP not fully captured |
| Redshift   | N/A            | ✅           | Native connector wrapped                        |
| BigQuery   | ✅ (REST API)  | ✅           | Client API wrapped                              |
| Databricks | ⚠️ Limited     | ✅           | Thrift client compatibility issues              |
| PostgreSQL | N/A            | ✅           | SQLAlchemy event capture                        |
| MySQL      | N/A            | ✅           | SQLAlchemy event capture                        |

**Note:** Snowflake and Databricks use vendored HTTP libraries that VCR cannot intercept. Recording may fail during connection setup. SQL queries are still captured via db_proxy.

### DataHub Backend

| Component        | Recording | Notes                     |
| ---------------- | --------- | ------------------------- |
| GMS REST API     | ✅        | Sink emissions captured   |
| GraphQL API      | ✅        | If used by source         |
| Stateful Backend | ✅        | Checkpoint calls captured |

## Troubleshooting

### "Module not found: vcrpy"

Install the debug-recording plugin:

```bash
pip install 'acryl-datahub[debug-recording]'
```

### "Checksum verification failed"

The archive may be corrupted. Re-download or re-record:

```bash
datahub recording info recording.zip --password mysecret
# Check for checksum errors in output
```

### "No match for request" during replay

The recorded cassette doesn't have a matching request. This can happen if:

1. Recording was incomplete (check `has_exception` in manifest)
2. Source behavior changed between recording and replay
3. Different credentials caused different API paths

**Solution:** Re-record with the exact same configuration.

### Replay produces different event count

A small difference in event count (e.g., 3259 vs 3251) is normal due to:

- Duplicate MCP emissions during recording
- Timing-dependent code paths
- Non-deterministic processing order

**Verification:** Use `datahub check metadata-diff` to confirm semantic equivalence:

```bash
datahub check metadata-diff \
    --ignore-path "root['*']['systemMetadata']['lastObserved']" \
    --ignore-path "root['*']['systemMetadata']['runId']" \
    recording_output.json replay_output.json
```

A "PERFECT SEMANTIC MATCH" confirms the replay is correct despite count differences.

### Recording takes too long

HTTP requests are serialized during recording for reliability. To speed up:

1. Reduce source scope with patterns
2. Use `--no-s3-upload` for local testing
3. Accept that recording is slower than normal ingestion

### Archive too large for S3 upload

Large archives may timeout during upload:

```bash
# Record locally first
datahub ingest run -c recipe.yaml --record --record-password mysecret --no-s3-upload

# Upload manually with multipart
aws s3 cp recording.zip s3://bucket/recordings/ --expected-size $(stat -f%z recording.zip)
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    IngestionRecorder                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │  HTTPRecorder   │  │   ModulePatcher │  │ QueryRecorder│ │
│  │  (VCR.py)       │  │   (DB proxies)  │  │  (JSONL)     │ │
│  └────────┬────────┘  └────────┬────────┘  └──────┬───────┘ │
│           │                    │                   │         │
│           ▼                    ▼                   ▼         │
│  ┌─────────────────────────────────────────────────────────┐│
│  │                 Encrypted Archive                        ││
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────┐  ││
│  │  │manifest  │ │ recipe   │ │cassette  │ │queries.jsonl│ ││
│  │  │.json     │ │ .yaml    │ │.yaml     │ │            │  ││
│  │  └──────────┘ └──────────┘ └──────────┘ └────────────┘  ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    IngestionReplayer                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │  HTTPReplayer   │  │  ReplayPatcher  │  │ QueryReplayer│ │
│  │  (VCR replay)   │  │  (Mock conns)   │  │  (Mock cursor│ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
│                              │                               │
│                              ▼                               │
│              ┌──────────────────────────────┐               │
│              │    Air-Gapped Replay         │               │
│              │  - No network required       │               │
│              │  - Full debugger support     │               │
│              │  - Exact reproduction        │               │
│              └──────────────────────────────┘               │
└─────────────────────────────────────────────────────────────┘
```

## Contributing

When adding new source connectors:

1. HTTP-based sources work automatically via VCR
2. Database sources may need additions to `patcher.py` for their specific connector
3. Test recording and replay with the new source before releasing

## See Also

- [DataHub Ingestion Framework](https://datahubproject.io/docs/metadata-ingestion)
- [VCR.py Documentation](https://vcrpy.readthedocs.io/)
- [Debugging Ingestion Issues](https://datahubproject.io/docs/how/debugging)
