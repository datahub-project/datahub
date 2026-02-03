# Sample Data Regeneration

This directory contains scripts for regenerating the sample data used in DataHub trial/demo instances.

## Quick Start (Recommended)

**Use the wrapper script** which handles everything automatically:

```bash
# Run from repository root
./metadata-ingestion-modules/acryl-cloud/scripts/regenerate_sample_data.sh
```

This script:

1. Exports sample data from fieldeng instance
2. Cleans dangling references (users, groups, glossary terms)
3. Generates `sample_data_mcp.json`
4. Runs integrity tests
5. Generates version-based manifest
6. Updates `CURRENT_VERSION` in Java code (if data changed)
7. Deletes old manifests (keeps only `001` and current)

**Prerequisites**:

- Valid access token in `~/.datahubenv` (run `~/bin/token.sh` to generate)
- Python 3.8+ with `requests` library
- Git repository (for change detection)

## Advanced Usage (Python Script)

For custom exports or debugging, use the Python script directly:

```bash
# Basic usage - export lineage from seed dataset
# Note: Use the ORIGINAL URN from fieldeng (without sample_data_ prefix)
# The script automatically adds sample_data_ prefix during transformation
python3 metadata-ingestion-modules/acryl-cloud/scripts/export_sample_data.py \
  --server https://fieldeng.acryl.io \
  --token 'YOUR_TOKEN' \
  --seed-urn 'urn:li:dataset:(urn:li:dataPlatform:snowflake,order_entry_db.analytics.order_details,PROD)' \
  --output datahub-upgrade/src/main/resources/boot/sample_data_mcp.json

# Limit lineage depth to 5 hops
python3 metadata-ingestion-modules/acryl-cloud/scripts/export_sample_data.py \
  --server https://fieldeng.acryl.io \
  --token 'YOUR_TOKEN' \
  --seed-urn 'urn:li:dataset:(...)' \
  --max-hops 5

# Exclude related entities (domains, tags, glossary terms)
python3 metadata-ingestion-modules/acryl-cloud/scripts/export_sample_data.py \
  --server https://fieldeng.acryl.io \
  --token 'YOUR_TOKEN' \
  --seed-urn 'urn:li:dataset:(...)' \
  --no-related

# Verbose output for debugging
python3 metadata-ingestion-modules/acryl-cloud/scripts/export_sample_data.py \
  --server https://fieldeng.acryl.io \
  --token 'YOUR_TOKEN' \
  --seed-urn 'urn:li:dataset:(...)' \
  --verbose
```

## What Gets Exported

1. **Lineage Graph**:

   - Starting from seed entity
   - All upstream entities (sources)
   - All downstream entities (consumers)
   - Traverses up to `--max-hops` (default 10)

2. **Related Entities** (optional, included by default):

   - Domains containing lineage entities
   - Containers for lineage entities
   - Owners (users and groups) of lineage entities
   - Data products containing lineage entities
   - Tags applied to lineage entities
   - Glossary terms linked to lineage entities

3. **All aspects** for each entity
4. **System metadata** (marked as sample data)

## What Gets Filtered

By default, the script filters out:

1. **Entities without soft delete support** (35 types):

   - `incident`, `dataPlatform`, `dataHubPolicy`, `dataHubIngestionSource`, etc.
   - See `EXCLUDED_ENTITIES` in script for full list

2. **Sensitive entities** (even if they support soft delete):

   - `application` - May contain deployment configs, credentials, connection strings

3. **Entities excluded to avoid dependencies**:

   - `structuredProperty` - References entity types which may not exist during sample data bootstrap

4. **Sensitive aspects** (filtered from all entities):

   - **Authentication/Authorization**: `corpUserCredentials`, `dataHubAccessTokenInfo`, `accessTokenMetadata`, `inviteToken`
   - **Connection/Configuration**: `dataHubConnectionDetails`, `dataHubSecretValue`, `dataHubIngestionSourceInfo`, `dataHubExecutionRequestInput`, `dataHubExecutionRequestResult`
   - **Internal/System**: `telemetryClientId`, `corpUserSettings`, `corpGroupSettings`
   - **References to excluded entities**: `incidentsSummary`, `structuredProperties`, `applications`

5. **Dangling references** (automatically cleaned):
   - References to `corpUser`, `corpGroup`, or `glossaryTerm` entities that don't exist in the export
   - Prevents stub entities showing GUIDs/IDs instead of names in UI

**Note**: The result will be smaller than the current sample_data_mcp.json since we're only exporting entities in the lineage graph plus their metadata, not the entire instance.

## Output Format

The output is a JSON array of MCPs (Metadata Change Proposals):

```json
[
  {
    "entityType": "dataset",
    "entityUrn": "urn:li:dataset:(...)",
    "changeType": "UPSERT",
    "aspectName": "datasetProperties",
    "aspect": { "json": {...} },
    "systemMetadata": {
      "properties": { "sampleData": "true" }
    }
  },
  ...
]
```

## Performance

The script uses batch API calls for optimal performance:

- **Batch size**: 100 entities per request (configurable)
- **Grouping**: Groups entities by type before fetching
- **Expected performance**: For 500 entities with 5 types, ~5-25 batch requests vs 500 individual requests (50-100x faster)

## Migration System

Sample data uses a version-based manifest system for incremental updates:

### Manifest Files

Located in `datahub-upgrade/src/main/resources/boot/manifests/`:

- **`001_manifest.json`**: Source manifest for unprefixed → prefixed migration

  - Represents old acryl-main sample data (1,060 entities)
  - **Temporary**: Can be removed once all production instances have migrated to this branch
  - Code has auto-detection fallback via `hasUnprefixedSampleData()` making this manifest optional

- **`{version}_manifest.json`**: Current version manifest
  - Version is first 12 chars of SHA256 hash of `sample_data_mcp.json` content
  - Contains all current sample data entities (1,049 entities)
  - Auto-generated by `regenerate_sample_data.sh`

### Migration Paths

**Fresh Install**:

- No version marker in database
- Directly ingests all entities from `sample_data_mcp.json`
- No manifests needed

**Incremental Update (001 → current)**:

- Existing instance on acryl-main (unprefixed sample data)
- Deletes all entities from `001_manifest.json`
- Ingests all entities from current `sample_data_mcp.json`

**Incremental Update (version → version)**:

- Instance already on this branch
- Calculates diff between old and new manifest
- Deletes only changed entities
- Re-ingests changed entities

### Version Management

The `CURRENT_VERSION` constant in [`IngestSampleDataStep.java`](../../../datahub-upgrade/src/main/java/com/linkedin/datahub/upgrade/system/sampledata/IngestSampleDataStep.java) tracks the current version.

**Auto-update**: The wrapper script automatically updates `CURRENT_VERSION` when sample data changes (detected via `git diff`).

**Manifest cleanup**: Old manifests (except `001`) are automatically deleted when new version is generated.

## Troubleshooting

**Authentication errors**: Ensure your token is valid and has read permissions.

**Timeout errors**: Large instances may take time. The script logs progress every batch.

**Missing entities**: Check that the source instance has the expected data loaded.

**Invalid URN format errors**: Ensure the seed URN is properly formatted: `urn:li:<entity_type>:...`

**CURRENT_VERSION not updated**: Run `git diff` to check if sample data actually changed. The script only updates if there are changes.

**Old manifests remain**: The script automatically deletes old manifests. Check script output for errors.
