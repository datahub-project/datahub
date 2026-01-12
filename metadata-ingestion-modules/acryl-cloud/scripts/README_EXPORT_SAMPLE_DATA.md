# Export Sample Data Script

This script exports entities from a live DataHub instance and generates the `sample_data_mcp.json` file used for trial/demo instances.

## Prerequisites

1. Access to the source Acryl DataHub instance
2. Valid access token with read permissions
3. Python 3.8+ with `requests` library

## Usage

```bash
# Basic usage - export lineage from seed dataset
python3 metadata-ingestion/scripts/export_sample_data.py \
  --server https://fieldeng.acryl.io \
  --token 'YOUR_TOKEN' \
  --seed-urn 'urn:li:dataset:(urn:li:dataPlatform:snowflake,order_entry_db.analytics.order_details,PROD)' \
  --output datahub-upgrade/src/main/resources/boot/sample_data_mcp.json

# Limit lineage depth to 5 hops
python3 metadata-ingestion/scripts/export_sample_data.py \
  --server https://fieldeng.acryl.io \
  --token 'YOUR_TOKEN' \
  --seed-urn 'urn:li:dataset:(...)' \
  --max-hops 5

# Exclude related entities (domains, tags, glossary terms)
python3 metadata-ingestion/scripts/export_sample_data.py \
  --server https://fieldeng.acryl.io \
  --token 'YOUR_TOKEN' \
  --seed-urn 'urn:li:dataset:(...)' \
  --no-related

# Verbose output for debugging
python3 metadata-ingestion/scripts/export_sample_data.py \
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

## Troubleshooting

**Authentication errors**: Ensure your token is valid and has read permissions.

**Timeout errors**: Large instances may take time. The script logs progress every batch.

**Missing entities**: Check that the source instance has the expected data loaded.

**Invalid URN format errors**: Ensure the seed URN is properly formatted: `urn:li:<entity_type>:...`
