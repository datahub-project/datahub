# Message Size Validation

## Overview

Protects DataHub from oversized messages that exceed Jackson limits by validating aspect sizes at two critical points in the MCP (MetadataChangeProposal) processing pipeline.

## Configuration

**Default Behavior:** Message size validation is **disabled by default**. Existing deployments will continue to work unchanged without any action required.

### Enable via Environment Variables

```bash
# Pre-patch validation (before patch application)
DATAHUB_VALIDATION_ASPECT_SIZE_PRE_PATCH_ENABLED=true
DATAHUB_VALIDATION_ASPECT_SIZE_PRE_PATCH_MAX_SIZE_BYTES=15728640  # 15MB
DATAHUB_VALIDATION_ASPECT_SIZE_PRE_PATCH_OVERSIZED_REMEDIATION=DELETE

# Post-patch validation (after patch, before DB write)
DATAHUB_VALIDATION_ASPECT_SIZE_POST_PATCH_ENABLED=true
DATAHUB_VALIDATION_ASPECT_SIZE_POST_PATCH_MAX_SIZE_BYTES=15728640  # 15MB
DATAHUB_VALIDATION_ASPECT_SIZE_POST_PATCH_OVERSIZED_REMEDIATION=DELETE
```

### Enable via application.yaml

```yaml
datahub:
  validation:
    aspectSize:
      prePatch:
        enabled: true
        maxSizeBytes: 15728640 # 15MB (raw JSON character count)
        oversizedRemediation: DELETE # DELETE or IGNORE
      postPatch:
        enabled: true
        maxSizeBytes: 15728640 # 15MB (serialized JSON character count)
        oversizedRemediation: DELETE # DELETE or IGNORE
```

## Validation Points

Message size validation can be enabled at two points in the processing pipeline:

### 1. Pre-Patch Existing Aspect

**When:** Before patch application, if aspect already exists in database
**Measures:** Raw JSON character count from database (via `SystemAspect.getRawMetadata().length()`)
**Performance:** Zero overhead - raw JSON already fetched from DB
**On Failure:**

- Logs WARNING with URN, aspect name, size, threshold, measurement type, and remediation strategy
- If `oversizedRemediation=DELETE`: Hard deletes oversized aspect from database
- If `oversizedRemediation=IGNORE`: Leaves aspect in database
- Routes original MCP to FailedMetadataChangeProposal topic

**Note:** Use this to catch and remove pre-existing oversized aspects before attempting patches that might fail.

### 2. Post-Patch Existing Aspect

**When:** In AspectDao, after serialization for DB write but before actual DB persist
**Measures:** Serialized JSON character count (from EntityAspect.getMetadata().length()) - same unit as pre-patch
**Performance:** **Zero overhead** - validation happens on the JSON string already created for DB write (no duplicate serialization)
**On Failure:**

- Logs WARNING with URN, aspect name, size, threshold, measurement type, and remediation strategy
- If `oversizedRemediation=DELETE`: Hard deletes the aspect from database
- If `oversizedRemediation=IGNORE`: Leaves aspect in database
- Throws AspectSizeExceededException which routes original MCP to FailedMetadataChangeProposal topic

**Note:** Use this to catch bloat from patch application before writing to database. The validation is integrated into the DAO layer and uses the JSON string already being created for the DB write - no additional serialization work.

## Failure Handling

### FailedMetadataChangeProposal Topic

Oversized messages are routed to the `FailedMetadataChangeProposal` topic with:

- Original MCP payload
- Error message with checkpoint name, actual size, threshold
- URN and aspect name
- Kafka partition and offset (when available)

### Logging

All size violations are logged with context:

```
WARN: Oversized aspect detected: urn=urn:li:dataset:(...), aspect=schemaMetadata,
      size=16000000 bytes, threshold=15728640 bytes
```

### Aspect Remediation Strategies

The `oversizedRemediation` setting controls how oversized aspects are handled:

**DELETE (default):**

- Oversized aspects are hard deleted from the database
- Prevents incomplete/partial aspects from remaining in storage
- Deletion is logged at WARNING level
- Original MCP still routed to FMCP topic for debugging

**IGNORE:**

- Oversized aspect remains in database
- Safer for initial rollout but may accumulate oversized data
- Still logs WARNING and rejects the MCP

**Configuration:**

```yaml
metadataChangeProposal:
  validation:
    messageSize:
      prePatchExistingAspect:
        oversizedRemediation: DELETE # or IGNORE
      postPatchExistingAspect:
        oversizedRemediation: DELETE # or IGNORE
```

**Future options:** DEAD_LETTER_QUEUE, TRUNCATE (not yet implemented)

## Recommended Limits

### Pre-Patch / Post-Patch Aspect: 15MB

- **Rationale:** Safety margin below Jackson's 16MB string deserialization limit
- **Jackson Limit:** `INGESTION_MAX_SERIALIZED_STRING_LENGTH=16000000` (16MB)
- **Note:** Aspects stored in database, not constrained by Kafka limits
- **Measurement:** Both use serialized JSON character count (same unit)
  - Pre-patch: Raw JSON string from database (zero cost)
  - Post-patch: JSON string created for DB write (zero additional cost - validation happens on data already being serialized)

### Custom Limits

Adjust limits based on your infrastructure:

```yaml
datahub:
  validation:
    aspectSize:
      prePatch:
        enabled: true
        maxSizeBytes: 20971520 # 20MB (if Jackson limit increased)
        oversizedRemediation: IGNORE # Use IGNORE during testing
      postPatch:
        enabled: true
        maxSizeBytes: 20971520 # 20MB (if Jackson limit increased)
        oversizedRemediation: IGNORE # Use IGNORE during testing
```

## Troubleshooting

### Pre-Patch Aspects Rejected

**Problem:** Existing aspect in database exceeds size limit before patch
**Solution:**

1. Check WARNING logs for URN, aspect name, size, and remediation strategy
2. If `oversizedRemediation=DELETE`:
   - Aspect was automatically deleted from database
   - Re-ingest with corrected, smaller data
3. If `oversizedRemediation=IGNORE`:
   - Aspect remains in database but MCP was rejected
   - Fix the oversized aspect before attempting patches
4. Options:
   - Increase `datahub.validation.aspectSize.prePatch.maxSizeBytes` (ensure Jackson can handle it)
   - Set `oversizedRemediation=IGNORE` temporarily to preserve data during investigation

### Post-Patch Aspects Rejected

**Problem:** Aspect size after patch application exceeds limit
**Solution:**

1. Check WARNING logs for URN, aspect name, size, measurement type, and remediation strategy
2. Determine if patch is creating bloat:
   - Review patch logic
   - Check if patch is adding large amounts of data
3. If aspect was oversized after patch:
   - Already deleted if `oversizedRemediation=DELETE`
   - Consider splitting aspect data
4. Options:
   - Increase `datahub.validation.aspectSize.postPatch.maxSizeBytes` (ensure Jackson can handle it)
   - Set `oversizedRemediation=IGNORE` to prevent automatic deletion
   - Modify patch to avoid creating bloat

## Performance Impact

- **CPU Overhead per MCP** (only when validation enabled):
  - Pre-Patch Aspect: **Zero overhead** - uses raw JSON string already fetched from DB
  - Post-Patch Aspect: **Zero overhead** - uses JSON string already being created for DB write (no duplicate serialization)
- **Memory Overhead:** Negligible (<1KB per validation)
- **Throughput Impact:** **<1%** - both validations use data already being processed

## Monitoring

Future Phase 1 will add:

- Prometheus metrics for size distributions
- Exemplar tracking for largest messages
- REST API for debugging oversized messages
- Grafana dashboards

For now, monitor:

- `FailedMetadataChangeProposal` topic volume
- WARNING logs for "Oversized aspect detected"
- Kafka producer/consumer errors
