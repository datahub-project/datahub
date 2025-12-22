# Message Size Validation

## Overview

Protects DataHub from oversized messages that exceed Kafka/Jackson limits by validating message sizes at four critical points in the MCP (MetadataChangeProposal) processing pipeline.

## Configuration

**Default Behavior:** Message size validation is **disabled by default**. Existing deployments will continue to work unchanged without any action required.

### Enable via Environment Variables

```bash
MCP_VALIDATION_MESSAGE_SIZE_INCOMING_ENABLED=true
MCP_VALIDATION_MESSAGE_SIZE_INCOMING_MAX_BYTES=4718592  # 4.5MB
MCP_VALIDATION_MESSAGE_SIZE_PRE_PATCH_ENABLED=true
MCP_VALIDATION_MESSAGE_SIZE_PRE_PATCH_MAX_BYTES=15728640  # 15MB
MCP_VALIDATION_MESSAGE_SIZE_PRE_PATCH_REMEDIATION=DELETE
MCP_VALIDATION_MESSAGE_SIZE_POST_PATCH_ENABLED=true
MCP_VALIDATION_MESSAGE_SIZE_POST_PATCH_MAX_BYTES=15728640  # 15MB
MCP_VALIDATION_MESSAGE_SIZE_POST_PATCH_REMEDIATION=DELETE
MCP_VALIDATION_MESSAGE_SIZE_OUTGOING_ENABLED=true
MCP_VALIDATION_MESSAGE_SIZE_OUTGOING_MAX_BYTES=4718592  # 4.5MB
```

### Enable via application.yaml

```yaml
metadataChangeProposal:
  validation:
    messageSize:
      incomingMcp:
        enabled: true
        maxSizeBytes: 4718592 # 4.5MB (Kafka serialized bytes)
      prePatchExistingAspect:
        enabled: true
        maxSizeBytes: 15728640 # 15MB (raw JSON character count)
        oversizedRemediation: DELETE # DELETE or IGNORE
      postPatchExistingAspect:
        enabled: true
        maxSizeBytes: 15728640 # 15MB (serialized JSON character count)
        oversizedRemediation: DELETE # DELETE or IGNORE
      outgoingMcl:
        enabled: true
        maxSizeBytes: 4718592 # 4.5MB (Avro serialized bytes)
```

## Validation Points

Message size validation can be enabled at four points in the MCP processing pipeline:

### 1. Incoming MCP (Kafka Consumer)

**When:** Message consumed from Kafka topic, before processing begins
**Measures:** Kafka serialized byte size (via `ConsumerRecord.serializedValueSize()`)
**Performance:** Zero overhead - uses pre-computed value from Kafka
**On Failure:** Routes to FailedMetadataChangeProposal topic, logs error with URN and aspect

### 2. Pre-Patch Existing Aspect

**When:** Before patch application, if aspect already exists in database
**Measures:** Raw JSON character count from database (via `SystemAspect.getRawMetadata().length()`)
**Performance:** Zero overhead - raw JSON already fetched from DB
**On Failure:**

- Logs WARNING with URN, aspect name, size, threshold, measurement type, and remediation strategy
- If `oversizedRemediation=DELETE`: Hard deletes oversized aspect from database
- If `oversizedRemediation=IGNORE`: Leaves aspect in database
- Routes original MCP to FailedMetadataChangeProposal topic

**Note:** Use this to catch and remove pre-existing oversized aspects before attempting patches that might fail.

### 3. Post-Patch Existing Aspect

**When:** In AspectDao, after serialization for DB write but before actual DB persist
**Measures:** Serialized JSON character count (from EntityAspect.getMetadata().length()) - same unit as pre-patch
**Performance:** **Zero overhead** - validation happens on the JSON string already created for DB write (no duplicate serialization)
**On Failure:**

- Logs WARNING with URN, aspect name, size, threshold, measurement type, and remediation strategy
- If `oversizedRemediation=DELETE`: Hard deletes the aspect from database
- If `oversizedRemediation=IGNORE`: Leaves aspect in database
- Throws AspectSizeExceededException which routes original MCP to FailedMetadataChangeProposal topic

**Note:** Use this to catch bloat from patch application before writing to database. The validation is integrated into the DAO layer and uses the JSON string already being created for the DB write - no additional serialization work.

### 4. Outgoing MCL (Kafka Producer)

**When:** Before emitting MetadataChangeLog to Kafka
**Measures:** Avro serialized byte size (already computed for Kafka emission)
**Performance:** Minimal overhead - already serializing for Kafka
**On Failure:** Throws exception, prevents emission to Kafka

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

### Incoming MCP / Outgoing MCL: 4.5MB

- **Rationale:** Safety margin below DataHub's 5MB Kafka message limit
- **Kafka Limit:** `max.message.bytes=5242880` (5MB)
- **AWS MSK:** 1MB default (compressed), 15MB max with quota increase
- **Measurement:** Actual serialized bytes

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
metadataChangeProposal:
  validation:
    messageSize:
      incomingMcp:
        enabled: true
        maxSizeBytes: 10485760 # 10MB (if Kafka limit increased)
      prePatchExistingAspect:
        enabled: true
        maxSizeBytes: 20971520 # 20MB (if Jackson limit increased)
        oversizedRemediation: IGNORE # Use IGNORE during testing
      postPatchExistingAspect:
        enabled: true
        maxSizeBytes: 20971520 # 20MB (if Jackson limit increased)
        oversizedRemediation: IGNORE # Use IGNORE during testing
      outgoingMcl:
        enabled: true
        maxSizeBytes: 10485760 # 10MB (match Kafka limit)
```

## Troubleshooting

### Incoming MCPs Rejected

**Problem:** MCPs from Kafka exceed size limit
**Solution:**

1. Check FailedMetadataChangeProposal topic for rejected messages
2. Check WARNING logs for topic, partition, offset
3. Identify which entity/aspect is too large
4. Options:
   - Increase `incomingMcp.maxSizeBytes` (ensure Kafka can handle it)
   - Fix ingestion source to send smaller messages
   - Split large aspects into multiple smaller updates

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
   - Increase `prePatchExistingAspect.maxSizeBytes` (ensure Jackson can handle it)
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
   - Increase `postPatchExistingAspect.maxSizeBytes` (ensure Jackson can handle it)
   - Set `oversizedRemediation=IGNORE` to prevent automatic deletion
   - Modify patch to avoid creating bloat

### Outgoing MCLs Rejected

**Problem:** MCL size exceeds Kafka limit
**Solution:**

1. This should be rare if other validations are working
2. Indicates aspect passed validation but MCL envelope exceeded Kafka limit
3. Check WARNING logs for URN, aspect, topic
4. Options:
   - Increase `outgoingMcl.maxSizeBytes` (must match Kafka limit)
   - Investigate why MCL is larger than expected
   - Enable pre-patch or post-patch validation to catch earlier

## Performance Impact

- **CPU Overhead per MCP** (only when validation enabled):
  - Incoming MCP: **Zero overhead** - uses pre-computed `ConsumerRecord.serializedValueSize()`
  - Pre-Patch Aspect: **Zero overhead** - uses raw JSON string already fetched from DB
  - Post-Patch Aspect: **~50ms per aspect** - serializes RecordTemplate to JSON (same unit as pre-patch)
    - Only runs when enabled - acceptable cost since pre-patch and incoming already validated
    - Catches bloat from patch application before DB write
  - Outgoing MCL: **Minimal overhead** - uses Avro serialization already done for Kafka
- **Memory Overhead:** Negligible (<1KB per validation)
- **Throughput Impact:**
  - With only incoming + pre-patch + outgoing enabled: **<1%** (zero-cost validations)
  - With post-patch also enabled: **variable** depending on aspect size and frequency (typically <5% for workloads with small/medium aspects)

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
